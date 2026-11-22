from __future__ import annotations

import ctypes
import json
import logging
import multiprocessing
import subprocess
import sys
import textwrap
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from email.message import Message
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from typing import TYPE_CHECKING, Literal, TypeAlias, cast
from urllib.error import HTTPError
from urllib.request import OpenerDirector, Request

import pytest

from closeyourit import (
    Client,
    Configuration,
    EventBuilder,
    HttpResponse,
    Transport,
    UrllibHttpRequester,
    budget,
)
from closeyourit.transport import _in_multiprocessing_child

if TYPE_CHECKING:
    from multiprocessing.context import ForkContext, ForkServerContext, SpawnContext
    from multiprocessing.sharedctypes import Synchronized
    from multiprocessing.synchronize import Event

Outcome: TypeAlias = (
    HttpResponse | BaseException | Callable[[str, bytes, Mapping[str, str], float], HttpResponse]
)


def valid_configuration(**overrides: object) -> Configuration:
    values: dict[str, object] = {
        "endpoint_url": "https://ingest.example.com",
        "token": "cyi_test_token",
        "project_id": "12345678-1234-1234-1234-123456789abc",
        "environment": "production",
    }
    values.update(overrides)
    return Configuration(**values)  # type: ignore[arg-type]


class ScriptedRequester:
    def __init__(self, *outcomes: Outcome) -> None:
        self.outcomes = deque(outcomes)
        self.calls: list[tuple[str, bytes, Mapping[str, str], float]] = []

    def send(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        self.calls.append((url, body, dict(headers), timeout))
        outcome = self.outcomes.popleft()
        if isinstance(outcome, BaseException):
            raise outcome
        if callable(outcome):
            return outcome(url, body, headers, timeout)
        return outcome


class BlockingRequester:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.bodies: list[bytes] = []

    def send(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        del url, headers, timeout
        self.bodies.append(body)
        self.entered.set()
        assert self.release.wait(2)
        return HttpResponse(202)


class SimulatedClock:
    """Monotonic time that moves only when a simulated wait spends it."""

    def __init__(self, spent: Synchronized[float] | None = None) -> None:
        self.now = 0.0
        self._spent = spent

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds
        if self._spent is not None:
            self._spent.value = self.now


class SimulatedCondition(threading.Condition):
    """A condition whose timed waits spend simulated time instead of real time."""

    def __init__(self, clock: SimulatedClock) -> None:
        super().__init__()
        self.clock = clock

    def wait(self, timeout: float | None = None) -> bool:
        if timeout is None:
            return super().wait()
        # Nothing notifies a stalled delivery: the whole timeout passes, without waiting for it.
        self.clock.advance(timeout)
        return False


def simulate_time(transport: Transport, clock: SimulatedClock) -> None:
    # Before the first enqueue: the worker shares the condition from then on.
    transport._monotonic = clock
    transport._condition = SimulatedCondition(clock)


class FakeUrlResponse:
    def __init__(self, status: int, headers: Mapping[str, str], body: bytes) -> None:
        self.status = status
        self.headers = Message()
        for key, value in headers.items():
            self.headers[key] = value
        self.body = body

    def __enter__(self) -> FakeUrlResponse:
        return self

    def __exit__(self, *args: object) -> None:
        del args

    def getcode(self) -> int:
        return self.status

    def read(self, limit: int) -> bytes:
        return self.body[:limit]


class FakeOpener:
    def __init__(self, outcome: FakeUrlResponse | HTTPError) -> None:
        self.outcome = outcome
        self.calls: list[tuple[Request, float]] = []

    def open(self, request: Request, *, timeout: float) -> FakeUrlResponse:
        self.calls.append((request, timeout))
        if isinstance(self.outcome, HTTPError):
            raise self.outcome
        return self.outcome


def test_urllib_requester_posts_bytes_without_implicit_network_or_redirects() -> None:
    opener = FakeOpener(FakeUrlResponse(202, {"X-Request-Id": "r1"}, b'{"ok":true}'))
    requester = UrllibHttpRequester(cast(OpenerDirector, opener))

    response = requester.send(
        "https://ingest.example.com/events",
        b'{"message":"safe"}',
        {"Authorization": "Bearer cyi_test"},
        1.25,
    )

    request, timeout = opener.calls[0]
    assert request.get_method() == "POST"
    assert request.data == b'{"message":"safe"}'
    assert request.get_header("Authorization") == "Bearer cyi_test"
    assert timeout == 1.25
    assert response == HttpResponse(202, {"X-Request-Id": "r1"}, b'{"ok":true}')


def test_urllib_requester_converts_http_errors_into_responses() -> None:
    headers = Message()
    headers["Retry-After"] = "1"
    error = HTTPError(
        "https://ingest.example.com/events",
        429,
        "rate limited",
        headers,
        BytesIO(b'{"error":true}'),
    )
    requester = UrllibHttpRequester(cast(OpenerDirector, FakeOpener(error)))

    response = requester.send("https://ingest.example.com/events", b"{}", {}, 1.0)

    assert response.status == 429
    assert response.headers["Retry-After"] == "1"
    assert response.body == b'{"error":true}'


def test_worker_is_lazy_and_body_is_snapshotted_before_enqueue() -> None:
    requester = BlockingRequester()
    transport = Transport(valid_configuration(), requester=requester)
    payload: dict[str, object] = {"message": "before", "nested": {"attempt": 1}}

    assert transport.stats.worker_alive is False
    assert transport.enqueue("events", payload) is True
    assert requester.entered.wait(1)
    payload["message"] = "after"
    payload["nested"] = {"attempt": 2}

    requester.release.set()
    assert transport.flush(1) is True
    assert json.loads(requester.bodies[0]) == {"message": "before", "nested": {"attempt": 1}}
    assert transport.stats.worker_alive is True
    transport.close()


def test_bounded_queue_accepts_x_minus_one_and_x_then_drops_x_plus_one() -> None:
    requester = BlockingRequester()
    transport = Transport(valid_configuration(), requester=requester, max_queue=2)

    assert transport.enqueue("events", {"n": 0}) is True
    assert requester.entered.wait(1)
    assert transport.enqueue("events", {"n": 1}) is True  # X-1
    assert transport.enqueue("events", {"n": 2}) is True  # X
    assert transport.enqueue("events", {"n": 3}) is False  # X+1

    snapshot = transport.stats
    assert snapshot.enqueued == 3
    assert snapshot.dropped == 1
    assert snapshot.pending == 3
    assert snapshot.queue_size == 2

    requester.release.set()
    assert transport.flush(1)
    assert transport.stats.sent == 3
    transport.close()


@pytest.mark.parametrize("status", [200, 202, 299])
def test_every_2xx_response_is_successful(status: int) -> None:
    requester = ScriptedRequester(HttpResponse(status))
    transport = Transport(valid_configuration(), requester=requester)

    assert transport.enqueue("logs", {"message": "ok"})
    assert transport.flush(1)

    assert transport.stats.sent == 1
    assert transport.stats.failed == 0
    expected_path = "/api/v1/projects/12345678-1234-1234-1234-123456789abc/logs"
    assert requester.calls[0][0].endswith(expected_path)
    assert requester.calls[0][3] == 3.0
    transport.close()


@pytest.mark.parametrize("status", [400, 401, 404, 413, 422])
def test_non_retryable_4xx_fails_once(status: int) -> None:
    requester = ScriptedRequester(HttpResponse(status, body=b'{"error":{"code":"R422-X-001"}}'))
    transport = Transport(valid_configuration(), requester=requester)

    assert transport.enqueue("events", {"message": "bad"})
    assert transport.flush(1)

    assert len(requester.calls) == 1
    assert transport.stats.failed == 1
    assert transport.stats.last_failure == f"http_{status}:R422-X-001"
    transport.close()


@pytest.mark.parametrize("status", [408, 425, 429, 500, 503, 599])
def test_retryable_statuses_are_retried_until_success(status: int) -> None:
    requester = ScriptedRequester(HttpResponse(status), HttpResponse(202))
    delays: list[float] = []
    transport = Transport(
        valid_configuration(),
        requester=requester,
        max_retries=2,
        retry_base_delay=0.1,
        sleep=delays.append,
        random_value=lambda: 0.5,
    )

    assert transport.enqueue("metrics", {"kind": "slow_method"})
    assert transport.flush(1)

    assert len(requester.calls) == 2
    assert delays == [pytest.approx(0.15)]
    assert transport.stats.retried == 1
    assert transport.stats.sent == 1
    transport.close()


def test_retry_after_seconds_is_honoured_without_jitter() -> None:
    requester = ScriptedRequester(
        HttpResponse(429, headers={"Retry-After": "2.5"}), HttpResponse(202)
    )
    delays: list[float] = []
    transport = Transport(
        valid_configuration(), requester=requester, sleep=delays.append, random_value=lambda: 0.9
    )

    assert transport.enqueue("events", {"message": "later"})
    assert transport.flush(1)

    assert delays == [2.5]
    transport.close()


def test_retry_after_http_date_is_capped_and_invalid_values_use_backoff() -> None:
    requester = ScriptedRequester(
        HttpResponse(503, headers={"Retry-After": "Thu, 01 Jan 1970 00:01:40 GMT"}),
        HttpResponse(503, headers={"Retry-After": "invalid"}),
        HttpResponse(202),
    )
    delays: list[float] = []
    transport = Transport(
        valid_configuration(),
        requester=requester,
        max_retry_delay=5,
        retry_base_delay=0.1,
        sleep=delays.append,
        random_value=lambda: 0,
        wall_time=lambda: 0,
    )

    assert transport.enqueue("events", {"message": "later"})
    assert transport.flush(1)

    assert delays == [5.0, 0.2]
    transport.close()


def test_network_failure_and_timeout_are_retried_then_diagnosed() -> None:
    requester = ScriptedRequester(TimeoutError("slow"), OSError("offline"), OSError("offline"))
    transport = Transport(
        valid_configuration(), requester=requester, max_retries=2, sleep=lambda _delay: None
    )

    assert transport.enqueue("events", {"message": "retry"})
    assert transport.flush(1)

    assert len(requester.calls) == 3
    assert transport.stats.retried == 2
    assert transport.stats.failed == 1
    assert transport.stats.last_failure == "network_error:OSError"
    transport.close()


_EVENTS_PATH = "/api/v1/projects/12345678-1234-1234-1234-123456789abc/events"


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("mixed_chain", [False, True])
@pytest.mark.parametrize(
    "target",
    [
        "https://user:password@other.example/private?token=secret",
        "http://ingest.example.com/private",
        "https://ingest.example.com:8443/private",
        "https://ingest.example.com:0/private",
        "https://ingest.example.com:-1/private",
        "https://ingest.example.com:65536/private",
        "https://ingest.example.com:invalid/private",
        "https://ingest.example.com:/private",
        "https://user:password@ingest.example.com/private",
        "https://ingest.example.com/white space",
        "https://ingest.example.com/line\nbreak",
        "https://[invalid/private",
        "https:///private",
        "https://:443/private",
        "https://user:password@/private",
        "//:443/private",
        "ftp://ingest.example.com/private",
    ],
)
def test_unsafe_redirect_is_terminal_without_disclosing_data(
    status: int, mixed_chain: bool, target: str, caplog: pytest.LogCaptureFixture
) -> None:
    outcomes = [HttpResponse(status, headers={"Location": target}), HttpResponse(202)]
    if mixed_chain:
        outcomes.insert(0, HttpResponse(status, headers={"Location": "/safe"}))
    requester = ScriptedRequester(*outcomes)
    delays: list[float] = []
    transport = Transport(
        valid_configuration(), requester=requester, max_retries=3, sleep=delays.append
    )
    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        assert transport.enqueue("events", {"message": "private payload"})
        assert transport.flush(1)
    assert len(requester.calls) == 1 + int(mixed_chain)
    assert all(call[1] == b'{"message":"private payload"}' for call in requester.calls)
    assert all(call[2]["Authorization"] == "Bearer cyi_test_token" for call in requester.calls)
    assert transport.stats.enqueued == transport.stats.failed == 1
    assert transport.stats.sent == transport.stats.retried == transport.stats.pending == 0
    assert transport.stats.last_failure == "redirect_rejected"
    assert delays == []
    assert [r.getMessage() for r in caplog.records if r.name == "closeyourit"] == [
        "closeyourit redirect rejected: invalid or different origin",
        "closeyourit failed 1 event(s), latest reason redirect_rejected",
    ]
    assert transport.close(1)


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("scheme,port", [("https", 443), ("http", 80)])
@pytest.mark.parametrize("relative", [True, False])
@pytest.mark.parametrize("explicit_source_port", [True, False])
def test_same_origin_preserves_body_and_authentication(
    status: int, scheme: str, port: int, relative: bool, explicit_source_port: bool
) -> None:
    source = f"{scheme}://ingest.example.com" + (f":{port}" if explicit_source_port else "")
    target_port = "" if explicit_source_port else f":{port}"
    target = "../next" if relative else f"{scheme}://INGEST.example.com{target_port}/next"
    requester = ScriptedRequester(HttpResponse(status, {"Location": target}), HttpResponse(202))
    transport = Transport(
        valid_configuration(endpoint_url=source, environment="test"),
        requester=requester,
    )
    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)
    first, second = requester.calls
    expected = f"{source}/api/v1/projects/next" if relative else target
    assert second[0] == expected
    assert first[1] == second[1] == b'{"message":"redirect"}'
    assert first[2] == second[2]
    assert second[2]["Authorization"] == "Bearer cyi_test_token"
    assert transport.stats.sent == 1
    assert transport.stats.failed == transport.stats.retried == transport.stats.pending == 0
    assert transport.close(1)


def test_redirect_to_same_site_resends_post_body_and_bearer() -> None:
    requester = ScriptedRequester(
        HttpResponse(307, headers={"location": f"https://ingest.example.com{_EVENTS_PATH}?r=1"}),
        HttpResponse(308, headers={"Location": "/api/v1/other"}),
        HttpResponse(202),
    )
    transport = Transport(valid_configuration(), requester=requester)

    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)

    first, same_path, other_path = requester.calls
    assert other_path[0] == "https://ingest.example.com/api/v1/other"
    assert first[1] == same_path[1] == other_path[1]
    assert first[2]["Authorization"] == "Bearer cyi_test_token"
    assert same_path[2]["Authorization"] == "Bearer cyi_test_token"
    assert other_path[2]["Authorization"] == "Bearer cyi_test_token"
    assert transport.stats.sent == 1
    assert transport.stats.failed == 0
    transport.close()


def test_redirect_to_other_domain_fails_without_resending_or_retrying(
    caplog: pytest.LogCaptureFixture,
) -> None:
    requester = ScriptedRequester(
        HttpResponse(307, headers={"location": f"https://ingest.example.com{_EVENTS_PATH}?r=1"}),
        HttpResponse(302, headers={"Location": f"https://other.example{_EVENTS_PATH}?k=secret"}),
    )
    transport = Transport(valid_configuration(), requester=requester, max_retries=3)

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        assert transport.enqueue("events", {"message": "redirect"})
        assert transport.flush(1)

    assert [call[0] for call in requester.calls] == [
        f"https://ingest.example.com{_EVENTS_PATH}",
        f"https://ingest.example.com{_EVENTS_PATH}?r=1",
    ]
    assert transport.stats.sent == 0
    assert transport.stats.retried == 0
    assert transport.stats.failed == 1
    assert transport.stats.last_failure == "redirect_rejected"
    messages = [record.getMessage() for record in caplog.records if record.name == "closeyourit"]
    assert messages[0] == "closeyourit redirect rejected: invalid or different origin"
    assert all("secret" not in message and "/api/v1" not in message for message in messages)
    transport.close()


def test_redirect_downgrade_to_http_is_rejected_before_anything_travels_in_clear() -> None:
    requester = ScriptedRequester(
        HttpResponse(301, headers={"Location": f"http://ingest.example.com{_EVENTS_PATH}"}),
    )
    transport = Transport(valid_configuration(), requester=requester)

    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)

    assert len(requester.calls) == 1
    assert transport.stats.failed == 1
    assert transport.stats.last_failure == "redirect_rejected"
    transport.close()


def test_redirect_to_other_port_on_same_host_is_rejected() -> None:
    requester = ScriptedRequester(
        HttpResponse(307, headers={"Location": f"https://ingest.example.com:8443{_EVENTS_PATH}"}),
    )
    transport = Transport(valid_configuration(), requester=requester)

    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)

    assert len(requester.calls) == 1
    assert transport.stats.failed == 1
    assert transport.stats.last_failure == "redirect_rejected"
    transport.close()


def test_redirect_www_swap_is_rejected() -> None:
    requester = ScriptedRequester(
        HttpResponse(308, headers={"Location": f"https://www.ingest.example.com{_EVENTS_PATH}"}),
        HttpResponse(307, headers={"Location": f"https://evil.example{_EVENTS_PATH}"}),
    )
    transport = Transport(valid_configuration(), requester=requester)

    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)

    assert len(requester.calls) == 1
    assert transport.stats.failed == 1
    assert transport.stats.last_failure == "redirect_rejected"
    transport.close()


def test_redirect_chain_is_measured_against_the_starting_address() -> None:
    # A permitted first hop cannot authorize a different host on the second hop.
    requester = ScriptedRequester(
        HttpResponse(302, headers={"Location": "https://ingest.example.com/one"}),
        HttpResponse(302, headers={"Location": "https://www.www.ingest.example.com/two"}),
    )
    transport = Transport(valid_configuration(), requester=requester)

    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)

    assert len(requester.calls) == 2
    assert transport.stats.last_failure == "redirect_rejected"
    transport.close()


def test_redirect_upgrade_to_https_is_rejected() -> None:
    configuration = valid_configuration(
        endpoint_url="http://localhost:3011", environment="development"
    )
    requester = ScriptedRequester(
        HttpResponse(308, headers={"Location": f"https://localhost:3011{_EVENTS_PATH}"}),
        HttpResponse(202),
    )
    transport = Transport(configuration, requester=requester)

    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)

    assert len(requester.calls) == 1
    assert transport.stats.sent == 0
    assert transport.stats.last_failure == "redirect_rejected"
    transport.close()


def test_redirect_chain_from_http_stops_before_reaching_https() -> None:
    configuration = valid_configuration(
        endpoint_url="http://localhost:3011", environment="development"
    )
    requester = ScriptedRequester(
        HttpResponse(308, headers={"Location": "https://localhost:3011/one"}),
        HttpResponse(307, headers={"Location": "http://localhost:3011/two"}),
        HttpResponse(202),
    )
    transport = Transport(configuration, requester=requester, max_redirects=3)

    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)

    assert [call[0] for call in requester.calls] == [
        f"http://localhost:3011{_EVENTS_PATH}",
    ]
    assert transport.stats.sent == 0
    assert transport.stats.retried == 0
    assert transport.stats.failed == 1
    assert transport.stats.last_failure == "redirect_rejected"
    transport.close()


@pytest.mark.parametrize("limit", [0, 1, 2])
def test_redirect_limit_is_bounded_on_the_same_site(limit: int) -> None:
    requester = ScriptedRequester(
        HttpResponse(301, headers={"Location": "/one"}),
        HttpResponse(308, headers={"Location": "/two"}),
        HttpResponse(307, headers={"Location": "/three"}),
    )
    transport = Transport(valid_configuration(), requester=requester, max_redirects=limit)

    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)

    assert len(requester.calls) == limit + 1
    assert all(call[1] == requester.calls[0][1] for call in requester.calls)
    assert all(call[2]["Authorization"] == "Bearer cyi_test_token" for call in requester.calls)
    assert transport.stats.failed == 1
    assert transport.stats.last_failure == f"http_{(301, 308, 307)[limit]}"
    assert transport.stats.retried == transport.stats.pending == 0
    transport.close()


@pytest.mark.parametrize(
    ("origin", "target", "allowed"),
    [
        ("https://ingest.example.com/a", "https://ingest.example.com/b", True),
        ("https://ingest.example.com/a", "https://INGEST.example.com:443/b", True),
        ("https://ingest.example.com/a", "https://www.ingest.example.com/b", False),
        ("https://www.ingest.example.com/a", "https://ingest.example.com/b", False),
        ("https://ingest.example.com:8443/a", "https://www.ingest.example.com:8443/b", False),
        ("http://ingest.example.com/a", "https://ingest.example.com/b", False),
        ("http://ingest.example.com/a", "http://ingest.example.com:80/b", True),
        ("http://[::1]:3011/a", "http://[::1]:3011/b", True),
        ("https://ingest.example.com/a", "http://ingest.example.com/b", False),
        ("https://ingest.example.com/a", "https://other.example/b", False),
        ("https://ingest.example.com/a", "https://ingest.example.com.evil/b", False),
        ("https://ingest.example.com/a", "https://ingest.example.com:8443/b", False),
        ("http://ingest.example.com/a", "https://ingest.example.com:8443/b", False),
        ("https://ingest.example.com/a", "https://www.www.ingest.example.com/b", False),
        ("https://ingest.example.com/a", "ftp://ingest.example.com/b", False),
        ("https://ingest.example.com/a", "https://ingest.example.com:99999/b", False),
    ],
)
def test_redirect_requires_the_same_authority(origin: str, target: str, allowed: bool) -> None:
    assert Transport._same_authority(origin, target) is allowed


def test_flush_timeout_and_close_are_idempotent_and_reject_new_work() -> None:
    requester = BlockingRequester()
    transport = Transport(valid_configuration(), requester=requester)

    assert transport.enqueue("events", {"n": 1})
    assert requester.entered.wait(1)
    assert transport.flush(0.001) is False

    requester.release.set()
    assert transport.close(1) is True
    assert transport.close(1) is True
    assert transport.enqueue("events", {"n": 2}) is False
    assert transport.stats.closed is True
    assert transport.stats.dropped == 1


def test_serialization_failure_is_dropped_without_starting_worker() -> None:
    transport = Transport(valid_configuration(), requester=ScriptedRequester())

    assert transport.enqueue("events", {"invalid": object()}) is False

    assert transport.stats.dropped == 1
    assert transport.stats.worker_alive is False
    assert transport.close() is True


def test_disabled_transport_and_unknown_channel_are_dropped() -> None:
    disabled = Transport(Configuration(), requester=ScriptedRequester())
    enabled = Transport(valid_configuration(), requester=ScriptedRequester())

    assert disabled.enqueue("events", {"message": "disabled"}) is False
    assert enabled.enqueue("unknown", {"message": "invalid"}) is False  # type: ignore[arg-type]

    assert disabled.stats.dropped == 1
    assert enabled.stats.dropped == 1
    assert disabled.close() is True
    assert enabled.close() is True


def test_pid_change_resets_inherited_state_and_starts_a_fresh_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requester = ScriptedRequester(HttpResponse(202))
    transport = Transport(valid_configuration(), requester=requester)
    parent_pid = transport.stats.pid

    monkeypatch.setattr("closeyourit.transport.os.getpid", lambda: parent_pid + 1)

    assert transport.enqueue("events", {"child": True})
    assert transport.flush(1)
    assert transport.stats.pid == parent_pid + 1
    assert transport.stats.enqueued == 1
    assert transport.stats.sent == 1
    transport.close()


def test_queue_full_drop_logs_a_warning_carrying_the_running_count(
    caplog: pytest.LogCaptureFixture,
) -> None:
    requester = BlockingRequester()
    transport = Transport(valid_configuration(), requester=requester, max_queue=1)

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        assert transport.enqueue("events", {"n": 0}) is True
        assert requester.entered.wait(1)
        assert transport.enqueue("events", {"n": 1}) is True
        assert transport.enqueue("events", {"n": 2}) is False

        warnings = [record for record in caplog.records if record.name == "closeyourit"]
        assert len(warnings) == 1
        assert warnings[0].levelno == logging.WARNING
        assert "dropped 1 event(s)" in warnings[0].getMessage()

    requester.release.set()
    assert transport.close(1) is True
    assert transport.stats.dropped == 1


def test_drops_log_on_the_first_event_and_then_every_interval(
    caplog: pytest.LogCaptureFixture,
) -> None:
    transport = Transport(Configuration(), requester=ScriptedRequester(), loss_log_interval=3)

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        for index in range(6):
            assert transport.enqueue("events", {"n": index}) is False

    messages = [record.getMessage() for record in caplog.records if record.name == "closeyourit"]
    assert len(messages) == 3
    assert "dropped 1 event(s)" in messages[0]
    assert "dropped 3 event(s)" in messages[1]
    assert "dropped 6 event(s)" in messages[2]
    assert transport.stats.dropped == 6
    assert transport.close() is True


def test_terminal_delivery_failure_logs_a_warning_with_the_last_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    requester = ScriptedRequester(HttpResponse(400, body=b'{"error":{"code":"R400-X"}}'))
    transport = Transport(valid_configuration(), requester=requester)

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        assert transport.enqueue("events", {"message": "bad"}) is True
        assert transport.flush(1) is True

        warnings = [record for record in caplog.records if record.name == "closeyourit"]
        assert len(warnings) == 1
        message = warnings[0].getMessage()
        assert "failed 1 event(s)" in message
        assert "http_400:R400-X" in message

    assert transport.stats.failed == 1
    transport.close()


def sized_payload(size: int) -> dict[str, object]:
    payload: dict[str, object] = {"event_id": "e1", "level": "info", "message": ""}
    payload["message"] = "x" * (size - len(budget.encode(payload)))
    assert len(budget.encode(payload)) == size
    return payload


@pytest.mark.parametrize("size", [budget.MAX_PAYLOAD_BYTES - 1, budget.MAX_PAYLOAD_BYTES])
def test_a_payload_up_to_the_limit_is_sent_unchanged(size: int) -> None:
    requester = ScriptedRequester(HttpResponse(202))
    transport = Transport(valid_configuration(), requester=requester)
    payload = sized_payload(size)

    assert transport.enqueue("logs", payload) is True
    assert transport.flush(1)

    assert requester.calls[0][1] == budget.encode(payload)
    assert "truncated" not in json.loads(requester.calls[0][1])
    transport.close()


def test_a_payload_one_byte_over_the_limit_is_shrunk_marked_and_sent() -> None:
    requester = ScriptedRequester(HttpResponse(202))
    transport = Transport(valid_configuration(), requester=requester)
    payload = sized_payload(budget.MAX_PAYLOAD_BYTES + 1)
    original = dict(payload)

    assert transport.enqueue("logs", payload) is True
    assert transport.flush(1)

    body = requester.calls[0][1]
    assert len(body) <= budget.MAX_PAYLOAD_BYTES
    sent = json.loads(body)
    assert sent["truncated"] is True
    assert sent["event_id"] == "e1"
    assert payload == original
    assert transport.stats.sent == 1
    transport.close()


def test_a_payload_that_cannot_fit_is_dropped_with_an_explicit_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    requester = ScriptedRequester()
    transport = Transport(valid_configuration(), requester=requester)
    payload: dict[str, object] = {
        "event_id": "e" * budget.MAX_PAYLOAD_BYTES,
        "level": "info",
        "message": "m",
    }

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        assert transport.enqueue("logs", payload) is False

    messages = [record.getMessage() for record in caplog.records if record.name == "closeyourit"]
    assert any("exceeds the 524288-byte limit" in message for message in messages)
    assert any("payload_too_large" in message for message in messages)
    assert transport.stats.dropped == 1
    assert transport.stats.enqueued == 0
    assert requester.calls == []
    transport.close()


def test_a_six_million_character_message_arrives_shortened_instead_of_lost() -> None:
    requester = ScriptedRequester(HttpResponse(202))
    configuration = valid_configuration()
    client = Client(configuration, sink=Transport(configuration, requester=requester))

    message = ("order failed " * 500_000)[:6_000_000]
    assert client.capture_message(message) is not None
    assert client.flush(2)

    body = requester.calls[0][1]
    assert len(body) <= budget.MAX_PAYLOAD_BYTES
    sent = json.loads(body)
    assert sent["truncated"] is True
    assert sent["message"]["formatted"].startswith("order failed order failed")
    assert sent["message"]["formatted"].endswith(budget.ELLIPSIS)
    client.close()


def test_a_log_with_megabytes_of_attributes_arrives_with_level_and_message() -> None:
    requester = ScriptedRequester(HttpResponse(202))
    configuration = valid_configuration()
    client = Client(configuration, sink=Transport(configuration, requester=requester))

    assert client.log("export done", level="warning", dump="d" * (3 * 1024 * 1024)) is not None
    assert client.flush(2)

    body = requester.calls[0][1]
    assert len(body) <= budget.MAX_PAYLOAD_BYTES
    sent = json.loads(body)
    assert "attributes" not in sent
    assert sent["level"] == "warning"
    assert sent["message"] == "export done"
    assert sent["truncated"] is True
    client.close()


def test_data_added_by_before_send_is_measured_too() -> None:
    def inflate(payload: dict[str, object]) -> dict[str, object]:
        payload["extra"] = {"dump": "x" * (2 * budget.MAX_PAYLOAD_BYTES)}
        return payload

    requester = ScriptedRequester(HttpResponse(202))
    configuration = valid_configuration(before_send=inflate)
    client = Client(configuration, sink=Transport(configuration, requester=requester))

    assert client.capture_message("checkout failed") is not None
    assert client.flush(2)

    sent = json.loads(requester.calls[0][1])
    assert "extra" not in sent
    assert sent["message"] == {"formatted": "checkout failed"}
    assert sent["truncated"] is True
    client.close()


def test_a_usual_error_is_sent_exactly_as_built() -> None:
    requester = ScriptedRequester(HttpResponse(202))
    configuration = valid_configuration()
    transport = Transport(configuration, requester=requester)
    try:
        raise RuntimeError("checkout failed " * 1_000)
    except RuntimeError as error:
        payload = EventBuilder(configuration).exception(error)

    assert transport.enqueue("events", payload) is True
    assert transport.flush(1)

    assert requester.calls[0][1] == budget.encode(payload)
    assert "truncated" not in json.loads(requester.calls[0][1])
    transport.close()


def test_main_process_registers_no_child_finalizer() -> None:
    transport = Transport(valid_configuration(), requester=ScriptedRequester(HttpResponse(202)))

    assert transport.enqueue("events", {"n": 1})
    assert transport.flush(1)

    assert _in_multiprocessing_child() is False
    assert transport._process_finalizer is None
    assert transport.close(1) is True


def test_child_detection_does_not_import_multiprocessing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "multiprocessing")

    assert _in_multiprocessing_child() is False
    assert "multiprocessing" not in sys.modules


def test_close_cancels_the_child_finalizer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("closeyourit.transport._in_multiprocessing_child", lambda: True)
    transport = Transport(valid_configuration(), requester=ScriptedRequester(HttpResponse(202)))

    assert transport.enqueue("events", {"n": 1})
    finalizer = transport._process_finalizer
    assert finalizer is not None
    assert finalizer.still_active() is True

    assert transport.close(1) is True
    assert finalizer.still_active() is False
    assert transport._process_finalizer is None
    assert transport.close(1) is True


def test_child_finalizer_and_atexit_share_one_deadline_and_send_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("closeyourit.transport._in_multiprocessing_child", lambda: True)
    requester = BlockingRequester()
    clock = SimulatedClock()
    transport = Transport(valid_configuration(), requester=requester, shutdown_timeout=2.0)
    simulate_time(transport, clock)

    assert transport.enqueue("events", {"n": 1})
    assert requester.entered.wait(1)
    finalizer = transport._process_finalizer
    assert finalizer is not None

    finalizer()
    # The stalled delivery spent the whole budget.
    assert clock.now == 2.0
    transport._exit_close()
    # The second hook got no fresh budget: it did not wait at all.
    assert clock.now == 2.0

    assert transport._process_finalizer is None
    assert transport._atexit_registered is False
    requester.release.set()
    worker = transport._worker
    assert worker is not None
    worker.join(1)
    assert not worker.is_alive()
    assert transport.close(1) is True
    assert requester.bodies == [b'{"n":1}']
    assert transport.stats.sent == 1


def test_exit_hooks_split_one_budget_when_the_first_ends_early(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = SimulatedClock()
    transport = Transport(
        valid_configuration(), requester=ScriptedRequester(), shutdown_timeout=2.0, monotonic=clock
    )
    budgets: list[float | None] = []

    def close_spending_half_a_second(timeout: float | None, *, allow_start: bool) -> bool:
        assert allow_start is False
        budgets.append(timeout)
        clock.advance(0.5)
        return True

    monkeypatch.setattr(transport, "_close", close_spending_half_a_second)

    transport._exit_close()
    transport._exit_close()
    transport._exit_close()

    assert budgets == [2.0, 1.5, 1.0]


def test_pid_change_cancels_the_inherited_child_finalizer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("closeyourit.transport._in_multiprocessing_child", lambda: True)
    transport = Transport(valid_configuration(), requester=ScriptedRequester(HttpResponse(202)))
    assert transport.enqueue("events", {"parent": True})
    assert transport.flush(1)
    inherited = transport._process_finalizer
    assert inherited is not None
    parent_pid = transport.stats.pid

    monkeypatch.setattr("closeyourit.transport.os.getpid", lambda: parent_pid + 1)

    assert transport.stats.pid == parent_pid + 1
    assert inherited.still_active() is False
    assert transport._process_finalizer is None
    assert transport.close(1) is True


# Real child processes. The fake ingest server lives in the test process; each child builds or
# inherits a client, reports one error and returns without flushing, as an application would.

StartMethod: TypeAlias = Literal["fork", "spawn", "forkserver"]
START_METHODS = [
    pytest.param(
        method,
        marks=pytest.mark.skipif(
            method not in multiprocessing.get_all_start_methods(),
            reason=f"the {method} start method is not available on this platform",
        ),
    )
    for method in ("fork", "spawn", "forkserver")
]
SHUTDOWN_TIMEOUT = 2.0
# Child teardown (interpreter shutdown, process reaping) on a busy CI runner, on top of the wait.
EXIT_MARGIN = 1.0


class IngestServer:
    """Loopback ingest recording every POST; a stalled one never answers while the test runs."""

    def __init__(self, *, stalled: bool) -> None:
        self.bodies: list[bytes] = []
        self.release = threading.Event()
        if not stalled:
            self.release.set()
        self.on_post: Callable[[], object] | None = None
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                server.bodies.append(self.rfile.read(length))
                if server.on_post is not None:
                    server.on_post()
                server.release.wait(30)
                self.send_response(202)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    def close(self) -> None:
        self.release.set()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(5)


@pytest.fixture
def ingest() -> Iterator[IngestServer]:
    server = IngestServer(stalled=False)
    yield server
    server.close()


@pytest.fixture
def stalled_ingest() -> Iterator[IngestServer]:
    server = IngestServer(stalled=True)
    yield server
    server.close()


def ingest_client(url: str) -> Client:
    return Client(valid_configuration(endpoint_url=url, environment="test"))


def report(client: Client, message: str) -> None:
    try:
        raise RuntimeError(message)
    except RuntimeError as error:
        assert client.capture_exception(error, handled=True) is not None


def child_reports_and_returns(
    url: str, flush_by_hand: bool, finished_at: Synchronized[float]
) -> None:
    client = ingest_client(url)
    report(client, "child failed")
    if flush_by_hand:
        assert client.flush(SHUTDOWN_TIMEOUT)
    finished_at.value = time.monotonic()


def child_reports_to_a_stalled_server(
    url: str, posted: Event, waited: Synchronized[float], finished_at: Synchronized[float]
) -> None:
    client = ingest_client(url)
    transport = client.sink
    assert isinstance(transport, Transport)
    # The exit hooks wait on simulated time: the budget they spend lands in waited.
    simulate_time(transport, SimulatedClock(waited))
    report(client, "child failed")
    # The delivery is in flight and the server will not answer it.
    assert posted.wait(SHUTDOWN_TIMEOUT * 5)
    finished_at.value = time.monotonic()


def child_reports_with_inherited_client(client: Client, finished_at: Synchronized[float]) -> None:
    report(client, "forked child failed")
    finished_at.value = time.monotonic()


def child_checks_its_exit_finalizer(url: str, finished_at: Synchronized[float]) -> None:
    client = ingest_client(url)
    report(client, "child failed")
    transport = client.sink
    assert isinstance(transport, Transport)
    finalizer = transport._process_finalizer
    # Registered after the child booted: still in the registry multiprocessing runs at exit.
    assert finalizer is not None
    assert finalizer.still_active()
    finished_at.value = time.monotonic()


def child_reports_nothing(url: str, finished_at: Synchronized[float]) -> None:
    ingest_client(url)
    finished_at.value = time.monotonic()


def run_child(
    method: StartMethod, target: Callable[..., None], *args: object
) -> tuple[int | None, float]:
    """Run a child to completion; return its exit code and the seconds it took to exit."""
    # A union of start methods falls through to the str overload, typed as a bare BaseContext.
    context = cast(
        "ForkContext | ForkServerContext | SpawnContext", multiprocessing.get_context(method)
    )
    finished_at = context.Value(ctypes.c_double, 0.0)
    process = context.Process(target=target, args=(*args, finished_at))
    process.start()
    process.join(SHUTDOWN_TIMEOUT * 5)
    exited_at = time.monotonic()
    if process.is_alive():
        process.kill()
        process.join()
        pytest.fail("the child process did not exit")
    assert finished_at.value > 0, "the child failed before reporting"
    return process.exitcode, exited_at - finished_at.value


def received_messages(server: IngestServer) -> list[str]:
    messages: list[str] = []
    for body in server.bodies:
        payload = json.loads(body)
        messages.extend(exception["value"] for exception in payload["exception"]["values"])
    return messages


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("method", START_METHODS)
def test_child_delivers_its_report_once_before_exiting(
    method: StartMethod, ingest: IngestServer
) -> None:
    exitcode, _ = run_child(method, child_reports_and_returns, ingest.url, False)

    assert exitcode == 0
    assert received_messages(ingest) == ["child failed"]


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("method", START_METHODS)
def test_child_keeps_its_exit_finalizer_on_every_python_version(
    method: StartMethod, ingest: IngestServer
) -> None:
    # From Python 3.13 a child also runs its own atexit hooks, so delivery alone would not notice
    # a finalizer lost to the boot-time reset: check the registration itself.
    exitcode, _ = run_child(method, child_checks_its_exit_finalizer, ingest.url)

    assert exitcode == 0
    assert received_messages(ingest) == ["child failed"]


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="the fork start method is not available on this platform",
)
def test_forked_child_delivers_with_the_client_inherited_from_its_parent(
    ingest: IngestServer,
) -> None:
    client = ingest_client(ingest.url)
    report(client, "parent failed")
    assert client.flush(SHUTDOWN_TIMEOUT)

    exitcode, _ = run_child("fork", child_reports_with_inherited_client, client)

    assert exitcode == 0
    assert received_messages(ingest) == ["parent failed", "forked child failed"]
    assert client.close(SHUTDOWN_TIMEOUT) is True
    assert received_messages(ingest) == ["parent failed", "forked child failed"]


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("method", START_METHODS)
def test_child_exit_waits_at_most_the_shutdown_timeout_on_a_stalled_server(
    method: StartMethod, stalled_ingest: IngestServer
) -> None:
    context = multiprocessing.get_context(method)
    posted = context.Event()
    waited = context.Value(ctypes.c_double, -1.0)
    stalled_ingest.on_post = posted.set

    exitcode, exit_duration = run_child(
        method, child_reports_to_a_stalled_server, stalled_ingest.url, posted, waited
    )

    assert exitcode == 0
    assert len(stalled_ingest.bodies) == 1
    # It waited for the stalled delivery, and only once: two waits would spend twice the budget.
    assert waited.value == SHUTDOWN_TIMEOUT
    # The wait was simulated, so the child exits at once instead of hanging on the server.
    assert exit_duration < EXIT_MARGIN


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("method", START_METHODS)
def test_child_that_reports_nothing_sends_nothing_and_exits_at_once(
    method: StartMethod, ingest: IngestServer
) -> None:
    exitcode, exit_duration = run_child(method, child_reports_nothing, ingest.url)

    assert exitcode == 0
    assert ingest.bodies == []
    assert exit_duration < EXIT_MARGIN


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("method", START_METHODS)
def test_child_that_flushes_by_hand_sends_its_report_once(
    method: StartMethod, ingest: IngestServer
) -> None:
    exitcode, _ = run_child(method, child_reports_and_returns, ingest.url, True)

    assert exitcode == 0
    assert received_messages(ingest) == ["child failed"]


def test_main_process_delivers_its_report_once_at_exit(ingest: IngestServer) -> None:
    script = textwrap.dedent(
        f"""
        import multiprocessing  # loaded, as in an application that also starts children

        from closeyourit import Client, Configuration

        client = Client(
            Configuration(
                endpoint_url={ingest.url!r},
                token="cyi_test_token",
                project_id="12345678-1234-1234-1234-123456789abc",
                environment="test",
            )
        )
        try:
            raise RuntimeError("main failed")
        except RuntimeError as error:
            client.capture_exception(error, handled=True)
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=15
    )

    assert result.returncode == 0, result.stderr
    assert received_messages(ingest) == ["main failed"]
