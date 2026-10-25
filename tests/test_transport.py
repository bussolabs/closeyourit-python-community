from __future__ import annotations

import json
import logging
import threading
from collections import deque
from collections.abc import Callable, Mapping
from email.message import Message
from io import BytesIO
from typing import TypeAlias, cast
from urllib.error import HTTPError
from urllib.request import OpenerDirector, Request

import pytest

from closeyourit import Configuration, HttpResponse, Transport, UrllibHttpRequester

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


def test_redirect_preserves_post_body_and_bearer_only_for_same_authority() -> None:
    path = "/api/v1/projects/12345678-1234-1234-1234-123456789abc/events"
    requester = ScriptedRequester(
        HttpResponse(307, headers={"location": f"https://ingest.example.com{path}?r=1"}),
        HttpResponse(302, headers={"Location": f"https://other.example{path}"}),
        HttpResponse(202),
    )
    transport = Transport(valid_configuration(), requester=requester)

    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)

    first, same_host, other_host = requester.calls
    assert first[1] == same_host[1] == other_host[1]
    assert first[2]["Authorization"] == "Bearer cyi_test_token"
    assert same_host[2]["Authorization"] == "Bearer cyi_test_token"
    assert "Authorization" not in other_host[2]
    assert transport.stats.sent == 1
    transport.close()


def test_redirect_downgrade_strips_bearer_and_redirect_limit_is_bounded() -> None:
    requester = ScriptedRequester(
        HttpResponse(301, headers={"Location": "http://ingest.example.com/one"}),
        HttpResponse(308, headers={"Location": "/two"}),
        HttpResponse(307, headers={"Location": "/three"}),
    )
    transport = Transport(valid_configuration(), requester=requester, max_redirects=2)

    assert transport.enqueue("events", {"message": "redirect"})
    assert transport.flush(1)

    assert len(requester.calls) == 3
    assert "Authorization" not in requester.calls[1][2]
    assert "Authorization" not in requester.calls[2][2]
    assert transport.stats.failed == 1
    assert transport.stats.last_failure == "http_307"
    transport.close()


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
