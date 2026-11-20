from __future__ import annotations

import asyncio
import threading
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import httpx
import pytest
import requests
from requests.adapters import BaseAdapter

from closeyourit import Client, current_scope
from closeyourit.integrations import (
    instrument_httpx,
    instrument_requests,
    normalize_http_target,
)
from closeyourit.integrations._http import (
    _REPEATED_HTTP_MAX_KEYS,
    HttpRecorder,
    RepeatedHttpTracker,
)
from tests.test_client import RecordingSink, valid_configuration


class StepClock:
    def __init__(self, *values: float) -> None:
        self._values = iter(values)

    def __call__(self) -> float:
        return next(self._values)


class StaticAdapter(BaseAdapter):
    def __init__(self, *, status: int = 200, error: Exception | None = None) -> None:
        self.status = status
        self.error = error

    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,
        timeout: float | tuple[float, float] | tuple[float, None] | None = None,
        verify: bool | str = True,
        cert: bytes | str | tuple[bytes | str, bytes | str] | None = None,
        proxies: Mapping[str, str] | None = None,
    ) -> requests.Response:
        del stream, timeout, verify, cert, proxies
        if self.error is not None:
            raise self.error
        response = requests.Response()
        response.status_code = self.status
        response.url = request.url or ""
        response.request = request
        response.elapsed = timedelta(milliseconds=12)
        response.raw = None
        return response

    def close(self) -> None:
        return None


def integration_client(**configuration: object) -> tuple[Client, RecordingSink]:
    sink = RecordingSink()
    client = Client(valid_configuration(**configuration), sink=sink)
    return client, sink


def metrics(sink: RecordingSink) -> list[dict[str, object]]:
    return [payload for channel, payload in sink.items if channel == "metrics"]


def test_normalize_http_target_removes_credentials_query_fragment_and_templates_ids() -> None:
    target = normalize_http_target(
        "HTTPS://user:password@API.Example.test:443/v1/orders/12345/"
        "550e8400-e29b-41d4-a716-446655440000?token=secret#private"
    )

    assert target is not None
    assert target.host == "api.example.test"
    assert target.path == "/v1/orders/<n>/<uuid>"
    assert target.safe_url == "https://api.example.test/v1/orders/<n>/<uuid>"
    assert "secret" not in repr(target)
    assert normalize_http_target("relative/path?token=secret") is None


def test_normalize_http_target_templates_short_numeric_path_segments() -> None:
    target = normalize_http_target("https://api.example.test/v1/users/7/orders/42")

    assert target is not None
    assert target.path == "/v1/users/<n>/orders/<n>"
    assert target.safe_url == "https://api.example.test/v1/users/<n>/orders/<n>"


@pytest.mark.parametrize(("duration_ms", "expected"), [(99.99, 0), (100.0, 1), (100.01, 1)])
def test_requests_slow_threshold_x_minus_one_x_x_plus_one(
    duration_ms: float, expected: int
) -> None:
    client, sink = integration_client(slow_external_threshold_ms=100)
    session = requests.Session()
    session.mount("https://", StaticAdapter())
    instrumentation = instrument_requests(
        session, client, monotonic=StepClock(1.0, 1.0 + duration_ms / 1000)
    )

    response = session.get("https://api.example.test/orders/12345?token=never-send")

    assert response.status_code == 200
    assert len(metrics(sink)) == expected
    breadcrumbs = client.builder.message("done")["breadcrumbs"]["values"]  # type: ignore[index]
    assert breadcrumbs[-1]["data"]["url"] == "https://api.example.test/orders/<n>"
    assert "never-send" not in str(breadcrumbs)
    instrumentation.uninstrument()


def test_requests_records_5xx_timeout_and_restores_the_session() -> None:
    client, sink = integration_client(slow_external_threshold_ms=0)
    session = requests.Session()
    original_send = session.send
    session.mount("https://", StaticAdapter(status=503))
    instrumentation = instrument_requests(
        session, client, monotonic=StepClock(1.0, 1.01, 2.0, 2.01)
    )

    assert session.get("https://api.example.test/fail?password=hidden").status_code == 503
    first = client.builder.message("first")["breadcrumbs"]["values"][-1]  # type: ignore[index]
    assert first["level"] == "warning"
    assert first["data"]["status_code"] == 503

    session.mount("https://", StaticAdapter(error=requests.Timeout("private timeout detail")))
    with pytest.raises(requests.Timeout, match="private timeout detail"):
        session.get("https://api.example.test/timeout?api_key=hidden")

    second = client.builder.message("second")["breadcrumbs"]["values"][-1]  # type: ignore[index]
    assert second["data"]["error_type"] == "Timeout"
    assert "private timeout detail" not in str(second)
    assert metrics(sink) == []

    instrumentation.uninstrument()
    assert session.send == original_send
    instrumentation.uninstrument()


def test_requests_repeated_http_emits_once_at_the_exact_threshold() -> None:
    client, sink = integration_client(
        slow_external_threshold_ms=0,
        repeated_http_threshold=3,
        repeated_http_window_ms=5_000,
    )
    session = requests.Session()
    session.mount("https://", StaticAdapter())
    instrumentation = instrument_requests(
        session,
        client,
        monotonic=StepClock(0, 0.001, 1, 1.001, 2, 2.001, 3, 3.001, 4, 4.001),
    )

    for identifier in (100, 200, 300, 400, 500):
        session.get(f"https://api.example.test/items/{identifier}")

    repeated = [item for item in metrics(sink) if item["subtype"] == "repeated_http"]
    assert len(repeated) == 1
    assert repeated[0]["query_count"] == 3
    assert repeated[0]["http_url"] == "/items/<n>"
    instrumentation.uninstrument()


def test_requests_instrumentation_is_thread_safe() -> None:
    client, sink = integration_client(slow_external_threshold_ms=0, repeated_http_threshold=4)
    session = requests.Session()
    session.mount("https://", StaticAdapter())
    instrumentation = instrument_requests(session, client)

    with ThreadPoolExecutor(max_workers=4) as executor:
        statuses = list(
            executor.map(
                lambda identifier: (
                    session.get(f"https://api.example.test/concurrent/{identifier}").status_code
                ),
                (100, 200, 300, 400),
            )
        )

    assert statuses == [200, 200, 200, 200]
    repeated = [item for item in metrics(sink) if item["subtype"] == "repeated_http"]
    assert len(repeated) == 1
    instrumentation.uninstrument()


def test_requests_repeated_http_emits_again_at_the_threshold_after_the_window_expires() -> None:
    client, sink = integration_client(
        slow_external_threshold_ms=0,
        repeated_http_threshold=2,
        repeated_http_window_ms=5_000,
    )
    session = requests.Session()
    session.mount("https://", StaticAdapter())
    instrumentation = instrument_requests(
        session,
        client,
        monotonic=StepClock(0, 0.001, 1, 1.001, 2, 2.001, 10, 10.001, 11, 11.001),
    )

    for identifier in (100, 200, 300, 400, 500):
        session.get(f"https://api.example.test/items/{identifier}")

    repeated = [item for item in metrics(sink) if item["subtype"] == "repeated_http"]
    assert [item["query_count"] for item in repeated] == [2, 2]
    assert {item["http_host"] for item in repeated} == {"api.example.test"}
    assert {item["http_url"] for item in repeated} == {"/items/<n>"}
    instrumentation.uninstrument()


def tracker(*, window_ms: float = 5_000, **options: object) -> RepeatedHttpTracker:
    return RepeatedHttpTracker(threshold=3, window_ms=window_ms, **options)  # type: ignore[arg-type]


def test_repeated_http_tracker_counts_hits_inside_the_window_and_expires_the_exact_boundary() -> (
    None
):
    instance = tracker()

    assert instance.record("GET\napi\n/a", now_ms=0) == 1
    assert instance.record("GET\napi\n/a", now_ms=4_999.999) == 2
    # A hit exactly one window old is expired, as before.
    assert instance.record("GET\napi\n/a", now_ms=9_999.999) == 1


def test_repeated_http_tracker_drops_other_keys_once_their_last_hit_expires() -> None:
    instance = tracker()
    instance.record("GET\napi\n/stale", now_ms=0)
    instance.record("GET\napi\n/fresh", now_ms=1)

    instance.record("GET\napi\n/current", now_ms=4_999.999)
    assert list(instance._hits) == ["GET\napi\n/stale", "GET\napi\n/fresh", "GET\napi\n/current"]

    instance.record("GET\napi\n/current", now_ms=5_000)
    assert list(instance._hits) == ["GET\napi\n/fresh", "GET\napi\n/current"]


def test_repeated_http_tracker_keeps_only_the_current_key_after_thousands_expire() -> None:
    instance = tracker()
    for index in range(3_000):
        instance.record(f"GET\napi\n/{index}", now_ms=index / 3)

    instance.record("GET\napi\n/current", now_ms=1_000 + 5_001)

    assert list(instance._hits) == ["GET\napi\n/current"]


def test_repeated_http_tracker_refreshes_the_least_recently_used_order() -> None:
    instance = tracker(max_keys=3)
    for index, key in enumerate(("a", "b", "c")):
        instance.record(key, now_ms=index)

    assert instance.record("a", now_ms=3) == 2
    instance.record("d", now_ms=4)

    assert list(instance._hits) == ["c", "a", "d"]


def test_repeated_http_tracker_default_cap_is_a_thousand_keys() -> None:
    instance = tracker()
    for index in range(_REPEATED_HTTP_MAX_KEYS):
        instance.record(f"GET\napi\n/{index}", now_ms=index / 1_000)

    assert len(instance._hits) == _REPEATED_HTTP_MAX_KEYS == 1_000
    assert "GET\napi\n/0" in instance._hits


def test_repeated_http_tracker_never_exceeds_the_cap_and_evicts_the_least_recent() -> None:
    instance = tracker()
    sizes = []
    for index in range(1_500):
        instance.record(f"GET\napi\n/{index}", now_ms=index * 0.5)
        sizes.append(len(instance._hits))

    assert max(sizes) == 1_000
    keys = list(instance._hits)
    assert (keys[0], keys[-1]) == ("GET\napi\n/500", "GET\napi\n/1499")


def test_repeated_http_tracker_with_a_zero_window_keeps_only_the_current_hit() -> None:
    instance = tracker(window_ms=0)

    assert instance.record("a", now_ms=10) == 1
    assert instance.record("a", now_ms=10) == 1
    assert instance.record("b", now_ms=10) == 1
    assert list(instance._hits) == ["b"]


def test_repeated_http_tracker_keeps_valid_hits_recorded_out_of_order() -> None:
    instance = tracker()

    # Concurrent requests read the clock before taking the lock, so an older timestamp can be
    # recorded after a newer one.
    assert instance.record("a", now_ms=6_000) == 1
    assert instance.record("a", now_ms=1_500) == 2
    assert instance.record("b", now_ms=1_200) == 1
    assert instance.record("a", now_ms=6_400) == 3
    assert list(instance._hits) == ["a"]
    # Only the hit at 1_500 is older than the window: the newer ones stay counted.
    assert instance.record("a", now_ms=6_600) == 3


def test_repeated_http_tracker_never_drops_a_valid_key_behind_an_out_of_order_one() -> None:
    instance = tracker()
    instance.record("late", now_ms=6_000)
    instance.record("valid", now_ms=1_500)

    instance.record("current", now_ms=6_400)

    assert list(instance._hits) == ["late", "valid", "current"]


def test_repeated_http_tracker_keeps_one_hit_past_the_threshold_per_key() -> None:
    instance = tracker()

    counts = [instance.record("GET\napi\n/a", now_ms=index / 10) for index in range(10_000)]

    assert counts[:5] == [1, 2, 3, 4, 4]
    assert counts.count(3) == 1
    assert set(counts[3:]) == {4}
    assert list(instance._hits["GET\napi\n/a"]) == [999.6, 999.7, 999.8, 999.9]


def test_repeated_http_tracker_capped_hits_still_drop_below_the_threshold_on_expiry() -> None:
    instance = tracker()
    for index in range(100):
        instance.record("a", now_ms=index)

    # Only the hits at 98 and 99 are still inside the window: the count is exact again.
    assert instance.record("a", now_ms=5_097.5) == 3
    assert instance.record("a", now_ms=5_098.5) == 3
    assert instance.record("a", now_ms=5_098.9) == 4


def test_repeated_http_tracker_reaches_the_threshold_again_after_an_expired_key_returns() -> None:
    instance = tracker()
    assert [instance.record("a", now_ms=index) for index in range(5)] == [1, 2, 3, 4, 4]
    instance.record("b", now_ms=10_000)
    assert list(instance._hits) == ["b"]

    counts = [instance.record("a", now_ms=10_001 + index) for index in range(5)]

    assert counts == [1, 2, 3, 4, 4]
    assert counts.count(instance.threshold) == 1


def test_repeated_http_tracker_counts_every_concurrent_hit_once() -> None:
    instance = RepeatedHttpTracker(threshold=40, window_ms=5_000, max_keys=8)
    keys = [f"GET\napi\n/{index}" for index in range(16)]
    start = threading.Barrier(8)

    def record(worker: int) -> list[int]:
        start.wait()
        return [
            instance.record(keys[(worker + step) % len(keys)], now_ms=1_000 + step)
            for step in range(200)
        ]

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = [count for counts in executor.map(record, range(8)) for count in counts]

    assert len(results) == 1_600
    assert max(results) <= instance.threshold + 1
    assert len(instance._hits) <= 8
    assert all(len(hits) <= instance.threshold + 1 for hits in instance._hits.values())
    assert all(list(hits) == sorted(hits) for hits in instance._hits.values())


def test_repeated_http_tracker_in_a_forked_child_starts_from_an_empty_list() -> None:
    pids = [4242]
    instance = tracker(pid=lambda: pids[-1])
    instance.record("GET\napi\n/a", now_ms=0)
    instance.record("GET\napi\n/a", now_ms=1)
    inherited_lock = instance._lock

    pids.append(424_242)

    assert instance.record("GET\napi\n/a", now_ms=2) == 1
    assert list(instance._hits) == ["GET\napi\n/a"]
    assert instance._lock is not inherited_lock
    assert instance.record("GET\napi\n/a", now_ms=3) == 2


def test_repeated_http_tracker_in_the_same_process_keeps_its_lock() -> None:
    instance = tracker(pid=lambda: 4242)
    lock = instance._lock

    instance.record("a", now_ms=0)

    assert instance._lock is lock
    assert instance.record("a", now_ms=1) == 2


def test_repeated_http_tracker_never_blocks_a_forked_child_on_an_inherited_lock() -> None:
    pids = [4242]
    instance = tracker(pid=lambda: pids[-1])
    instance.record("GET\napi\n/a", now_ms=0)
    # At fork time another thread could hold the lock: it does not exist in the child, so the
    # inherited lock would never be released.
    instance._lock.acquire()
    pids.append(424_242)
    counts: list[int] = []
    recorded = threading.Event()

    def record_in_the_child() -> None:
        counts.append(instance.record("GET\napi\n/a", now_ms=1))
        recorded.set()

    threading.Thread(target=record_in_the_child, daemon=True).start()

    assert recorded.wait(5.0) is True
    assert counts == [1]


def test_http_recorder_counts_only_the_calls_of_its_own_process() -> None:
    client, sink = integration_client(
        slow_external_threshold_ms=0,
        repeated_http_threshold=3,
        repeated_http_window_ms=5_000,
    )
    pids = [4242]
    recorder = HttpRecorder(client, monotonic=StepClock(1, 2, 3, 4))
    recorder.tracker = tracker(pid=lambda: pids[-1])

    for _ in range(2):
        recorder.complete(method="get", raw_url="https://api.example.test/a", started_at=0)
    pids.append(424_242)
    recorder.complete(method="get", raw_url="https://api.example.test/a", started_at=0)

    assert [item for item in metrics(sink) if item["subtype"] == "repeated_http"] == []
    recorder.complete(method="get", raw_url="https://api.example.test/a", started_at=0)
    assert list(recorder.tracker._hits) == ["GET\napi.example.test\n/a"]
    assert len(recorder.tracker._hits["GET\napi.example.test\n/a"]) == 2


def test_httpx_sync_uses_hooks_handles_5xx_and_uninstruments() -> None:
    client, sink = integration_client(slow_external_threshold_ms=10)
    http_client = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(500, request=request))
    )
    original_send = http_client.send
    instrumentation = instrument_httpx(http_client, client, monotonic=StepClock(1.0, 1.02))

    assert len(http_client.event_hooks["request"]) == 1
    assert len(http_client.event_hooks["response"]) == 1
    assert http_client.get("https://api.example.test/users/123?token=hidden").status_code == 500
    assert metrics(sink)[0]["subtype"] == "slow_external_http"

    instrumentation.uninstrument()
    assert http_client.send == original_send
    assert http_client.event_hooks == {"request": [], "response": []}
    http_client.close()


def test_httpx_sync_timeout_is_recorded_without_swallowing_the_error() -> None:
    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("private timeout detail", request=request)

    client, _sink = integration_client(slow_external_threshold_ms=0)
    http_client = httpx.Client(transport=httpx.MockTransport(timeout))
    instrumentation = instrument_httpx(http_client, client, monotonic=StepClock(1.0, 1.01))

    with pytest.raises(httpx.ReadTimeout, match="private timeout detail"):
        http_client.get("https://api.example.test/timeout?secret=hidden")

    breadcrumb = client.builder.message("done")["breadcrumbs"]["values"][-1]  # type: ignore[index]
    assert breadcrumb["data"]["error_type"] == "ReadTimeout"
    assert "private timeout detail" not in str(breadcrumb)
    instrumentation.uninstrument()
    http_client.close()


def test_httpx_async_concurrent_requests_are_isolated() -> None:
    async def scenario() -> tuple[list[int], list[dict[str, object]]]:
        client, sink = integration_client(slow_external_threshold_ms=0, repeated_http_threshold=3)

        async def response(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0)
            return httpx.Response(200, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http_client:
            instrumentation = instrument_httpx(http_client, client)
            responses = await asyncio.gather(
                *(
                    http_client.get(f"https://api.example.test/jobs/{identifier}?token=hidden")
                    for identifier in (100, 200, 300)
                )
            )
            instrumentation.uninstrument()
        return [response.status_code for response in responses], metrics(sink)

    statuses, captured = asyncio.run(scenario())

    assert statuses == [200, 200, 200]
    repeated = [item for item in captured if item["subtype"] == "repeated_http"]
    assert len(repeated) == 1
    assert repeated[0]["query_count"] == 3


def test_http_instrumentation_skips_the_closeyourit_ingest_host() -> None:
    client, sink = integration_client(slow_external_threshold_ms=0, repeated_http_threshold=1)
    session = requests.Session()
    session.mount("https://", StaticAdapter())
    instrumentation = instrument_requests(session, client)

    session.get("https://www.closeyour.it/api/v1/projects/id/events?token=internal")

    assert sink.items == []
    assert client.builder.message("done").get("breadcrumbs") is None
    instrumentation.uninstrument()


def test_http_capture_host_allowlist_skips_unlisted_destinations() -> None:
    client, sink = integration_client(
        slow_external_threshold_ms=1,
        repeated_http_threshold=1,
        http_capture_hosts=("api.allowed.test",),
    )
    session = requests.Session()
    session.mount("https://", StaticAdapter())
    instrumentation = instrument_requests(session, client, monotonic=StepClock(1.0, 1.1, 2.0, 2.1))

    session.get("https://api.blocked.test/private?token=hidden")
    session.get("https://api.allowed.test/public?token=hidden")

    assert len(metrics(sink)) == 2
    breadcrumbs = client.builder.message("done")["breadcrumbs"]["values"]  # type: ignore[index]
    assert len(breadcrumbs) == 1
    assert breadcrumbs[0]["data"]["url"] == "https://api.allowed.test/public"
    instrumentation.uninstrument()


def test_httpx_w3c_trace_propagation_is_opt_in_and_rechecked_on_redirect() -> None:
    seen: list[tuple[str, str | None]] = []

    def response(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.host, request.headers.get("traceparent")))
        if request.url.host == "api.allowed.test":
            return httpx.Response(
                302,
                headers={"location": "https://api.blocked.test/redirected?token=hidden"},
                request=request,
            )
        return httpx.Response(200, request=request)

    client, _sink = integration_client(
        trace_propagation_enabled=True,
        trace_propagation_hosts=("api.allowed.test",),
    )
    current_scope().trace_id = "550e8400-e29b-41d4-a716-446655440000"
    http_client = httpx.Client(transport=httpx.MockTransport(response), follow_redirects=True)
    instrumentation = instrument_httpx(
        http_client,
        client,
        span_id_factory=lambda: "0123456789abcdef",
    )

    assert http_client.get("https://api.allowed.test/start").status_code == 200

    assert seen == [
        (
            "api.allowed.test",
            "00-550e8400e29b41d4a716446655440000-0123456789abcdef-01",
        ),
        ("api.blocked.test", None),
    ]
    instrumentation.uninstrument()
    http_client.close()
