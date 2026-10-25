from __future__ import annotations

import asyncio
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
