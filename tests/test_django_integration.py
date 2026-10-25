from __future__ import annotations

import subprocess
import sys
from collections.abc import Awaitable
from typing import cast

import pytest

from closeyourit import Client, Configuration, DjangoMiddleware, current_scope
from closeyourit.client import Channel

django = pytest.importorskip("django")
from django.conf import settings  # type: ignore[import-untyped]  # noqa: E402
from django.http import (  # type: ignore[import-untyped]  # noqa: E402
    HttpRequest,
    HttpResponse,
    HttpResponseNotFound,
    HttpResponseServerError,
    StreamingHttpResponse,
)
from django.test import RequestFactory  # type: ignore[import-untyped]  # noqa: E402

if not settings.configured:
    settings.configure(
        ALLOWED_HOSTS=["testserver"],
        DEFAULT_CHARSET="utf-8",
        SECRET_KEY="test-only",
    )


class RecordingSink:
    def __init__(self) -> None:
        self.items: list[tuple[Channel, dict[str, object]]] = []

    def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
        self.items.append((channel, payload))
        return True


def configured_client(*, threshold: float = 1000.0) -> tuple[Client, RecordingSink]:
    sink = RecordingSink()
    configuration = Configuration(
        endpoint_url="https://ingest.example.test",
        token="cyi_server-token",
        project_id="12345678-1234-1234-1234-123456789abc",
        environment="production",
        slow_request_threshold_ms=threshold,
    )
    return Client(configuration, sink=sink), sink


def test_django_module_does_not_import_framework_at_package_import_time() -> None:
    command = "import sys, closeyourit.django; assert 'django' not in sys.modules"
    result = subprocess.run([sys.executable, "-c", command], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_django_middleware_enriches_route_without_reading_body_and_preserves_order() -> None:
    client, _sink = configured_client()
    request = RequestFactory().post(
        "/orders/42?token=private",
        data=b"private body",
        content_type="application/octet-stream",
        HTTP_ACCEPT="application/json",
        HTTP_AUTHORIZATION="Bearer private",
    )
    seen: list[dict[str, object]] = []
    middleware: DjangoMiddleware

    def view(_request: HttpRequest) -> HttpResponse:
        raise AssertionError("only used as resolver metadata")

    def get_response(inner_request: HttpRequest) -> HttpResponse:
        middleware.process_view(inner_request, view, (), {"id": 42})
        seen.append(current_scope().snapshot(client.configuration).to_event_data())
        return HttpResponse("created", status=201)

    view.closeyourit_route = "/orders/<int:id>"  # type: ignore[attr-defined]
    middleware = DjangoMiddleware(get_response, client=client)

    response = cast(HttpResponse, middleware(request))

    assert response.status_code == 201
    assert response.content == b"created"
    assert seen[0]["request"] == {
        "method": "POST",
        "url": "http://testserver/orders/42",
        "path": "/orders/42",
        "route": "/orders/<int:id>",
        "headers": {"Accept": "application/json", "Content-Type": "application/octet-stream"},
    }
    assert current_scope().snapshot().to_event_data() == {}


def test_django_404_and_handled_500_keep_response_semantics() -> None:
    client, sink = configured_client()
    factory = RequestFactory()

    not_found = DjangoMiddleware(lambda _request: HttpResponseNotFound("missing"), client=client)
    assert cast(HttpResponse, not_found(factory.get("/missing"))).status_code == 404
    assert sink.items == []

    middleware: DjangoMiddleware

    def handled(_request: HttpRequest) -> HttpResponse:
        error = RuntimeError("handled by django")
        middleware.process_exception(_request, error)
        return HttpResponseServerError("safe response")

    middleware = DjangoMiddleware(handled, client=client)
    response = cast(HttpResponse, middleware(factory.get("/boom")))

    assert response.status_code == 500
    assert response.content == b"safe response"
    events = [payload for channel, payload in sink.items if channel == "events"]
    assert len(events) == 1
    assert events[0]["request"]["status_code"] == 500  # type: ignore[index]
    assert events[0]["exception"]["values"][-1]["mechanism"]["handled"] is False  # type: ignore[index]


def test_django_propagated_exception_is_captured_once_and_reraised() -> None:
    client, sink = configured_client()

    def explode(_request: HttpRequest) -> HttpResponse:
        raise ValueError("django exploded")

    with pytest.raises(ValueError, match="django exploded"):
        DjangoMiddleware(explode, client=client)(RequestFactory().get("/boom"))

    assert len([item for item in sink.items if item[0] == "events"]) == 1
    assert current_scope().snapshot().to_event_data() == {}


def test_django_supports_async_get_response_and_isolates_concurrent_requests() -> None:
    client, _sink = configured_client()
    seen: dict[str, str] = {}

    async def get_response(request: HttpRequest) -> HttpResponse:
        await __import__("asyncio").sleep(0)
        seen[request.path] = str(current_scope().snapshot().request["path"])  # type: ignore[index]
        return HttpResponse("ok")

    middleware = DjangoMiddleware(get_response, client=client)

    async def run() -> None:
        await cast(Awaitable[object], middleware(RequestFactory().get("/probe")))
        first = cast(Awaitable[object], middleware(RequestFactory().get("/one")))
        second = cast(Awaitable[object], middleware(RequestFactory().get("/two")))
        await __import__("asyncio").gather(first, second)

    __import__("asyncio").run(run())
    assert seen == {"/probe": "/probe", "/one": "/one", "/two": "/two"}


def test_django_streaming_defers_scope_reset_and_duration_until_consumed() -> None:
    client, sink = configured_client(threshold=10.0)
    request = RequestFactory().get("/stream")
    seen: list[str] = []

    def content() -> object:
        seen.append(str(current_scope().snapshot().request["path"]))  # type: ignore[index]
        yield b"one"
        seen.append(str(current_scope().snapshot().request["path"]))  # type: ignore[index]
        yield b"two"

    times = iter([1.0, 1.02])
    middleware = DjangoMiddleware(
        lambda _request: StreamingHttpResponse(content()),
        client=client,
        monotonic=lambda: next(times),
    )

    response = cast(StreamingHttpResponse, middleware(request))
    assert seen == []
    assert sink.items == []
    assert b"".join(response.streaming_content) == b"onetwo"

    assert seen == ["/stream", "/stream"]
    assert len([item for item in sink.items if item[0] == "metrics"]) == 1
    assert current_scope().snapshot().to_event_data() == {}


def test_django_async_streaming_preserves_scope() -> None:
    client, _sink = configured_client()
    request = RequestFactory().get("/async-stream")
    seen: list[str] = []

    async def content() -> object:
        seen.append(str(current_scope().snapshot().request["path"]))  # type: ignore[index]
        yield b"one"
        await __import__("asyncio").sleep(0)
        seen.append(str(current_scope().snapshot().request["path"]))  # type: ignore[index]
        yield b"two"

    async def get_response(_request: HttpRequest) -> StreamingHttpResponse:
        return StreamingHttpResponse(content())

    async def run() -> bytes:
        response = cast(
            StreamingHttpResponse,
            await cast(Awaitable[object], DjangoMiddleware(get_response, client=client)(request)),
        )
        return b"".join([chunk async for chunk in response.streaming_content])

    assert __import__("asyncio").run(run()) == b"onetwo"
    assert seen == ["/async-stream", "/async-stream"]
