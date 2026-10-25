from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from closeyourit import ASGIMiddleware, Client, Configuration, current_scope
from closeyourit.client import Channel

Message = dict[str, object]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]


class RecordingSink:
    def __init__(self) -> None:
        self.items: list[tuple[Channel, dict[str, object]]] = []

    def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
        self.items.append((channel, payload))
        return True


def configured_client(*, threshold: float = 1000.0) -> tuple[Client, RecordingSink]:
    sink = RecordingSink()
    config = Configuration(
        endpoint_url="https://ingest.example.test",
        token="cyi_token",
        project_id="12345678-1234-1234-1234-123456789abc",
        environment="production",
        slow_request_threshold_ms=threshold,
    )
    return Client(config, sink=sink), sink


def scope(path: str = "/users/7") -> dict[str, object]:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.5"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"token=private",
        "server": ("api.example.test", 443),
        "headers": [
            (b"accept", b"application/json"),
            (b"user-agent", b"pytest"),
            (b"authorization", b"Bearer private"),
            (b"cookie", b"session=private"),
        ],
    }


@pytest.mark.asyncio
async def test_asgi_preserves_messages_without_reading_body_and_records_context() -> None:
    client, _sink = configured_client()
    received = False
    sent: list[Message] = []
    seen: list[dict[str, object]] = []

    async def receive() -> Message:
        nonlocal received
        received = True
        return {"type": "http.request", "body": b"private"}

    async def send(message: Message) -> None:
        sent.append(message)

    async def app(app_scope: dict[str, object], app_receive: Receive, app_send: Send) -> None:
        del app_receive
        app_scope["route"] = type("Route", (), {"path": "/users/{id}"})()
        seen.append(current_scope().snapshot(client.configuration).to_event_data())
        await app_send({"type": "http.response.start", "status": 204, "headers": []})
        await app_send({"type": "http.response.body", "body": b"", "more_body": False})

    await ASGIMiddleware(app, client)(scope(), receive, send)

    assert received is False
    assert sent == [
        {"type": "http.response.start", "status": 204, "headers": []},
        {"type": "http.response.body", "body": b"", "more_body": False},
    ]
    assert seen[0]["request"] == {
        "method": "GET",
        "url": "https://api.example.test/users/7",
        "path": "/users/7",
        "headers": {"Accept": "application/json", "User-Agent": "pytest"},
    }
    assert current_scope().snapshot().to_event_data() == {}


@pytest.mark.asyncio
async def test_asgi_captures_unhandled_error_and_reraises() -> None:
    client, sink = configured_client()

    async def app(_scope: dict[str, object], _receive: Receive, _send: Send) -> None:
        raise RuntimeError("asgi failed")

    async def receive() -> Message:
        raise AssertionError("body must stay unread")

    async def send(_message: Message) -> None:
        return None

    times = iter([2.0, 2.025])
    with pytest.raises(RuntimeError, match="asgi failed"):
        await ASGIMiddleware(app, client, monotonic=lambda: next(times))(scope(), receive, send)

    event = sink.items[0][1]
    assert event["request"]["duration_ms"] == 25.0  # type: ignore[index]
    assert event["exception"]["values"][-1]["mechanism"]["handled"] is False  # type: ignore[index]
    assert current_scope().snapshot().to_event_data() == {}


@pytest.mark.asyncio
async def test_asgi_concurrent_requests_and_streaming_are_isolated() -> None:
    client, sink = configured_client(threshold=10.0)
    seen: dict[str, list[str]] = {"/one": [], "/two": []}
    barrier = asyncio.Barrier(2)

    async def app(app_scope: dict[str, object], _receive: Receive, send: Send) -> None:
        path = str(app_scope["path"])
        app_scope["route"] = type("Route", (), {"path": "/{name}"})()
        await barrier.wait()
        seen[path].append(str(current_scope().snapshot().request["path"]))  # type: ignore[index]
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"a", "more_body": True})
        await asyncio.sleep(0)
        seen[path].append(str(current_scope().snapshot().request["path"]))  # type: ignore[index]
        await send({"type": "http.response.body", "body": b"b", "more_body": False})

    async def receive() -> Message:
        raise AssertionError("unused")

    async def send(_message: Message) -> None:
        return None

    ticks = iter([0.0, 0.0, 0.02, 0.02])
    middleware = ASGIMiddleware(app, client, monotonic=lambda: next(ticks))
    await asyncio.gather(
        middleware(scope("/one"), receive, send), middleware(scope("/two"), receive, send)
    )

    assert seen == {"/one": ["/one", "/one"], "/two": ["/two", "/two"]}
    metrics = [payload for channel, payload in sink.items if channel == "metrics"]
    assert len(metrics) == 2
    assert {metric["route"] for metric in metrics} == {"/{name}"}


@pytest.mark.asyncio
async def test_asgi_non_http_scope_passes_through_without_telemetry() -> None:
    client, sink = configured_client()
    called = False

    async def app(_scope: dict[str, object], _receive: Receive, _send: Send) -> None:
        nonlocal called
        called = True

    async def receive() -> Message:
        return {"type": "lifespan.startup"}

    async def send(_message: Message) -> None:
        return None

    await ASGIMiddleware(app, client)({"type": "lifespan"}, receive, send)

    assert called is True
    assert sink.items == []


@pytest.mark.asyncio
async def test_asgi_send_disconnect_is_not_reported_as_application_error() -> None:
    client, sink = configured_client()

    async def app(_scope: dict[str, object], _receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200})

    async def receive() -> Message:
        raise AssertionError("unused")

    async def disconnected(_message: Message) -> None:
        raise OSError("client disconnected")

    with pytest.raises(OSError, match="client disconnected"):
        await ASGIMiddleware(app, client)(scope(), receive, disconnected)

    assert [item for item in sink.items if item[0] == "events"] == []
    assert current_scope().snapshot().to_event_data() == {}
