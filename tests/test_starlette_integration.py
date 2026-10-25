from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import httpx
import pytest
from fastapi import FastAPI
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import PlainTextResponse, StreamingResponse
from starlette.routing import Mount, Route

from closeyourit import current_scope, instrument_fastapi, instrument_starlette
from tests.test_asgi import configured_client, scope


@pytest.mark.asyncio
@pytest.mark.parametrize("framework", ["starlette", "fastapi"])
async def test_real_framework_templates_and_privacy(framework: str) -> None:
    client, sink = configured_client(threshold=0)

    async def endpoint(request: Request) -> PlainTextResponse:
        return PlainTextResponse("unchanged")

    app: Starlette
    if framework == "fastapi":
        app = FastAPI()
        app.add_api_route("/users/{user_id}", endpoint)
        observed = instrument_fastapi(app, client)
    else:
        app = Starlette(routes=[Route("/users/{user_id}", endpoint)])
        observed = instrument_starlette(app, client)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        response = await http.get("/users/7?password=private", headers={"Authorization": "private"})
    assert response.text == "unchanged"
    metrics = [payload for channel, payload in sink.items if channel == "metrics"]
    assert len(metrics) == 1
    assert metrics[0]["route"] == "/users/{user_id}"
    assert "private" not in json.dumps(sink.items)
    assert current_scope().trace_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
async def test_handled_http_errors_are_opt_in_and_keep_headers(capture: bool) -> None:
    client, sink = configured_client()

    async def endpoint(request: Request) -> PlainTextResponse:
        raise HTTPException(409, "conflict", headers={"X-Original": "yes"})

    app = Starlette(routes=[Route("/handled", endpoint)])
    observed = instrument_starlette(app, client, capture_handled_errors=capture)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        response = await http.get("/handled")
    assert response.status_code == 409
    assert response.headers["X-Original"] == "yes"
    errors = [payload for channel, payload in sink.items if channel == "events"]
    assert len(errors) == int(capture)
    if capture:
        assert cast(Any, errors[0]["exception"])["values"][-1]["mechanism"]["handled"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("background", [False, True])
async def test_unhandled_errors_are_captured_once_and_reraised(background: bool) -> None:
    client, sink = configured_client()
    error = RuntimeError("failure")

    async def fail() -> None:
        raise error

    async def endpoint(request: Request) -> PlainTextResponse:
        if background:
            return PlainTextResponse("sent", background=BackgroundTask(fail))
        raise error

    app = Starlette(routes=[Route("/failure", endpoint)])
    observed = instrument_starlette(app, client, capture_handled_errors=True)
    assert instrument_starlette(app, client) is observed
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        with pytest.raises(RuntimeError) as caught:
            await http.get("/failure")
    assert caught.value is error
    errors = [payload for channel, payload in sink.items if channel == "events"]
    assert len(errors) == 1
    assert cast(Any, errors[0]["exception"])["values"][-1]["mechanism"]["handled"] is False


@pytest.mark.asyncio
async def test_mounted_route_template_does_not_include_parameter_values() -> None:
    client, sink = configured_client(threshold=0)

    async def endpoint(request: Request) -> PlainTextResponse:
        return PlainTextResponse("ok")

    child = Starlette(routes=[Route("/users/{user_id:int}", endpoint)])
    app = Starlette(routes=[Mount("/teams/{team_id}", child)])
    observed = instrument_starlette(app, client)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        assert (await http.get("/teams/private/users/7")).status_code == 200
    metric = next(payload for channel, payload in sink.items if channel == "metrics")
    assert metric["route"] == "/teams/{team_id}/users/{user_id}"


@pytest.mark.asyncio
async def test_uninstall_restores_custom_handler_identity_and_keeps_later_handlers() -> None:
    client, sink = configured_client()

    def handler(request: Request, error: Exception) -> PlainTextResponse:
        return PlainTextResponse("custom", status_code=409)

    async def endpoint(request: Request) -> PlainTextResponse:
        raise ValueError("handled")

    app = Starlette(routes=[Route("/", endpoint)], exception_handlers={ValueError: handler})
    observed = instrument_starlette(app, client, capture_handled_errors=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        assert (await http.get("/")).text == "custom"
        assert len(sink.items) == 1
        observed.uninstall()
        observed.uninstall()
        assert app.exception_handlers[ValueError] is handler
        assert HTTPException not in app.exception_handlers
        assert (await http.get("/")).text == "custom"
        assert len(sink.items) == 1
    app = Starlette(exception_handlers={ValueError: handler})
    again = instrument_starlette(app, client, capture_handled_errors=True)
    app.exception_handlers[ValueError] = handler
    again.uninstall()
    assert app.exception_handlers[ValueError] is handler


@pytest.mark.asyncio
async def test_concurrent_streams_preserve_context_and_messages() -> None:
    client, sink = configured_client(threshold=0)
    barrier = asyncio.Barrier(2)
    seen: list[tuple[str, str | None]] = []

    async def endpoint(request: Request) -> StreamingResponse:
        async def chunks() -> AsyncIterator[bytes]:
            await barrier.wait()
            seen.append((request.path_params["name"], current_scope().trace_id))
            yield b"first"
            await asyncio.sleep(0)
            yield b"second"

        return StreamingResponse(chunks())

    app = Starlette(routes=[Route("/{name}", endpoint)])
    observed = instrument_starlette(app, client)

    async def run(name: str) -> list[dict[str, object]]:
        request_scope = scope("/" + name)
        request_scope["headers"] = [(b"x-request-id", name.encode())]
        sent: list[dict[str, object]] = []

        async def receive() -> dict[str, object]:
            # Older Starlette listens for disconnects while streaming.
            await asyncio.Event().wait()
            return {"type": "http.disconnect"}

        async def send(message: dict[str, object]) -> None:
            sent.append(message)

        await observed(request_scope, receive, send)
        return sent

    results = await asyncio.gather(run("alpha"), run("beta"))
    assert sorted(seen) == [("alpha", "alpha"), ("beta", "beta")]
    for messages in results:
        assert [
            message["body"] for message in messages if message["type"] == "http.response.body"
        ] == [b"first", b"second", b""]
    assert len([item for item in sink.items if item[0] == "metrics"]) == 2
    assert current_scope().trace_id is None


@pytest.mark.asyncio
async def test_cancellation_and_lifespan_are_not_error_events() -> None:
    client, sink = configured_client(threshold=0)
    events: list[str] = []

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        events.append("start")
        yield
        events.append("stop")

    async def endpoint(request: Request) -> PlainTextResponse:
        raise asyncio.CancelledError()

    app = Starlette(routes=[Route("/", endpoint)], lifespan=lifespan)
    observed = instrument_starlette(app, client)
    queue = asyncio.Queue[dict[str, object]]()
    await queue.put({"type": "lifespan.startup"})
    await queue.put({"type": "lifespan.shutdown"})
    sent: list[dict[str, object]] = []

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    await observed({"type": "lifespan"}, queue.get, send)
    with pytest.raises(asyncio.CancelledError):
        await observed(scope("/"), queue.get, send)
    assert events == ["start", "stop"]
    assert not sink.items
    assert current_scope().trace_id is None


@pytest.mark.asyncio
async def test_fastapi_validation_inputs_and_http_details_are_not_telemetry() -> None:
    client, sink = configured_client(threshold=0)
    app = FastAPI()

    @app.get("/items/{item_id}")
    async def item(item_id: int) -> dict[str, int]:
        return {"id": item_id}

    @app.get("/private")
    async def private() -> None:
        raise HTTPException(403, "private-body-sentinel")

    observed = instrument_fastapi(app, client, capture_handled_errors=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        assert (await http.get("/items/not-an-integer")).status_code == 422
        response = await http.get("/private")
        assert response.json() == {"detail": "private-body-sentinel"}
    errors = [payload for channel, payload in sink.items if channel == "events"]
    assert len(errors) == 1
    assert "private-body-sentinel" not in json.dumps(sink.items)
    assert "HTTP 403" in json.dumps(errors)


@pytest.mark.asyncio
async def test_nested_core_wrapper_does_not_duplicate_capture() -> None:
    from closeyourit import ASGIMiddleware

    client, sink = configured_client(threshold=0)
    app = Starlette(routes=[Route("/", lambda request: PlainTextResponse("ok"))])
    observed = ASGIMiddleware(instrument_starlette(app, client), client)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        assert (await http.get("/")).status_code == 200
    assert len(sink.items) == 1


@pytest.mark.asyncio
async def test_server_error_handler_and_handler_failure_keep_original_exception() -> None:
    client, sink = configured_client()
    original = RuntimeError("original")
    calls: list[Exception] = []

    async def endpoint(request: Request) -> None:
        raise original

    async def handler(request: Request, error: Exception) -> PlainTextResponse:
        calls.append(error)
        return PlainTextResponse("custom", status_code=503)

    app = Starlette(routes=[Route("/", endpoint)], exception_handlers={Exception: handler})
    observed = instrument_starlette(app, client, capture_handled_errors=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed), raise_app_exceptions=False),
        base_url="http://test",
    ) as http:
        assert (await http.get("/")).status_code == 503
    assert calls == [original]
    assert app.exception_handlers[Exception] is handler
    assert len(sink.items) == 1
    assert cast(Any, sink.items[0][1]["exception"])["values"][-1]["mechanism"]["handled"] is False


@pytest.mark.asyncio
async def test_instrumentation_failures_do_not_change_application_response() -> None:
    from closeyourit import Client

    client, _ = configured_client(threshold=0)

    class BrokenSink:
        def enqueue(self, channel: Any, payload: Any) -> bool:
            raise RuntimeError("telemetry unavailable")

    broken = Client(client.configuration, sink=BrokenSink())
    app = Starlette(routes=[Route("/", lambda request: PlainTextResponse("ok"))])
    observed = instrument_starlette(app, broken)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        assert (await http.get("/")).text == "ok"


def test_installation_guards_and_foreign_stack_preservation() -> None:
    from closeyourit import StarletteIntegration

    client, _ = configured_client()
    app = Starlette()
    observed = instrument_starlette(app, client)
    with pytest.raises(ValueError):
        StarletteIntegration(app, client)
    other, _ = configured_client()
    with pytest.raises(ValueError):
        instrument_starlette(app, other)
    with pytest.raises(TypeError):
        instrument_fastapi(cast(Any, app), client)
    with pytest.raises(TypeError):
        StarletteIntegration(cast(Any, object()), client)
    foreign = app.build_middleware_stack()
    app.middleware_stack = foreign
    observed.uninstall()
    assert app.middleware_stack is foreign
    with pytest.raises(RuntimeError):
        instrument_starlette(app, client)


@pytest.mark.asyncio
async def test_adapter_does_not_read_receive_for_a_bodyless_endpoint() -> None:
    client, sink = configured_client(threshold=0)
    app = Starlette(routes=[Route("/", lambda request: PlainTextResponse("ok"), methods=["POST"])])
    observed = instrument_starlette(app, client)
    request_scope = scope("/")
    request_scope["method"] = "POST"
    sent: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        raise AssertionError("The adapter must not consume a request body")

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    await observed(request_scope, receive, send)
    assert sent[-1]["body"] == b"ok"
    assert len(sink.items) == 1


@pytest.mark.asyncio
async def test_route_resolution_never_replays_parameter_conversion() -> None:
    from starlette.convertors import Convertor, register_url_convertor

    calls: list[str] = []

    class CountingConvertor(Convertor[str]):
        regex = "[a-z]+"

        def convert(self, value: str) -> str:
            calls.append(value)
            return value

    register_url_convertor("u27count", CountingConvertor())
    client, sink = configured_client(threshold=0)
    app = Starlette(
        routes=[
            Mount(
                "/teams/{team:u27count}",
                Starlette(
                    routes=[
                        Route("/users/{user:u27count}", lambda request: PlainTextResponse("ok"))
                    ]
                ),
            )
        ]
    )
    observed = instrument_starlette(app, client)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        assert (await http.get("/teams/alpha/users/beta")).status_code == 200
    assert calls == ["alpha", "beta"]
    assert sink.items[0][1]["route"] == "/teams/{team}/users/{user}"


def test_concurrent_installation_reuses_one_handler_owner() -> None:
    from concurrent.futures import ThreadPoolExecutor

    client, _ = configured_client()
    app = Starlette()
    with ThreadPoolExecutor(max_workers=4) as executor:
        integrations = list(
            executor.map(
                lambda _: instrument_starlette(app, client, capture_handled_errors=True), range(8)
            )
        )
    assert all(value is integrations[0] for value in integrations)
    integrations[0].uninstall()
    assert not app.exception_handlers


@pytest.mark.asyncio
@pytest.mark.parametrize("before_first", [False, True])
async def test_uninstall_never_adopts_a_foreign_stack_after_dispatch(before_first: bool) -> None:
    client, _ = configured_client()
    app = Starlette(routes=[Route("/", lambda request: PlainTextResponse("ok"))])
    observed = instrument_starlette(app, client, capture_handled_errors=True)
    with pytest.raises(ValueError, match="policy"):
        instrument_starlette(app, client, capture_handled_errors=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        if not before_first:
            assert (await http.get("/")).status_code == 200
        original = app.middleware_stack or app.build_middleware_stack()
        calls = []

        async def foreign(scope: Any, receive: Any, send: Any) -> None:
            calls.append("called")
            await original(scope, receive, send)

        app.middleware_stack = foreign
        assert (await http.get("/")).status_code == 200
        observed.uninstall()
        assert app.middleware_stack is foreign
        assert calls == ["called"]


@pytest.mark.asyncio
async def test_stream_failure_keeps_already_sent_bytes_and_reports_once() -> None:
    client, sink = configured_client()
    error = RuntimeError("stream failed")

    async def endpoint(request: Request) -> StreamingResponse:
        async def chunks() -> AsyncIterator[bytes]:
            yield b"already sent"
            raise error

        return StreamingResponse(chunks())

    app = Starlette(routes=[Route("/", endpoint)])
    observed = instrument_starlette(app, client)
    messages = []

    async def receive() -> dict[str, object]:
        await asyncio.Event().wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, object]) -> None:
        messages.append(message)

    with pytest.raises((RuntimeError, ExceptionGroup)):
        await observed(scope("/"), receive, send)
    assert messages[1]["body"] == b"already sent"
    assert messages[1]["more_body"] is True
    assert len(sink.items) == 1
    assert sink.items[0][0] == "events"


@pytest.mark.asyncio
async def test_unmatched_and_method_not_allowed_use_bounded_templates() -> None:
    client, sink = configured_client(threshold=0)
    app = Starlette(routes=[Route("/items/{id}", lambda request: PlainTextResponse("ok"))])
    observed = instrument_starlette(app, client)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        assert (await http.get("/missing/7")).status_code == 404
        assert (await http.post("/items/7")).status_code == 405
    assert [value["route"] for channel, value in sink.items] == ["/<unmatched>", "/items/{id}"]


@pytest.mark.asyncio
@pytest.mark.parametrize("reraised_request", [False, True])
async def test_fastapi_validation_failures_never_capture_request_or_response_input(
    reraised_request: bool,
) -> None:
    from fastapi.exceptions import RequestValidationError

    client, sink = configured_client()
    app = FastAPI()
    original_errors = []
    if reraised_request:

        @app.exception_handler(RequestValidationError)
        async def reraising_handler(request: Request, error: RequestValidationError) -> None:
            original_errors.append(error)
            raise error

        @app.get("/{value}")
        async def validated_input(value: int) -> dict[str, int]:
            return {"value": value}
    else:

        @app.get("/{value}", response_model=int)
        async def validated_output(value: str) -> str:
            return "private-response-sentinel"

    observed = instrument_fastapi(app, client, capture_handled_errors=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed)), base_url="http://test"
    ) as http:
        with pytest.raises(Exception) as caught:
            await http.get("/invalid")
    if reraised_request:
        assert caught.value is original_errors[0]
        assert not sink.items
    else:
        assert "private-response-sentinel" in str(caught.value)
        assert len(sink.items) == 1
        assert "private-response-sentinel" not in json.dumps(sink.items)
        assert "Response validation failed" in json.dumps(sink.items)


@pytest.mark.asyncio
@pytest.mark.parametrize("wrapped", [False, True])
async def test_custom_validation_handlers_do_not_reintroduce_inputs(wrapped: bool) -> None:
    from fastapi.exceptions import ResponseValidationError

    client, sink = configured_client()
    app = FastAPI()

    @app.get("/", response_model=int)
    async def invalid() -> str:
        return "private-response-sentinel"

    @app.exception_handler(ResponseValidationError)
    async def handle(request: Request, error: ResponseValidationError) -> PlainTextResponse:
        if wrapped:
            raise RuntimeError("validation wrapper") from error
        return PlainTextResponse("custom safe response", status_code=503)

    observed = instrument_fastapi(app, client, capture_handled_errors=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=cast(Any, observed), raise_app_exceptions=False),
        base_url="http://test",
    ) as http:
        response = await http.get("/")
    assert response.status_code == (500 if wrapped else 503)
    assert len(sink.items) == 1
    assert "private-response-sentinel" not in json.dumps(sink.items)
    assert "Response validation failed" in json.dumps(sink.items)
