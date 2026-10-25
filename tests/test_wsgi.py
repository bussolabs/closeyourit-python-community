from __future__ import annotations

import io
import sys
import threading
from collections.abc import Callable, Iterable, Iterator
from types import TracebackType
from typing import cast
from uuid import UUID

import pytest

from closeyourit import Client, Configuration, WSGIMiddleware, current_scope
from closeyourit.client import Channel


class RecordingSink:
    def __init__(self) -> None:
        self.items: list[tuple[Channel, dict[str, object]]] = []

    def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
        self.items.append((channel, payload))
        return True


def configured_client(
    *, threshold: float = 1000.0, usage_enabled: bool = False
) -> tuple[Client, RecordingSink]:
    sink = RecordingSink()
    configuration = Configuration(
        endpoint_url="https://ingest.example.test",
        token="cyi_server-token",
        project_id="12345678-1234-1234-1234-123456789abc",
        environment="production",
        slow_request_threshold_ms=threshold,
        usage_enabled=usage_enabled,
    )
    return Client(configuration, sink=sink), sink


def environ(path: str = "/orders/42", *, body: io.BytesIO | None = None) -> dict[str, object]:
    return {
        "REQUEST_METHOD": "POST",
        "SCRIPT_NAME": "",
        "PATH_INFO": path,
        "QUERY_STRING": "token=secret&page=2",
        "SERVER_NAME": "app.example.test",
        "SERVER_PORT": "443",
        "SERVER_PROTOCOL": "HTTP/1.1",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "https",
        "wsgi.input": body or io.BytesIO(b"private request body"),
        "wsgi.errors": io.StringIO(),
        "wsgi.multithread": True,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
        "HTTP_ACCEPT": "application/json",
        "HTTP_USER_AGENT": "pytest",
        "HTTP_X_REQUEST_ID": "request-a, proxy-id",
        "HTTP_AUTHORIZATION": "Bearer private",
        "HTTP_COOKIE": "session=private",
    }


def consume(
    middleware: WSGIMiddleware, request_environ: dict[str, object]
) -> tuple[list[bytes], list[tuple[str, list[tuple[str, str]]]]]:
    starts: list[tuple[str, list[tuple[str, str]]]] = []

    def start_response(
        status: str,
        headers: list[tuple[str, str]],
        exc_info: tuple[type[BaseException], BaseException, object] | None = None,
    ) -> Callable[[bytes], None]:
        del exc_info
        starts.append((status, headers))
        return lambda _data: None

    response = middleware(request_environ, start_response)
    try:
        return list(response), starts
    finally:
        close = getattr(response, "close", None)
        if close is not None:
            close()


def test_wsgi_adds_private_request_context_and_preserves_response() -> None:
    client, _sink = configured_client()
    seen: list[dict[str, object]] = []

    def app(
        _environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        seen.append(current_scope().snapshot(client.configuration).to_event_data())
        start_response("201 Created", [("Content-Type", "text/plain")])
        return [b"one", b"two"]

    body, starts = consume(WSGIMiddleware(app, client), environ())

    request = seen[0]["request"]
    assert request == {
        "method": "POST",
        "url": "https://app.example.test/orders/42",
        "path": "/orders/42",
        "headers": {"Accept": "application/json", "User-Agent": "pytest"},
    }
    assert seen[0]["trace_id"] == "request-a"
    assert body == [b"one", b"two"]
    assert starts == [("201 Created", [("Content-Type", "text/plain")])]
    assert current_scope().snapshot().to_event_data() == {}


def test_wsgi_never_reads_the_request_body_and_uses_late_templated_route() -> None:
    client, sink = configured_client(threshold=10.0)

    class UnreadableBody(io.BytesIO):
        def read(self, *args: object, **kwargs: object) -> bytes:
            raise AssertionError(f"body read: {args!r} {kwargs!r}")

    def app(
        app_environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        app_environ["closeyourit.route"] = "/orders/<int:id>"
        start_response("200 OK", [])
        return [b"ok"]

    times = iter([1.0, 1.02])
    body, _starts = consume(
        WSGIMiddleware(app, client, monotonic=lambda: next(times)),
        environ(body=UnreadableBody(b"secret")),
    )

    assert body == [b"ok"]
    assert len(sink.items) == 1
    channel, metric = sink.items[0]
    assert channel == "metrics"
    assert metric["subtype"] == "slow_request"
    assert metric["route"] == "/orders/<int:id>"
    assert metric["http_path"] == "/orders/42"
    assert metric["duration_ms"] == 20.0


def test_wsgi_captures_and_reraises_unhandled_errors_with_status_and_duration() -> None:
    client, sink = configured_client(threshold=1000.0)

    def app(
        _environ: dict[str, object],
        _start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        raise RuntimeError("application exploded")

    times = iter([1.0, 1.005])
    middleware = WSGIMiddleware(app, client, monotonic=lambda: next(times))

    with pytest.raises(RuntimeError, match="application exploded"):
        consume(middleware, environ())

    event = sink.items[0][1]
    assert event["request"]["duration_ms"] == 5.0  # type: ignore[index]
    exception = event["exception"]["values"][-1]  # type: ignore[index]
    assert exception["mechanism"] == {"type": "python", "handled": False}
    assert current_scope().snapshot().to_event_data() == {}


def test_wsgi_stream_is_lazy_closes_underlying_iterable_and_captures_iteration_errors() -> None:
    client, sink = configured_client()
    actions: list[str] = []

    class Stream(Iterator[bytes]):
        def __iter__(self) -> Stream:
            return self

        def __next__(self) -> bytes:
            actions.append(str(current_scope().snapshot().request["path"]))  # type: ignore[index]
            if len(actions) == 1:
                return b"first"
            raise ValueError("stream failed")

        def close(self) -> None:
            actions.append("closed")

    def app(
        _environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        start_response("200 OK", [])
        return Stream()

    starts: list[str] = []

    def start_response(status: str, _headers: list[tuple[str, str]]) -> Callable[[bytes], None]:
        starts.append(status)
        return lambda _data: None

    response = WSGIMiddleware(app, client)(environ(), start_response)
    assert actions == []
    assert next(iter(response)) == b"first"
    with pytest.raises(ValueError, match="stream failed"):
        next(iter(response))
    response.close()
    response.close()

    assert starts == ["200 OK"]
    assert actions == ["/orders/42", "/orders/42", "closed"]
    assert len([item for item in sink.items if item[0] == "events"]) == 1


def test_wsgi_threaded_requests_do_not_share_scope() -> None:
    client, _sink = configured_client()
    barrier = threading.Barrier(2)
    seen: dict[str, str] = {}

    def app(
        app_environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        barrier.wait()
        path = str(app_environ["PATH_INFO"])
        seen[path] = str(current_scope().snapshot().request["path"])  # type: ignore[index]
        start_response("200 OK", [])
        return [b"ok"]

    middleware = WSGIMiddleware(app, client)
    threads = [
        threading.Thread(target=consume, args=(middleware, environ(path)))
        for path in ("/one", "/two")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert seen == {"/one": "/one", "/two": "/two"}


def test_wsgi_forbidden_headers_stay_private_even_in_custom_allowlist() -> None:
    configuration = Configuration(
        endpoint_url="https://ingest.example.test",
        token="cyi_server-token",
        project_id="12345678-1234-1234-1234-123456789abc",
        request_header_allowlist=("Authorization", "Cookie", "X-Custom", "Accept"),
    )
    client = Client(configuration, sink=RecordingSink())
    seen: list[dict[str, object]] = []

    def app(
        _environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        seen.append(current_scope().snapshot(configuration).to_event_data())
        start_response("200 OK", [])
        return [b"ok"]

    request_environ = environ()
    request_environ["HTTP_X_CUSTOM"] = "public-value"
    consume(WSGIMiddleware(app, client), request_environ)

    assert seen[0]["request"]["headers"] == {  # type: ignore[index]
        "X-Custom": "public-value",
        "Accept": "application/json",
    }


def test_wsgi_telemetry_failures_never_replace_application_results() -> None:
    class FailingSink:
        def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
            del channel, payload
            raise RuntimeError("sink unavailable")

    configuration = Configuration(
        endpoint_url="https://ingest.example.test",
        token="cyi_server-token",
        project_id="12345678-1234-1234-1234-123456789abc",
        slow_request_threshold_ms=0,
    )
    client = Client(configuration, sink=FailingSink())

    def ok(
        _environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        start_response("200 OK", [])
        return [b"safe"]

    assert consume(WSGIMiddleware(ok, client), environ())[0] == [b"safe"]

    def explode(
        _environ: dict[str, object],
        _start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        raise ValueError("original failure")

    with pytest.raises(ValueError, match="original failure"):
        consume(WSGIMiddleware(explode, client), environ())


def test_wsgi_exc_info_is_captured_once_as_handled_and_forwarded_unchanged() -> None:
    client, sink = configured_client()
    forwarded: list[tuple[type[BaseException], BaseException, TracebackType]] = []

    def app(
        _environ: dict[str, object],
        start_response: Callable[
            [
                str,
                list[tuple[str, str]],
                tuple[type[BaseException], BaseException, TracebackType],
            ],
            Callable[[bytes], None],
        ],
    ) -> Iterable[bytes]:
        try:
            try:
                raise ValueError("root cause")
            except ValueError as cause:
                raise RuntimeError("handled response") from cause
        except RuntimeError:
            exc_info = sys.exc_info()
        assert exc_info[0] is not None and exc_info[1] is not None and exc_info[2] is not None
        typed_info = (exc_info[0], exc_info[1], exc_info[2])
        start_response("500 Internal Server Error", [], typed_info)
        start_response("500 Internal Server Error", [], typed_info)
        return [b"safe error response"]

    def start_response(
        _status: str,
        _headers: list[tuple[str, str]],
        exc_info: tuple[type[BaseException], BaseException, TracebackType],
    ) -> Callable[[bytes], None]:
        forwarded.append(exc_info)
        return lambda _data: None

    response = WSGIMiddleware(app, client)(environ(), start_response)
    assert list(response) == [b"safe error response"]
    response.close()

    assert len(forwarded) == 2
    assert forwarded[0] is forwarded[1]
    events = [payload for channel, payload in sink.items if channel == "events"]
    assert len(events) == 1
    values = events[0]["exception"]["values"]  # type: ignore[index]
    assert [value["type"] for value in values] == ["ValueError", "RuntimeError"]
    assert values[-1]["mechanism"]["handled"] is True


def test_wsgi_exc_info_preserves_server_reraise_semantics() -> None:
    client, sink = configured_client()
    original_errors: list[RuntimeError] = []

    def app(
        _environ: dict[str, object],
        start_response: Callable[..., Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        try:
            raise RuntimeError("headers already sent")
        except RuntimeError as error:
            original_errors.append(error)
            exc_info = sys.exc_info()
        start_response("500 Internal Server Error", [], exc_info)
        return [b"unreachable"]

    def rejecting_start_response(
        _status: str,
        _headers: list[tuple[str, str]],
        exc_info: tuple[type[BaseException], BaseException, TracebackType],
    ) -> Callable[[bytes], None]:
        raise exc_info[1].with_traceback(exc_info[2])

    with pytest.raises(RuntimeError, match="headers already sent") as raised:
        WSGIMiddleware(app, client)(environ(), rejecting_start_response)

    assert raised.value is original_errors[0]
    events = [payload for channel, payload in sink.items if channel == "events"]
    assert len(events) == 1
    assert events[0]["exception"]["values"][-1]["mechanism"]["handled"] is True  # type: ignore[index]


@pytest.mark.parametrize(
    "unsafe_request_id",
    [
        "private@example.test",
        "contains spaces",
        "../path",
        "ü-unicode",
        "a" * 129,
    ],
)
def test_wsgi_rejects_unsafe_or_unbounded_request_ids(unsafe_request_id: str) -> None:
    client, _sink = configured_client()
    seen: list[str] = []

    def app(
        _environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        seen.append(str(current_scope().trace_id))
        start_response("200 OK", [])
        return [b"ok"]

    request_environ = environ()
    request_environ["HTTP_X_REQUEST_ID"] = unsafe_request_id
    consume(WSGIMiddleware(app, client), request_environ)

    assert seen[0] != unsafe_request_id
    assert str(UUID(seen[0])) == seen[0]


def test_wsgi_accepts_a_safe_request_id_at_the_maximum_length() -> None:
    client, _sink = configured_client()
    safe_request_id = "a" * 128
    seen: list[str] = []

    def app(
        _environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        seen.append(str(current_scope().trace_id))
        start_response("200 OK", [])
        return [b"ok"]

    request_environ = environ()
    request_environ["HTTP_X_REQUEST_ID"] = safe_request_id
    consume(WSGIMiddleware(app, client), request_environ)

    assert seen == [safe_request_id]


def test_wsgi_records_the_resolved_route_as_a_usage_symbol() -> None:
    client, sink = configured_client(usage_enabled=True)

    def app(
        app_environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        app_environ["closeyourit.route"] = "/orders/<int:id>"
        start_response("200 OK", [])
        return [b"ok"]

    consume(WSGIMiddleware(app, client), environ())
    client.flush()

    usage = [payload for channel, payload in sink.items if channel == "usages"]
    symbols = cast(list[dict[str, object]], usage[0]["symbols"])
    assert [(item["kind"], item["symbol"]) for item in symbols] == [("route", "/orders/:id")]


def test_wsgi_never_records_a_concrete_path_as_a_usage_symbol() -> None:
    client, sink = configured_client(usage_enabled=True)

    def app(
        _environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        start_response("200 OK", [])
        return [b"ok"]

    consume(WSGIMiddleware(app, client), environ())
    client.flush()

    assert [channel for channel, _payload in sink.items] == []


def test_wsgi_records_no_usage_while_the_channel_stays_opt_out() -> None:
    client, sink = configured_client()

    def app(
        app_environ: dict[str, object],
        start_response: Callable[[str, list[tuple[str, str]]], Callable[[bytes], None]],
    ) -> Iterable[bytes]:
        app_environ["closeyourit.route"] = "/orders/<int:id>"
        start_response("200 OK", [])
        return [b"ok"]

    consume(WSGIMiddleware(app, client), environ())
    client.flush()

    assert [channel for channel, _payload in sink.items] == []
