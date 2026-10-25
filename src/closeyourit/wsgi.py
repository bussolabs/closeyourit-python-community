"""Dependency-free PEP 3333 middleware."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from contextvars import Context, copy_context
from types import TracebackType

from closeyourit.client import Client
from closeyourit.scope import with_scope
from closeyourit.web import Monotonic, RequestLifecycle, wsgi_request

Write = Callable[[bytes], object]
ExcInfo = tuple[type[BaseException], BaseException, TracebackType] | None
StartResponse = Callable[[str, list[tuple[str, str]], ExcInfo], Write]
WSGIApp = Callable[..., Iterable[bytes]]

_LIFECYCLE_KEY = "closeyourit.lifecycle"


class _WSGIResponse(Iterator[bytes]):
    def __init__(
        self,
        iterable: Iterable[bytes],
        context: Context,
        lifecycle: RequestLifecycle,
        environ: dict[str, object],
    ) -> None:
        self._iterable = iterable
        self._iterator = iter(iterable)
        self._context = context
        self._lifecycle = lifecycle
        self._environ = environ
        self._closed = False

    def __iter__(self) -> _WSGIResponse:
        return self

    def __next__(self) -> bytes:
        def advance() -> bytes:
            try:
                return next(self._iterator)
            except StopIteration:
                self._finish()
                raise
            except Exception as error:
                self._finish(error)
                raise

        return self._context.run(advance)

    def close(self) -> None:
        if self._closed:
            return

        def close_in_context() -> None:
            try:
                close = getattr(self._iterable, "close", None)
                if close is not None:
                    close()
            except Exception as error:
                self._finish(error)
                raise
            finally:
                self._finish()

        try:
            self._context.run(close_in_context)
        finally:
            self._closed = True

    def _finish(self, error: BaseException | None = None) -> None:
        self._lifecycle.finish(error)
        self._environ.pop(_LIFECYCLE_KEY, None)


class WSGIMiddleware:
    """Capture WSGI request context without buffering response or reading request body."""

    def __init__(
        self,
        app: WSGIApp,
        client: Client,
        *,
        monotonic: Monotonic = time.monotonic,
    ) -> None:
        self.app = app
        self.client = client
        self.monotonic = monotonic

    def __call__(
        self,
        environ: dict[str, object],
        start_response: Callable[..., Write],
    ) -> _WSGIResponse:
        request, trace_id = wsgi_request(
            environ, self.client.configuration.request_header_allowlist
        )
        lifecycle = RequestLifecycle(
            self.client,
            request,
            trace_id,
            route_resolver=lambda: str(environ.get("closeyourit.route") or "") or None,
            monotonic=self.monotonic,
        )

        def observed_start_response(
            status: str,
            headers: list[tuple[str, str]],
            exc_info: ExcInfo = None,
        ) -> Write:
            lifecycle.set_status(status)
            if exc_info is not None and not lifecycle.finished:
                lifecycle.set_exception(exc_info[1], handled=True)
            if lifecycle.pending_error is not None:
                lifecycle.finish()
            if exc_info is None:
                return start_response(status, headers)
            return start_response(status, headers, exc_info)

        with with_scope():
            lifecycle.begin()
            environ[_LIFECYCLE_KEY] = lifecycle
            try:
                iterable = self.app(environ, observed_start_response)
            except Exception as error:
                try:
                    lifecycle.finish(error)
                finally:
                    environ.pop(_LIFECYCLE_KEY, None)
                raise
            context = copy_context()
        return _WSGIResponse(iterable, context, lifecycle, environ)
