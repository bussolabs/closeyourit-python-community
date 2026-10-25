"""Optional, instance-local Starlette and FastAPI instrumentation."""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Any
from weakref import WeakValueDictionary

from closeyourit.asgi import LIFECYCLE_KEY, ASGIMiddleware, Receive, Send
from closeyourit.client import Client
from closeyourit.web import RequestLifecycle, route_from_asgi

if TYPE_CHECKING:
    from starlette.applications import Starlette

_LOCK = threading.RLock()
_INSTALLED: WeakValueDictionary[int, StarletteIntegration] = WeakValueDictionary()


class ResponseValidationFailure(Exception):
    """A safe diagnostic replacing framework validation details containing input."""


def _validation_kind(error: BaseException) -> str | None:
    current: BaseException | None = error
    seen: set[int] = set()
    result: str | None = None
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        for cls in type(current).__mro__:
            if cls.__module__ == "fastapi.exceptions":
                if cls.__name__ == "RequestValidationError":
                    return cls.__name__
                if cls.__name__ == "ResponseValidationError":
                    result = cls.__name__
        current = current.__cause__ or (
            current.__context__ if not current.__suppress_context__ else None
        )
    return result


def _safe_error(error: Exception) -> Exception | None:
    kind = _validation_kind(error)
    if kind == "RequestValidationError":
        return None
    if kind == "ResponseValidationError":
        return ResponseValidationFailure("Response validation failed").with_traceback(
            error.__traceback__
        )
    return error


class _FrameworkMiddleware(ASGIMiddleware):
    def _finish_error(self, lifecycle: RequestLifecycle, error: Exception) -> None:
        captured = _safe_error(error)
        if captured is None:
            lifecycle.finish()
        else:
            lifecycle.finish(captured)


class StarletteIntegration:
    """Wrap an application before serving; uninstall never closes its shared client."""

    def __init__(self, app: Starlette, client: Client, *, capture_handled_errors: bool = False):
        from starlette.applications import Starlette
        from starlette.exceptions import HTTPException
        from starlette.middleware.exceptions import ExceptionMiddleware

        if not isinstance(app, Starlette):
            raise TypeError("Expected a Starlette or FastAPI application")
        with _LOCK:
            if id(app) in _INSTALLED:
                raise ValueError("Use instrument_starlette to reuse an installed integration")
            if app.middleware_stack is not None:
                raise RuntimeError("Install the integration before the application starts")
            self.app = app
            self.client = client
            self.active = True
            self.capture_handled_errors = capture_handled_errors
            self._stack: object = None
            self._handlers: dict[Any, tuple[Any, Any]] = {}
            self._middleware = _FrameworkMiddleware(self._dispatch, client)
            if capture_handled_errors:
                handlers = dict(app.exception_handlers)
                handlers.setdefault(HTTPException, ExceptionMiddleware(app).http_exception)
                for key, handler in handlers.items():
                    # ServerErrorMiddleware always re-raises; the outer wrapper captures it.
                    if key in (500, Exception):
                        continue
                    wrapped = self._handler(handler)
                    self._handlers[key] = (app.exception_handlers.get(key), wrapped)
                    app.exception_handlers[key] = wrapped
            _INSTALLED[id(app)] = self

    def _handler(self, original: Callable[..., Any]) -> Callable[..., Any]:
        from starlette._utils import is_async_callable
        from starlette.concurrency import run_in_threadpool
        from starlette.exceptions import HTTPException

        async def handled(request: Any, error: Exception) -> Any:
            if is_async_callable(original):
                response = await original(request, error)
            else:
                response = await run_in_threadpool(original, request, error)
            if self.active and request.scope.get("type") == "http":
                lifecycle = request.scope.get(LIFECYCLE_KEY)
                if isinstance(lifecycle, RequestLifecycle):
                    with suppress(Exception):
                        captured = _safe_error(error)
                        if captured is None:
                            return response
                        if isinstance(error, HTTPException):
                            # Expected failures can embed request data in detail or headers.
                            captured = HTTPException(
                                error.status_code, detail=f"HTTP {error.status_code}"
                            ).with_traceback(error.__traceback__)
                        lifecycle.set_exception(captured, handled=True)
            return response

        return handled

    async def _dispatch(self, scope: dict[str, object], receive: Receive, send: Send) -> None:
        original_scope = dict(scope)
        with _LOCK:
            if self.app.middleware_stack is None:
                self._stack = self.app.build_middleware_stack()
                self.app.middleware_stack = self._stack
        try:
            await self.app(scope, receive, send)  # type: ignore[arg-type]
        finally:
            # Resolve only after dispatch. Never replay custom route matching callbacks.
            with suppress(Exception):
                route = _route(self.app.routes, original_scope, scope)
                lifecycle = scope.get(LIFECYCLE_KEY)
                if isinstance(lifecycle, RequestLifecycle):
                    lifecycle.set_route(route or "/<unmatched>")

    async def __call__(self, scope: dict[str, object], receive: Receive, send: Send) -> None:
        if self.active:
            await self._middleware(scope, receive, send)
        else:
            await self.app(scope, receive, send)  # type: ignore[arg-type]

    def uninstall(self) -> None:
        with _LOCK:
            if not self.active:
                return
            self.active = False
            for key, (original, wrapped) in self._handlers.items():
                if self.app.exception_handlers.get(key) is wrapped:
                    if original is None:
                        self.app.exception_handlers.pop(key, None)
                    else:
                        self.app.exception_handlers[key] = original
            # In-flight requests retain their stack. Never replace a foreign stack.
            if self.app.middleware_stack is self._stack:
                self.app.middleware_stack = None
            if _INSTALLED.get(id(self.app)) is self:
                del _INSTALLED[id(self.app)]


def _route(routes: Any, original: dict[str, object], dispatched: dict[str, object]) -> str | None:
    from starlette.routing import Mount, Route

    if not any(isinstance(route, Mount) for route in routes):
        resolved = getattr(dispatched.get("route"), "path_format", None) or route_from_asgi(
            dispatched
        )
        if resolved:
            return resolved
    partial: str | None = None
    path = str(original.get("path", ""))
    root = str(original.get("root_path", ""))
    if root and (path == root or path.startswith(root + "/")):
        path = path[len(root) :]
    for route in routes:
        if isinstance(route, Mount) and type(route).matches is Mount.matches:
            match = route.path_regex.match(path)
            if match:
                remaining = "/" + match.group("path")
                child = {**original, "root_path": root + path[: -len(remaining)]}
                nested = _route(route.routes, child, dispatched)
                return route.path_format.removesuffix("/{path}") + nested if nested else None
        elif isinstance(route, Route) and type(route).matches is Route.matches:
            if route.path_regex.match(path):
                if not route.methods or original.get("method") in route.methods:
                    return str(route.path_format)
                partial = str(route.path_format)
    return partial


def instrument_starlette(
    app: Starlette, client: Client, *, capture_handled_errors: bool | None = None
) -> StarletteIntegration:
    """Return the same installed wrapper for an application and client."""
    with _LOCK:
        existing = _INSTALLED.get(id(app))
        if existing is not None:
            if existing.client is not client:
                raise ValueError("The application already uses another CloseYourIt client")
            if (
                capture_handled_errors is not None
                and capture_handled_errors != existing.capture_handled_errors
            ):
                raise ValueError("The application already uses another handled-error policy")
            return existing
        return StarletteIntegration(
            app, client, capture_handled_errors=bool(capture_handled_errors)
        )
