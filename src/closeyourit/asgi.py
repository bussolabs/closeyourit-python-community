"""Dependency-free ASGI 3 middleware."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from closeyourit.client import Client
from closeyourit.scope import with_scope
from closeyourit.web import Monotonic, RequestLifecycle, asgi_request, route_from_asgi

Message = dict[str, object]
ASGIApp = Callable[..., Awaitable[None]]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
LIFECYCLE_KEY = "closeyourit.http_lifecycle"


class ASGIMiddleware:
    """Capture one HTTP connection scope while forwarding receive/send unchanged."""

    def __init__(
        self,
        app: ASGIApp,
        client: Client,
        *,
        monotonic: Monotonic = time.monotonic,
    ) -> None:
        self.app = app
        self.client = client
        self.monotonic = monotonic

    async def __call__(
        self,
        scope: dict[str, object],
        receive: Receive,
        send: Send,
    ) -> None:
        if scope.get("type") != "http" or isinstance(scope.get(LIFECYCLE_KEY), RequestLifecycle):
            await self.app(scope, receive, send)
            return

        request, trace_id = asgi_request(scope, self.client.configuration.request_header_allowlist)
        lifecycle = RequestLifecycle(
            self.client,
            request,
            trace_id,
            route_resolver=lambda: route_from_asgi(scope),
            monotonic=self.monotonic,
        )
        send_error: Exception | None = None

        async def observed_send(message: Message) -> None:
            nonlocal send_error
            if message.get("type") == "http.response.start":
                lifecycle.set_status(message.get("status"))
            try:
                await send(message)
            except Exception as error:
                send_error = error
                raise

        previous = scope.get(LIFECYCLE_KEY)
        scope[LIFECYCLE_KEY] = lifecycle
        try:
            with with_scope():
                lifecycle.begin()
                try:
                    await self.app(scope, receive, observed_send)
                except Exception as error:
                    if error is send_error:
                        lifecycle.finish()
                    else:
                        self._finish_error(lifecycle, error)
                    raise
                else:
                    lifecycle.finish()
        finally:
            if previous is None:
                scope.pop(LIFECYCLE_KEY, None)
            else:
                scope[LIFECYCLE_KEY] = previous

    def _finish_error(self, lifecycle: RequestLifecycle, error: Exception) -> None:
        lifecycle.finish(error)
