"""Optional Flask integration with lazy framework imports."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, cast

from closeyourit.client import Client
from closeyourit.web import Monotonic, RequestLifecycle
from closeyourit.wsgi import WSGIApp, WSGIMiddleware

_EXTENSION_KEY = "closeyourit"
_LIFECYCLE_KEY = "closeyourit.lifecycle"


class FlaskIntegration:
    """Install route and exception hooks around the current Flask WSGI stack."""

    def __init__(
        self,
        app: Any | None = None,
        *,
        client: Client | None = None,
        monotonic: Monotonic = time.monotonic,
    ) -> None:
        self.client = client or Client()
        self.monotonic = monotonic
        self._exception_handler: Callable[..., None] | None = None
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Any) -> None:
        extensions = app.extensions
        if _EXTENSION_KEY in extensions:
            return

        from flask import got_request_exception, request

        def set_route() -> None:
            url_rule = getattr(request, "url_rule", None)
            route = getattr(url_rule, "rule", None)
            if not route:
                return
            request.environ["closeyourit.route"] = route
            lifecycle = request.environ.get(_LIFECYCLE_KEY)
            if isinstance(lifecycle, RequestLifecycle):
                lifecycle.set_route(str(route))

        def capture_exception(
            _sender: object,
            *,
            exception: BaseException,
            **_extra: object,
        ) -> None:
            lifecycle = request.environ.get(_LIFECYCLE_KEY)
            if isinstance(lifecycle, RequestLifecycle):
                lifecycle.set_exception(exception, handled=False)

        app.before_request(set_route)
        got_request_exception.connect(capture_exception, app, weak=False)
        self._exception_handler = capture_exception
        app.wsgi_app = WSGIMiddleware(
            cast(WSGIApp, app.wsgi_app), self.client, monotonic=self.monotonic
        )
        extensions[_EXTENSION_KEY] = self
