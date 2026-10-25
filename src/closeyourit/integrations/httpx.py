"""HTTPX instrumentation using official event hooks plus exception-safe send wrappers."""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable
from contextlib import suppress
from threading import Lock
from types import MethodType
from typing import Any

from closeyourit.client import Client
from closeyourit.integrations._http import HttpRecorder

_MISSING = object()


class HttpxInstrumentation:
    def __init__(
        self,
        http_client: Any,
        client: Client,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        span_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._client = http_client
        self._recorder = HttpRecorder(client, monotonic=monotonic, span_id_factory=span_id_factory)
        self._state_lock = Lock()
        self._active = True
        self._started_key = f"closeyourit.started.{id(self)}"
        self._previous_send = http_client.__dict__.get("send", _MISSING)
        self._original_send: Callable[..., Any] = http_client.send
        self._async = inspect.iscoroutinefunction(self._original_send)
        self._request_hook: Callable[..., Any]
        self._response_hook: Callable[..., Any]

        if self._async:
            self._request_hook = self._async_request
            self._response_hook = self._async_response
        else:
            self._request_hook = self._sync_request
            self._response_hook = self._sync_response
        http_client.event_hooks.setdefault("request", []).append(self._request_hook)
        http_client.event_hooks.setdefault("response", []).append(self._response_hook)
        self._install_send_wrapper()

    def _start_request(self, request: Any) -> None:
        self._recorder.inject_traceparent(str(request.url), request.headers)
        if self._started_key not in request.extensions:
            request.extensions[self._started_key] = self._recorder.start()

    def _finish_response(self, response: Any) -> None:
        request = response.request
        started_at = request.extensions.pop(self._started_key, None)
        if started_at is None:
            return
        self._recorder.complete(
            method=str(request.method),
            raw_url=str(request.url),
            started_at=float(started_at),
            status_code=int(response.status_code),
        )

    def _finish_error(self, request: Any, error: Exception) -> None:
        started_at = request.extensions.pop(self._started_key, None)
        if started_at is None:
            return
        self._recorder.complete(
            method=str(request.method),
            raw_url=str(request.url),
            started_at=float(started_at),
            error_type=type(error).__name__,
        )

    def _sync_request(self, request: Any) -> None:
        self._start_request(request)

    def _sync_response(self, response: Any) -> None:
        self._finish_response(response)

    async def _async_request(self, request: Any) -> None:
        self._start_request(request)

    async def _async_response(self, response: Any) -> None:
        self._finish_response(response)

    def _install_send_wrapper(self) -> None:
        original_send = self._original_send
        send_wrapper: Callable[..., Any]
        if self._async:

            async def async_send(bound_client: Any, request: Any, *args: Any, **kwargs: Any) -> Any:
                del bound_client
                self._start_request(request)
                try:
                    return await original_send(request, *args, **kwargs)
                except Exception as error:
                    self._finish_error(request, error)
                    raise

            send_wrapper = async_send

        else:

            def sync_send(bound_client: Any, request: Any, *args: Any, **kwargs: Any) -> Any:
                del bound_client
                self._start_request(request)
                try:
                    return original_send(request, *args, **kwargs)
                except Exception as error:
                    self._finish_error(request, error)
                    raise

            send_wrapper = sync_send

        self._client.send = MethodType(send_wrapper, self._client)

    def uninstrument(self) -> None:
        with self._state_lock:
            if not self._active:
                return
            self._active = False
            for name, hook in (
                ("request", self._request_hook),
                ("response", self._response_hook),
            ):
                hooks = self._client.event_hooks.get(name, [])
                with suppress(ValueError):
                    hooks.remove(hook)
            if self._previous_send is _MISSING:
                self._client.__dict__.pop("send", None)
            else:
                self._client.send = self._previous_send

    def __enter__(self) -> HttpxInstrumentation:
        return self

    def __exit__(self, *args: object) -> None:
        del args
        self.uninstrument()


def instrument_httpx(
    http_client: Any,
    client: Client,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    span_id_factory: Callable[[], str] | None = None,
) -> HttpxInstrumentation:
    """Instrument one ``httpx.Client`` or ``httpx.AsyncClient`` instance."""
    return HttpxInstrumentation(
        http_client,
        client,
        monotonic=monotonic,
        span_id_factory=span_id_factory,
    )
