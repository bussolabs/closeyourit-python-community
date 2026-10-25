"""Per-session Requests instrumentation; no process-wide monkeypatching."""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import suppress
from threading import Lock
from types import MethodType
from typing import Any

from closeyourit.client import Client
from closeyourit.integrations._http import HttpRecorder

_MISSING = object()


class RequestsInstrumentation:
    def __init__(
        self,
        session: Any,
        client: Client,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        span_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._session = session
        self._recorder = HttpRecorder(client, monotonic=monotonic, span_id_factory=span_id_factory)
        self._starts: dict[int, float] = {}
        self._starts_lock = Lock()
        self._state_lock = Lock()
        self._active = True
        self._previous_send = session.__dict__.get("send", _MISSING)
        self._original_send: Callable[..., Any] = session.send

        hooks = session.hooks.setdefault("response", [])
        hooks.append(self._response_hook)

        original_send = self._original_send

        def send(bound_session: Any, request: Any, **kwargs: Any) -> Any:
            del bound_session
            self._recorder.inject_traceparent(str(request.url), request.headers)
            started_at = self._recorder.start()
            with self._starts_lock:
                self._starts[id(request)] = started_at
            try:
                return original_send(request, **kwargs)
            except Exception as error:
                with self._starts_lock:
                    started_at = self._starts.pop(id(request), started_at)
                self._recorder.complete(
                    method=str(request.method),
                    raw_url=str(request.url),
                    started_at=started_at,
                    error_type=type(error).__name__,
                )
                raise

        session.send = MethodType(send, session)

    def _response_hook(self, response: Any, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        request = response.request
        with self._starts_lock:
            started_at = self._starts.pop(id(request), None)
        if started_at is None:
            return
        self._recorder.complete(
            method=str(request.method),
            raw_url=str(request.url),
            started_at=started_at,
            status_code=int(response.status_code),
        )

    def uninstrument(self) -> None:
        with self._state_lock:
            if not self._active:
                return
            self._active = False
            hooks = self._session.hooks.get("response", [])
            with suppress(ValueError):
                hooks.remove(self._response_hook)
            if self._previous_send is _MISSING:
                self._session.__dict__.pop("send", None)
            else:
                self._session.send = self._previous_send
            with self._starts_lock:
                self._starts.clear()

    def __enter__(self) -> RequestsInstrumentation:
        return self

    def __exit__(self, *args: object) -> None:
        del args
        self.uninstrument()


def instrument_requests(
    session: Any,
    client: Client,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    span_id_factory: Callable[[], str] | None = None,
) -> RequestsInstrumentation:
    """Instrument one ``requests.Session`` and return a reversible handle."""
    return RequestsInstrumentation(
        session,
        client,
        monotonic=monotonic,
        span_id_factory=span_id_factory,
    )
