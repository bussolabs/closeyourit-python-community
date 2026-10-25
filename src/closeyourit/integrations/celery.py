"""Fail-safe Celery signal integration without task arguments or results."""

from __future__ import annotations

import math
import re
import threading
import time
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from contextlib import suppress
from typing import Protocol, cast

from closeyourit.client import Client
from closeyourit.integrations._jobs import bounded_text, timing_context

Receiver = Callable[..., object]

_PUBLISHED_AT_HEADER = "closeyourit-published-at"
_DEFAULT_CORRELATION_HEADERS = (
    "traceparent",
    "x-correlation-id",
    "x-request-id",
)
_MAX_CORRELATION_VALUE_BYTES = 128
_UNKNOWN_TASK = "unknown"
_CORRELATION_IDENTIFIER = re.compile(r"[A-Za-z0-9._:/-]{1,128}\Z")
_TRACEPARENT = re.compile(r"00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})\Z")


class _Signal(Protocol):
    def connect(self, receiver: Receiver, **kwargs: object) -> object: ...

    def disconnect(self, receiver: Receiver, **kwargs: object) -> object: ...


class _Signals(Protocol):
    before_task_publish: _Signal
    task_prerun: _Signal
    task_postrun: _Signal
    task_failure: _Signal
    task_retry: _Signal
    task_revoked: _Signal


def _load_celery_signals() -> _Signals | None:
    try:
        from celery import signals  # type: ignore[import-untyped]
    except ImportError:
        return None
    return cast(_Signals, signals)


def _clean_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _attribute(value: object, name: str) -> object | None:
    try:
        return getattr(value, name, None)
    except BaseException:
        return None


class CeleryIntegration:
    """Observe Celery tasks through official signals while preserving task semantics."""

    def __init__(
        self,
        client: Client,
        *,
        task_threshold_ms: float = 500.0,
        queue_threshold_ms: float = 500.0,
        lifecycle_logs: bool = False,
        correlation_headers: Sequence[str] = _DEFAULT_CORRELATION_HEADERS,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        signals: object | None = None,
    ) -> None:
        self.client = client
        self.task_threshold_ms = max(0.0, float(task_threshold_ms))
        self.queue_threshold_ms = max(0.0, float(queue_threshold_ms))
        self.lifecycle_logs = lifecycle_logs
        self.correlation_headers = frozenset(
            header.strip().casefold()
            for header in correlation_headers
            if header.strip() and header.strip().casefold() != _PUBLISHED_AT_HEADER
        )
        self.monotonic = monotonic
        self.wall_clock = wall_clock
        self._signals = cast(_Signals, signals) if signals is not None else None
        self._installed = False
        self._closed = False
        self._starts: dict[tuple[str, int | None], tuple[float, dict[str, object]]] = {}
        self._lock = threading.Lock()
        self._connections: tuple[tuple[str, Receiver], ...] = (
            ("before_task_publish", self._on_before_task_publish),
            ("task_prerun", self._on_task_prerun),
            ("task_postrun", self._on_task_postrun),
            ("task_failure", self._on_task_failure),
            ("task_retry", self._on_task_retry),
            ("task_revoked", self._on_task_revoked),
        )

    @property
    def available(self) -> bool:
        """Whether Celery signals were resolved successfully."""
        return self._signals is not None

    @property
    def installed(self) -> bool:
        return self._installed

    def install(self) -> CeleryIntegration:
        """Connect each receiver exactly once; missing Celery is a safe no-op."""
        if self._closed or self._installed:
            return self
        if self._signals is None:
            self._signals = _load_celery_signals()
        if self._signals is None:
            return self

        connected: list[tuple[_Signal, Receiver]] = []
        try:
            for name, receiver in self._connections:
                signal = cast(_Signal, getattr(self._signals, name))
                signal.connect(receiver, weak=False)
                connected.append((signal, receiver))
        except BaseException:
            for signal, receiver in reversed(connected):
                with suppress(BaseException):
                    signal.disconnect(receiver)
            return self

        self._installed = True
        return self

    def uninstall(self) -> None:
        """Disconnect owned receivers and discard unfinished timing state."""
        signals = self._signals
        if signals is not None and self._installed:
            for name, receiver in reversed(self._connections):
                with suppress(BaseException):
                    cast(_Signal, getattr(signals, name)).disconnect(receiver)
        self._installed = False
        with self._lock:
            self._starts.clear()

    def close(self) -> None:
        """Permanently uninstall this integration without taking ownership of the client."""
        if self._closed:
            return
        self._closed = True
        self.uninstall()

    def _on_before_task_publish(
        self,
        sender: object = None,
        *,
        headers: object = None,
        **kwargs: object,
    ) -> None:
        del sender, kwargs
        try:
            if isinstance(headers, MutableMapping):
                headers[_PUBLISHED_AT_HEADER] = self.wall_clock()
        except BaseException:
            pass

    def _on_task_prerun(
        self,
        sender: object = None,
        *,
        task_id: object = None,
        task: object = None,
        **kwargs: object,
    ) -> None:
        del kwargs
        try:
            active_task = task if task is not None else sender
            request = _attribute(active_task, "request")
            identifier = self._task_id(task_id, request)
            context = self._job_context(active_task, request, identifier, started=self.wall_clock())
            if identifier is not None:
                with self._lock:
                    self._starts[self._attempt_key(identifier, request)] = (
                        self.monotonic(),
                        context,
                    )

            task_name = self._task_name(active_task, request, sender)
            if task_name != _UNKNOWN_TASK:
                # A starting task is the proof that the job really runs: it is recorded here, before
                # any threshold, because a fast task is still a used task.
                self.client.record_usage("job", task_name)

            latency_ms = context["queue_wait_ms"]
            if not isinstance(latency_ms, int | float):
                return
            if latency_ms < self.queue_threshold_ms:
                return
            self.client.capture_duration(
                f"celery.queue.latency.{task_name}",
                latency_ms,
                contexts={
                    "celery": self._context(active_task, request, task_id=identifier),
                    "job": {**context, "measurement": "queue_wait"},
                },
            )
        except BaseException:
            pass

    def _on_task_postrun(
        self,
        sender: object = None,
        *,
        task_id: object = None,
        task: object = None,
        state: object = None,
        **kwargs: object,
    ) -> None:
        del kwargs
        try:
            active_task = task if task is not None else sender
            request = _attribute(active_task, "request")
            identifier = self._task_id(task_id, request)
            if identifier is None:
                return
            with self._lock:
                timing = self._starts.pop(self._attempt_key(identifier, request), None)
            if timing is None:
                return
            started_at, context = timing
            duration_ms = max(0.0, (self.monotonic() - started_at) * 1000.0)
            outcome, terminal = {
                "SUCCESS": ("success", True),
                "FAILURE": ("error", True),
                "RETRY": ("retry", False),
                "REVOKED": ("revoked", True),
            }.get(state if isinstance(state, str) else "", ("unknown", None))
            context = {
                **context,
                "duration_ms": duration_ms,
                "attempt_outcome": outcome,
                "terminal": terminal,
            }
            self._log_outcome(context)
            if duration_ms < self.task_threshold_ms:
                return
            task_name = self._task_name(active_task, request, sender)
            self.client.capture_duration(
                f"celery.task.duration.{task_name}",
                duration_ms,
                contexts={
                    "job": {**context, "measurement": "duration"},
                    "celery": self._context(
                        active_task,
                        request,
                        task_id=identifier,
                        state=state,
                    ),
                },
            )
        except BaseException:
            pass

    def _on_task_failure(
        self,
        sender: object = None,
        *,
        task_id: object = None,
        exception: object = None,
        **kwargs: object,
    ) -> None:
        del kwargs
        try:
            request = _attribute(sender, "request")
            error = (
                exception
                if isinstance(exception, BaseException)
                else RuntimeError("Celery task failed")
            )
            self.client.capture_exception(
                error,
                handled=False,
                contexts={
                    "celery": self._context(
                        sender,
                        request,
                        task_id=self._task_id(task_id, request),
                        state="FAILURE",
                    )
                },
            )
        except BaseException:
            pass

    def _on_task_retry(
        self,
        sender: object = None,
        *,
        request: object = None,
        reason: object = None,
        **kwargs: object,
    ) -> None:
        del kwargs
        try:
            error = (
                reason if isinstance(reason, BaseException) else RuntimeError("Celery task retry")
            )
            self.client.capture_exception(
                error,
                handled=True,
                contexts={
                    "celery": self._context(
                        sender,
                        request,
                        task_id=self._task_id(None, request),
                        state="RETRY",
                    )
                },
            )
        except BaseException:
            pass

    def _on_task_revoked(
        self,
        sender: object = None,
        *,
        request: object = None,
        terminated: object = None,
        expired: object = None,
        **kwargs: object,
    ) -> None:
        del kwargs
        try:
            identifier = self._task_id(None, request)
            timing = None
            if identifier is not None:
                with self._lock:
                    timing = self._starts.pop(self._attempt_key(identifier, request), None)
            job_context = (
                timing[1]
                if timing
                else self._job_context(sender, request, identifier, started=None)
            )
            self._log_outcome(
                {
                    **job_context,
                    "attempt_outcome": "revoked",
                    "terminal": True,
                    "duration_ms": max(0.0, (self.monotonic() - timing[0]) * 1000)
                    if timing
                    else None,
                }
            )
            context = self._context(
                sender,
                request,
                task_id=identifier,
                state="REVOKED",
            )
            if isinstance(terminated, bool):
                context["terminated"] = terminated
            if isinstance(expired, bool):
                context["expired"] = expired
            self.client.capture_exception(
                RuntimeError("Celery task revoked"),
                handled=True,
                contexts={"celery": context},
            )
        except BaseException:
            pass

    @staticmethod
    def _attempt_key(identifier: str, request: object) -> tuple[str, int | None]:
        retries = _attribute(request, "retries")
        return identifier, retries if isinstance(retries, int) and not isinstance(
            retries, bool
        ) and retries >= 0 else None

    def _job_context(
        self, task: object, request: object, identifier: str | None, *, started: object
    ) -> dict[str, object]:
        legacy = self._context(task, request, task_id=identifier)
        retries = self._attempt_key(identifier or "", request)[1]
        return {
            "schema_version": 1,
            "framework": "celery",
            "name": bounded_text(self._task_name(task, request, task), 200) or _UNKNOWN_TASK,
            "job_id": bounded_text(identifier, 256),
            "queue": bounded_text(legacy.get("queue"), 128),
            "retry_count": retries,
            "attempt": retries + 1 if retries is not None else None,
            "attempt_outcome": "unknown",
            "terminal": None,
            "duration_ms": None,
            **timing_context(
                enqueued=self._published_at(request),
                scheduled=_attribute(request, "eta"),
                started=started,
            ),
        }

    def _log_outcome(self, context: dict[str, object]) -> None:
        if self.lifecycle_logs:
            self.client.log(
                "Background job attempt finished",
                level="info",
                logger="closeyourit.jobs",
                contexts={"job": context},
            )

    def _task_id(self, task_id: object, request: object) -> str | None:
        direct = _clean_text(task_id)
        if direct is not None:
            return direct
        return _clean_text(_attribute(request, "id"))

    def _task_name(self, task: object, request: object, sender: object) -> str:
        for candidate in (
            _attribute(task, "name"),
            _attribute(request, "task"),
            sender,
        ):
            cleaned = _clean_text(candidate)
            if cleaned is not None:
                return cleaned
        return _UNKNOWN_TASK

    def _published_at(self, request: object) -> float | None:
        headers = _attribute(request, "headers")
        if not isinstance(headers, Mapping):
            return None
        value = headers.get(_PUBLISHED_AT_HEADER)
        if isinstance(value, bool) or not isinstance(value, int | float):
            return None
        result = float(value)
        return result if math.isfinite(result) else None

    def _context(
        self,
        task: object,
        request: object,
        *,
        task_id: str | None,
        state: object = None,
    ) -> dict[str, object]:
        context: dict[str, object] = {"task_name": self._task_name(task, request, task)}
        if task_id is not None:
            context["task_id"] = task_id

        delivery_info = _attribute(request, "delivery_info")
        if isinstance(delivery_info, Mapping):
            queue = _clean_text(delivery_info.get("routing_key")) or _clean_text(
                delivery_info.get("queue")
            )
            if queue is not None:
                context["queue"] = queue

        retries = _attribute(request, "retries")
        if isinstance(retries, int) and not isinstance(retries, bool):
            context["retry_count"] = max(0, retries)

        state_name = _clean_text(state)
        if state_name is not None:
            context["state"] = state_name

        headers = self._correlation_context(request)
        if headers:
            context["headers"] = headers
        return context

    def _correlation_context(self, request: object) -> dict[str, object]:
        headers = _attribute(request, "headers")
        if not isinstance(headers, Mapping):
            return {}
        candidates: dict[str, list[object]] = {}
        for raw_key, raw_value in headers.items():
            if not isinstance(raw_key, str):
                continue
            key = raw_key.strip().casefold()
            if key in self.correlation_headers:
                candidates.setdefault(key, []).append(raw_value)

        result: dict[str, object] = {}
        for key in sorted(candidates):
            values = candidates[key]
            if len(values) != 1:
                continue
            value = self._validated_correlation_value(key, values[0])
            if value is not None:
                result[key] = value
        return result

    def _validated_correlation_value(self, key: str, value: object) -> str | None:
        if isinstance(value, bytes):
            if len(value) > _MAX_CORRELATION_VALUE_BYTES:
                return None
            try:
                normalized = value.decode("ascii")
            except UnicodeDecodeError:
                return None
        elif isinstance(value, str):
            if len(value) > _MAX_CORRELATION_VALUE_BYTES:
                return None
            normalized = value
        else:
            return None

        if key == "traceparent":
            match = _TRACEPARENT.fullmatch(normalized)
            if match is None or set(match.group(1)) == {"0"} or set(match.group(2)) == {"0"}:
                return None
            return normalized
        return normalized if _CORRELATION_IDENTIFIER.fullmatch(normalized) is not None else None
