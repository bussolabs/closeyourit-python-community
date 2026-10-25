"""Public capture facade independent from the concrete asynchronous transport."""

from __future__ import annotations

import math
import random
import threading
import time
from collections.abc import Callable, Mapping
from typing import Literal, Protocol, TypeVar, runtime_checkable

from closeyourit._diagnostics import logger
from closeyourit.configuration import Configuration
from closeyourit.events import EventBuilder
from closeyourit.models import EventPayload
from closeyourit.scope import current_scope
from closeyourit.usage import UsageRegistry, UsageWindow

Channel = Literal["events", "logs", "metrics", "usages"]
T = TypeVar("T")


class EventSink(Protocol):
    def enqueue(self, channel: Channel, payload: EventPayload) -> bool: ...


@runtime_checkable
class FlushableSink(Protocol):
    def flush(self, timeout: float | None = None) -> bool: ...


@runtime_checkable
class CloseableSink(Protocol):
    def close(self, timeout: float | None = None) -> bool: ...


class NullSink:
    def enqueue(self, channel: Channel, payload: EventPayload) -> bool:
        del channel, payload
        return False


class Client:
    """Build, scrub and hand payloads to a replaceable non-blocking sink."""

    def __init__(
        self,
        configuration: Configuration | None = None,
        *,
        sink: EventSink | None = None,
        random_value: Callable[[], float] = random.random,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.configuration = configuration or Configuration()
        disabled_reason = self.configuration.disabled_reason
        if disabled_reason is not None:
            logger.warning("closeyourit is disabled and will send nothing: %s", disabled_reason)
        if sink is None and self.configuration.enabled:
            from closeyourit.transport import Transport

            sink = Transport(self.configuration)
        self.sink = sink or NullSink()
        self.random_value = random_value
        self.monotonic = monotonic
        self.builder = EventBuilder(self.configuration)
        self._usage: UsageRegistry | None = None
        self._usage_lock = threading.Lock()

    def capture_exception(
        self,
        error: BaseException,
        *,
        handled: bool = False,
        level: str = "error",
        contexts: Mapping[str, object] | None = None,
    ) -> str | None:
        if not self._sampled(self.configuration.sample_rate):
            return None
        return self._submit(
            "events",
            self.builder.exception(error, handled=handled, level=level, contexts=contexts),
        )

    def capture_message(self, message: object, *, level: str = "info") -> str | None:
        if not self._sampled(self.configuration.sample_rate):
            return None
        return self._submit("events", self.builder.message(message, level=level))

    def log(
        self,
        message: object,
        *,
        level: str = "info",
        logger: str | None = None,
        **attributes: object,
    ) -> str | None:
        if not self._sampled(self.configuration.logs_sample_rate):
            return None
        return self._submit(
            "logs",
            self.builder.log(message, level=level, logger=logger, attributes=attributes),
        )

    def add_breadcrumb(
        self,
        *,
        message: str | None = None,
        category: str | None = None,
        type: str = "default",
        level: str = "info",
        data: Mapping[str, object] | None = None,
    ) -> None:
        if not self.configuration.breadcrumbs_enabled:
            return
        breadcrumb: EventPayload = {
            "timestamp": self.builder.timestamp(),
            "type": type,
            "level": level,
        }
        if message is not None:
            breadcrumb["message"] = message
        if category is not None:
            breadcrumb["category"] = category
        if data:
            breadcrumb["data"] = dict(data)
        current_scope().add_breadcrumb(
            breadcrumb, max_breadcrumbs=self.configuration.max_breadcrumbs
        )

    def measure(self, label: str, operation: Callable[[], T]) -> T:
        label = label.strip()
        if not label:
            raise ValueError("measurement label must not be blank")
        started_at = self.monotonic()
        try:
            return operation()
        finally:
            duration_ms = (self.monotonic() - started_at) * 1000.0
            if duration_ms >= self.configuration.slow_method_threshold_ms:
                self.capture_duration(label, duration_ms)

    def capture_duration(
        self,
        label: str,
        duration_ms: float,
        *,
        contexts: Mapping[str, object] | None = None,
    ) -> str | None:
        """Capture a finite duration enriched with release and scrubbed integration context."""
        try:
            normalized_duration = float(duration_ms)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(normalized_duration):
            return None
        payload = self.builder.slow_method(label, max(0.0, normalized_duration))
        if self.configuration.release is not None:
            payload["release"] = self.configuration.release
        if contexts:
            payload["contexts"] = self.builder.scrubber.scrub(dict(contexts))
        return self._submit("metrics", payload)

    def capture_slow_request(
        self,
        *,
        route: str,
        path: str,
        url: str,
        duration_ms: float,
    ) -> str | None:
        return self._submit(
            "metrics",
            self.builder.slow_request(
                route=route,
                path=path,
                url=url,
                duration_ms=duration_ms,
            ),
        )

    def capture_slow_query(
        self,
        sql: str,
        duration_ms: float,
        *,
        db_system: str | None = None,
        source: str | None = None,
    ) -> str | None:
        return self._submit(
            "metrics",
            self.builder.slow_query(sql, duration_ms, db_system=db_system, source=source),
        )

    def capture_performance_issue(
        self,
        subtype: str,
        *,
        duration_ms: float | None = None,
        sql: str | None = None,
        source: str | None = None,
        route: str | None = None,
        http_host: str | None = None,
        http_url: str | None = None,
        query_count: int | None = None,
        total_query_time_ms: float | None = None,
    ) -> str | None:
        return self._submit(
            "metrics",
            self.builder.performance_issue(
                subtype,
                duration_ms=duration_ms,
                sql=sql,
                source=source,
                route=route,
                http_host=http_host,
                http_url=http_url,
                query_count=query_count,
                total_query_time_ms=total_query_time_ms,
            ),
        )

    @property
    def usage(self) -> UsageRegistry:
        """Lazily built usage registry; no buffer and no timer until something is recorded."""
        with self._usage_lock:
            if self._usage is None:
                self._usage = UsageRegistry(self.configuration, self._flush_usage_window)
            return self._usage

    def record_usage(self, kind: str, symbol: str) -> bool:
        """Count one sighting of a `route`, `job` or `custom` symbol in the current window."""
        if not (self.configuration.usage_enabled and self.configuration.enabled):
            return False
        return self.usage.record(kind, symbol)

    def used(self, key: str) -> bool:
        """Mark a literal feature key as used: "this part of the product actually runs"."""
        return self.record_usage("custom", key)

    def flush(self, timeout: float | None = None) -> bool:
        """Wait for an asynchronous sink, or succeed immediately for a synchronous sink."""
        registry = self._usage
        if registry is not None:
            registry.flush()
        if isinstance(self.sink, FlushableSink):
            return self.sink.flush(timeout)
        return True

    def close(self, timeout: float | None = None) -> bool:
        """Close a managed sink idempotently."""
        registry = self._usage
        if registry is not None:
            registry.shutdown()
        if isinstance(self.sink, CloseableSink):
            return self.sink.close(timeout)
        return True

    def _flush_usage_window(self, window: UsageWindow) -> None:
        # The usage flush does NOT go through `before_send`: it is not an application event but the
        # census of executed symbols, free of user data by construction. Having it filtered by
        # a hook meant for errors would silently erase the proof that a
        # route is alive. Parity with CloseYourIt-Ruby.
        self.sink.enqueue("usages", self.builder.usage(window))

    def _sampled(self, rate: float) -> bool:
        return (
            self.configuration.enabled
            and rate > 0.0
            and (rate >= 1.0 or self.random_value() < rate)
        )

    def _submit(self, channel: Channel, payload: EventPayload) -> str | None:
        if not self.configuration.enabled:
            return None
        prepared = self.configuration.apply_before_send(payload)
        if prepared is None or not self.sink.enqueue(channel, prepared):
            return None
        identifier = prepared.get("event_id", prepared.get("sample_id"))
        return str(identifier) if identifier is not None else None
