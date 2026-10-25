"""Pure builders for the shared CloseYourIt ingest payloads."""

from __future__ import annotations

import platform
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isfinite
from pathlib import Path
from types import TracebackType
from typing import cast
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

from closeyourit._version import __version__
from closeyourit.configuration import Configuration
from closeyourit.models import EventPayload
from closeyourit.scope import current_scope
from closeyourit.scrubber import Scrubber
from closeyourit.sql import sql_fingerprint
from closeyourit.usage import KINDS, SYMBOL_FORMAT, UsageWindow

Clock = Callable[[], datetime]
IdFactory = Callable[[], UUID]

_LEVELS = frozenset({"debug", "info", "warning", "error", "fatal"})
_LEVEL_ALIASES = {"warn": "warning", "err": "error", "critical": "fatal"}


def _now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    formatted = normalized.isoformat(timespec=timespec)
    return formatted.replace("+00:00", "Z")


def _level(value: object, *, fallback: str) -> str:
    normalized = str(value).strip().casefold()
    normalized = _LEVEL_ALIASES.get(normalized, normalized)
    return normalized if normalized in _LEVELS else fallback


def _compact(payload: EventPayload) -> EventPayload:
    return {key: value for key, value in payload.items() if value is not None}


def _duration(value: float | None) -> float:
    normalized = 0.0 if value is None else float(value)
    if not isfinite(normalized) or normalized < 0:
        raise ValueError("duration must be finite and non-negative")
    return round(normalized, 2)


def _sql(value: str) -> str:
    normalized = sql_fingerprint(value)
    if not normalized:
        raise ValueError("SQL must contain a query structure")
    return normalized


def _merge(left: EventPayload, right: Mapping[str, object]) -> EventPayload:
    result = dict(left)
    for key, value in right.items():
        existing = result.get(key)
        if isinstance(existing, dict) and isinstance(value, Mapping):
            result[key] = _merge(existing, cast(Mapping[str, object], value))
        else:
            result[key] = value
    return result


def _safe_request_url(url: str, path: str) -> tuple[str, str, str]:
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as error:
        raise ValueError("slow request url must be a valid absolute HTTP URL") from error

    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"} or hostname is None:
        raise ValueError("slow request url must be a valid absolute HTTP URL")

    safe_path = path.replace("\x00", "").split("?", 1)[0].split("#", 1)[0]
    safe_path = safe_path if safe_path.startswith("/") else f"/{safe_path}"
    safe_path = safe_path or "/"
    host = f"[{hostname}]" if ":" in hostname else hostname
    default_port = (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    authority = f"{host}:{port}" if port is not None and not default_port else host
    return urlunsplit((scheme, authority, safe_path, "", "")), hostname, safe_path


@dataclass(frozen=True, slots=True)
class EventBuilder:
    configuration: Configuration
    clock: Clock = _now
    id_factory: IdFactory = uuid4

    @property
    def scrubber(self) -> Scrubber:
        return Scrubber(self.configuration.filter_parameters)

    def timestamp(self) -> str:
        return _timestamp(self.clock())

    def exception(
        self,
        error: BaseException,
        *,
        handled: bool = False,
        level: str = "error",
        contexts: Mapping[str, object] | None = None,
    ) -> EventPayload:
        payload = self._event_base(level=_level(level, fallback="error"))
        payload["exception"] = {
            "values": [self._exception_value(item, handled=handled) for item in self._chain(error)]
        }
        if contexts:
            payload = _merge(payload, {"contexts": self.scrubber.scrub(dict(contexts))})
        return payload

    def message(self, message: object, *, level: str = "info") -> EventPayload:
        formatted = self._message(message)
        payload = self._event_base(level=_level(level, fallback="info"))
        payload["message"] = {"formatted": formatted}
        return payload

    def log(
        self,
        message: object,
        *,
        level: str = "info",
        logger: str | None = None,
        attributes: Mapping[str, object] | None = None,
    ) -> EventPayload:
        formatted = self._message(message)
        snapshot = current_scope().snapshot(self.configuration)
        trace_id = snapshot.trace_id
        return _compact(
            {
                "event_id": self.id_factory().hex,
                "timestamp": _timestamp(self.clock()),
                "level": _level(level, fallback="info"),
                "message": formatted,
                "attributes": self.scrubber.scrub(dict(attributes or {})),
                "logger": logger,
                "trace_id": trace_id,
                "environment": self.configuration.environment,
                "release": self.configuration.release,
                "sdk": self._sdk(),
            }
        )

    def slow_method(self, label: str, duration_ms: float) -> EventPayload:
        label = label.strip()
        if not label:
            raise ValueError("slow method label must not be blank")
        trace_id = current_scope().trace_id
        return _compact(
            {
                "kind": "slow_method",
                "sample_id": str(self.id_factory()),
                "duration_ms": _duration(duration_ms),
                "occurred_at": _timestamp(self.clock()),
                "environment": self.configuration.environment,
                "trace_id": trace_id,
                "label": label,
                "sdk": self._sdk(),
            }
        )

    def slow_request(
        self,
        *,
        route: str,
        path: str,
        url: str,
        duration_ms: float,
    ) -> EventPayload:
        route = route.strip()
        if not route:
            raise ValueError("slow request route must not be blank")
        safe_url, hostname, safe_path = _safe_request_url(url, path)
        return _compact(
            {
                "kind": "performance_issue",
                "subtype": "slow_request",
                "sample_id": str(self.id_factory()),
                "duration_ms": _duration(duration_ms),
                "occurred_at": _timestamp(self.clock()),
                "environment": self.configuration.environment,
                "release": (
                    self.scrubber.scrub_message(self.configuration.release)
                    if self.configuration.release
                    else None
                ),
                "trace_id": current_scope().trace_id,
                "route": route,
                "http_host": hostname,
                "http_url": safe_url,
                "http_path": safe_path,
                "source": "python",
                "sdk": self._sdk(),
            }
        )

    def slow_query(
        self,
        sql: str,
        duration_ms: float,
        *,
        db_system: str | None = None,
        source: str | None = None,
    ) -> EventPayload:
        """Build a privacy-safe query timing without raw bind values."""
        normalized_sql = _sql(sql)
        return _compact(
            {
                "kind": "slow_query",
                "sample_id": str(self.id_factory()),
                "duration_ms": _duration(duration_ms),
                "occurred_at": _timestamp(self.clock()),
                "environment": self.configuration.environment,
                "release": (
                    self.scrubber.scrub_message(self.configuration.release)
                    if self.configuration.release
                    else None
                ),
                "trace_id": current_scope().trace_id,
                "sql": normalized_sql,
                "db_system": db_system,
                "source": source,
                "sdk": self._sdk(),
            }
        )

    def performance_issue(
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
    ) -> EventPayload:
        """Build an aggregate performance verdict using the shared wire names."""
        normalized_subtype = subtype.strip()
        if not normalized_subtype:
            raise ValueError("performance issue subtype must not be blank")
        normalized_sql = _sql(sql) if sql is not None else None
        return _compact(
            {
                "kind": "performance_issue",
                "subtype": normalized_subtype,
                "sample_id": str(self.id_factory()),
                "duration_ms": _duration(duration_ms),
                "occurred_at": _timestamp(self.clock()),
                "environment": self.configuration.environment,
                "release": (
                    self.scrubber.scrub_message(self.configuration.release)
                    if self.configuration.release
                    else None
                ),
                "trace_id": current_scope().trace_id,
                "sql": normalized_sql,
                "source": source,
                "route": route,
                "http_host": http_host,
                "http_url": http_url,
                "query_count": max(0, int(query_count)) if query_count is not None else None,
                "total_query_time_ms": (
                    _duration(total_query_time_ms) if total_query_time_ms is not None else None
                ),
                "sdk": self._sdk(),
            }
        )

    def usage(self, window: UsageWindow) -> EventPayload:
        """Build the usage flush envelope: one request per window, no user data by construction.

        Last filter before the wire: an out-of-contract symbol is dropped on its own, never failing
        the whole window — losing every symbol because of one dirty entry would fabricate exactly
        the false "never seen" the channel exists to avoid.
        """
        return _compact(
            {
                "environment": self.configuration.environment,
                "release": self.configuration.release,
                "sdk": self._sdk(),
                "window_started_at": _timestamp(window.started_at),
                "window_ended_at": _timestamp(window.ended_at),
                "truncated": window.truncated,
                "symbols": [
                    {
                        "kind": item.kind,
                        "symbol": item.symbol,
                        "count": max(0, int(item.count)),
                        "last_seen_at": _timestamp(item.last_seen_at),
                    }
                    for item in window.symbols
                    if item.kind in KINDS and SYMBOL_FORMAT.fullmatch(item.symbol) is not None
                ],
            }
        )

    def _event_base(self, *, level: str) -> EventPayload:
        base: EventPayload = _compact(
            {
                "event_id": self.id_factory().hex,
                "timestamp": _timestamp(self.clock()),
                "platform": "python",
                "level": level,
                "environment": self.configuration.environment,
                "release": self.configuration.release,
                "contexts": {"runtime": {"name": "python", "version": platform.python_version()}},
                "sdk": self._sdk(),
            }
        )
        return _merge(base, current_scope().snapshot(self.configuration).to_event_data())

    def _exception_value(self, error: BaseException, *, handled: bool) -> EventPayload:
        return {
            "type": type(error).__name__,
            "value": self.scrubber.scrub_message(str(error)),
            "stacktrace": {"frames": self._frames(error.__traceback__)},
            "mechanism": {"type": "python", "handled": handled},
        }

    def _frames(self, traceback: TracebackType | None) -> list[EventPayload]:
        frames: list[EventPayload] = []
        current = traceback
        root = Path.cwd().resolve()
        while current is not None:
            frame = current.tb_frame
            absolute = Path(frame.f_code.co_filename).resolve()
            try:
                filename = str(absolute.relative_to(root))
                in_app = True
            except ValueError:
                filename = absolute.name
                in_app = False
            frames.append(
                {
                    "filename": filename,
                    "abs_path": str(absolute),
                    "function": frame.f_code.co_name,
                    "lineno": current.tb_lineno,
                    "module": frame.f_globals.get("__name__"),
                    "in_app": in_app,
                }
            )
            current = current.tb_next
        return frames

    def _chain(self, error: BaseException) -> list[BaseException]:
        chain: list[BaseException] = []
        seen: set[int] = set()
        current: BaseException | None = error
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            chain.append(current)
            current = current.__cause__
            if current is None and not chain[-1].__suppress_context__:
                current = chain[-1].__context__
        chain.reverse()
        return chain

    def _sdk(self) -> EventPayload:
        return {"name": "closeyourit-python", "version": __version__}

    def _message(self, value: object) -> str:
        formatted = self.scrubber.scrub_message(str(value))
        if not formatted.strip():
            raise ValueError("message must not be blank")
        return formatted
