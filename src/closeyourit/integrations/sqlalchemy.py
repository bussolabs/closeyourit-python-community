"""SQLAlchemy Engine events with private-by-default query profiling."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

from closeyourit.client import Client
from closeyourit.sql import sql_fingerprint


@dataclass(slots=True)
class _QueryGroup:
    sql: str
    count: int = 0
    duration_ms: float = 0.0


@dataclass(slots=True)
class _QueryProfile:
    route: str | None
    query_count: int = 0
    total_query_time_ms: float = 0.0
    groups: dict[str, _QueryGroup] = field(default_factory=dict)

    def add(self, sql: str, duration_ms: float) -> None:
        self.query_count += 1
        self.total_query_time_ms += duration_ms
        group = self.groups.setdefault(sql, _QueryGroup(sql))
        group.count += 1
        group.duration_ms += duration_ms


class SQLAlchemyInstrumentation:
    def __init__(
        self,
        engine: Any,
        client: Client,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        from sqlalchemy import event

        self._event = event
        self._target = getattr(engine, "sync_engine", engine)
        self._client = client
        self._monotonic = monotonic
        self._starts: dict[int, float] = {}
        self._starts_lock = Lock()
        self._state_lock = Lock()
        self._active = True
        self._current_profile: ContextVar[_QueryProfile | None] = ContextVar(
            f"closeyourit_sqlalchemy_profile_{id(self)}", default=None
        )
        event.listen(self._target, "before_cursor_execute", self._before_cursor_execute)
        event.listen(self._target, "after_cursor_execute", self._after_cursor_execute)
        event.listen(self._target, "handle_error", self._handle_error)

    def _before_cursor_execute(
        self,
        connection: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        del connection, cursor, statement, parameters, executemany
        with self._starts_lock:
            self._starts[id(context)] = self._monotonic()

    def _after_cursor_execute(
        self,
        connection: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        del cursor, parameters, executemany
        self._complete(connection, statement, context)

    def _handle_error(self, exception_context: Any) -> None:
        context = exception_context.execution_context
        if context is None:
            return
        statement = exception_context.statement or ""
        self._complete(exception_context.connection, statement, context)

    def _complete(self, connection: Any, statement: str, context: Any) -> None:
        try:
            with self._starts_lock:
                started_at = self._starts.pop(id(context), None)
            if started_at is None:
                return
            duration_ms = max(0.0, (self._monotonic() - started_at) * 1_000.0)
            fingerprint = sql_fingerprint(statement)
            if not fingerprint:
                return
            self._client.add_breadcrumb(
                category="query",
                type="query",
                data={"sql": fingerprint, "duration_ms": round(duration_ms, 2)},
            )
            profile = self._current_profile.get()
            if profile is not None:
                profile.add(fingerprint, duration_ms)
            threshold = self._client.configuration.slow_query_threshold_ms
            if threshold > 0 and duration_ms >= threshold:
                dialect = getattr(getattr(connection, "dialect", None), "name", None)
                self._client.capture_slow_query(
                    fingerprint,
                    duration_ms,
                    db_system=str(dialect) if dialect is not None else None,
                )
        except Exception:
            return

    @contextmanager
    def profile(self, *, route: str | None = None) -> Iterator[None]:
        profile = _QueryProfile(route=route)
        token = self._current_profile.set(profile)
        try:
            yield
        finally:
            self._current_profile.reset(token)
            self._emit_profile(profile)

    def _emit_profile(self, profile: _QueryProfile) -> None:
        try:
            configuration = self._client.configuration
            for group in profile.groups.values():
                if group.count > configuration.n_plus_one_threshold:
                    self._client.capture_performance_issue(
                        "n_plus_one",
                        duration_ms=group.duration_ms,
                        sql=group.sql,
                        query_count=group.count,
                    )
            if profile.query_count > configuration.query_count_threshold:
                self._client.capture_performance_issue(
                    "high_query_count",
                    duration_ms=profile.total_query_time_ms,
                    route=profile.route,
                    query_count=profile.query_count,
                    total_query_time_ms=profile.total_query_time_ms,
                )
        except Exception:
            return

    def uninstrument(self) -> None:
        with self._state_lock:
            if not self._active:
                return
            self._active = False
            for name, listener in (
                ("before_cursor_execute", self._before_cursor_execute),
                ("after_cursor_execute", self._after_cursor_execute),
                ("handle_error", self._handle_error),
            ):
                self._event.remove(self._target, name, listener)
            with self._starts_lock:
                self._starts.clear()

    def __enter__(self) -> SQLAlchemyInstrumentation:
        return self

    def __exit__(self, *args: object) -> None:
        del args
        self.uninstrument()


def instrument_sqlalchemy(
    engine: Any,
    client: Client,
    *,
    monotonic: Callable[[], float] = time.monotonic,
) -> SQLAlchemyInstrumentation:
    """Instrument one sync or async SQLAlchemy engine via official event listeners."""
    return SQLAlchemyInstrumentation(engine, client, monotonic=monotonic)
