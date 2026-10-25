"""Opt-in Django query profiles using the framework's execution-wrapper protocol."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import RLock
from typing import Any
from weakref import WeakKeyDictionary, WeakSet

from closeyourit._diagnostics import logger
from closeyourit.client import Client
from closeyourit.sql import sql_fingerprint

_ProfileRoute = str | Callable[[], str | None] | None
_current: ContextVar[DjangoQueryProfile | None] = ContextVar(
    "closeyourit_django_query", default=None
)
_capturing: ContextVar[bool] = ContextVar("closeyourit_django_query_capture", default=False)
_lock = RLock()
_owners: WeakSet[DjangoQueryInstrumentation] = WeakSet()
_connections: WeakKeyDictionary[Any, bool] = WeakKeyDictionary()


@dataclass
class DjangoQueryProfile:
    """One bounded query window, shared across Django's thread-sensitive bridge."""

    owner: DjangoQueryInstrumentation
    route: _ProfileRoute
    count: int = 0
    duration_ms: float = 0.0
    groups: dict[str, tuple[int, float]] = field(default_factory=dict)
    deferred: bool = False
    finished: bool = False
    lock: Any = field(default_factory=RLock)

    def record(self, sql: str, duration_ms: float) -> None:
        with self.lock:
            if self.finished:
                return
            self.count += 1
            self.duration_ms += duration_ms
            if sql in self.groups or len(self.groups) < 1000:
                count, duration = self.groups.get(sql, (0, 0.0))
                self.groups[sql] = count + 1, duration + duration_ms

    def finish(self) -> None:
        with self.lock:
            if self.finished:
                return
            self.finished = True
        if not self.owner.active:
            return
        token = _capturing.set(True)
        try:
            client = self.owner.client
            configuration = client.configuration
            for sql, (count, duration) in self.groups.items():
                if count > configuration.n_plus_one_threshold:
                    client.capture_performance_issue(
                        "n_plus_one", duration_ms=duration, sql=sql, query_count=count
                    )
            if self.count > configuration.query_count_threshold:
                route = self.route() if callable(self.route) else self.route
                client.capture_performance_issue(
                    "high_query_count",
                    duration_ms=self.duration_ms,
                    route=route,
                    query_count=self.count,
                    total_query_time_ms=self.duration_ms,
                )
        except Exception:
            logger.warning("Django query profile could not be reported")
        finally:
            _capturing.reset(token)


def _execute(execute: Callable[..., Any], sql: Any, params: Any, many: bool, context: Any) -> Any:
    profile = _current.get()
    if profile is None or profile.finished or not profile.owner.active or _capturing.get():
        return execute(sql, params, many, context)
    try:
        started = profile.owner.monotonic()
    except Exception:
        return execute(sql, params, many, context)
    try:
        return execute(sql, params, many, context)
    finally:
        token = _capturing.set(True)
        try:
            duration = max(0.0, (profile.owner.monotonic() - started) * 1000.0)
            # Never inspect bind objects, cursor results, connection credentials,
            # or partial SQL strings whose truncated literals might expose data.
            if profile.finished or not profile.owner.active:
                pass
            elif isinstance(sql, str) and len(sql) <= 16384:
                fingerprint = sql_fingerprint(sql)
                if fingerprint:
                    profile.record(fingerprint, duration)
                    client = profile.owner.client
                    client.add_breadcrumb(
                        category="query",
                        type="query",
                        data={
                            "sql": fingerprint,
                            "duration_ms": round(duration, 2),
                        },
                    )
                    threshold = client.configuration.slow_query_threshold_ms
                    if threshold > 0 and duration >= threshold:
                        vendor = getattr(context.get("connection"), "vendor", None)
                        client.capture_slow_query(fingerprint, duration, db_system=vendor)
            else:
                logger.warning("Django query text exceeded the capture limit or was unsupported")
        except Exception:
            logger.warning("Django query observation failed")
        finally:
            _capturing.reset(token)


def _attach(connection: Any) -> None:
    with _lock:
        if not _owners or connection in _connections:
            return
        # execute_wrapper() itself appends here and blindly pops on exit. A shared
        # ASGI hook needs identity-based removal to preserve overlapping wrappers.
        connection.execute_wrappers.insert(0, _execute)
        _connections[connection] = True


def _created(sender: Any, connection: Any, **kwargs: Any) -> None:
    del sender, kwargs
    try:
        _attach(connection)
    except Exception:
        logger.warning("Django query hook could not be installed")


def _request_started(sender: Any = None, **kwargs: Any) -> None:
    del sender, kwargs
    try:
        from django.db import connections  # type: ignore[import-untyped]

        for connection in connections.all(initialized_only=True):
            _attach(connection)
    except Exception:
        logger.warning("Django query hooks could not be installed on existing connections")


class DjangoQueryInstrumentation:
    """Install once before worker startup; observe only explicit query profiles."""

    def __init__(self, client: Client, *, monotonic: Callable[[], float] = time.monotonic) -> None:
        self.client = client
        self.monotonic = monotonic
        self.active = False
        try:
            from django.core.signals import request_started  # type: ignore[import-untyped]
            from django.db.backends import signals  # type: ignore[import-untyped]
        except ImportError:
            logger.warning("Django query monitoring is unavailable because Django is not installed")
            return
        with _lock:
            _owners.add(self)
            request_started.connect(
                _request_started, dispatch_uid="closeyourit.queries.request", weak=False
            )
            signals.connection_created.connect(
                _created, dispatch_uid="closeyourit.queries.connection", weak=False
            )
            self.active = True
        _request_started()

    async def attach_async(self) -> None:
        if self.active:
            from asgiref.sync import sync_to_async

            try:
                await sync_to_async(_request_started, thread_sensitive=True)()
            except Exception:
                logger.warning("Django async query hooks could not be installed")

    @asynccontextmanager
    async def async_profile(
        self, *, route: _ProfileRoute = None
    ) -> AsyncIterator[DjangoQueryProfile]:
        await self.attach_async()
        with self.profile(route=route) as profile:
            yield profile

    @contextmanager
    def profile(self, *, route: _ProfileRoute = None) -> Iterator[DjangoQueryProfile]:
        _request_started()
        profile = DjangoQueryProfile(self, route)
        token = _current.set(profile)
        try:
            yield profile
        finally:
            _current.reset(token)
            if not profile.deferred:
                profile.finish()

    def uninstrument(self) -> None:
        with _lock:
            if not self.active:
                return
            self.active = False
            _owners.discard(self)
            if _owners:
                return
            from django.core.signals import request_started
            from django.db.backends import signals

            request_started.disconnect(dispatch_uid="closeyourit.queries.request")
            signals.connection_created.disconnect(dispatch_uid="closeyourit.queries.connection")
            for connection in list(_connections):
                wrappers = connection.execute_wrappers
                if _execute in wrappers:
                    wrappers.remove(_execute)
            _connections.clear()

    def __enter__(self) -> DjangoQueryInstrumentation:
        return self

    def __exit__(self, *args: object) -> None:
        self.uninstrument()


def instrument_django(
    client: Client, *, monotonic: Callable[[], float] = time.monotonic
) -> DjangoQueryInstrumentation:
    """Install reversible query hooks without importing Django in the SDK core."""
    return DjangoQueryInstrumentation(client, monotonic=monotonic)
