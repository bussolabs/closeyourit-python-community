"""Opt-in usage telemetry: which registered symbols an application actually runs.

CYSK-29 — port of CloseYourIt-Ruby's usage registry. It records WHICH symbols actually run
(routes as templates, tasks, literal custom keys) and flushes them in one POST per window. On the
hot path: one lookup and one increment.

The rules that make the registry loss-proof, identical across SDKs:

- counts are INDICATIVE, only ``last_seen_at`` is load-bearing: a lost window costs one
  window on a symbol that shows up again right after;
- the registry EMPTIES at every flush: the cap (``usage_max_symbols``) limits the distinct symbols
  seen in a window — a quantity tied to traffic, not to code size;
- past the cap the window carries ``truncated=True``, which disqualifies the kind on the scanner
  side;
- no sampling, ever: sampling a route called three times a month fabricates exactly the
  false "never seen" the channel exists to avoid;
- the symbol is a code identity, NEVER a URL: no concrete paths, parameters, users or IPs.
"""

from __future__ import annotations

import os
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from closeyourit._diagnostics import logger
from closeyourit.configuration import Configuration

KINDS = ("route", "job", "custom")
# Symbols come from framework payloads or literal strings: the pattern IS the contract,
# shared character by character with the Ruby gem and with the ingest.
SYMBOL_FORMAT = re.compile(r"[A-Za-z0-9_:#./-]{1,200}\Z", re.ASCII)
# Django and Flask write the converter BEFORE the name (`<int:pk>`), Starlette and FastAPI AFTER
# (`{id:path}`): the delimiter tells which side the parameter name is on.
_PLACEHOLDER = re.compile(r"[<{]([^<>{}]*)[>}]")
_UNSAFE_NAME = re.compile(r"[^A-Za-z0-9_]")


def _now() -> datetime:
    return datetime.now(UTC)


def _placeholder(match: re.Match[str]) -> str:
    parts = match.group(1).split(":")
    name = parts[-1] if match.group(0).startswith("<") else parts[0]
    return f":{_UNSAFE_NAME.sub('', name) or 'param'}"


def normalize_route(route: str | None) -> str | None:
    """Turn a framework route template into a shared usage symbol, or reject it.

    Route templates contain no user data by construction, but use delimiters outside the
    contract: ``orders/<int:pk>`` and ``/orders/{order_id}`` become ``/orders/:pk`` and
    ``/orders/:order_id`` respectively. Anything outside the shared format — a concrete path with a
    query string, a space, a non-ASCII character — returns ``None`` and is not recorded.
    """
    if not isinstance(route, str):
        return None
    candidate = _PLACEHOLDER.sub(_placeholder, route.strip())
    if not candidate:
        return None
    if not candidate.startswith("/"):
        candidate = f"/{candidate}"
    return candidate if SYMBOL_FORMAT.fullmatch(candidate) is not None else None


@dataclass(frozen=True, slots=True)
class UsageSymbol:
    """One symbol seen running, with its indicative count and load-bearing last sighting."""

    kind: str
    symbol: str
    count: int
    last_seen_at: datetime


@dataclass(frozen=True, slots=True)
class UsageWindow:
    """One drained observation window, ready for the wire builder."""

    symbols: tuple[UsageSymbol, ...]
    truncated: bool
    started_at: datetime
    ended_at: datetime

    @property
    def empty(self) -> bool:
        """Whether the window carries nothing worth a request."""
        return not self.symbols and not self.truncated


class UsageRegistry:
    """Bounded per-process symbol counter flushed on a timer, fail-safe by construction."""

    def __init__(
        self,
        configuration: Configuration,
        flush_window: Callable[[UsageWindow], None],
        *,
        clock: Callable[[], datetime] = _now,
        pid: Callable[[], int] = os.getpid,
        shutdown_timeout: float = 2.0,
    ) -> None:
        self.configuration = configuration
        self._flush_window = flush_window
        self._clock = clock
        self._pid_source = pid
        self._shutdown_timeout = max(0.0, float(shutdown_timeout))
        self._pid = pid()
        self._initialize_process_state()

    def _initialize_process_state(self) -> None:
        # The lock is recreated with the rest of the state: at fork time it could be held by the
        # timer thread, which does not exist in the child — inheriting it acquired means blocking
        # the first registration forever, on the application's hot path.
        self._lock = threading.Lock()
        self._symbols: dict[tuple[str, str], UsageSymbol] = {}
        self._truncated = False
        self._window_started_at = self._clock()
        self._stopped = threading.Event()
        self._timer: threading.Thread | None = None

    @property
    def enabled(self) -> bool:
        """Whether recording is both opted in and backed by a usable configuration."""
        return self.configuration.usage_enabled and self.configuration.enabled

    @property
    def running(self) -> bool:
        """Whether the flush timer of this process is alive."""
        timer = self._timer
        return timer is not None and timer.is_alive()

    def record(self, kind: str, symbol: str) -> bool:
        """Count one sighting; returns whether the symbol entered the current window."""
        if not self.enabled:
            return False
        normalized_kind = str(kind).strip()
        normalized_symbol = str(symbol).strip()
        if normalized_kind not in KINDS or SYMBOL_FORMAT.fullmatch(normalized_symbol) is None:
            return False

        self._ensure_process()
        key = (normalized_kind, normalized_symbol)
        with self._lock:
            if self._stopped.is_set():
                return False
            entry = self._symbols.get(key)
            if entry is None and len(self._symbols) >= self.configuration.usage_max_symbols:
                self._truncated = True
                recorded = False
            else:
                self._symbols[key] = UsageSymbol(
                    kind=normalized_kind,
                    symbol=normalized_symbol,
                    count=1 if entry is None else entry.count + 1,
                    last_seen_at=self._clock(),
                )
                recorded = True
        # Even a window made only of truncation must be sent: the flag disqualifies the kind on the
        # scanner side, and without a timer it would stay in memory until shutdown.
        self._start_timer()
        return recorded

    def flush(self) -> None:
        """Drain the window and hand it over; a failed send costs one window, never an exception."""
        try:
            window = self._drain()
            if window.empty:
                return
            self._flush_window(window)
        except Exception as error:
            # The flush runs on the timer thread: an error must neither propagate into the app nor
            # kill it. The load-bearing semantics is last_seen_at, and a lost window falsifies
            # nothing — the symbol shows up again in the next window.
            logger.warning("closeyourit usage flush failed: %s: %s", type(error).__name__, error)

    def shutdown(self) -> None:
        """Stop the timer, refuse further sightings and flush what is still buffered."""
        self._stopped.set()
        timer = self._timer
        if timer is not None and timer is not threading.current_thread():
            timer.join(self._shutdown_timeout)
        self.flush()

    def _drain(self) -> UsageWindow:
        self._ensure_process()
        ended_at = self._clock()
        with self._lock:
            window = UsageWindow(
                symbols=tuple(self._symbols.values()),
                truncated=self._truncated,
                started_at=self._window_started_at,
                ended_at=ended_at,
            )
            self._symbols = {}
            self._truncated = False
            self._window_started_at = ended_at
            return window

    def _start_timer(self) -> None:
        with self._lock:
            timer = self._timer
            if self._stopped.is_set() or (timer is not None and timer.is_alive()):
                return
            timer = threading.Thread(target=self._run, name="closeyourit-usage", daemon=True)
            self._timer = timer
            timer.start()

    def _run(self) -> None:
        interval = self.configuration.usage_flush_interval
        while not self._stopped.wait(interval):
            self.flush()

    def _ensure_process(self) -> None:
        # After a fork the child inherits the parent's buffer (duplicate counts) and the reference
        # to a thread that does not exist in the child: without a reset no timer would restart and
        # the symbols would stay in memory forever.
        current = self._pid_source()
        if current != self._pid:
            self._pid = current
            self._initialize_process_state()
