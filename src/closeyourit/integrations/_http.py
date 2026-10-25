"""Shared privacy and aggregation logic for outbound HTTP integrations."""

from __future__ import annotations

import re
import secrets
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Protocol
from urllib.parse import urlsplit

from closeyourit.client import Client
from closeyourit.scope import current_scope

_UUID = re.compile(
    r"(?<![0-9a-f])[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?![0-9a-f])",
    re.IGNORECASE,
)
_LONG_HEX = re.compile(r"(?<![0-9a-f])[0-9a-f]{16,}(?![0-9a-f])", re.IGNORECASE)
_LONG_NUMBER = re.compile(r"(?<!\d)\d{3,}(?!\d)")


class AnyHeaders(Protocol):
    def get(self, key: str) -> object | None: ...

    def pop(self, key: str, default: object | None = None) -> object | None: ...

    def __setitem__(self, key: str, value: str) -> None: ...


@dataclass(frozen=True, slots=True)
class HttpTarget:
    host: str
    path: str
    safe_url: str


def _normalized_path(path: str) -> str:
    value = re.sub(r"/{2,}", "/", path or "/")
    value = _UUID.sub("<uuid>", value)
    value = _LONG_HEX.sub("<hex>", value)
    value = _LONG_NUMBER.sub("<n>", value)
    segments = [
        (
            "<n>"
            if item.isdigit()
            else "<redacted>"
            if "@" in item or "%40" in item.casefold()
            else item
        )
        for item in value.split("/")
    ]
    value = "/".join(segments)
    if value != "/":
        value = value.rstrip("/")
    return value or "/"


def normalize_http_target(raw_url: str) -> HttpTarget | None:
    """Return only a normalized origin/path; credentials, query and fragment are discarded."""
    try:
        parsed = urlsplit(raw_url)
        scheme = parsed.scheme.casefold()
        hostname = parsed.hostname
        if scheme not in {"http", "https"} or hostname is None:
            return None
        normalized_hostname = hostname.encode("idna").decode("ascii").casefold()
        port = parsed.port
    except (UnicodeError, ValueError):
        return None

    if ":" in normalized_hostname:
        normalized_hostname = f"[{normalized_hostname}]"
    default_port = 443 if scheme == "https" else 80
    host = normalized_hostname if port in {None, default_port} else f"{normalized_hostname}:{port}"
    path = _normalized_path(parsed.path)
    return HttpTarget(host=host, path=path, safe_url=f"{scheme}://{host}{path}")


class RepeatedHttpTracker:
    def __init__(self, *, threshold: int, window_ms: float) -> None:
        self.threshold = threshold
        self.window_ms = window_ms
        self._hits: dict[str, deque[float]] = {}
        self._lock = Lock()

    def record(self, key: str, *, now_ms: float) -> int:
        cutoff = now_ms - self.window_ms
        with self._lock:
            hits = self._hits.setdefault(key, deque())
            while hits and hits[0] <= cutoff:
                hits.popleft()
            hits.append(now_ms)
            return len(hits)


class HttpRecorder:
    """Fail-safe recorder shared by sync and async clients."""

    def __init__(
        self,
        client: Client,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        span_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.client = client
        self.monotonic = monotonic
        self.span_id_factory = span_id_factory or (lambda: secrets.token_hex(8))
        configuration = client.configuration
        self.tracker = RepeatedHttpTracker(
            threshold=configuration.repeated_http_threshold,
            window_ms=configuration.repeated_http_window_ms,
        )
        endpoint = normalize_http_target(configuration.endpoint_url or "")
        self.endpoint_host = endpoint.host if endpoint else None

    def start(self) -> float:
        return self.monotonic()

    def inject_traceparent(self, raw_url: str, headers: AnyHeaders) -> None:
        """Propagate W3C context only after explicit opt-in and host allowlisting."""
        try:
            configuration = self.client.configuration
            target = normalize_http_target(raw_url)
            if not configuration.trace_propagation_enabled or target is None:
                return
            if target.host not in configuration.trace_propagation_hosts:
                headers.pop("traceparent", None)
                return
            if headers.get("traceparent") is not None:
                return
            trace_id = (current_scope().trace_id or "").replace("-", "").casefold()
            span_id = self.span_id_factory().casefold()
            if (
                re.fullmatch(r"[0-9a-f]{32}", trace_id) is None
                or trace_id == "0" * 32
                or re.fullmatch(r"[0-9a-f]{16}", span_id) is None
                or span_id == "0" * 16
            ):
                return
            headers["traceparent"] = f"00-{trace_id}-{span_id}-01"
        except Exception:
            return

    def complete(
        self,
        *,
        method: str,
        raw_url: str,
        started_at: float,
        status_code: int | None = None,
        error_type: str | None = None,
    ) -> None:
        try:
            finished_at = self.monotonic()
            target = normalize_http_target(raw_url)
            if target is None or target.host == self.endpoint_host:
                return
            allowed_hosts = self.client.configuration.http_capture_hosts
            if allowed_hosts and target.host not in allowed_hosts:
                return
            duration_ms = max(0.0, (finished_at - started_at) * 1_000.0)
            threshold = self.client.configuration.slow_external_threshold_ms
            slow = threshold > 0 and duration_ms >= threshold
            warning = error_type is not None or (status_code or 0) >= 500 or slow
            data: dict[str, object] = {
                "method": method.upper(),
                "url": target.safe_url,
                "duration_ms": round(duration_ms, 2),
            }
            if status_code is not None:
                data["status_code"] = status_code
            if error_type is not None:
                data["error_type"] = error_type
            self.client.add_breadcrumb(
                category="http",
                type="http",
                level="warning" if warning else "info",
                data=data,
            )
            if slow:
                self.client.capture_performance_issue(
                    "slow_external_http",
                    duration_ms=duration_ms,
                    http_host=target.host,
                    http_url=target.path,
                )
            count = self.tracker.record(
                f"{method.upper()}\n{target.host}\n{target.path}",
                now_ms=finished_at * 1_000.0,
            )
            if count == self.client.configuration.repeated_http_threshold:
                self.client.capture_performance_issue(
                    "repeated_http",
                    http_host=target.host,
                    http_url=target.path,
                    query_count=count,
                )
        except Exception:
            return
