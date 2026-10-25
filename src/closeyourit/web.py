"""Shared privacy-safe request lifecycle used by WSGI and ASGI integrations."""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from uuid import uuid4

from closeyourit.client import Client
from closeyourit.scope import current_scope
from closeyourit.usage import normalize_route

Monotonic = Callable[[], float]
RouteResolver = Callable[[], str | None]

_TRACE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z", re.ASCII)

_FORBIDDEN_HEADERS = frozenset(
    {
        "authorization",
        "cookie",
        "proxy-authorization",
        "set-cookie",
        "x-api-key",
    }
)


def _canonical_header(name: str) -> str:
    return "-".join(part.capitalize() for part in name.split("-"))


def _allowed_headers(values: Mapping[str, str], allowlist: Sequence[str]) -> dict[str, str]:
    normalized = {name.casefold(): value for name, value in values.items()}
    result: dict[str, str] = {}
    for requested in allowlist:
        key = requested.casefold()
        if key in _FORBIDDEN_HEADERS or key not in normalized:
            continue
        result[_canonical_header(requested)] = normalized[key]
    return result


def _trace_id(headers: Mapping[str, str]) -> str:
    value = headers.get("x-request-id", "").split(",", 1)[0].strip()
    return value if _TRACE_ID_PATTERN.fullmatch(value) is not None else str(uuid4())


def wsgi_request(
    environ: Mapping[str, object], allowlist: Sequence[str]
) -> tuple[dict[str, object], str]:
    headers: dict[str, str] = {}
    for key, value in environ.items():
        if key.startswith("HTTP_"):
            headers[key[5:].replace("_", "-").casefold()] = str(value)
        elif key in {"CONTENT_LENGTH", "CONTENT_TYPE"}:
            headers[key.replace("_", "-").casefold()] = str(value)

    scheme = str(environ.get("wsgi.url_scheme") or "http")
    host = str(environ.get("HTTP_HOST") or environ.get("SERVER_NAME") or "")
    port = str(environ.get("SERVER_PORT") or "")
    if (
        host
        and ":" not in host
        and port
        and not ((scheme == "http" and port == "80") or (scheme == "https" and port == "443"))
    ):
        host = f"{host}:{port}"
    path = f"{environ.get('SCRIPT_NAME') or ''}{environ.get('PATH_INFO') or ''}" or "/"
    request: dict[str, object] = {
        "method": str(environ.get("REQUEST_METHOD") or ""),
        "url": f"{scheme}://{host}{path}" if host else path,
        "path": path,
        "headers": _allowed_headers(headers, allowlist),
    }
    return request, _trace_id(headers)


def asgi_request(
    scope: Mapping[str, object], allowlist: Sequence[str]
) -> tuple[dict[str, object], str]:
    headers: dict[str, str] = {}
    raw_headers = scope.get("headers", ())
    if isinstance(raw_headers, Sequence):
        for pair in raw_headers:
            if not isinstance(pair, Sequence) or len(pair) != 2:
                continue
            raw_name, raw_value = pair
            if isinstance(raw_name, bytes) and isinstance(raw_value, bytes):
                headers[raw_name.decode("latin-1").casefold()] = raw_value.decode("latin-1")

    scheme = str(scope.get("scheme") or "http")
    server = scope.get("server")
    host = ""
    if isinstance(server, Sequence) and len(server) >= 2:
        hostname = str(server[0])
        raw_port = server[1]
        try:
            port = int(raw_port) if raw_port is not None else None
        except (TypeError, ValueError):
            port = None
        host = hostname
        if port is not None and not (
            (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
        ):
            host = f"{hostname}:{port}"
    path = str(scope.get("path") or "/")
    request: dict[str, object] = {
        "method": str(scope.get("method") or ""),
        "url": f"{scheme}://{host}{path}" if host else path,
        "path": path,
        "headers": _allowed_headers(headers, allowlist),
    }
    return request, _trace_id(headers)


def route_from_asgi(scope: Mapping[str, object]) -> str | None:
    route = scope.get("route")
    for attribute in ("path", "path_format"):
        value = getattr(route, attribute, None)
        if isinstance(value, str) and value:
            return value
    value = scope.get("closeyourit.route")
    return str(value) if value else None


@dataclass(slots=True)
class RequestLifecycle:
    client: Client
    request: dict[str, object]
    trace_id: str
    route_resolver: RouteResolver
    monotonic: Monotonic = time.monotonic
    started_at: float = field(init=False)
    status_code: int | None = None
    route: str | None = None
    finished: bool = False
    pending_error: BaseException | None = None
    pending_handled: bool = False

    def __post_init__(self) -> None:
        self.started_at = self.monotonic()

    def begin(self) -> None:
        current_scope().trace_id = self.trace_id
        current_scope().set_request(self.request)

    def set_status(self, value: object) -> None:
        try:
            self.status_code = int(str(value).split(" ", 1)[0])
        except (TypeError, ValueError):
            return
        self.request["status_code"] = self.status_code
        self._refresh_scope()

    def set_route(self, value: str | None) -> None:
        if value:
            self.route = value
            self.request["route"] = value
            self._refresh_scope()

    def set_exception(self, error: BaseException, *, handled: bool) -> None:
        self.pending_error = error
        self.pending_handled = handled

    def finish(self, error: BaseException | None = None, *, handled: bool = False) -> None:
        if self.finished:
            return
        if error is not None:
            self.set_exception(error, handled=handled)
        self.finished = True
        duration_ms = max(0.0, (self.monotonic() - self.started_at) * 1000.0)
        self.request["duration_ms"] = round(duration_ms, 2)
        if self.status_code is not None:
            self.request["status_code"] = self.status_code
        try:
            self.route = self.route or self.route_resolver()
        except Exception:
            self.route = self.route or None
        if self.route:
            self.request["route"] = self.route
            # Only the template resolved by the framework becomes a usage symbol: the concrete path
            # is a URL carrying the request identifiers, and it never enters the census.
            symbol = normalize_route(self.route)
            if symbol is not None:
                with suppress(Exception):
                    self.client.record_usage("route", symbol)
        self._refresh_scope()

        if self.pending_error is not None:
            pending_error = self.pending_error
            pending_handled = self.pending_handled
            self.pending_error = None
            with suppress(Exception):
                self.client.capture_exception(pending_error, handled=pending_handled)
        if duration_ms >= self.client.configuration.slow_request_threshold_ms:
            path = str(self.request.get("path") or "/")
            with suppress(Exception):
                self.client.capture_slow_request(
                    route=self.route or path,
                    path=path,
                    url=str(self.request.get("url") or path),
                    duration_ms=duration_ms,
                )

    def _refresh_scope(self) -> None:
        current_scope().set_request(self.request)
