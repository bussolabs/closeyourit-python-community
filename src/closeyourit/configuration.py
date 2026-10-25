"""Immutable, fail-safe configuration for the CloseYourIt client."""

from __future__ import annotations

import os
import re
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Literal, TypeAlias
from urllib.parse import urlsplit
from uuid import UUID

from closeyourit._diagnostics import logger
from closeyourit.models import BeforeSend, EventPayload, FilterParameter

DisabledReason: TypeAlias = Literal[
    "missing_endpoint_url",
    "missing_token",
    "missing_project_id",
    "invalid_endpoint_url",
    "insecure_endpoint_in_production",
    "invalid_token",
    "invalid_project_id",
]


_TRUTHY = frozenset({"1", "on", "true", "yes"})
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _optional_environment(name: str) -> str | None:
    return os.environ.get(name)


def _flag_environment(name: str) -> bool:
    value = os.environ.get(name)
    return value is not None and value.strip().casefold() in _TRUTHY


def _environment() -> str:
    return os.environ.get("CLOSEYOURIT_ENVIRONMENT", "development")


# A semver tag (with optional `v`) is preferred over the short SHA as the release: it converges with
# what CI records (it tags), while the short SHA creates duplicate releases on the backend.
_SEMVER_TAG = re.compile(r"\Av?\d+\.\d+\.\d+([-+.].+)?\Z")


def _detect_tag() -> str | None:
    """Semver tag from APP_GIT_TAG then GIT_TAG: accepts only a non-blank value in semver form

    (optional `v` + MAJOR.MINOR.PATCH + optional suffix). Discards empty, `unknown`, branch
    names, etc. → None, so detection falls through to the short SHA chain. The preference between
    the two variables is by presence, as in CloseYourIt-Ruby: if APP_GIT_TAG is defined GIT_TAG is
    not looked at. Permissive regex on the suffix, validation and returning the raw value mirror the
    Ruby library 1:1 (intended cross-SDK parity): a matching tag has no surrounding spaces by
    construction (`\\A...\\Z`) and the value is normalized downstream by `_clean_optional` anyway.
    """
    tag = os.environ.get("APP_GIT_TAG")
    if tag is None:
        tag = os.environ.get("GIT_TAG")
    if tag is None or not tag.strip():
        return None
    return tag if _SEMVER_TAG.match(tag) else None


def _detect_release() -> str | None:
    """Automatic release detection, aligned with CloseYourIt-Ruby: first a semver tag, then

    the short SHA from the deploy/CI env. No git subprocess: there is no `.git` in the container and
    it would make the tests non-deterministic.
    """
    return (
        _detect_tag()
        or _optional_environment("KAMAL_VERSION")
        or _optional_environment("GIT_SHA")
        or _optional_environment("GIT_REVISION")
        or _optional_environment("SOURCE_VERSION")
        or _optional_environment("HEROKU_SLUG_COMMIT")
    )


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


@dataclass(frozen=True, slots=True)
class Configuration:
    """Validated settings; invalid or incomplete values make the SDK a no-op."""

    endpoint_url: str | None = field(
        default_factory=lambda: _optional_environment("CLOSEYOURIT_ENDPOINT_URL")
    )
    token: str | None = field(default_factory=lambda: _optional_environment("CLOSEYOURIT_TOKEN"))
    project_id: str | None = field(
        default_factory=lambda: _optional_environment("CLOSEYOURIT_PROJECT_ID")
    )
    environment: str = field(default_factory=_environment)
    release: str | None = field(
        default_factory=lambda: _optional_environment("CLOSEYOURIT_RELEASE") or _detect_release()
    )
    send_pii: bool = False
    filter_parameters: tuple[FilterParameter, ...] = ()
    before_send: BeforeSend | None = None
    sample_rate: float = 1.0
    logs_sample_rate: float = 1.0
    breadcrumbs_enabled: bool = True
    max_breadcrumbs: int = 100
    slow_method_threshold_ms: float = 500.0
    slow_request_threshold_ms: float = 1000.0
    request_header_allowlist: tuple[str, ...] = (
        "Accept",
        "Content-Type",
        "User-Agent",
    )
    slow_query_threshold_ms: float = 100.0
    slow_external_threshold_ms: float = 1_000.0
    repeated_http_threshold: int = 5
    repeated_http_window_ms: float = 5_000.0
    n_plus_one_threshold: int = 10
    query_count_threshold: int = 100
    http_capture_hosts: tuple[str, ...] = ()
    trace_propagation_enabled: bool = False
    trace_propagation_hosts: tuple[str, ...] = ()
    # Opt-in usage telemetry (CYPY-13): an extra channel does not switch itself on in
    # an application that upgraded the SDK without asking for it.
    usage_enabled: bool = field(
        default_factory=lambda: _flag_environment("CLOSEYOURIT_USAGE_ENABLED")
    )
    usage_flush_interval: float = 300.0
    usage_max_symbols: int = 2000

    def __post_init__(self) -> None:
        endpoint = _clean_optional(self.endpoint_url)
        object.__setattr__(self, "endpoint_url", endpoint.rstrip("/") if endpoint else None)
        object.__setattr__(self, "token", _clean_optional(self.token))
        object.__setattr__(self, "project_id", _clean_optional(self.project_id))
        environment = self.environment.strip().casefold() or "development"
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "release", _clean_optional(self.release))
        object.__setattr__(self, "filter_parameters", tuple(self.filter_parameters))
        object.__setattr__(self, "sample_rate", min(1.0, max(0.0, float(self.sample_rate))))
        object.__setattr__(
            self, "logs_sample_rate", min(1.0, max(0.0, float(self.logs_sample_rate)))
        )
        object.__setattr__(self, "max_breadcrumbs", max(0, int(self.max_breadcrumbs)))
        object.__setattr__(
            self, "slow_method_threshold_ms", max(0.0, float(self.slow_method_threshold_ms))
        )
        object.__setattr__(
            self, "slow_request_threshold_ms", max(0.0, float(self.slow_request_threshold_ms))
        )
        object.__setattr__(
            self,
            "request_header_allowlist",
            tuple(
                dict.fromkeys(
                    name.strip() for name in self.request_header_allowlist if name.strip()
                )
            ),
        )
        object.__setattr__(
            self, "slow_query_threshold_ms", max(0.0, float(self.slow_query_threshold_ms))
        )
        object.__setattr__(
            self,
            "slow_external_threshold_ms",
            max(0.0, float(self.slow_external_threshold_ms)),
        )
        object.__setattr__(
            self, "repeated_http_threshold", max(1, int(self.repeated_http_threshold))
        )
        object.__setattr__(
            self, "repeated_http_window_ms", max(0.0, float(self.repeated_http_window_ms))
        )
        object.__setattr__(self, "n_plus_one_threshold", max(0, int(self.n_plus_one_threshold)))
        object.__setattr__(self, "query_count_threshold", max(0, int(self.query_count_threshold)))
        object.__setattr__(
            self,
            "http_capture_hosts",
            tuple(host.strip().casefold() for host in self.http_capture_hosts if host.strip()),
        )
        object.__setattr__(
            self,
            "trace_propagation_hosts",
            tuple(host.strip().casefold() for host in self.trace_propagation_hosts if host.strip()),
        )
        object.__setattr__(self, "usage_enabled", bool(self.usage_enabled))
        interval = float(self.usage_flush_interval)
        object.__setattr__(self, "usage_flush_interval", interval if interval > 0 else 300.0)
        object.__setattr__(self, "usage_max_symbols", max(0, int(self.usage_max_symbols)))
        self._warn_about_plain_http()

    def _warn_about_plain_http(self) -> None:
        """Plain http is rejected in production; elsewhere it is tolerated only on loopback.

        A remote host over http sends the token and the events in clear, so an environment
        that merely is not called "production" still gets a warning on the diagnostic logger.
        """
        if self.production or self.endpoint_url is None:
            return
        endpoint = urlsplit(self.endpoint_url)
        if endpoint.scheme != "http" or endpoint.hostname in _LOOPBACK_HOSTS:
            return
        logger.warning(
            "CloseYourIt endpoint %s uses plain http on a remote host: "
            "token and events travel unencrypted, use https",
            self.endpoint_url,
        )

    @property
    def production(self) -> bool:
        return self.environment == "production"

    @property
    def enabled(self) -> bool:
        return self.disabled_reason is None

    @property
    def disabled_reason(self) -> DisabledReason | None:
        if self.endpoint_url is None:
            return "missing_endpoint_url"
        if self.token is None:
            return "missing_token"
        if self.project_id is None:
            return "missing_project_id"

        endpoint = urlsplit(self.endpoint_url)
        if (
            endpoint.scheme not in {"http", "https"}
            or endpoint.hostname is None
            or endpoint.username is not None
            or endpoint.password is not None
        ):
            return "invalid_endpoint_url"
        if self.production and endpoint.scheme != "https":
            return "insecure_endpoint_in_production"
        if not self.token.startswith("cyi_"):
            return "invalid_token"
        try:
            if str(UUID(self.project_id)) != self.project_id.casefold():
                return "invalid_project_id"
        except ValueError:
            return "invalid_project_id"

        return None

    def apply_before_send(self, event: EventPayload) -> EventPayload | None:
        """Run the consumer hook on a copy; callback failures safely drop the event."""
        try:
            payload = deepcopy(event)
            if self.before_send is None:
                return payload
            result = self.before_send(payload)
            return deepcopy(result) if isinstance(result, dict) else None
        except Exception:
            return None
