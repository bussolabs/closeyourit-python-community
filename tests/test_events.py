from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from closeyourit import Configuration, current_scope
from closeyourit._version import __version__
from closeyourit.events import EventBuilder

NOW = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
EVENT_ID = UUID("12345678-1234-4234-8234-123456789abc")


def builder(configuration: Configuration | None = None) -> EventBuilder:
    return EventBuilder(
        configuration or Configuration(environment="production", release="v0.1.0"),
        clock=lambda: NOW,
        id_factory=lambda: EVENT_ID,
    )


def raise_nested_exception() -> BaseException:
    try:
        try:
            raise ValueError("token=private")
        except ValueError as cause:
            raise RuntimeError("operazione fallita") from cause
    except RuntimeError as error:
        return error


def test_exception_event_matches_the_shared_sentry_shape() -> None:
    current_scope().set_user({"id": "account-1", "email": "private@example.com"})
    current_scope().set_tag("tenant", "acme")
    current_scope().trace_id = "trace-1"

    payload = builder().exception(raise_nested_exception(), handled=True)

    assert payload["event_id"] == EVENT_ID.hex
    assert payload["timestamp"] == "2026-07-13T14:00:00Z"
    assert payload["platform"] == "python"
    assert payload["environment"] == "production"
    assert payload["release"] == "v0.1.0"
    assert payload["trace_id"] == "trace-1"
    assert payload["user"] == {"id": "account-1"}
    assert payload["tags"] == {"tenant": "acme"}

    values = payload["exception"]["values"]  # type: ignore[index]
    assert [value["type"] for value in values] == ["ValueError", "RuntimeError"]
    assert values[0]["value"] == "token=[FILTERED]"
    assert values[-1]["mechanism"] == {"type": "python", "handled": True}
    frame = values[-1]["stacktrace"]["frames"][-1]
    assert frame["filename"] == str(Path(__file__).resolve().relative_to(Path.cwd()))
    assert frame["abs_path"] == __file__
    assert frame["function"] == "raise_nested_exception"
    assert isinstance(frame["lineno"], int)


def test_exception_event_supports_exceptions_without_a_traceback_and_unicode() -> None:
    current_scope().set_context("request", {"route": "/orders/:id"})
    payload = builder().exception(RuntimeError("caffè ☕"), contexts={"custom": {"attempt": 2}})

    value = payload["exception"]["values"][-1]  # type: ignore[index]
    assert value["value"] == "caffè ☕"
    assert value["stacktrace"] == {"frames": []}
    assert payload["contexts"]["request"] == {"route": "/orders/:id"}  # type: ignore[index]
    assert payload["contexts"]["custom"] == {"attempt": 2}  # type: ignore[index]


def test_exception_event_scrubs_sensitive_keys_in_explicit_contexts() -> None:
    payload = builder().exception(
        RuntimeError("boom"),
        contexts={"auth": {"password": "hunter2", "nested": {"api_key": "k"}}, "safe": "ok"},
    )

    assert payload["contexts"]["auth"] == {  # type: ignore[index]
        "password": "[FILTERED]",
        "nested": {"api_key": "[FILTERED]"},
    }
    assert payload["contexts"]["safe"] == "ok"  # type: ignore[index]


def test_message_log_and_metric_builders_use_canonical_wire_fields() -> None:
    event_builder = builder()

    message = event_builder.message("password=hunter2", level="warning")
    log = event_builder.log(
        "token=secret",
        level="WARN",
        logger="billing",
        attributes={"email": "private@example.com", "attempt": 2},
    )
    metric = event_builder.slow_method("checkout.total", 12.345)

    assert message["message"] == {"formatted": "password=[FILTERED]"}
    assert message["level"] == "warning"
    assert log == {
        "event_id": EVENT_ID.hex,
        "timestamp": "2026-07-13T14:00:00Z",
        "level": "warning",
        "message": "token=[FILTERED]",
        "attributes": {"email": "[FILTERED]", "attempt": 2},
        "logger": "billing",
        "environment": "production",
        "release": "v0.1.0",
        "sdk": {"name": "closeyourit-python", "version": __version__},
    }
    assert metric == {
        "kind": "slow_method",
        "sample_id": str(EVENT_ID),
        "duration_ms": 12.35,
        "occurred_at": "2026-07-13T14:00:00Z",
        "environment": "production",
        "label": "checkout.total",
        "sdk": {"name": "closeyourit-python", "version": __version__},
    }


def test_unknown_log_level_falls_back_to_info() -> None:
    assert builder().log("hello", level="custom")["level"] == "info"


def test_sql_metric_builders_fingerprint_raw_sql_and_supply_canonical_durations() -> None:
    event_builder = builder()

    slow_query = event_builder.slow_query(
        "SELECT * FROM users WHERE email = 'private@example.test' AND id = 42 -- secret",
        12.345,
    )
    performance = event_builder.performance_issue(
        " n_plus_one ",
        sql="SELECT * FROM users WHERE token = 'private' AND id = :identifier",
        query_count=3,
    )

    assert slow_query["sql"] == "SELECT * FROM users WHERE email = ? AND id = ?"
    assert slow_query["duration_ms"] == 12.35
    assert performance["subtype"] == "n_plus_one"
    assert performance["sql"] == "SELECT * FROM users WHERE token = ? AND id = ?"
    assert performance["duration_ms"] == 0.0


@pytest.mark.parametrize("duration", [-1.0, float("nan"), float("inf"), float("-inf")])
def test_metric_builders_reject_negative_or_non_finite_durations(duration: float) -> None:
    event_builder = builder()

    with pytest.raises(ValueError, match="duration"):
        event_builder.slow_method("checkout", duration)
    with pytest.raises(ValueError, match="duration"):
        event_builder.slow_query("SELECT 1", duration)
    with pytest.raises(ValueError, match="duration"):
        event_builder.performance_issue("n_plus_one", duration_ms=duration)
    with pytest.raises(ValueError, match="duration"):
        event_builder.performance_issue("high_query_count", total_query_time_ms=duration)


@pytest.mark.parametrize("sql", ["", "   ", "-- comment only", "/* private */"])
def test_sql_metric_builders_reject_blank_or_structureless_sql(sql: str) -> None:
    event_builder = builder()

    with pytest.raises(ValueError, match="SQL"):
        event_builder.slow_query(sql, 1)
    with pytest.raises(ValueError, match="SQL"):
        event_builder.performance_issue("n_plus_one", sql=sql)


def test_performance_issue_rejects_blank_subtype() -> None:
    with pytest.raises(ValueError, match="subtype"):
        builder().performance_issue("   ")


def test_slow_method_rejects_a_blank_label() -> None:
    with pytest.raises(ValueError, match="label"):
        builder().slow_method("   ", 12.0)


def test_slow_request_builds_only_a_safe_canonical_url() -> None:
    payload = builder().slow_request(
        route="  /orders/{id}  ",
        path="/orders/42?token=private#fragment",
        url="https://user:password@example.test:8443/ignored?secret=value#fragment",
        duration_ms=12.345,
    )

    assert payload["route"] == "/orders/{id}"
    assert payload["http_host"] == "example.test"
    assert payload["http_path"] == "/orders/42"
    assert payload["http_url"] == "https://example.test:8443/orders/42"
    assert payload["duration_ms"] == 12.35


@pytest.mark.parametrize("route", ["", "   "])
def test_slow_request_rejects_blank_routes(route: str) -> None:
    with pytest.raises(ValueError, match="route"):
        builder().slow_request(route=route, path="/", url="https://example.test/", duration_ms=1)


@pytest.mark.parametrize("duration", [-0.01, float("nan"), float("inf"), float("-inf")])
def test_slow_request_rejects_invalid_durations(duration: float) -> None:
    with pytest.raises(ValueError, match="duration"):
        builder().slow_request(
            route="/orders/{id}",
            path="/orders/42",
            url="https://example.test/orders/42",
            duration_ms=duration,
        )


@pytest.mark.parametrize(
    "url",
    ["/relative", "ftp://example.test/path", "https://example.test:invalid/path"],
)
def test_slow_request_rejects_invalid_base_urls(url: str) -> None:
    with pytest.raises(ValueError, match="url"):
        builder().slow_request(route="/orders/{id}", path="/orders/42", url=url, duration_ms=1)


def test_message_and_log_reject_blank_wire_values() -> None:
    with pytest.raises(ValueError, match="message"):
        builder().message("")
    with pytest.raises(ValueError, match="message"):
        builder().log("   ")


def test_external_stack_frames_use_the_basename_and_are_not_in_app() -> None:
    namespace: dict[str, object] = {}
    source = "def fail():\n    raise RuntimeError('boom')"
    exec(compile(source, "/tmp/external.py", "exec"), namespace)
    try:
        namespace["fail"]()  # type: ignore[operator]
    except RuntimeError as error:
        payload = builder().exception(error)

    frames = payload["exception"]["values"][-1]["stacktrace"]["frames"]  # type: ignore[index]
    assert frames[-1]["filename"] == "external.py"
    assert frames[-1]["in_app"] is False


def test_message_and_exception_events_never_carry_the_value_after_bearer() -> None:
    """CYPY-15. An access header quoted in an error or a message does not reach the wire.

    The placeholder is not a real credential: it only shows that the value separated
    by the space disappears together with the scheme, and that the surrounding diagnosis stays
    readable.
    """
    credential = "EXAMPLE_NOT_A_CREDENTIAL"
    event_builder = builder()

    message = event_builder.message(f"Authorization: Bearer {credential}", level="warning")
    exception = event_builder.exception(
        ValueError(f"401 on /orders with Authorization: Bearer {credential}"), handled=True
    )

    assert message["message"] == {"formatted": "Authorization=[FILTERED]"}
    value = exception["exception"]["values"][0]["value"]  # type: ignore[index]
    assert credential not in value
    assert value.startswith("401 on /orders with")


def test_query_metrics_preserve_configured_release_and_omit_absent_release() -> None:
    configured = builder(Configuration(environment="production", release="app-1.2.3"))
    for payload in (
        configured.slow_query("SELECT 1", 1),
        configured.performance_issue("n_plus_one", sql="SELECT 1", query_count=3),
    ):
        assert payload["release"] == "app-1.2.3"
        assert payload["sdk"]["version"] == __version__  # type: ignore[index]
    absent = builder(Configuration(environment="production"))
    assert "release" not in absent.slow_query("SELECT 1", 1)
    assert "release" not in absent.performance_issue("high_query_count", query_count=3)


@pytest.mark.parametrize(
    "release,expected",
    [(None, None), ("app-1.2.3", "app-1.2.3"), ("token=private", "token=[FILTERED]")],
)
def test_request_metrics_include_only_scrubbed_configured_release(
    release: str | None, expected: str | None
) -> None:
    payload = builder(Configuration(release=release)).slow_request(
        route="/items/{id}", path="/items/1", url="https://test/items/1", duration_ms=1
    )
    assert payload.get("release") == expected
