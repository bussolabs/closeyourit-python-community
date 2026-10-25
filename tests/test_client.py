from __future__ import annotations

import logging
import math
from collections.abc import Callable
from typing import TypeVar, cast

import pytest

from closeyourit import Client, Configuration, Transport, current_scope
from closeyourit.client import Channel

T = TypeVar("T")


class RecordingSink:
    def __init__(self) -> None:
        self.items: list[tuple[Channel, dict[str, object]]] = []

    def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
        self.items.append((channel, payload))
        return True


def valid_configuration(**overrides: object) -> Configuration:
    values: dict[str, object] = {
        "endpoint_url": "https://www.closeyour.it",
        "token": "cyi_server-token",
        "project_id": "12345678-1234-1234-1234-123456789abc",
        "environment": "production",
    }
    values.update(overrides)
    return Configuration(**values)  # type: ignore[arg-type]


def client(
    *,
    configuration: Configuration | None = None,
    random_value: Callable[[], float] = lambda: 0.5,
    monotonic_values: list[float] | None = None,
) -> tuple[Client, RecordingSink]:
    sink = RecordingSink()
    values = iter(monotonic_values or [0.0, 0.0])
    instance = Client(
        configuration or valid_configuration(),
        sink=sink,
        random_value=random_value,
        monotonic=lambda: next(values),
    )
    return instance, sink


def test_capture_exception_message_and_log_enqueue_the_expected_channels() -> None:
    instance, sink = client()

    assert instance.capture_exception(RuntimeError("boom"), handled=True) is not None
    assert instance.capture_message("warning", level="warning") is not None
    assert instance.log("created", logger="billing", order_id=7) is not None

    assert [channel for channel, _payload in sink.items] == ["events", "events", "logs"]
    assert sink.items[-1][1]["attributes"] == {"order_id": 7}


@pytest.mark.parametrize(
    ("sample_rate", "random_value", "accepted"),
    [(0.0, 0.0, False), (1.0, 0.999, True)],
)
def test_sampling_boundaries_are_deterministic(
    sample_rate: float, random_value: float, accepted: bool
) -> None:
    instance, sink = client(
        configuration=valid_configuration(sample_rate=sample_rate),
        random_value=lambda: random_value,
    )

    result = instance.capture_message("sample me")

    assert (result is not None) is accepted
    assert bool(sink.items) is accepted


def test_exception_and_log_sampling_can_be_disabled_independently() -> None:
    instance, sink = client(configuration=valid_configuration(sample_rate=0, logs_sample_rate=0))

    assert instance.capture_exception(RuntimeError("ignored")) is None
    assert instance.log("ignored") is None
    assert sink.items == []


def test_disabled_configuration_and_before_send_drop_are_noops() -> None:
    disabled, disabled_sink = client(configuration=Configuration())
    dropping, dropping_sink = client(
        configuration=valid_configuration(before_send=lambda _payload: None)
    )

    assert disabled.capture_message("ignored") is None
    assert dropping.capture_message("ignored") is None
    assert disabled_sink.items == []
    assert dropping_sink.items == []


def test_breadcrumbs_keep_zero_one_and_last_n_items_and_scrub_data() -> None:
    instance, _sink = client(configuration=valid_configuration(max_breadcrumbs=2))

    assert current_scope().snapshot().to_event_data() == {}
    instance.add_breadcrumb(message="first", data={"token": "one"})
    instance.add_breadcrumb(message="second", category="query", data={"token": "two"})
    instance.add_breadcrumb(message="third")

    payload = instance.builder.message("boom")
    values = payload["breadcrumbs"]["values"]  # type: ignore[index]
    assert [item["message"] for item in values] == ["second", "third"]
    assert values[0]["data"] == {"token": "[FILTERED]"}


def test_disabled_or_zero_capacity_breadcrumbs_are_noops() -> None:
    disabled, _sink = client(configuration=valid_configuration(breadcrumbs_enabled=False))
    zero_capacity, _sink = client(configuration=valid_configuration(max_breadcrumbs=0))

    disabled.add_breadcrumb(message="disabled")
    zero_capacity.add_breadcrumb(message="zero")

    assert current_scope().snapshot().to_event_data() == {}


def test_minimal_breadcrumb_omits_optional_fields() -> None:
    instance, _sink = client()

    instance.add_breadcrumb()

    values = instance.builder.message("boom")["breadcrumbs"]["values"]  # type: ignore[index]
    assert values == [
        {
            "timestamp": values[0]["timestamp"],
            "type": "default",
            "level": "info",
        }
    ]


def test_default_sink_and_disabled_measure_are_noops() -> None:
    without_sink = Client(Configuration())
    disabled, sink = client(
        configuration=Configuration(slow_method_threshold_ms=0),
        monotonic_values=[1.0, 1.1],
    )

    assert without_sink.capture_message("ignored") is None
    assert without_sink.flush() is True
    assert without_sink.close() is True
    assert disabled.measure("disabled", lambda: 42) == 42
    assert sink.items == []


def test_valid_client_builds_the_production_transport_lazily() -> None:
    instance = Client(valid_configuration())

    assert isinstance(instance.sink, Transport)
    assert instance.sink.stats.worker_alive is False
    assert instance.close() is True


def test_disabled_configuration_logs_one_diagnostic_warning_with_the_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        Client(valid_configuration(token="missing-prefix"))

    warnings = [record for record in caplog.records if record.name == "closeyourit"]
    assert len(warnings) == 1
    assert warnings[0].levelno == logging.WARNING
    assert "invalid_token" in warnings[0].getMessage()


def test_valid_configuration_does_not_log_a_disabled_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        instance = Client(valid_configuration())
        assert instance.close() is True

    assert [record for record in caplog.records if record.name == "closeyourit"] == []


def test_client_flush_and_close_delegate_when_supported() -> None:
    class ManagedSink(RecordingSink):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[tuple[str, float | None]] = []

        def flush(self, timeout: float | None = None) -> bool:
            self.calls.append(("flush", timeout))
            return True

        def close(self, timeout: float | None = None) -> bool:
            self.calls.append(("close", timeout))
            return True

    sink = ManagedSink()
    instance = Client(valid_configuration(), sink=sink)

    assert instance.flush(0.5) is True
    assert instance.close(1.0) is True
    assert sink.calls == [("flush", 0.5), ("close", 1.0)]


@pytest.mark.parametrize(("duration_ms", "expected_count"), [(9.99, 0), (10.0, 1), (10.01, 1)])
def test_measure_reports_at_the_threshold_boundary(duration_ms: float, expected_count: int) -> None:
    instance, sink = client(
        configuration=valid_configuration(slow_method_threshold_ms=10.0),
        monotonic_values=[1.0, 1.0 + duration_ms / 1000],
    )

    result = instance.measure("checkout.total", lambda: 42)

    assert result == 42
    assert len(sink.items) == expected_count
    if sink.items:
        assert sink.items[0][0] == "metrics"


def test_measure_preserves_application_exceptions_and_still_records_duration() -> None:
    instance, sink = client(
        configuration=valid_configuration(slow_method_threshold_ms=10.0),
        monotonic_values=[1.0, 1.02],
    )

    def fail() -> int:
        raise RuntimeError("application failure")

    with pytest.raises(RuntimeError, match="application failure"):
        instance.measure("checkout.failure", fail)

    assert [channel for channel, _payload in sink.items] == ["metrics"]


def test_measure_rejects_a_blank_label_before_running_the_operation() -> None:
    called = False

    def operation() -> None:
        nonlocal called
        called = True

    instance, _sink = client()

    with pytest.raises(ValueError, match="label"):
        instance.measure("  ", operation)

    assert called is False


def test_capture_duration_coexists_with_other_metric_facades_and_scrubs_contexts() -> None:
    instance, sink = client(configuration=valid_configuration(release="v2.4.0"))

    identifier = instance.capture_duration(
        "celery.task.duration.checkout",
        12.345,
        contexts={"celery": {"task_name": "checkout", "token": "secret"}},
    )
    instance.capture_slow_query("SELECT 1", 5.0)
    instance.capture_performance_issue("repeated_http", query_count=3)

    assert identifier is not None
    assert [channel for channel, _payload in sink.items] == ["metrics", "metrics", "metrics"]
    duration = sink.items[0][1]
    assert duration["duration_ms"] == 12.35
    assert duration["release"] == "v2.4.0"
    assert duration["contexts"] == {"celery": {"task_name": "checkout", "token": "[FILTERED]"}}


@pytest.mark.parametrize(
    "duration_ms",
    [math.nan, math.inf, -math.inf, cast(float, object()), cast(float, "not-a-number")],
)
def test_capture_duration_drops_non_finite_or_non_convertible_values(
    duration_ms: float,
) -> None:
    instance, sink = client()

    assert instance.capture_duration("celery.task.invalid", duration_ms) is None
    assert sink.items == []


def test_capture_duration_clamps_a_negative_clock_delta_to_zero() -> None:
    instance, sink = client()

    assert instance.capture_duration("celery.task.clock-skew", -0.01) is not None
    assert sink.items[0][1]["duration_ms"] == 0.0
