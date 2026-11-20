from __future__ import annotations

import logging
import math
from collections.abc import Callable, Iterator
from contextlib import contextmanager
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


def test_measure_runs_the_operation_unmeasured_when_the_label_is_blank(
    caplog: pytest.LogCaptureFixture,
) -> None:
    instance, sink = client(configuration=valid_configuration(slow_method_threshold_ms=0))

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        result = instance.measure("  ", lambda: 42)

    assert result == 42
    assert sink.items == []
    assert "label" in closeyourit_warnings(caplog)[0]


def test_measure_with_a_blank_label_raises_the_application_error_unchained() -> None:
    instance, sink = client(configuration=valid_configuration(slow_method_threshold_ms=0))
    original = RuntimeError("application failure")

    def fail() -> int:
        raise original

    with pytest.raises(RuntimeError) as raised:
        instance.measure("  ", fail)

    assert raised.value is original
    assert raised.value.__context__ is None
    assert sink.items == []


def test_measure_keeps_the_application_error_when_recording_the_duration_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingSink:
        def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
            raise OSError("sink unavailable")

    ticks = iter([1.0])

    def clock() -> float:
        return next(ticks)

    broken_clock = Client(
        valid_configuration(slow_method_threshold_ms=0), sink=RecordingSink(), monotonic=clock
    )
    broken_sink = Client(
        valid_configuration(slow_method_threshold_ms=0),
        sink=FailingSink(),
        monotonic=lambda: 1.0,
    )
    original = RuntimeError("application failure")

    def fail() -> int:
        raise original

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        for instance in (broken_clock, broken_sink):
            with pytest.raises(RuntimeError) as raised:
                instance.measure("checkout.failure", fail)
            assert raised.value is original

    warnings = closeyourit_warnings(caplog)
    assert any("StopIteration" in message for message in warnings)
    assert any("sink unavailable" in message for message in warnings)


class UnprintableError(RuntimeError):
    def __str__(self) -> str:
        raise ValueError("text unavailable")


class Unprintable:
    def __str__(self) -> str:
        raise ValueError("text unavailable")


def closeyourit_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "closeyourit" and record.levelno == logging.WARNING
    ]


def test_blank_messages_and_logs_are_dropped_with_a_warning_instead_of_raising(
    caplog: pytest.LogCaptureFixture,
) -> None:
    instance, sink = client()

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        assert instance.capture_message("   ") is None
        assert instance.log("") is None

    assert sink.items == []
    warnings = closeyourit_warnings(caplog)
    assert len(warnings) == 2
    assert "capture_message" in warnings[0]
    assert "message must not be blank" in warnings[0]
    assert "log" in warnings[1]


def test_an_error_with_unreadable_text_is_sent_with_a_fallback_text() -> None:
    instance, sink = client()

    assert instance.capture_exception(UnprintableError()) is not None
    assert instance.capture_message(Unprintable()) is not None

    exception_payload = sink.items[0][1]
    values = exception_payload["exception"]["values"]  # type: ignore[index]
    assert values[-1]["type"] == "UnprintableError"
    assert values[-1]["value"] == "<unprintable UnprintableError object>"
    message_payload = sink.items[1][1]
    assert message_payload["message"] == {"formatted": "<unprintable Unprintable object>"}


def test_a_failing_sink_is_reported_as_a_warning_instead_of_raising(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingSink:
        def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
            raise OSError("sink unavailable")

    instance = Client(valid_configuration(), sink=FailingSink(), random_value=lambda: 0.5)

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        assert instance.capture_exception(RuntimeError("boom")) is None
        assert instance.capture_message("hello") is None
        assert instance.log("hello") is None

    assert len(closeyourit_warnings(caplog)) == 3


def test_an_internal_failure_with_unreadable_text_is_still_reported(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingSink:
        def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
            raise UnprintableError()

    instance = Client(valid_configuration(), sink=FailingSink(), random_value=lambda: 0.5)

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        assert instance.capture_message("hello") is None

    assert closeyourit_warnings(caplog) == [
        "closeyourit dropped capture_message: UnprintableError: <unprintable>"
    ]


class RaisingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        raise RuntimeError("application log filter failure")


class RaisingHandler(logging.Handler):
    def handle(self, record: logging.LogRecord) -> bool:
        raise RuntimeError("application log handler failure")


@pytest.mark.parametrize("hook", ["filter", "handler"])
def test_a_failing_application_log_hook_never_turns_a_drop_into_an_error(hook: str) -> None:
    diagnostics = logging.getLogger("closeyourit")
    raising_filter = RaisingFilter()
    raising_handler = RaisingHandler()
    if hook == "filter":
        diagnostics.addFilter(raising_filter)
    else:
        diagnostics.addHandler(raising_handler)

    class FailingSink:
        def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
            raise OSError("sink unavailable")

    instance, sink = client(configuration=valid_configuration(slow_method_threshold_ms=0))
    broken_sink = Client(
        valid_configuration(slow_method_threshold_ms=0),
        sink=FailingSink(),
        monotonic=lambda: 1.0,
    )
    original = RuntimeError("application failure")

    def fail() -> int:
        raise original

    try:
        assert instance.capture_message("  ") is None
        assert instance.log("") is None
        assert instance.capture_duration("job", math.nan) is None
        assert instance.measure("  ", lambda: 42) == 42
        assert broken_sink.capture_message("hello") is None
        with pytest.raises(RuntimeError) as raised:
            broken_sink.measure("checkout.failure", fail)
        assert raised.value is original
    finally:
        diagnostics.removeFilter(raising_filter)
        diagnostics.removeHandler(raising_handler)

    assert sink.items == []


APPLICATION_LOG_HOOKS = ["none", "filter", "handler"]


@contextmanager
def application_log_hook(hook: str) -> Iterator[None]:
    """Install the application's log hook on the SDK logger and always restore logging after."""
    diagnostics = logging.getLogger("closeyourit")
    filters = list(diagnostics.filters)
    handlers = list(diagnostics.handlers)
    if hook == "filter":
        diagnostics.addFilter(RaisingFilter())
    elif hook == "handler":
        diagnostics.addHandler(RaisingHandler())
    try:
        yield
    finally:
        diagnostics.filters[:] = filters
        diagnostics.handlers[:] = handlers


@pytest.mark.parametrize("hook", APPLICATION_LOG_HOOKS)
def test_blank_and_whitespace_texts_are_dropped_and_valid_calls_still_work(hook: str) -> None:
    instance, sink = client()

    with application_log_hook(hook):
        assert instance.capture_message("") is None
        assert instance.capture_message("   ") is None
        assert instance.capture_message(" \n\t ") is None
        assert instance.log("") is None
        assert instance.log("   ", logger="billing", order_id=7) is None
        assert sink.items == []

        assert instance.capture_message("hello") is not None
        assert instance.log("created", logger="billing") is not None
        assert instance.capture_exception(RuntimeError("boom")) is not None

    assert [channel for channel, _payload in sink.items] == ["events", "logs", "events"]


@pytest.mark.parametrize("hook", APPLICATION_LOG_HOOKS)
def test_unreadable_texts_never_raise_and_valid_calls_still_work(hook: str) -> None:
    instance, sink = client()

    with application_log_hook(hook):
        assert instance.capture_exception(UnprintableError()) is not None
        assert instance.capture_message(Unprintable()) is not None
        assert instance.log(UnprintableError()) is not None
        assert instance.capture_message("hello") is not None

    exception_values = sink.items[0][1]["exception"]["values"]  # type: ignore[index]
    assert exception_values[-1]["value"] == "<unprintable UnprintableError object>"
    assert sink.items[1][1]["message"] == {"formatted": "<unprintable Unprintable object>"}
    assert sink.items[2][1]["message"] == "<unprintable UnprintableError object>"
    assert sink.items[3][1]["message"] == {"formatted": "hello"}


@pytest.mark.parametrize("hook", APPLICATION_LOG_HOOKS)
def test_unusable_payloads_are_never_enqueued_even_when_the_drop_warning_fails(
    hook: str,
) -> None:
    instance, sink = client()

    with application_log_hook(hook):
        assert instance.capture_exception(cast(BaseException, Unprintable())) is None
        assert instance.capture_exception(RuntimeError("x"), level=cast(str, Unprintable())) is None
        assert instance.capture_message("hello", level=cast(str, Unprintable())) is None
        assert sink.items == []

        assert instance.capture_message("hello") is not None

    assert len(sink.items) == 1


@pytest.mark.parametrize("hook", APPLICATION_LOG_HOOKS)
def test_an_unreadable_internal_failure_never_raises_through_a_failing_log_hook(
    hook: str,
) -> None:
    class FailingSink:
        def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
            raise UnprintableError()

    instance = Client(valid_configuration(), sink=FailingSink(), random_value=lambda: 0.5)

    with application_log_hook(hook):
        assert instance.capture_exception(RuntimeError("boom")) is None
        assert instance.capture_message("hello") is None
        assert instance.log("hello") is None


def raised_with_cause(error: BaseException, cause: BaseException) -> BaseException:
    try:
        try:
            raise cause
        except BaseException as inner:
            raise error from inner
    except BaseException as outer:
        return outer


def raised_with_context(error: BaseException, context: BaseException) -> BaseException:
    try:
        try:
            raise context
        except BaseException:
            # The implicit context chain, not an explicit cause, is what is under test.
            raise error  # noqa: B904
    except BaseException as outer:
        return outer


@pytest.mark.parametrize("hook", APPLICATION_LOG_HOOKS)
def test_unreadable_texts_in_the_causal_chain_are_sent_with_a_fallback_text(hook: str) -> None:
    instance, sink = client()
    errors = [
        raised_with_cause(RuntimeError("outer"), UnprintableError()),
        raised_with_cause(UnprintableError(), KeyError("inner")),
        raised_with_context(RuntimeError("outer"), UnprintableError()),
    ]

    with application_log_hook(hook):
        for error in errors:
            assert instance.capture_exception(error) is not None

    texts: list[list[tuple[object, object]]] = []
    for _channel, payload in sink.items:
        values = cast(dict[str, list[dict[str, object]]], payload["exception"])["values"]
        texts.append([(value["type"], value["value"]) for value in values])
    assert texts == [
        [("UnprintableError", "<unprintable UnprintableError object>"), ("RuntimeError", "outer")],
        [("KeyError", "'inner'"), ("UnprintableError", "<unprintable UnprintableError object>")],
        [("UnprintableError", "<unprintable UnprintableError object>"), ("RuntimeError", "outer")],
    ]


UNUSABLE_TEXT_CALLS: list[tuple[str, Callable[[Client], object]]] = [
    ("message_empty", lambda c: c.capture_message("")),
    ("message_blank", lambda c: c.capture_message(" \n\t ")),
    ("message_unprintable", lambda c: c.capture_message(Unprintable())),
    ("log_empty", lambda c: c.log("")),
    ("log_blank", lambda c: c.log("   ", logger="billing", order_id=7)),
    ("log_unprintable", lambda c: c.log(Unprintable())),
    ("exception_unprintable", lambda c: c.capture_exception(UnprintableError())),
    (
        "exception_unprintable_cause",
        lambda c: c.capture_exception(raised_with_cause(RuntimeError("x"), UnprintableError())),
    ),
]


@pytest.mark.parametrize(
    "call",
    [call for _name, call in UNUSABLE_TEXT_CALLS],
    ids=[name for name, _call in UNUSABLE_TEXT_CALLS],
)
def test_unusable_texts_are_silent_noops_when_disabled_or_sampled_out(
    call: Callable[[Client], object], caplog: pytest.LogCaptureFixture
) -> None:
    configurations = [
        valid_configuration(token=None),
        valid_configuration(sample_rate=0, logs_sample_rate=0),
    ]

    for configuration in configurations:
        instance, sink = client(configuration=configuration)
        for hook in APPLICATION_LOG_HOOKS:
            with application_log_hook(hook):
                assert call(instance) is None
        assert sink.items == []

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        caplog.clear()
        for configuration in configurations:
            call(client(configuration=configuration)[0])
    # Disabled or sampled out, nothing is built: no payload, so nothing to drop or warn about.
    assert not any("dropped" in message for message in closeyourit_warnings(caplog))


HOSTILE_CALLS: list[tuple[str, Callable[[Client], object], bool]] = [
    ("exception_none", lambda c: c.capture_exception(cast(BaseException, None)), True),
    (
        "exception_contexts",
        lambda c: c.capture_exception(RuntimeError("x"), contexts=cast(dict[str, object], 42)),
        True,
    ),
    (
        "exception_level",
        lambda c: c.capture_exception(RuntimeError("x"), level=cast(str, Unprintable())),
        True,
    ),
    ("message_empty", lambda c: c.capture_message(""), True),
    ("message_blank", lambda c: c.capture_message(" \n\t "), True),
    ("log_empty", lambda c: c.log(""), True),
    ("log_blank", lambda c: c.log("   ", logger="billing", order_id=7), True),
    ("breadcrumb_data", lambda c: c.add_breadcrumb(data=cast(dict[str, object], 42)), True),
    ("measure_blank", lambda c: c.measure("", lambda: "result"), True),
    ("measure_none", lambda c: c.measure(cast(str, None), lambda: "result"), True),
    ("duration_blank", lambda c: c.capture_duration("  ", 5.0), True),
    ("duration_none", lambda c: c.capture_duration(cast(str, None), 5.0), True),
    ("duration_nan", lambda c: c.capture_duration("job", math.nan), True),
    ("duration_text", lambda c: c.capture_duration("job", cast(float, "slow")), True),
    (
        "duration_contexts",
        lambda c: c.capture_duration("job", 5.0, contexts=cast(dict[str, object], 42)),
        True,
    ),
    (
        "slow_request_blank_route",
        lambda c: c.capture_slow_request(
            route="", path="/", url="https://example.test/", duration_ms=1.0
        ),
        True,
    ),
    (
        "slow_request_negative_duration",
        lambda c: c.capture_slow_request(
            route="/", path="/", url="https://example.test/", duration_ms=-1.0
        ),
        True,
    ),
    ("slow_query_blank", lambda c: c.capture_slow_query("", 1.0), True),
    ("slow_query_nan", lambda c: c.capture_slow_query("SELECT 1", math.nan), True),
    ("performance_blank", lambda c: c.capture_performance_issue(""), True),
    (
        "performance_count",
        lambda c: c.capture_performance_issue("n_plus_one", query_count=cast(int, "many")),
        True,
    ),
    ("usage_unprintable", lambda c: c.record_usage("custom", cast(str, Unprintable())), True),
    ("usage_unknown_kind", lambda c: c.record_usage("unknown", "checkout"), False),
    ("used_empty", lambda c: c.used(""), False),
]


@pytest.mark.parametrize(
    ("call", "warns"),
    [(call, warns) for _name, call, warns in HOSTILE_CALLS],
    ids=[name for name, _call, _warns in HOSTILE_CALLS],
)
def test_hostile_values_never_raise_and_match_the_disabled_library(
    call: Callable[[Client], object], warns: bool, caplog: pytest.LogCaptureFixture
) -> None:
    enabled, enabled_sink = client(
        configuration=valid_configuration(slow_method_threshold_ms=0, usage_enabled=True)
    )
    disabled, disabled_sink = client(
        configuration=Configuration(slow_method_threshold_ms=0, usage_enabled=True)
    )

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        caplog.clear()
        enabled_result = call(enabled)
        enabled_warnings = closeyourit_warnings(caplog)
        disabled_result = call(disabled)

    assert enabled_result == disabled_result
    assert enabled_result in (None, False, "result")
    assert enabled_sink.items == []
    assert disabled_sink.items == []
    assert bool(enabled_warnings) is warns
    assert enabled.close() is True


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
