from __future__ import annotations

import json
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from closeyourit import CeleryIntegration, Client, Configuration
from closeyourit.client import Channel
from closeyourit.integrations import celery as celery_integration

CONTRACT = Path(__file__).parents[1] / "contracts" / "ingest" / "v1"
VALID_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


class RecordingSink:
    def __init__(self, *, fail: bool = False) -> None:
        self.items: list[tuple[Channel, dict[str, object]]] = []
        self.fail = fail

    def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
        if self.fail:
            raise RuntimeError("sink unavailable")
        self.items.append((channel, payload))
        return True


class FakeSignal:
    def __init__(self) -> None:
        self.receivers: list[Callable[..., object]] = []
        self.connect_options: list[dict[str, object]] = []

    def connect(self, receiver: Callable[..., object], **kwargs: object) -> None:
        if receiver not in self.receivers:
            self.receivers.append(receiver)
            self.connect_options.append(dict(kwargs))

    def disconnect(self, receiver: Callable[..., object], **kwargs: object) -> bool:
        del kwargs
        if receiver not in self.receivers:
            return False
        self.receivers.remove(receiver)
        return True

    def send(self, sender: object = None, **kwargs: object) -> None:
        for receiver in tuple(self.receivers):
            receiver(sender=sender, signal=self, **kwargs)


class BrokenSignal(FakeSignal):
    def connect(self, receiver: Callable[..., object], **kwargs: object) -> None:
        del receiver, kwargs
        raise RuntimeError("signal unavailable")


@dataclass
class FakeSignals:
    before_task_publish: FakeSignal = field(default_factory=FakeSignal)
    task_prerun: FakeSignal = field(default_factory=FakeSignal)
    task_postrun: FakeSignal = field(default_factory=FakeSignal)
    task_failure: FakeSignal = field(default_factory=FakeSignal)
    task_retry: FakeSignal = field(default_factory=FakeSignal)
    task_revoked: FakeSignal = field(default_factory=FakeSignal)

    def all(self) -> tuple[FakeSignal, ...]:
        return (
            self.before_task_publish,
            self.task_prerun,
            self.task_postrun,
            self.task_failure,
            self.task_retry,
            self.task_revoked,
        )


@dataclass
class Request:
    id: str
    task: str
    retries: int = 0
    headers: dict[str, object] = field(default_factory=dict)
    delivery_info: dict[str, object] = field(
        default_factory=lambda: {"routing_key": "jobs.default"}
    )


@dataclass
class Task:
    name: str
    request: Request


def enabled_client(
    *, fail: bool = False, usage_enabled: bool = False
) -> tuple[Client, RecordingSink]:
    sink = RecordingSink(fail=fail)
    configuration = Configuration(
        endpoint_url="https://ingest.example.com",
        token="cyi_test",
        project_id="12345678-1234-1234-1234-123456789abc",
        environment="production",
        release="v2.4.0",
        usage_enabled=usage_enabled,
    )
    return Client(configuration, sink=sink), sink


def integration(
    signals: FakeSignals,
    *,
    monotonic: Callable[[], float] = lambda: 1.0,
    wall_clock: Callable[[], float] = lambda: 1.0,
    task_threshold_ms: float = 0.0,
    queue_threshold_ms: float = 0.0,
    fail: bool = False,
    usage_enabled: bool = False,
) -> tuple[CeleryIntegration, RecordingSink]:
    client, sink = enabled_client(fail=fail, usage_enabled=usage_enabled)
    instance = CeleryIntegration(
        client,
        task_threshold_ms=task_threshold_ms,
        queue_threshold_ms=queue_threshold_ms,
        correlation_headers=("traceparent", "x-request-id"),
        monotonic=monotonic,
        wall_clock=wall_clock,
        signals=signals,
    )
    return instance, sink


def usage_symbols(sink: RecordingSink) -> list[dict[str, object]]:
    payloads = [payload for channel, payload in sink.items if channel == "usages"]
    return [
        item for payload in payloads for item in cast(list[dict[str, object]], payload["symbols"])
    ]


def metric_payloads(sink: RecordingSink) -> list[dict[str, object]]:
    return [payload for channel, payload in sink.items if channel == "metrics"]


def event_payloads(sink: RecordingSink) -> list[dict[str, object]]:
    return [payload for channel, payload in sink.items if channel == "events"]


def assert_valid_slow_method(payload: dict[str, object]) -> None:
    schema = json.loads((CONTRACT / "schema.json").read_text(encoding="utf-8"))
    validator = Draft202012Validator(
        {"$ref": "#/$defs/slow_method", "$defs": schema["$defs"]},
        format_checker=FormatChecker(),
    )
    assert list(validator.iter_errors(payload)) == []


def test_framework_absent_is_a_reversible_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _sink = enabled_client()
    monkeypatch.setattr(celery_integration, "_load_celery_signals", lambda: None)

    instance = CeleryIntegration(client).install().install()

    assert instance.available is False
    assert instance.installed is False
    instance.uninstall()
    instance.close()


def test_signal_lifecycle_is_idempotent_reversible_and_rolls_back_partial_install() -> None:
    signals = FakeSignals()
    instance, _sink = integration(signals)

    assert instance.install().install() is instance
    assert instance.available is True
    assert instance.installed is True
    assert [len(signal.receivers) for signal in signals.all()] == [1, 1, 1, 1, 1, 1]
    assert [signal.connect_options for signal in signals.all()] == [[{"weak": False}]] * 6

    instance.uninstall()
    instance.uninstall()
    assert [signal.receivers for signal in signals.all()] == [[], [], [], [], [], []]

    instance.install()
    instance.close()
    instance.close()
    instance.install()
    assert [signal.receivers for signal in signals.all()] == [[], [], [], [], [], []]

    broken = FakeSignals(task_postrun=BrokenSignal())
    broken_instance, _sink = integration(broken)
    broken_instance.install()
    assert broken_instance.installed is False
    assert [signal.receivers for signal in broken.all()] == [[], [], [], [], [], []]


@pytest.mark.parametrize(
    ("duration_ms", "expected"),
    [(9.99, 0), (10.0, 1), (10.01, 1)],
)
def test_queue_latency_uses_x_minus_one_x_x_plus_one_boundaries(
    duration_ms: float, expected: int
) -> None:
    signals = FakeSignals()
    values = iter((1.0, 1.0 + duration_ms / 1000.0))
    instance, sink = integration(
        signals,
        wall_clock=lambda: next(values),
        queue_threshold_ms=10.0,
        task_threshold_ms=1000.0,
    )
    instance.install()
    headers: dict[str, object] = {
        "traceparent": VALID_TRACEPARENT,
        "authorization": "Bearer private",
        "custom": "must-not-leak",
    }
    task = Task("billing.charge", Request("task-1", "billing.charge", headers=headers))

    signals.before_task_publish.send(sender=task.name, headers=headers)
    signals.task_prerun.send(sender=task, task_id=task.request.id, task=task)

    metrics = metric_payloads(sink)
    assert len(metrics) == expected
    if metrics:
        assert metrics[0]["label"] == "celery.queue.latency.billing.charge"
        assert metrics[0]["duration_ms"] == round(duration_ms, 2)
        assert metrics[0]["release"] == "v2.4.0"
        assert cast(dict[str, Any], metrics[0]["contexts"])["celery"] == {
            "task_id": "task-1",
            "task_name": "billing.charge",
            "queue": "jobs.default",
            "retry_count": 0,
            "headers": {"traceparent": VALID_TRACEPARENT},
        }
        assert_valid_slow_method(metrics[0])
        assert "Bearer private" not in repr(metrics[0])
        assert "must-not-leak" not in repr(metrics[0])


def test_clock_skew_is_clamped_to_zero() -> None:
    signals = FakeSignals()
    values = iter((10.0, 9.0))
    instance, sink = integration(signals, wall_clock=lambda: next(values))
    instance.install()
    headers: dict[str, object] = {}
    task = Task("clock.skew", Request("task-skew", "clock.skew", headers=headers))

    signals.before_task_publish.send(sender=task.name, headers=headers)
    signals.task_prerun.send(sender=task, task_id=task.request.id, task=task)

    assert metric_payloads(sink)[0]["duration_ms"] == 0.0


@pytest.mark.parametrize(
    ("value", "accepted"),
    [("a" * 127, True), ("a" * 128, True), ("a" * 129, False), (b"a" * 129, False)],
)
def test_correlation_identifiers_have_a_bounded_ascii_shape(value: object, accepted: bool) -> None:
    signals = FakeSignals()
    instance, sink = integration(signals)
    instance.install()
    task = Task(
        "jobs.correlation",
        Request("task-correlation", "jobs.correlation", headers={"x-request-id": value}),
    )

    signals.task_failure.send(
        sender=task,
        task_id=task.request.id,
        exception=RuntimeError("failed"),
    )

    context = event_payloads(sink)[0]["contexts"]["celery"]  # type: ignore[index]
    assert ("headers" in context) is accepted


@pytest.mark.parametrize(
    "headers",
    [
        {"traceparent": "00-00000000000000000000000000000000-00f067aa0ba902b7-01"},
        {"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"},
        {"traceparent": VALID_TRACEPARENT.upper()},
        {"x-request-id": "customer@example.com"},
        {"x-request-id": "request id with spaces"},
        {"x-request-id": 1234},
        {"x-request-id": b"\xff"},
        {"X-Request-ID": "first", "x-request-id": "second"},
    ],
)
def test_invalid_duplicate_or_pii_shaped_correlation_headers_are_dropped(
    headers: dict[str, object],
) -> None:
    signals = FakeSignals()
    instance, sink = integration(signals)
    instance.install()
    task = Task("jobs.invalid", Request("task-invalid", "jobs.invalid", headers=headers))

    signals.task_failure.send(
        sender=task,
        task_id=task.request.id,
        exception=RuntimeError("failed"),
    )

    context = event_payloads(sink)[0]["contexts"]["celery"]  # type: ignore[index]
    assert "headers" not in context


def test_task_duration_is_keyed_by_task_id_for_interleaved_tasks() -> None:
    signals = FakeSignals()
    values = iter((1.0, 2.0, 2.01, 2.03))
    instance, sink = integration(signals, monotonic=lambda: next(values))
    instance.install()
    first = Task("jobs.first", Request("first", "jobs.first"))
    second = Task("jobs.second", Request("second", "jobs.second", retries=2))

    signals.task_prerun.send(sender=first, task_id="first", task=first)
    signals.task_prerun.send(sender=second, task_id="second", task=second)
    signals.task_postrun.send(sender=first, task_id="first", task=first, state="SUCCESS")
    signals.task_postrun.send(sender=second, task_id="second", task=second, state="RETRY")

    metrics = metric_payloads(sink)
    assert [payload["label"] for payload in metrics] == [
        "celery.task.duration.jobs.first",
        "celery.task.duration.jobs.second",
    ]
    assert [payload["duration_ms"] for payload in metrics] == [1010.0, 30.0]
    assert cast(dict[str, Any], metrics[1]["contexts"])["celery"] == {
        "task_id": "second",
        "task_name": "jobs.second",
        "queue": "jobs.default",
        "retry_count": 2,
        "state": "RETRY",
    }


@pytest.mark.parametrize(
    ("duration_ms", "expected"),
    [(9.99, 0), (10.0, 1), (10.01, 1)],
)
def test_task_duration_uses_x_minus_one_x_x_plus_one_boundaries(
    duration_ms: float, expected: int
) -> None:
    signals = FakeSignals()
    values = iter((1.0, 1.0 + duration_ms / 1000.0))
    instance, sink = integration(
        signals,
        monotonic=lambda: next(values),
        task_threshold_ms=10.0,
        queue_threshold_ms=1000.0,
    )
    instance.install()
    task = Task("jobs.boundary", Request("task-boundary", "jobs.boundary"))

    signals.task_prerun.send(sender=task, task_id=task.request.id, task=task)
    signals.task_postrun.send(
        sender=task,
        task_id=task.request.id,
        task=task,
        state="SUCCESS",
        retval="must-not-be-read",
    )

    metrics = metric_payloads(sink)
    assert len(metrics) == expected
    if metrics:
        assert metrics[0]["duration_ms"] == round(duration_ms, 2)
        assert "must-not-be-read" not in repr(metrics[0])
        assert_valid_slow_method(metrics[0])


def test_concurrent_tasks_do_not_share_start_times() -> None:
    signals = FakeSignals()
    instance, sink = integration(signals)
    instance.install()
    barrier = threading.Barrier(2)

    def run(task_id: str) -> None:
        task = Task(f"jobs.{task_id}", Request(task_id, f"jobs.{task_id}"))
        signals.task_prerun.send(sender=task, task_id=task_id, task=task)
        barrier.wait()
        signals.task_postrun.send(sender=task, task_id=task_id, task=task, state="SUCCESS")

    threads = [threading.Thread(target=run, args=(task_id,)) for task_id in ("one", "two")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    labels = {payload["label"] for payload in metric_payloads(sink)}
    assert labels == {"celery.task.duration.jobs.one", "celery.task.duration.jobs.two"}


def test_failure_retry_and_revoked_exclude_args_kwargs_results_and_clear_timing() -> None:
    signals = FakeSignals()
    instance, sink = integration(signals)
    instance.install()
    headers: dict[str, object] = {
        "x-request-id": "request-7",
        "cookie": "secret-cookie",
        "private": "secret-header",
    }
    request = Request("task-7", "billing.charge", retries=3, headers=headers)
    task = Task("billing.charge", request)

    signals.task_prerun.send(sender=task, task_id=request.id, task=task)
    signals.task_failure.send(
        sender=task,
        task_id=request.id,
        exception=RuntimeError("token=private"),
        args=("secret-arg",),
        kwargs={"password": "secret-kwarg"},
        einfo=object(),
    )
    signals.task_retry.send(
        sender=task,
        request=request,
        reason=ValueError("retry token=private"),
        einfo=object(),
    )
    signals.task_revoked.send(
        sender=task,
        request=request,
        terminated=True,
        expired=False,
        args=("secret-arg",),
        retval="secret-result",
    )
    signals.task_postrun.send(sender=task, task_id=request.id, task=task, state="REVOKED")

    events = event_payloads(sink)
    assert [event["contexts"]["celery"]["state"] for event in events] == [  # type: ignore[index]
        "FAILURE",
        "RETRY",
        "REVOKED",
    ]
    assert events[0]["exception"]["values"][-1]["mechanism"]["handled"] is False  # type: ignore[index]
    assert events[1]["exception"]["values"][-1]["mechanism"]["handled"] is True  # type: ignore[index]
    assert events[2]["exception"]["values"][-1]["value"] == "Celery task revoked"  # type: ignore[index]
    assert metric_payloads(sink) == []
    serialized = repr(events)
    for forbidden in (
        "secret-arg",
        "secret-kwarg",
        "secret-result",
        "secret-cookie",
        "secret-header",
    ):
        assert forbidden not in serialized
    assert "token=[FILTERED]" in serialized
    assert "x-request-id" in serialized


def test_handlers_are_fail_safe_when_the_client_raises() -> None:
    signals = FakeSignals()
    instance, _sink = integration(signals, fail=True)
    instance.install()
    task = Task("jobs.fail_safe", Request("task-fail-safe", "jobs.fail_safe"))

    signals.task_prerun.send(sender=task, task_id=task.request.id, task=task)
    signals.task_postrun.send(sender=task, task_id=task.request.id, task=task, state="SUCCESS")
    signals.task_failure.send(
        sender=task,
        task_id=task.request.id,
        exception=RuntimeError("application error"),
    )


def test_real_celery_eager_execution_preserves_failure_and_retry_semantics() -> None:
    celery = pytest.importorskip("celery")
    client, sink = enabled_client()
    instance = CeleryIntegration(client, task_threshold_ms=0, queue_threshold_ms=0).install()
    app = celery.Celery("closeyourit-tests", broker="memory://", backend="cache+memory://")
    app.conf.update(
        task_always_eager=True,
        task_eager_propagates=False,
        task_store_eager_result=False,
    )

    def succeed() -> int:
        return 42

    def fail() -> None:
        raise RuntimeError("eager failure")

    attempts: list[int] = []

    def retry_once() -> int:
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            raise RuntimeError("retry once")
        return len(attempts)

    succeed_task = app.task(name="closeyourit.tests.success")(succeed)
    fail_task = app.task(name="closeyourit.tests.failure")(fail)
    retry_task = app.task(
        name="closeyourit.tests.retry",
        autoretry_for=(RuntimeError,),
        retry_kwargs={"max_retries": 1},
    )(retry_once)

    try:
        success = succeed_task.delay()
        failure = fail_task.delay()
        retried = retry_task.delay()
    finally:
        instance.uninstall()

    assert success.state == "SUCCESS"
    assert failure.state == "FAILURE"
    assert retried.state == "SUCCESS"
    assert retried.result == 2
    assert attempts == [1, 2]
    labels = {payload["label"] for payload in metric_payloads(sink)}
    assert "celery.task.duration.closeyourit.tests.success" in labels
    assert "celery.task.duration.closeyourit.tests.failure" in labels
    assert "celery.task.duration.closeyourit.tests.retry" in labels
    states = [
        event["contexts"]["celery"]["state"]  # type: ignore[index]
        for event in event_payloads(sink)
    ]
    assert "FAILURE" in states
    assert "RETRY" in states


def test_real_celery_lifecycle_keeps_retry_attempts_distinct_without_promoting_logs() -> None:
    celery = pytest.importorskip("celery")
    client, sink = enabled_client()
    integration = CeleryIntegration(client, lifecycle_logs=True, task_threshold_ms=0).install()
    app = celery.Celery("job-contract", broker="memory://", backend="cache+memory://")
    app.conf.update(task_always_eager=True, task_eager_propagates=False)

    @app.task(bind=True, name="jobs.retry.contract", max_retries=1)  # type: ignore[untyped-decorator]
    def task(self: Any) -> int:
        if self.request.retries == 0:
            raise self.retry(exc=ValueError("retry boundary"), countdown=0)
        return 42

    try:
        result = task.delay()
    finally:
        integration.uninstall()
        app.close()
    assert result.get() == 42
    logs = [payload for channel, payload in sink.items if channel == "logs"]
    assert len(logs) == 2
    contexts = sorted(
        [cast(dict[str, Any], item["attributes"])["contexts"]["job"] for item in logs],
        key=lambda item: item["attempt"],
    )
    assert [(item["attempt"], item["attempt_outcome"], item["terminal"]) for item in contexts] == [
        (1, "retry", False),
        (2, "success", True),
    ]
    assert all(
        item["level"] == "info" and item["message"] == "Background job attempt finished"
        for item in logs
    )
    assert all(item["schema_version"] == 1 and item["job_id"] == result.id for item in contexts)
    assert all(item["queue_wait_ms"] is None for item in contexts)


def test_a_started_task_is_recorded_as_a_used_job_symbol() -> None:
    signals = FakeSignals()
    instance, sink = integration(signals, usage_enabled=True, queue_threshold_ms=10_000.0)
    instance.install()
    task = Task(name="billing.charge", request=Request(id="task-1", task="billing.charge"))

    signals.task_prerun.send(sender=task, task_id="task-1", task=task)
    signals.task_prerun.send(sender=task, task_id="task-2", task=task)
    instance.client.flush()

    assert usage_symbols(sink) == [
        {
            "kind": "job",
            "symbol": "billing.charge",
            "count": 2,
            "last_seen_at": usage_symbols(sink)[0]["last_seen_at"],
        }
    ]
    assert metric_payloads(sink) == []


def test_a_task_without_a_resolvable_name_is_never_recorded_as_used() -> None:
    signals = FakeSignals()
    instance, sink = integration(signals, usage_enabled=True)
    instance.install()

    signals.task_prerun.send(sender=None, task_id="task-1", task=None)
    instance.client.flush()

    assert usage_symbols(sink) == []


def test_no_job_symbol_is_recorded_while_the_channel_stays_opt_out() -> None:
    signals = FakeSignals()
    instance, sink = integration(signals)
    instance.install()
    task = Task(name="billing.charge", request=Request(id="task-1", task="billing.charge"))

    signals.task_prerun.send(sender=task, task_id="task-1", task=task)
    instance.client.flush()

    assert usage_symbols(sink) == []
