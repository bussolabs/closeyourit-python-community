from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta

import pytest

from closeyourit import Client, Configuration, HttpResponse, Transport, UsageRegistry, __version__
from closeyourit.client import Channel
from closeyourit.usage import KINDS, SYMBOL_FORMAT, UsageWindow, normalize_route

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


class RecordingSink:
    def __init__(self, accepted: bool = True) -> None:
        self.items: list[tuple[Channel, dict[str, object]]] = []
        self.accepted = accepted

    def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
        self.items.append((channel, payload))
        return self.accepted


class RecordingRequester:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def send(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        del body, headers, timeout
        self.urls.append(url)
        return HttpResponse(202)


class StepClock:
    """Deterministic UTC clock advancing one second per read."""

    def __init__(self, start: datetime = NOW) -> None:
        self.current = start

    def __call__(self) -> datetime:
        value = self.current
        self.current = self.current + timedelta(seconds=1)
        return value


def valid_configuration(**overrides: object) -> Configuration:
    values: dict[str, object] = {
        "endpoint_url": "https://www.closeyour.it",
        "token": "cyi_server-token",
        "project_id": "12345678-1234-1234-1234-123456789abc",
        "environment": "production",
        "usage_enabled": True,
    }
    values.update(overrides)
    return Configuration(**values)  # type: ignore[arg-type]


def registry(
    *,
    configuration: Configuration | None = None,
    clock: StepClock | None = None,
    pid: Callable[[], int] = lambda: 4242,
) -> tuple[UsageRegistry, list[UsageWindow]]:
    windows: list[UsageWindow] = []
    instance = UsageRegistry(
        configuration or valid_configuration(),
        windows.append,
        clock=clock or StepClock(),
        pid=pid,
    )
    return instance, windows


@pytest.mark.parametrize(
    ("route", "expected"),
    [
        ("orders/<int:pk>", "/orders/:pk"),
        ("/orders/<int:pk>/items/<slug:slug>", "/orders/:pk/items/:slug"),
        ("/orders/{order_id}", "/orders/:order_id"),
        ("/files/{path:path}", "/files/:path"),
        ("/orders/<pk>", "/orders/:pk"),
        ("  /health  ", "/health"),
        ("/", "/"),
    ],
)
def test_normalize_route_turns_framework_templates_into_shared_symbols(
    route: str, expected: str
) -> None:
    assert normalize_route(route) == expected


@pytest.mark.parametrize(
    "route",
    [
        None,
        "",
        "   ",
        "/orders/ 42",
        "/orders/€",
        "/orders/42?token=secret",
        "/" + "a" * 200,
    ],
)
def test_normalize_route_rejects_blank_and_out_of_contract_routes(route: str | None) -> None:
    assert normalize_route(route) is None


def test_record_is_a_no_op_while_usage_stays_opt_out() -> None:
    instance, windows = registry(configuration=valid_configuration(usage_enabled=False))

    assert instance.record("custom", "billing.export") is False

    instance.flush()
    assert windows == []


def test_record_is_a_no_op_when_the_configuration_is_disabled() -> None:
    instance, windows = registry(configuration=Configuration(usage_enabled=True))

    assert instance.record("custom", "billing.export") is False

    instance.flush()
    assert windows == []


@pytest.mark.parametrize(
    ("kind", "symbol"),
    [
        ("unknown", "billing.export"),
        ("route", ""),
        ("route", "   "),
        ("custom", "billing export"),
        ("custom", "a" * 201),
        ("custom", "billing.export\x00"),
    ],
)
def test_record_drops_kinds_and_symbols_outside_the_shared_contract(kind: str, symbol: str) -> None:
    instance, windows = registry()

    assert instance.record(kind, symbol) is False

    instance.flush()
    assert windows == []


def test_every_shared_kind_is_accepted() -> None:
    instance, windows = registry()

    for kind in KINDS:
        assert instance.record(kind, f"symbol.{kind}") is True

    instance.flush()
    assert sorted(item.kind for item in windows[0].symbols) == sorted(KINDS)


def test_repeated_symbols_accumulate_counts_and_refresh_last_seen_at() -> None:
    clock = StepClock()
    instance, windows = registry(clock=clock)

    assert instance.record("route", "/orders/:pk") is True
    assert instance.record("route", "/orders/:pk") is True
    instance.flush()

    (symbol,) = windows[0].symbols
    assert (symbol.kind, symbol.symbol, symbol.count) == ("route", "/orders/:pk", 2)
    assert symbol.last_seen_at > windows[0].started_at


def test_flush_empties_the_registry_and_opens_the_next_window() -> None:
    instance, windows = registry()
    instance.record("job", "billing.charge")

    instance.flush()
    instance.record("job", "billing.charge")
    instance.flush()

    assert [len(window.symbols) for window in windows] == [1, 1]
    assert [window.symbols[0].count for window in windows] == [1, 1]
    assert windows[1].started_at == windows[0].ended_at


def test_the_symbol_cap_truncates_the_window_without_dropping_known_symbols() -> None:
    instance, windows = registry(configuration=valid_configuration(usage_max_symbols=1))

    assert instance.record("custom", "first") is True
    assert instance.record("custom", "second") is False
    assert instance.record("custom", "first") is True

    instance.flush()
    window = windows[0]
    assert window.truncated is True
    assert [(item.symbol, item.count) for item in window.symbols] == [("first", 2)]


def test_a_zero_cap_keeps_nothing_and_still_reports_the_truncation() -> None:
    instance, windows = registry(configuration=valid_configuration(usage_max_symbols=0))

    assert instance.record("custom", "first") is False

    instance.flush()
    assert windows[0].symbols == ()
    assert windows[0].truncated is True


def test_an_empty_window_is_never_sent() -> None:
    instance, windows = registry()

    instance.flush()

    assert windows == []


def test_a_failing_flush_is_swallowed_and_reported_on_the_diagnostic_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def explode(window: UsageWindow) -> None:
        del window
        raise RuntimeError("sink unavailable")

    instance = UsageRegistry(valid_configuration(), explode, clock=StepClock())
    instance.record("custom", "billing.export")

    with caplog.at_level(logging.WARNING, logger="closeyourit"):
        instance.flush()

    assert "sink unavailable" in caplog.text
    assert instance.record("custom", "billing.export") is True


def test_the_timer_flushes_the_window_and_shutdown_stops_it() -> None:
    flushed = threading.Event()
    windows: list[UsageWindow] = []

    def capture(window: UsageWindow) -> None:
        windows.append(window)
        flushed.set()

    instance = UsageRegistry(
        valid_configuration(usage_flush_interval=0.01),
        capture,
        clock=StepClock(),
    )
    try:
        assert instance.record("custom", "billing.export") is True
        assert flushed.wait(5.0) is True
    finally:
        instance.shutdown()

    assert instance.running is False
    assert windows[0].symbols[0].symbol == "billing.export"


def test_shutdown_flushes_what_is_still_buffered_and_stops_accepting() -> None:
    instance, windows = registry()
    instance.record("custom", "billing.export")

    instance.shutdown()

    assert [item.symbol for item in windows[0].symbols] == ["billing.export"]
    assert instance.record("custom", "billing.export") is False
    instance.shutdown()
    assert len(windows) == 1


def test_shutdown_without_a_single_sighting_starts_no_timer_and_sends_nothing() -> None:
    instance, windows = registry()

    instance.shutdown()

    assert instance.running is False
    assert windows == []


def test_a_forked_child_never_inherits_a_lock_held_by_a_vanished_thread() -> None:
    pids = [4242]
    instance, windows = registry(pid=lambda: pids[-1])
    instance.record("custom", "billing.export")
    # At fork time the lock could be held by the timer thread, which does not exist in the
    # child: without a reset the first registration would stay blocked forever.
    instance._lock.acquire()
    pids.append(424_242)
    recorded = threading.Event()

    def record_in_the_child() -> None:
        instance.record("custom", "billing.charge")
        recorded.set()

    threading.Thread(target=record_in_the_child, daemon=True).start()

    assert recorded.wait(5.0) is True
    instance.flush()
    assert [item.symbol for item in windows[0].symbols] == ["billing.charge"]


def test_a_forked_child_starts_from_an_empty_window() -> None:
    pids = [4242]
    instance, windows = registry(pid=lambda: pids[-1])
    instance.record("custom", "billing.export")

    pids.append(424_242)
    assert instance.record("custom", "billing.charge") is True
    instance.flush()

    assert [item.symbol for item in windows[0].symbols] == ["billing.charge"]


def test_client_record_usage_enqueues_the_shared_usage_envelope() -> None:
    sink = RecordingSink()
    client = Client(valid_configuration(release="v1.4.2"), sink=sink)

    assert client.record_usage("route", "/orders/:pk") is True
    assert client.used("billing.export") is True
    client.flush()

    (channel, payload) = sink.items[0]
    assert channel == "usages"
    assert payload["environment"] == "production"
    assert payload["release"] == "v1.4.2"
    assert payload["sdk"] == {"name": "closeyourit-python", "version": __version__}
    assert payload["truncated"] is False
    assert payload["symbols"] == [
        {
            "kind": "route",
            "symbol": "/orders/:pk",
            "count": 1,
            "last_seen_at": payload["symbols"][0]["last_seen_at"],  # type: ignore[index]
        },
        {
            "kind": "custom",
            "symbol": "billing.export",
            "count": 1,
            "last_seen_at": payload["symbols"][1]["last_seen_at"],  # type: ignore[index]
        },
    ]
    assert str(payload["window_started_at"]).endswith("Z")
    assert str(payload["window_ended_at"]).endswith("Z")


def test_client_usage_never_runs_before_send() -> None:
    sink = RecordingSink()
    client = Client(
        valid_configuration(before_send=lambda payload: None),
        sink=sink,
    )

    client.used("billing.export")
    client.flush()

    assert [channel for channel, _payload in sink.items] == ["usages"]


def test_client_record_usage_stays_a_no_op_while_opt_out() -> None:
    sink = RecordingSink()
    client = Client(valid_configuration(usage_enabled=False), sink=sink)

    assert client.record_usage("route", "/orders/:pk") is False
    assert client.used("billing.export") is False
    client.flush()

    assert sink.items == []


def test_client_close_shuts_the_registry_down_after_a_final_flush() -> None:
    sink = RecordingSink()
    client = Client(valid_configuration(), sink=sink)
    client.used("billing.export")

    assert client.close() is True

    assert [channel for channel, _payload in sink.items] == ["usages"]
    assert client.record_usage("custom", "billing.export") is False


def test_the_transport_posts_a_usage_window_to_the_shared_endpoint() -> None:
    requester = RecordingRequester()
    transport = Transport(valid_configuration(), requester=requester)
    client = Client(valid_configuration(), sink=transport)

    client.used("billing.export")
    client.flush(2.0)

    assert requester.urls == [
        "https://www.closeyour.it/api/v1/projects/12345678-1234-1234-1234-123456789abc/usages"
    ]
    assert transport.stats.sent == 1
    transport.close()


def test_every_symbol_the_registry_keeps_matches_the_shared_wire_format() -> None:
    instance, windows = registry()
    instance.record("route", "/orders/:pk")

    instance.flush()

    for item in windows[0].symbols:
        assert SYMBOL_FORMAT.fullmatch(item.symbol) is not None
