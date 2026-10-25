from datetime import UTC, datetime

import pytest

from closeyourit.integrations._jobs import timing_context


def test_planned_delay_is_not_queue_wait() -> None:
    result = timing_context(enqueued=100.0, scheduled=700.0, started=730.0)
    assert result["queue_wait_ms"] == 30_000
    assert result["scheduled_delay_ms"] == 600_000
    assert result["started_at"] == "1970-01-01T00:12:10.000000Z"


@pytest.mark.parametrize(
    "scheduled", [700.0, "1970-01-01T00:11:40Z", datetime.fromtimestamp(700, UTC)]
)
def test_supported_schedule_shapes_have_identical_meaning(scheduled: object) -> None:
    assert (
        timing_context(enqueued=100.0, scheduled=scheduled, started=720.0)["queue_wait_ms"]
        == 20_000
    )


@pytest.mark.parametrize("scheduled", [None, "invalid", "1970-01-01T00:11:40", True, float("inf")])
def test_invalid_schedule_falls_back_without_inventing_delay(scheduled: object) -> None:
    result = timing_context(enqueued=100.0, scheduled=scheduled, started=120.0)
    assert result["queue_wait_ms"] == 20_000
    assert result["scheduled_delay_ms"] is None


def test_missing_enqueue_is_unknown_even_with_schedule() -> None:
    result = timing_context(enqueued=None, scheduled=700.0, started=720.0)
    assert result["queue_wait_ms"] is None
    assert result["scheduled_delay_ms"] is None


def test_skew_is_visible_and_clamped() -> None:
    result = timing_context(enqueued=100.0, scheduled=700.0, started=690.0)
    assert result["clock_skew"] is True
    assert result["queue_wait_ms"] == 0
