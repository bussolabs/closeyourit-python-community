"""Shared job timing semantics without reading arguments or results."""

from __future__ import annotations

import math
import re
from datetime import UTC, datetime


def bounded_text(value: object, limit: int) -> str | None:
    if (
        isinstance(value, str)
        and 0 < len(value) <= limit
        and not re.search(r"[\x00-\x1f\x7f]", value)
    ):
        return value
    return None


def instant(value: object) -> datetime | None:
    try:
        if isinstance(value, bool):
            return None
        if isinstance(value, int | float) and math.isfinite(value):
            return datetime.fromtimestamp(value, UTC)
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value.astimezone(UTC)
    except (ValueError, OverflowError, OSError):
        pass
    return None


def timing_context(*, enqueued: object, scheduled: object, started: object) -> dict[str, object]:
    enqueue_time, schedule_time, start_time = map(instant, (enqueued, scheduled, started))
    due = max(enqueue_time, schedule_time) if enqueue_time and schedule_time else enqueue_time
    wait = (start_time - due).total_seconds() * 1000 if start_time and due else None
    delay = (
        (schedule_time - enqueue_time).total_seconds() * 1000
        if schedule_time and enqueue_time
        else None
    )
    return {
        "enqueued_at": _iso(enqueue_time),
        "scheduled_at": _iso(schedule_time),
        "started_at": _iso(start_time),
        "queue_wait_ms": max(0.0, wait) if wait is not None else None,
        "scheduled_delay_ms": max(0.0, delay) if delay is not None else None,
        "clock_skew": wait is not None and wait < 0,
    }


def _iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None
