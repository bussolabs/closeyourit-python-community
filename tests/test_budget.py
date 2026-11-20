from __future__ import annotations

import json
from typing import Any

import pytest

from closeyourit.budget import (
    ELLIPSIS,
    MAX_PAYLOAD_BYTES,
    MAX_TEXT_CHARS,
    cap_text,
    encode,
    fit,
)

LIMIT = MAX_PAYLOAD_BYTES
EMOJI = "\U0001f600"


def padded(payload: dict[str, Any], key: str, size: int) -> dict[str, Any]:
    """Return ``payload`` with ``key`` padded so that the encoded body is exactly ``size`` bytes."""
    result = dict(payload)
    result[key] = ""
    result[key] = "x" * (size - len(encode(result)))
    assert len(encode(result)) == size
    return result


def decoded(body: bytes | None) -> dict[str, Any]:
    assert body is not None
    assert len(body) <= LIMIT
    payload = json.loads(body)
    assert isinstance(payload, dict)
    return payload


def event(**fields: Any) -> dict[str, Any]:
    return {
        "event_id": "12345678123442348234123456789abc",
        "timestamp": "2026-07-13T14:00:00Z",
        "platform": "python",
        "level": "error",
        "environment": "production",
        "release": "v1.0.0",
        "contexts": {"runtime": {"name": "python", "version": "3.14.0"}},
        "sdk": {"name": "closeyourit-python", "version": "0.4.2"},
        **fields,
    }


def exception(value: str = "boom", frames: int = 3) -> dict[str, Any]:
    return {
        "values": [
            {
                "type": "RuntimeError",
                "value": value,
                "stacktrace": {
                    "frames": [
                        {"filename": "app.py", "function": f"f{index}", "lineno": index + 1}
                        for index in range(frames)
                    ]
                },
                "mechanism": {"type": "python", "handled": False},
            }
        ]
    }


def log(**fields: Any) -> dict[str, Any]:
    return {
        "event_id": "12345678123442348234123456789abc",
        "timestamp": "2026-07-13T14:00:00Z",
        "level": "warning",
        "message": "order charged",
        "environment": "production",
        "sdk": {"name": "closeyourit-python", "version": "0.4.2"},
        **fields,
    }


def metric(**fields: Any) -> dict[str, Any]:
    return {
        "kind": "slow_method",
        "sample_id": "12345678-1234-4234-8234-123456789abc",
        "duration_ms": 12.35,
        "occurred_at": "2026-07-13T14:00:00Z",
        "environment": "production",
        "label": "checkout.total",
        "sdk": {"name": "closeyourit-python", "version": "0.4.2"},
        **fields,
    }


@pytest.mark.parametrize("channel", ["events", "logs", "metrics", "usages"])
@pytest.mark.parametrize("size", [LIMIT - 1, LIMIT])
def test_a_body_up_to_the_limit_is_returned_byte_for_byte(channel: str, size: int) -> None:
    body = encode(padded({"kind": "slow_method"}, "message", size))

    assert fit(channel, body) is body


def test_a_body_one_byte_over_the_limit_is_shrunk_and_marked() -> None:
    body = encode(padded(event(), "message", LIMIT + 1))

    payload = decoded(fit("events", body))

    assert payload["truncated"] is True
    assert payload["message"].endswith(ELLIPSIS)
    assert payload["event_id"] == "12345678123442348234123456789abc"


def test_the_marker_is_absent_from_a_body_that_fits() -> None:
    body = encode(event(message={"formatted": "hello"}))

    assert "truncated" not in decoded(fit("events", body))


def test_an_emoji_message_just_over_the_limit_is_cut_between_whole_emoji() -> None:
    payload = event(message={"formatted": ""})
    base = len(encode(payload))
    count = (LIMIT - base) // 4 + 1  # four bytes each: one emoji over the limit
    payload["message"] = {"formatted": EMOJI * count}
    assert len(encode(payload)) > LIMIT

    result = decoded(fit("events", encode(payload)))

    formatted = result["message"]["formatted"]
    assert formatted.endswith(ELLIPSIS)
    assert set(formatted[:-1]) == {EMOJI}
    assert len(formatted) > count - 10
    assert result["truncated"] is True


def test_texts_with_escaped_characters_are_measured_as_encoded() -> None:
    payload = log(message="\x00" * (LIMIT // 3))  # six bytes on the wire for each character

    result = decoded(fit("logs", encode(payload)))

    assert result["message"].endswith(ELLIPSIS)
    assert set(result["message"][:-1]) == {"\x00"}


def test_the_oldest_breadcrumbs_go_first_and_the_error_stays_intact() -> None:
    crumbs = [{"message": f"crumb-{index}", "data": "y" * 20_000} for index in range(100)]
    payload = event(breadcrumbs={"values": crumbs}, exception=exception("checkout failed"))

    result = decoded(fit("events", encode(payload)))

    kept = [crumb["message"] for crumb in result["breadcrumbs"]["values"]]
    assert kept
    assert kept[-1] == "crumb-99"
    assert kept == [f"crumb-{index}" for index in range(100 - len(kept), 100)]
    assert result["exception"] == payload["exception"]
    assert result["truncated"] is True


def test_breadcrumbs_disappear_entirely_before_the_request_is_touched() -> None:
    crumbs = {"values": [{"message": "small"}]}
    payload = event(breadcrumbs=crumbs, request={"body": "r" * LIMIT}, message="hello")

    result = decoded(fit("events", encode(payload)))

    assert "breadcrumbs" not in result
    assert "request" not in result
    assert result["message"] == "hello"


def test_breadcrumbs_with_an_unexpected_shape_are_removed() -> None:
    payload = event(breadcrumbs="b" * LIMIT, message="hello")

    result = decoded(fit("events", encode(payload)))

    assert "breadcrumbs" not in result
    assert result["message"] == "hello"


def test_personal_contexts_go_after_the_request_and_keep_the_runtime() -> None:
    payload = event(
        request={"url": "https://example.com"},
        user={"id": "u" * LIMIT},
        extra={"kept": True},
        message="hello",
    )
    payload["contexts"]["custom"] = {"big": "c" * 10}

    result = decoded(fit("events", encode(payload)))

    assert "request" not in result
    assert "user" not in result
    assert result["contexts"] == {"runtime": {"name": "python", "version": "3.14.0"}}
    assert result["extra"] == {"kept": True}


def test_contexts_with_an_unexpected_shape_are_removed() -> None:
    payload = event(message="hello", tags={"kept": True})
    payload["contexts"] = "c" * LIMIT

    result = decoded(fit("events", encode(payload)))

    assert "contexts" not in result
    assert result["tags"] == {"kept": True}


def test_extra_goes_before_tags_and_tags_before_frames() -> None:
    payload = event(extra={"big": "e" * LIMIT}, tags={"kept": True}, exception=exception())

    result = decoded(fit("events", encode(payload)))

    assert "extra" not in result
    assert result["tags"] == {"kept": True}
    assert result["exception"] == payload["exception"]

    payload = event(tags={"big": "t" * LIMIT}, exception=exception())

    result = decoded(fit("events", encode(payload)))

    assert "tags" not in result
    assert result["exception"] == payload["exception"]


def test_outermost_frames_go_first_and_the_innermost_frame_stays() -> None:
    payload = event(exception=exception(frames=20_000))

    result = decoded(fit("events", encode(payload)))

    value = result["exception"]["values"][0]
    frames = value["stacktrace"]["frames"]
    assert 0 < len(frames) < 20_000
    assert frames[-1]["function"] == "f19999"
    assert value["type"] == "RuntimeError"
    assert value["value"] == "boom"


def test_frames_of_every_exception_in_the_chain_can_be_dropped() -> None:
    without_frames = {"type": "KeyError", "value": "k", "stacktrace": None}
    cause = exception(value="cause", frames=12_000)["values"][0]
    error = exception(frames=12_000)["values"][0]
    chain = {"values": [without_frames, "not an exception value", cause, error]}
    payload = event(exception=chain)

    result = decoded(fit("events", encode(payload)))

    values = result["exception"]["values"]
    assert values[:2] == [without_frames, "not an exception value"]
    assert values[2]["stacktrace"]["frames"] == []
    assert 0 < len(values[3]["stacktrace"]["frames"]) < 12_000


def test_the_largest_text_is_shortened_first() -> None:
    payload = event(
        message={"formatted": "m" * LIMIT}, exception=exception("checkout failed", frames=0)
    )

    result = decoded(fit("events", encode(payload)))

    assert result["message"]["formatted"].endswith(ELLIPSIS)
    assert result["exception"]["values"][0]["value"] == "checkout failed"


def test_several_texts_are_shortened_when_one_is_not_enough() -> None:
    chain = exception("a" * LIMIT, frames=0)
    chain["values"].append(exception("b" * LIMIT, frames=0)["values"][0])
    payload = event(exception=chain)

    result = decoded(fit("events", encode(payload)))

    first, second = (value["value"] for value in result["exception"]["values"])
    assert first.endswith(ELLIPSIS)
    assert second.endswith(ELLIPSIS)


def test_an_event_whose_protected_fields_exceed_the_limit_is_refused() -> None:
    payload = event(message="hello", sdk={"name": "closeyourit-python", "version": "v" * LIMIT})

    assert fit("events", encode(payload)) is None


def test_texts_already_at_their_shortest_are_skipped() -> None:
    payload = event(message=ELLIPSIS, exception=exception(ELLIPSIS, frames=0))
    payload["platform"] = "p" * LIMIT

    assert fit("events", encode(payload)) is None


def test_a_huge_log_loses_its_attributes_and_keeps_level_and_message() -> None:
    payload = log(attributes={"dump": "a" * (3 * 1024 * 1024)}, logger="billing")

    result = decoded(fit("logs", encode(payload)))

    assert "attributes" not in result
    assert result["level"] == "warning"
    assert result["message"] == "order charged"
    assert result["logger"] == "billing"
    assert result["truncated"] is True


def test_a_huge_log_message_is_shortened_after_the_attributes() -> None:
    payload = log(message="m" * (2 * LIMIT), attributes={"order_id": 7})

    result = decoded(fit("logs", encode(payload)))

    assert "attributes" not in result
    assert result["message"].startswith("mmm")
    assert result["message"].endswith(ELLIPSIS)


def test_a_log_whose_protected_fields_exceed_the_limit_is_refused() -> None:
    assert fit("logs", encode(log(logger="l" * LIMIT))) is None


def test_a_metric_loses_custom_fields_before_its_texts_are_shortened() -> None:
    payload = metric(contexts={"job": "c" * LIMIT}, custom="kept-until-needed", trace_id="trace-1")

    result = decoded(fit("metrics", encode(payload)))

    assert "contexts" not in result
    assert "custom" not in result
    assert result["label"] == "checkout.total"
    assert result["trace_id"] == "trace-1"
    assert result["sample_id"] == "12345678-1234-4234-8234-123456789abc"


def test_metric_free_texts_are_shortened_and_identity_fields_are_untouched() -> None:
    payload = metric(kind="slow_query", sql="SELECT " + "c, " * LIMIT, db_system="postgresql")
    del payload["label"]

    result = decoded(fit("metrics", encode(payload)))

    assert result["sql"].startswith("SELECT c, ")
    assert result["sql"].endswith(ELLIPSIS)
    assert result["db_system"] == "postgresql"
    assert result["kind"] == "slow_query"
    assert result["duration_ms"] == 12.35


def test_a_metric_whose_identity_exceeds_the_limit_is_refused() -> None:
    assert fit("metrics", encode(metric(environment="e" * LIMIT))) is None


def test_a_usage_window_is_capped_with_its_truncated_flag_never_split() -> None:
    symbols = [
        {
            "kind": "route",
            "symbol": f"/{index:05d}/" + "s" * 190,
            "count": 1,
            "last_seen_at": "2026-07-13T14:00:00Z",
        }
        for index in range(5000)
    ]
    payload = {
        "environment": "production",
        "window_started_at": "2026-07-13T14:00:00Z",
        "window_ended_at": "2026-07-13T14:05:00Z",
        "truncated": False,
        "symbols": symbols,
    }

    result = decoded(fit("usages", encode(payload)))

    kept = result["symbols"]
    assert 0 < len(kept) < 5000
    assert kept == symbols[: len(kept)]
    assert result["truncated"] is True


def test_a_usage_window_whose_envelope_exceeds_the_limit_is_refused() -> None:
    payload = {"environment": "e" * LIMIT, "symbols": []}

    assert fit("usages", encode(payload)) is None


def test_unknown_channels_and_non_object_bodies_are_refused_when_oversized() -> None:
    assert fit("pageviews", encode(padded({}, "message", LIMIT + 1))) is None
    assert fit("logs", encode(["x" * LIMIT])) is None


def test_cap_text_keeps_whole_characters_and_leaves_short_texts_alone() -> None:
    assert cap_text("short", 10) == "short"
    assert cap_text("x" * 10, 10) == "x" * 10
    assert cap_text("x" * 11, 10) == "x" * 9 + ELLIPSIS
    assert cap_text(EMOJI * 11, 10) == EMOJI * 9 + ELLIPSIS
    assert cap_text("x" * (MAX_TEXT_CHARS + 1)) == "x" * (MAX_TEXT_CHARS - 1) + ELLIPSIS
