"""Byte budget of one ingest request: measure the encoded body, shrink it only when oversized.

The ingest refuses a body over 5 MB as a whole, and that refusal is never retried: the event is
lost. Every body is therefore measured in UTF-8 bytes, which is what the server compares and where
an emoji weighs 4. A body up to ``MAX_PAYLOAD_BYTES`` is sent byte for byte as it was built. A body
over it loses its least useful parts first, in a fixed order per channel, and carries
``truncated: true``. The fields that identify a send are never touched: when they alone still
exceed the limit, :func:`fit` returns ``None`` and the transport drops the body with an explicit
reason instead of sending a request the server would refuse.

Usage windows are never split into several requests: a symbol that does not fit is dropped and the
window keeps the ``truncated`` flag it already uses for the per-window symbol cap.
"""

from __future__ import annotations

import json
from collections.abc import Callable, MutableMapping
from typing import Any, cast

MAX_PAYLOAD_BYTES = 512 * 1024
# A text with more characters than the byte budget cannot fit whatever the rest of the payload is,
# because every character costs at least one byte. Cutting it there BEFORE scrubbing bounds the
# scrubber's work and can never change a body that would otherwise have fit.
MAX_TEXT_CHARS = MAX_PAYLOAD_BYTES
TRUNCATION_MARKER = "truncated"
ELLIPSIS = "…"

Payload = MutableMapping[str, Any]
_Step = Callable[[Payload, int], None]


def encode(payload: object) -> bytes:
    """Serialize exactly like the transport does, so the measure is the size on the wire."""
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def cap_text(text: str, max_chars: int = MAX_TEXT_CHARS) -> str:
    """Cut a text to at most ``max_chars`` whole characters, ellipsis included."""
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - len(ELLIPSIS))] + ELLIPSIS


def fit(channel: str, body: bytes, limit: int = MAX_PAYLOAD_BYTES) -> bytes | None:
    """Return ``body`` untouched when it fits, else a shrunk and marked body, else ``None``."""
    if len(body) <= limit:
        return body
    # Decoding the body yields a private deep copy: the caller's payload is never mutated.
    payload = json.loads(body)
    if not isinstance(payload, dict):
        return None
    payload[TRUNCATION_MARKER] = True
    for step in _STEPS.get(channel, ()):
        step(payload, limit)
        if _overflow(payload, limit) <= 0:
            return encode(payload)
    return None


def _overflow(payload: object, limit: int) -> int:
    return len(encode(payload)) - limit


def _trim(payload: Payload, items: list[Any], limit: int, *, oldest_first: bool) -> None:
    # Each item costs its encoded size plus one separator. Sizes are measured once per pass and the
    # loop re-measures the whole body, so the estimate never decides alone when to stop.
    while items:
        overflow = _overflow(payload, limit)
        if overflow <= 0:
            return
        order = range(len(items)) if oldest_first else range(len(items) - 1, -1, -1)
        freed = 0
        count = 0
        for index in order:
            freed += len(encode(items[index])) + 1
            count += 1
            if freed >= overflow:
                break
        if oldest_first:
            del items[:count]
        else:
            del items[len(items) - count :]


def _drop_breadcrumbs(payload: Payload, limit: int) -> None:
    breadcrumbs = payload.get("breadcrumbs")
    values = breadcrumbs.get("values") if isinstance(breadcrumbs, dict) else None
    if isinstance(values, list):
        _trim(payload, values, limit, oldest_first=True)
        if values:
            return
    payload.pop("breadcrumbs", None)


def _drop(*keys: str) -> _Step:
    def step(payload: Payload, limit: int) -> None:
        del limit
        for key in keys:
            payload.pop(key, None)

    return step


def _drop_personal_contexts(payload: Payload, limit: int) -> None:
    del limit
    payload.pop("user", None)
    contexts = payload.get("contexts")
    if not isinstance(contexts, dict):
        payload.pop("contexts", None)
        return
    # The runtime context is SDK-built and tiny; everything else came from the application.
    runtime = contexts.get("runtime")
    contexts.clear()
    if runtime is not None:
        contexts["runtime"] = runtime


def _exception_values(payload: Payload) -> list[dict[str, Any]]:
    exception = payload.get("exception")
    values = exception.get("values") if isinstance(exception, dict) else None
    if not isinstance(values, list):
        return []
    return [value for value in values if isinstance(value, dict)]


def _drop_frames(payload: Payload, limit: int) -> None:
    # The innermost frames, where the error was raised, are the last ones and go last.
    for value in _exception_values(payload):
        stacktrace = value.get("stacktrace")
        frames = stacktrace.get("frames") if isinstance(stacktrace, dict) else None
        if isinstance(frames, list):
            _trim(payload, frames, limit, oldest_first=True)
        if _overflow(payload, limit) <= 0:
            return


def _fit_text(text: str, max_encoded: int) -> str:
    """Longest whole-character prefix that, with the ellipsis, encodes within ``max_encoded``."""
    low, high = 0, min(len(text) - 1, max(0, max_encoded))
    while low < high:
        middle = (low + high + 1) // 2
        if len(encode(text[:middle] + ELLIPSIS)) <= max_encoded:
            low = middle
        else:
            high = middle - 1
    return text[:low] + ELLIPSIS


_TextRef = tuple[MutableMapping[str, Any], str]


def _shrink_texts(payload: Payload, refs: list[_TextRef], limit: int) -> None:
    # Always shorten the largest text first, so a short exception value survives a huge message.
    floor = len(encode(ELLIPSIS))
    candidates = [ref for ref in refs if isinstance(ref[0].get(ref[1]), str)]
    while candidates:
        overflow = _overflow(payload, limit)
        if overflow <= 0:
            return
        sizes = [len(encode(container[key])) for container, key in candidates]
        largest = max(range(len(candidates)), key=sizes.__getitem__)
        container, key = candidates.pop(largest)
        if sizes[largest] <= floor:
            continue
        container[key] = _fit_text(cast(str, container[key]), sizes[largest] - overflow)


def _event_texts(payload: Payload, limit: int) -> None:
    refs: list[_TextRef] = []
    message = payload.get("message")
    if isinstance(message, dict):
        refs.append((message, "formatted"))
    else:
        refs.append((payload, "message"))
    refs.extend((value, "value") for value in _exception_values(payload))
    _shrink_texts(payload, refs, limit)


def _log_texts(payload: Payload, limit: int) -> None:
    _shrink_texts(payload, [(payload, "message")], limit)


# Metric fields that identify the sample or carry its measure: never removed, never shortened.
_METRIC_FIELDS = frozenset(
    {
        "kind",
        "subtype",
        "sample_id",
        "duration_ms",
        "occurred_at",
        "environment",
        "release",
        "trace_id",
        "sdk",
        "query_count",
        "total_query_time_ms",
        TRUNCATION_MARKER,
    }
)
# Free texts written by the application or derived from its code; shortened, never removed.
_METRIC_TEXTS = (
    "label",
    "sql",
    "route",
    "http_host",
    "http_url",
    "http_path",
    "source",
    "db_system",
)


def _drop_metric_custom_fields(payload: Payload, limit: int) -> None:
    del limit
    keep = _METRIC_FIELDS.union(_METRIC_TEXTS)
    for key in [key for key in payload if key not in keep]:
        del payload[key]


def _metric_texts(payload: Payload, limit: int) -> None:
    _shrink_texts(payload, [(payload, key) for key in _METRIC_TEXTS], limit)


def _drop_usage_symbols(payload: Payload, limit: int) -> None:
    symbols = payload.get("symbols")
    if isinstance(symbols, list):
        _trim(payload, symbols, limit, oldest_first=False)


_STEPS: dict[str, tuple[_Step, ...]] = {
    "events": (
        _drop_breadcrumbs,
        _drop("request"),
        _drop_personal_contexts,
        _drop("extra"),
        _drop("tags"),
        _drop_frames,
        _event_texts,
    ),
    "logs": (_drop("attributes"), _log_texts),
    "metrics": (_drop_metric_custom_fields, _metric_texts),
    "usages": (_drop_usage_symbols,),
}
