"""Shared types and immutable JSON snapshots used by the SDK core."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from re import Pattern
from types import MappingProxyType
from typing import TypeAlias, cast

JsonScalar: TypeAlias = bool | int | float | str | None
JsonValue: TypeAlias = JsonScalar | dict[str, "JsonValue"] | list["JsonValue"]
FrozenJsonValue: TypeAlias = (
    JsonScalar | Mapping[str, "FrozenJsonValue"] | tuple["FrozenJsonValue", ...]
)
EventPayload: TypeAlias = dict[str, object]
BeforeSend: TypeAlias = Callable[[EventPayload], EventPayload | None]
FilterParameter: TypeAlias = str | Pattern[str]

CIRCULAR = "[Circular]"


def freeze_json(value: object, *, _seen: set[int] | None = None) -> FrozenJsonValue:
    """Create a deeply immutable, JSON-compatible snapshot without leaking object reprs."""
    seen = set() if _seen is None else _seen

    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").replace("\x00", "")

    if isinstance(value, Mapping):
        identity = id(value)
        if identity in seen:
            return CIRCULAR
        seen.add(identity)
        try:
            frozen = {
                str(key).replace("\x00", ""): freeze_json(item, _seen=seen)
                for key, item in value.items()
            }
            return cast(Mapping[str, FrozenJsonValue], MappingProxyType(frozen))
        finally:
            seen.remove(identity)

    if isinstance(value, Sequence):
        identity = id(value)
        if identity in seen:
            return CIRCULAR
        seen.add(identity)
        try:
            return tuple(freeze_json(item, _seen=seen) for item in value)
        finally:
            seen.remove(identity)

    return f"<{type(value).__name__}>"


def thaw_json(value: FrozenJsonValue) -> JsonValue:
    """Return a mutable JSON-compatible copy of a frozen snapshot."""
    if isinstance(value, Mapping):
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value
