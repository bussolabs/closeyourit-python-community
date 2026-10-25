"""Context-local scope with copy-on-write state and immutable capture snapshots."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import cast

from closeyourit.configuration import Configuration
from closeyourit.models import FrozenJsonValue, freeze_json, thaw_json
from closeyourit.scrubber import Scrubber


def _empty_mapping() -> Mapping[str, FrozenJsonValue]:
    return MappingProxyType({})


def _freeze_mapping(value: Mapping[str, object]) -> Mapping[str, FrozenJsonValue]:
    return cast(Mapping[str, FrozenJsonValue], freeze_json(value))


def _replace_key(
    mapping: Mapping[str, FrozenJsonValue], key: str, value: object
) -> Mapping[str, FrozenJsonValue]:
    updated = dict(mapping)
    updated[str(key)] = freeze_json(value)
    return MappingProxyType(updated)


@dataclass(frozen=True, slots=True)
class _ScopeState:
    user: Mapping[str, FrozenJsonValue] = field(default_factory=_empty_mapping)
    tags: Mapping[str, FrozenJsonValue] = field(default_factory=_empty_mapping)
    extra: Mapping[str, FrozenJsonValue] = field(default_factory=_empty_mapping)
    contexts: Mapping[str, FrozenJsonValue] = field(default_factory=_empty_mapping)
    request: Mapping[str, FrozenJsonValue] | None = None
    trace_id: str | None = None
    breadcrumbs: tuple[Mapping[str, FrozenJsonValue], ...] = ()


@dataclass(frozen=True, slots=True)
class ScopeSnapshot:
    """Deeply immutable scope state captured on the caller's execution context."""

    user: Mapping[str, FrozenJsonValue] = field(default_factory=_empty_mapping)
    tags: Mapping[str, FrozenJsonValue] = field(default_factory=_empty_mapping)
    extra: Mapping[str, FrozenJsonValue] = field(default_factory=_empty_mapping)
    contexts: Mapping[str, FrozenJsonValue] = field(default_factory=_empty_mapping)
    request: Mapping[str, FrozenJsonValue] | None = None
    trace_id: str | None = None
    breadcrumbs: tuple[Mapping[str, FrozenJsonValue], ...] = ()

    def to_event_data(self) -> dict[str, object]:
        result: dict[str, object] = {}
        for key in ("user", "tags", "extra", "contexts"):
            value = cast(Mapping[str, FrozenJsonValue], getattr(self, key))
            if value:
                result[key] = thaw_json(value)
        if self.request:
            result["request"] = thaw_json(self.request)
        if self.trace_id:
            result["trace_id"] = self.trace_id
        if self.breadcrumbs:
            result["breadcrumbs"] = {
                "values": [thaw_json(breadcrumb) for breadcrumb in self.breadcrumbs]
            }
        return result


_EMPTY_STATE = _ScopeState()
_SCOPE_STATE: ContextVar[_ScopeState] = ContextVar("closeyourit_scope", default=_EMPTY_STATE)


class Scope:
    """Facade whose mutations update only the current ContextVar execution context."""

    def set_user(self, attributes: Mapping[str, object] | None) -> None:
        state = _SCOPE_STATE.get()
        user = _empty_mapping() if attributes is None else _freeze_mapping(attributes)
        _SCOPE_STATE.set(replace(state, user=user))

    def set_tag(self, key: str, value: object) -> None:
        state = _SCOPE_STATE.get()
        _SCOPE_STATE.set(replace(state, tags=_replace_key(state.tags, key, value)))

    def set_tags(self, attributes: Mapping[str, object]) -> None:
        state = _SCOPE_STATE.get()
        tags = dict(state.tags)
        tags.update({str(key): freeze_json(value) for key, value in attributes.items()})
        _SCOPE_STATE.set(replace(state, tags=MappingProxyType(tags)))

    def set_extra(self, key: str, value: object) -> None:
        state = _SCOPE_STATE.get()
        _SCOPE_STATE.set(replace(state, extra=_replace_key(state.extra, key, value)))

    def set_context(self, key: str, attributes: Mapping[str, object]) -> None:
        state = _SCOPE_STATE.get()
        _SCOPE_STATE.set(
            replace(state, contexts=_replace_key(state.contexts, key, dict(attributes)))
        )

    def set_request(self, request: Mapping[str, object] | None) -> None:
        state = _SCOPE_STATE.get()
        frozen = None if request is None else _freeze_mapping(request)
        _SCOPE_STATE.set(replace(state, request=frozen))

    @property
    def trace_id(self) -> str | None:
        return _SCOPE_STATE.get().trace_id

    @trace_id.setter
    def trace_id(self, value: str | None) -> None:
        normalized = value.strip() if value is not None else None
        _SCOPE_STATE.set(replace(_SCOPE_STATE.get(), trace_id=normalized or None))

    def clear(self) -> None:
        _SCOPE_STATE.set(_EMPTY_STATE)

    def add_breadcrumb(self, breadcrumb: Mapping[str, object], *, max_breadcrumbs: int) -> None:
        if max_breadcrumbs <= 0:
            return
        state = _SCOPE_STATE.get()
        items = (*state.breadcrumbs, _freeze_mapping(breadcrumb))[-max_breadcrumbs:]
        _SCOPE_STATE.set(replace(state, breadcrumbs=items))

    def snapshot(self, configuration: Configuration | None = None) -> ScopeSnapshot:
        config = Configuration() if configuration is None else configuration
        state = _SCOPE_STATE.get()
        scrubber = Scrubber(config.filter_parameters)

        raw_user = cast(dict[str, object], thaw_json(state.user))
        if not config.send_pii:
            raw_user = {"id": raw_user["id"]} if "id" in raw_user else {}

        return ScopeSnapshot(
            user=_freeze_mapping(cast(Mapping[str, object], raw_user)),
            tags=self._scrub_mapping(scrubber, state.tags),
            extra=self._scrub_mapping(scrubber, state.extra),
            contexts=self._scrub_mapping(scrubber, state.contexts),
            request=self._scrub_request(scrubber, state.request),
            trace_id=state.trace_id,
            breadcrumbs=tuple(
                self._scrub_breadcrumb(scrubber, breadcrumb) for breadcrumb in state.breadcrumbs
            ),
        )

    def _scrub_mapping(
        self, scrubber: Scrubber, value: Mapping[str, FrozenJsonValue]
    ) -> Mapping[str, FrozenJsonValue]:
        scrubbed = scrubber.scrub(thaw_json(value))
        return _freeze_mapping(cast(Mapping[str, object], scrubbed))

    def _scrub_request(
        self, scrubber: Scrubber, value: Mapping[str, FrozenJsonValue] | None
    ) -> Mapping[str, FrozenJsonValue] | None:
        if value is None:
            return None
        scrubbed = scrubber.scrub(thaw_json(value))
        return _freeze_mapping(cast(Mapping[str, object], scrubbed))

    def _scrub_breadcrumb(
        self, scrubber: Scrubber, value: Mapping[str, FrozenJsonValue]
    ) -> Mapping[str, FrozenJsonValue]:
        thawed = cast(dict[str, object], thaw_json(value))
        if isinstance(thawed.get("message"), str):
            thawed["message"] = scrubber.scrub_message(cast(str, thawed["message"]))
        return _freeze_mapping(cast(Mapping[str, object], scrubber.scrub(thawed)))


_CURRENT_SCOPE = Scope()


def current_scope() -> Scope:
    return _CURRENT_SCOPE


def reset_scope() -> None:
    _SCOPE_STATE.set(_EMPTY_STATE)


@contextmanager
def with_scope() -> Iterator[Scope]:
    """Fork the current state and restore it even if the block raises."""
    token = _SCOPE_STATE.set(_SCOPE_STATE.get())
    try:
        yield _CURRENT_SCOPE
    finally:
        _SCOPE_STATE.reset(token)
