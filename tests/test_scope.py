from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from closeyourit.configuration import Configuration
from closeyourit.scope import current_scope, reset_scope, with_scope
from closeyourit.scrubber import FILTERED


@pytest.fixture(autouse=True)
def clean_scope() -> None:
    reset_scope()


def test_scope_is_empty_by_default() -> None:
    assert current_scope().snapshot().to_event_data() == {}
    assert current_scope().trace_id is None


def test_scope_serializes_and_scrubs_context_without_pii_by_default() -> None:
    scope = current_scope()
    scope.set_user({"id": "account-1", "email": "private@example.com"})
    scope.set_tags({"tenant": "acme", "tier": "gold"})
    scope.set_tag("area", "checkout")
    scope.set_extra("token", "secret")
    scope.set_context("runtime", {"name": "python", "version": "3.14"})
    scope.set_request(
        {"url": "https://example.com/path?token=secret", "headers": {"Cookie": "a=b"}}
    )
    scope.trace_id = "trace-1"

    assert scope.snapshot().to_event_data() == {
        "user": {"id": "account-1"},
        "tags": {"tenant": "acme", "tier": "gold", "area": "checkout"},
        "extra": {"token": FILTERED},
        "contexts": {"runtime": {"name": "python", "version": "3.14"}},
        "request": {
            "url": "https://example.com/path",
            "headers": {"Cookie": FILTERED},
        },
        "trace_id": "trace-1",
    }


def test_scope_can_include_user_pii_only_when_explicitly_enabled() -> None:
    current_scope().set_user(
        {"id": "account-1", "email": "private@example.com", "username": "alice"}
    )
    configuration = Configuration(send_pii=True)

    assert current_scope().snapshot(configuration).to_event_data()["user"] == {
        "id": "account-1",
        "email": "private@example.com",
        "username": "alice",
    }


def test_scope_snapshot_is_deeply_immutable_and_detached_from_inputs() -> None:
    context = {"nested": {"count": 1}}
    current_scope().set_context("custom", context)
    context["nested"]["count"] = 2

    snapshot = current_scope().snapshot()

    assert snapshot.to_event_data()["contexts"] == {"custom": {"nested": {"count": 1}}}
    with pytest.raises(TypeError):
        snapshot.contexts["new"] = "value"  # type: ignore[index]
    custom = snapshot.contexts["custom"]
    assert isinstance(custom, dict) is False
    with pytest.raises(TypeError):
        custom["new"] = "value"  # type: ignore[index]


def test_with_scope_inherits_then_restores_parent_state() -> None:
    current_scope().set_tag("tenant", "parent")

    with with_scope() as scope:
        scope.set_tag("tenant", "child")
        scope.set_tag("temporary", "yes")
        assert scope.snapshot().to_event_data()["tags"] == {
            "tenant": "child",
            "temporary": "yes",
        }

    assert current_scope().snapshot().to_event_data()["tags"] == {"tenant": "parent"}


def test_with_scope_restores_parent_after_exception() -> None:
    current_scope().set_tag("tenant", "parent")

    with pytest.raises(RuntimeError), with_scope() as scope:
        scope.set_tag("tenant", "child")
        raise RuntimeError("boom")

    assert current_scope().snapshot().to_event_data()["tags"] == {"tenant": "parent"}


def test_async_tasks_do_not_leak_scope_state() -> None:
    async def capture(user_id: str) -> dict[str, object]:
        current_scope().set_user({"id": user_id})
        await asyncio.sleep(0)
        return current_scope().snapshot().to_event_data()

    async def run() -> list[dict[str, object]]:
        first, second = await asyncio.gather(capture("first"), capture("second"))
        return [first, second]

    assert asyncio.run(run()) == [
        {"user": {"id": "first"}},
        {"user": {"id": "second"}},
    ]
    assert current_scope().snapshot().to_event_data() == {}


def test_threads_start_with_an_empty_scope() -> None:
    current_scope().set_user({"id": "parent"})
    barrier = Barrier(2)

    def capture(user_id: str) -> dict[str, object]:
        before = current_scope().snapshot().to_event_data()
        barrier.wait()
        current_scope().set_user({"id": user_id})
        return {"before": before, "after": current_scope().snapshot().to_event_data()}

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(capture, ["first", "second"]))

    assert results == [
        {"before": {}, "after": {"user": {"id": "first"}}},
        {"before": {}, "after": {"user": {"id": "second"}}},
    ]
    assert current_scope().snapshot().to_event_data() == {"user": {"id": "parent"}}


def test_custom_filter_parameters_apply_at_snapshot_time() -> None:
    current_scope().set_extra("customer_reference", "private")
    configuration = Configuration(filter_parameters=("customer_reference",))

    assert current_scope().snapshot(configuration).to_event_data() == {
        "extra": {"customer_reference": FILTERED}
    }


def test_reset_scope_clears_all_state() -> None:
    current_scope().set_user({"id": "account-1"})
    current_scope().set_tag("tenant", "acme")

    reset_scope()

    assert current_scope().snapshot().to_event_data() == {}


def test_scope_clear_removes_current_context_state() -> None:
    scope = current_scope()
    scope.set_tag("worker", "billing")
    scope.trace_id = " trace-123 "

    assert scope.trace_id == "trace-123"

    scope.clear()

    assert scope.trace_id is None
    assert scope.snapshot().to_event_data() == {}
