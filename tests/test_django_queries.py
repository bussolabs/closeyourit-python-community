from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast

import pytest

from closeyourit import instrument_django
from tests.test_sqlalchemy_integration import StepClock, integration_client, metrics

pytest.importorskip("django")
import django  # type: ignore[import-untyped]
from asgiref.sync import sync_to_async
from django.conf import settings  # type: ignore[import-untyped]
from django.db import OperationalError, connections  # type: ignore[import-untyped]
from django.http import StreamingHttpResponse  # type: ignore[import-untyped]
from django.test import RequestFactory  # type: ignore[import-untyped]

if not settings.configured:
    settings.configure(SECRET_KEY="test-only", ALLOWED_HOSTS=["testserver"])
settings.DATABASES = {
    "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
    "secondary": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
}

django.setup()


def query(value: int = 1, alias: str = "default") -> int:
    with connections[alias].cursor() as cursor:
        cursor.execute("SELECT %s", [value])
        return int(cursor.fetchone()[0])


def close_test_connections() -> None:
    for connection in connections.all(initialized_only=True):
        if connection.connection is not None:
            connection.connection.close()
            connection.connection = None


@pytest.fixture(autouse=True)
def close_connections() -> Iterator[None]:
    yield
    close_test_connections()

    async def close_worker() -> None:
        await sync_to_async(close_test_connections)()

    asyncio.run(close_worker())


@pytest.mark.parametrize("count", [0, 1, 2, 3])
def test_django_query_profile_thresholds_and_multiple_connections(count: int) -> None:
    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=2, query_count_threshold=2
    )
    handle = instrument_django(client)
    with handle.profile(route="orders.index"):
        for value in range(count):
            assert query(value, "secondary" if value % 2 else "default") == value
    captured = metrics(sink)
    assert len(captured) == (2 if count > 2 else 0)
    if captured:
        assert {item["subtype"] for item in captured} == {"n_plus_one", "high_query_count"}
        assert all(item["query_count"] == count for item in captured)
    handle.uninstrument()


@pytest.mark.parametrize(("duration", "expected"), [(0.0099, 0), (0.010, 1), (0.0101, 1)])
def test_django_slow_query_is_private_and_preserves_result(duration: float, expected: int) -> None:
    client, sink = integration_client(slow_query_threshold_ms=10)
    handle = instrument_django(client, monotonic=StepClock(0.0, duration))
    with handle.profile(), connections["default"].cursor() as cursor:
        cursor.execute("SELECT %s, 'private@example.test' /* private note */", ["private-token"])
        assert cursor.fetchone() == ("private-token", "private@example.test")
    captured = metrics(sink)
    assert len(captured) == expected
    if captured:
        assert captured[0]["kind"] == "slow_query"
        assert captured[0]["db_system"] == "sqlite"
        assert "private" not in json.dumps(captured)
        assert "params" not in json.dumps(captured)
    handle.uninstrument()


def test_django_nested_profiles_do_not_double_count_and_uninstall_preserves_other_wrappers() -> (
    None
):
    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=100, query_count_threshold=1
    )
    first = instrument_django(client)
    second = instrument_django(client)
    connection = connections["default"]
    called: list[str] = []

    def other(execute: Any, sql: str, params: Any, many: bool, context: Any) -> Any:
        called.append(sql)
        return execute(sql, params, many, context)

    with connection.execute_wrapper(other), first.profile(route="outer"):
        query()
        with second.profile(route="inner"):
            query()
            query()
        query()
        first.uninstrument()
        second.uninstrument()
        assert connection.execute_wrappers == [other]
        query()
    counts = [(item["route"], item["query_count"]) for item in metrics(sink)]
    assert counts == [("inner", 2)]
    assert len(called) == 5
    first.uninstrument()
    second.uninstrument()
    assert connection.execute_wrappers == []


def test_django_sync_to_async_and_parallel_threads_keep_profiles_isolated() -> None:
    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=100, query_count_threshold=1
    )
    handle = instrument_django(client)

    async def request(route: str, count: int) -> None:
        with handle.profile(route=route):
            for value in range(count):
                assert await sync_to_async(query, thread_sensitive=True)(value) == value
                await asyncio.sleep(0)

    async def run() -> None:
        await asyncio.gather(request("one", 2), request("two", 3))

    asyncio.run(run())

    def worker(count: int) -> None:
        with handle.profile(route=f"thread-{count}"):
            for value in range(count):
                query(value)
        close_test_connections()

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(worker, (4, 5)))
    assert sorted((item["route"], item["query_count"]) for item in metrics(sink)) == [
        ("one", 2),
        ("thread-4", 4),
        ("thread-5", 5),
        ("two", 3),
    ]
    handle.uninstrument()


def test_django_failed_execution_is_counted_once_and_propagates_original_error() -> None:

    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=0, query_count_threshold=0
    )
    handle = instrument_django(client)
    with (
        pytest.raises(OperationalError),
        handle.profile(),
        connections["default"].cursor() as cursor,
    ):
        cursor.execute("SELECT * FROM u26_nonexistent_table")
    assert [item["query_count"] for item in metrics(sink)] == [1, 1]
    handle.uninstrument()


def test_django_middleware_profiles_streaming_until_close() -> None:

    from closeyourit import DjangoMiddleware

    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=100, query_count_threshold=1
    )

    def content() -> Iterator[bytes]:
        query(2)
        yield b"first"
        query(3)
        yield b"second"

    def response(request: Any) -> Any:
        query(1)
        return StreamingHttpResponse(content())

    middleware = DjangoMiddleware(response, client=client, query_monitoring=True)
    streamed = cast(Any, middleware(RequestFactory().get("/orders")))
    assert not metrics(sink)
    assert b"".join(streamed.streaming_content) == b"firstsecond"
    streamed.close()
    assert [item["query_count"] for item in metrics(sink)] == [3]
    middleware.uninstrument()


def test_django_middleware_finishes_unconsumed_stream_once() -> None:

    from closeyourit import DjangoMiddleware

    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=100, query_count_threshold=0
    )

    def response(request: Any) -> Any:
        query()
        return StreamingHttpResponse(iter([b"never consumed"]))

    middleware = DjangoMiddleware(response, client=client, query_monitoring=True)
    streamed = cast(Any, middleware(RequestFactory().get("/orders")))
    streamed.close()
    streamed.close()
    assert [item["query_count"] for item in metrics(sink)] == [1]
    middleware.uninstrument()


def test_django_async_middleware_streaming_and_concurrent_requests() -> None:

    from closeyourit import DjangoMiddleware

    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=100, query_count_threshold=1
    )

    async def content(count: int) -> Any:
        for value in range(count):
            assert await sync_to_async(query)(value) == value
            yield b"chunk"

    async def response(request: Any) -> Any:
        await sync_to_async(query)()
        return StreamingHttpResponse(content(int(request.path[1:])))

    middleware = DjangoMiddleware(response, client=client, query_monitoring=True)

    async def request(count: int) -> None:
        streamed = await middleware(RequestFactory().get(f"/{count}"))  # type: ignore[misc]
        assert len([part async for part in streamed.streaming_content]) == count
        await sync_to_async(streamed.close)()

    async def run() -> None:
        await asyncio.gather(request(2), request(3))

    asyncio.run(run())
    assert sorted(cast(int, item["query_count"]) for item in metrics(sink)) == [3, 4]
    middleware.uninstrument()


def test_django_async_profile_observes_existing_worker_connection() -> None:
    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=100, query_count_threshold=1
    )

    async def run() -> None:
        await sync_to_async(query)()
        handle = instrument_django(client)
        async with handle.async_profile(route="async-existing"):
            await sync_to_async(query)()
            await sync_to_async(query)()
        handle.uninstrument()

    asyncio.run(run())
    assert [(item["route"], item["query_count"]) for item in metrics(sink)] == [
        ("async-existing", 2)
    ]


def test_django_executemany_is_one_execution_without_retaining_parameters() -> None:
    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=0, query_count_threshold=0
    )
    handle = instrument_django(client)
    with connections["default"].cursor() as cursor:
        cursor.execute("CREATE TABLE u26_values (value TEXT)")
        with handle.profile():
            cursor.executemany(
                "INSERT INTO u26_values VALUES (%s)", [("private-one",), ("private-two",)]
            )
        cursor.execute("SELECT COUNT(*) FROM u26_values")
        assert cursor.fetchone()[0] == 2
        cursor.execute("DROP TABLE u26_values")
    assert [item["query_count"] for item in metrics(sink)] == [1, 1]
    assert "private" not in json.dumps(metrics(sink))
    handle.uninstrument()


def test_django_observation_failures_do_not_change_database_behavior() -> None:
    client, sink = integration_client(slow_query_threshold_ms=0)

    def failed_clock() -> float:
        raise RuntimeError("injected observer clock failure")

    handle = instrument_django(client, monotonic=failed_clock)
    with handle.profile():
        assert query(42) == 42
    handle.uninstrument()
    assert sink.items == []


def test_django_closed_or_absent_profiles_do_not_observe_late_queries() -> None:
    client, sink = integration_client(slow_query_threshold_ms=0, query_count_threshold=0)
    handle = instrument_django(client)
    query()
    with handle.profile() as profile:
        profile.finish()
        profile.finish()
        query()
    handle.uninstrument()
    with handle.profile():
        query()
    assert sink.items == []


def test_django_query_module_is_optional_in_a_core_only_installation() -> None:
    import subprocess
    import sys
    from pathlib import Path

    program = (
        "import sys; sys.path.insert(0, sys.argv[1]); "
        "from closeyourit import Client, Configuration, instrument_django; "
        "handle=instrument_django(Client(Configuration())); "
        "assert not handle.active; handle.uninstrument()"
    )
    result = subprocess.run(
        [sys.executable, "-S", "-c", program, str(Path(__file__).parents[1] / "src")],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_django_stream_failure_preserves_exception_and_finishes_query_profile() -> None:
    from closeyourit import DjangoMiddleware

    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=100, query_count_threshold=0
    )

    def content() -> Iterator[bytes]:
        query()
        yield b"first"
        raise ValueError("application stream failure")

    middleware = DjangoMiddleware(
        lambda request: StreamingHttpResponse(content()), client=client, query_monitoring=True
    )
    response = cast(Any, middleware(RequestFactory().get("/stream")))
    with pytest.raises(ValueError, match="application stream failure"):
        list(response.streaming_content)
    response.close()
    assert [
        item["query_count"] for item in metrics(sink) if item.get("subtype") == "high_query_count"
    ] == [1]
    assert len([item for channel, item in sink.items if channel == "events"]) == 1
    middleware.uninstrument()


def test_real_django_orm_sync_and_async_queries_share_safe_fingerprints() -> None:
    from django.db import models

    class QueryRecord(models.Model):  # type: ignore[misc]
        name = models.CharField(max_length=100)

        class Meta:
            app_label = "u26"
            db_table = "u26_query_record"

    def create() -> None:
        with connections["default"].schema_editor() as editor:
            editor.create_model(QueryRecord)
        QueryRecord.objects.bulk_create(
            [QueryRecord(name="private-one"), QueryRecord(name="private-two")]
        )

    def drop() -> None:
        with connections["default"].schema_editor() as editor:
            editor.delete_model(QueryRecord)

    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=1, query_count_threshold=1
    )
    handle = instrument_django(client)
    create()
    try:
        with handle.profile(route="orm-sync"):
            assert QueryRecord.objects.filter(name="private-one").exists()
            assert QueryRecord.objects.filter(name="private-two").exists()
    finally:
        drop()

    async def run() -> None:
        await sync_to_async(create)()
        try:
            async with handle.async_profile(route="orm-async"):
                assert await QueryRecord.objects.filter(name="private-one").aexists()
                assert await QueryRecord.objects.filter(name="private-two").aexists()
        finally:
            await sync_to_async(drop)()

    asyncio.run(run())
    handle.uninstrument()
    assert len(metrics(sink)) == 4
    assert all(item["query_count"] == 2 for item in metrics(sink))
    assert "private-one" not in json.dumps(metrics(sink))
    assert "private-two" not in json.dumps(metrics(sink))


def test_install_inside_existing_django_wrapper_preserves_its_stack_exit() -> None:
    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=100, query_count_threshold=0
    )
    called: list[bool] = []

    def other(execute: Any, sql: Any, params: Any, many: bool, context: Any) -> Any:
        called.append(True)
        return execute(sql, params, many, context)

    connection = connections["default"]
    with connection.execute_wrapper(other):
        handle = instrument_django(client)
        with handle.profile():
            query()
    assert other not in connection.execute_wrappers
    with handle.profile():
        query()
    assert len(called) == 1
    assert [item["query_count"] for item in metrics(sink)] == [1, 1]
    handle.uninstrument()
    assert connection.execute_wrappers == []


def test_unavailable_async_attachment_executor_does_not_break_http_response() -> None:
    from asgiref.sync import SyncToAsync, ThreadSensitiveContext
    from django.http import HttpResponse

    from closeyourit import DjangoMiddleware

    client, _sink = integration_client()

    async def response(request: Any) -> Any:
        return HttpResponse("still available")

    middleware = DjangoMiddleware(response, client=client, query_monitoring=True)

    async def run() -> None:
        async with ThreadSensitiveContext() as context:  # type: ignore[no-untyped-call, unused-ignore]
            executor = ThreadPoolExecutor(max_workers=1)
            executor.shutdown()
            SyncToAsync.context_to_thread_executor[context] = executor
            result = await middleware(RequestFactory().get("/health"))  # type: ignore[misc]
            assert result.content == b"still available"

    try:
        asyncio.run(run())
    finally:
        middleware.uninstrument()


def test_large_query_is_diagnosed_without_truncating_a_sensitive_literal(caplog: Any) -> None:
    client, sink = integration_client(slow_query_threshold_ms=0, query_count_threshold=0)
    with (
        instrument_django(client) as handle,
        handle.profile(),
        connections["default"].cursor() as cursor,
    ):
        cursor.execute("SELECT '" + "sensitive" * 3000 + "'")
        assert cursor.fetchone()[0] == "sensitive" * 3000
    assert sink.items == []
    assert "capture limit" in caplog.text
    assert "sensitive" not in caplog.text


def test_broken_profile_route_is_fail_safe_and_diagnostic(caplog: Any) -> None:
    client, sink = integration_client(
        slow_query_threshold_ms=0, n_plus_one_threshold=100, query_count_threshold=0
    )

    def route() -> str:
        raise RuntimeError("private route details")

    with instrument_django(client) as handle, handle.profile(route=route):
        assert query(42) == 42
    assert sink.items == []
    assert "could not be reported" in caplog.text
    assert "private route details" not in caplog.text


def test_query_release_receives_normal_sdk_privacy_filtering() -> None:
    client, sink = integration_client(slow_query_threshold_ms=1, release="token=private-value")
    with instrument_django(client, monotonic=StepClock(0, 0.02)) as handle, handle.profile():
        query()
    assert len(metrics(sink)) == 1
    assert "private-value" not in json.dumps(metrics(sink))
    assert "release" in metrics(sink)[0]
