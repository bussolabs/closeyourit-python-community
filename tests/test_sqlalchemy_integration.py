from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

from closeyourit import Client
from closeyourit.integrations import instrument_sqlalchemy, sql_fingerprint
from tests.test_client import RecordingSink, valid_configuration


class StepClock:
    def __init__(self, *values: float) -> None:
        self._values = iter(values)

    def __call__(self) -> float:
        return next(self._values)


def integration_client(**configuration: object) -> tuple[Client, RecordingSink]:
    sink = RecordingSink()
    client = Client(valid_configuration(**configuration), sink=sink)
    return client, sink


def metrics(sink: RecordingSink) -> list[dict[str, object]]:
    return [payload for channel, payload in sink.items if channel == "metrics"]


def test_sql_fingerprint_removes_literals_comments_and_bind_values() -> None:
    fingerprint = sql_fingerprint(
        "SELECT * FROM users WHERE email = 'private@example.test' "
        "AND id = 12345 AND token = X'DEADBEEF' -- private comment"
    )

    assert fingerprint == "SELECT * FROM users WHERE email = ? AND id = ? AND token = ?"
    assert "private" not in fingerprint
    assert "12345" not in fingerprint
    assert "DEADBEEF" not in fingerprint


@pytest.mark.parametrize(("duration_ms", "expected"), [(9.99, 0), (10.0, 1), (10.01, 1)])
def test_slow_query_threshold_x_minus_one_x_x_plus_one(duration_ms: float, expected: int) -> None:
    client, sink = integration_client(slow_query_threshold_ms=10)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    instrumentation = instrument_sqlalchemy(
        engine, client, monotonic=StepClock(1.0, 1.0 + duration_ms / 1000)
    )

    with engine.connect() as connection:
        assert connection.execute(text("SELECT 12345")).scalar_one() == 12345

    captured = [item for item in metrics(sink) if item["kind"] == "slow_query"]
    assert len(captured) == expected
    if captured:
        assert captured[0]["sql"] == "SELECT ?"
        assert "parameters" not in captured[0]
        assert "bindings" not in captured[0]
    instrumentation.uninstrument()
    engine.dispose()


def test_sqlalchemy_profile_reports_query_count_and_n_plus_one_without_raw_binds() -> None:
    client, sink = integration_client(
        slow_query_threshold_ms=0,
        n_plus_one_threshold=2,
        query_count_threshold=3,
    )
    engine = create_engine("sqlite+pysqlite:///:memory:")
    instrumentation = instrument_sqlalchemy(engine, client)

    with instrumentation.profile(route="orders.index"), engine.connect() as connection:
        for identifier in (100, 200, 300, 400):
            assert (
                connection.execute(
                    text("SELECT :identifier"), {"identifier": identifier}
                ).scalar_one()
                == identifier
            )

    captured = metrics(sink)
    n_plus_one = [item for item in captured if item.get("subtype") == "n_plus_one"]
    high_count = [item for item in captured if item.get("subtype") == "high_query_count"]
    assert len(n_plus_one) == 1
    assert n_plus_one[0]["query_count"] == 4
    assert n_plus_one[0]["sql"] == "SELECT ?"
    assert len(high_count) == 1
    assert high_count[0]["query_count"] == 4
    assert high_count[0]["route"] == "orders.index"
    assert all("100" not in str(item.get("sql", "")) for item in captured)
    instrumentation.uninstrument()
    engine.dispose()


@pytest.mark.parametrize(("count", "expected"), [(1, 0), (2, 0), (3, 1)])
def test_n_plus_one_uses_strictly_greater_than_threshold(count: int, expected: int) -> None:
    client, sink = integration_client(
        slow_query_threshold_ms=0,
        n_plus_one_threshold=2,
        query_count_threshold=100,
    )
    engine = create_engine("sqlite+pysqlite:///:memory:")
    instrumentation = instrument_sqlalchemy(engine, client)

    with instrumentation.profile(), engine.connect() as connection:
        for identifier in range(count):
            connection.execute(text("SELECT :identifier"), {"identifier": identifier})

    assert len([item for item in metrics(sink) if item.get("subtype") == "n_plus_one"]) == expected
    instrumentation.uninstrument()
    engine.dispose()


@pytest.mark.parametrize(("count", "expected"), [(2, 0), (3, 0), (4, 1)])
def test_high_query_count_uses_strictly_greater_than_threshold(count: int, expected: int) -> None:
    client, sink = integration_client(
        slow_query_threshold_ms=0,
        n_plus_one_threshold=100,
        query_count_threshold=3,
    )
    engine = create_engine("sqlite+pysqlite:///:memory:")
    instrumentation = instrument_sqlalchemy(engine, client)

    with instrumentation.profile(), engine.connect() as connection:
        for identifier in range(count):
            connection.execute(text("SELECT :identifier"), {"identifier": identifier})

    assert (
        len([item for item in metrics(sink) if item.get("subtype") == "high_query_count"])
        == expected
    )
    instrumentation.uninstrument()
    engine.dispose()


def test_empty_profile_and_uninstrument_are_noops() -> None:
    client, sink = integration_client()
    engine = create_engine("sqlite+pysqlite:///:memory:")
    instrumentation = instrument_sqlalchemy(engine, client)

    with instrumentation.profile():
        pass
    instrumentation.uninstrument()
    instrumentation.uninstrument()
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))

    assert sink.items == []
    engine.dispose()


def test_nested_profiles_restore_the_outer_query_count() -> None:
    client, sink = integration_client(
        slow_query_threshold_ms=0,
        n_plus_one_threshold=100,
        query_count_threshold=1,
    )
    engine = create_engine("sqlite+pysqlite:///:memory:")
    instrumentation = instrument_sqlalchemy(engine, client)

    with instrumentation.profile(route="outer"), engine.connect() as connection:
        connection.execute(text("SELECT 1"))
        with instrumentation.profile(route="inner"):
            connection.execute(text("SELECT 2"))
            connection.execute(text("SELECT 3"))
        connection.execute(text("SELECT 4"))

    high_counts = [item for item in metrics(sink) if item.get("subtype") == "high_query_count"]
    assert [(item["route"], item["query_count"]) for item in high_counts] == [
        ("inner", 2),
        ("outer", 2),
    ]
    instrumentation.uninstrument()
    engine.dispose()


def test_profiles_do_not_collect_queries_from_other_instrumented_engines() -> None:
    first_client, first_sink = integration_client(
        slow_query_threshold_ms=0,
        n_plus_one_threshold=100,
        query_count_threshold=1,
    )
    second_client, _second_sink = integration_client(slow_query_threshold_ms=0)
    first_engine = create_engine("sqlite+pysqlite:///:memory:")
    second_engine = create_engine("sqlite+pysqlite:///:memory:")
    first = instrument_sqlalchemy(first_engine, first_client)
    second = instrument_sqlalchemy(second_engine, second_client)

    with first.profile(route="first"):
        with first_engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        with second_engine.connect() as connection:
            connection.execute(text("SELECT 2"))

    assert [item for item in metrics(first_sink) if item.get("subtype")] == []
    first.uninstrument()
    second.uninstrument()
    first_engine.dispose()
    second_engine.dispose()


def test_async_engine_concurrency_keeps_profiles_isolated() -> None:
    pytest.importorskip("aiosqlite")

    async def scenario() -> list[dict[str, object]]:
        client, sink = integration_client(
            slow_query_threshold_ms=0,
            n_plus_one_threshold=100,
            query_count_threshold=1,
        )
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        instrumentation = instrument_sqlalchemy(engine, client)

        async def execute(route: str, values: tuple[int, ...]) -> None:
            with instrumentation.profile(route=route):
                async with engine.connect() as connection:
                    for value in values:
                        await connection.execute(text("SELECT :value"), {"value": value})
                        await asyncio.sleep(0)

        await asyncio.gather(execute("one", (1, 2)), execute("two", (3, 4, 5)))
        instrumentation.uninstrument()
        await engine.dispose()
        return metrics(sink)

    captured = asyncio.run(scenario())
    high_counts = [item for item in captured if item.get("subtype") == "high_query_count"]
    assert sorted((item["route"], item["query_count"]) for item in high_counts) == [
        ("one", 2),
        ("two", 3),
    ]
