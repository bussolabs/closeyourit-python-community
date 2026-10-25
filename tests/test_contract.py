from __future__ import annotations

import hashlib
import json
import platform
from collections import deque
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeAlias
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from closeyourit import Client, Configuration, HttpResponse, Transport
from closeyourit._version import __version__
from closeyourit.client import Channel
from closeyourit.events import EventBuilder
from closeyourit.models import EventPayload

ROOT = Path(__file__).parents[1] / "contracts" / "ingest"
CONTRACT = ROOT / "v1"
NOW = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
IDENTIFIER = UUID("12345678-1234-4234-8234-123456789abc")
Outcome: TypeAlias = HttpResponse | BaseException


def load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def pin_sdk_version(payload: dict[str, object]) -> None:
    """Align the golden fixture's `sdk.version` with the current package version.

    The fixture pins the *wire contract*, not the SDK version: that one changes at every
    release and would block CI at every bump. Same treatment as `contexts.runtime.version`.
    """
    sdk = payload.get("sdk")
    assert isinstance(sdk, dict)
    sdk["version"] = __version__


def schema_and_manifest() -> tuple[dict[str, object], dict[str, object]]:
    schema = load_json(CONTRACT / "schema.json")
    manifest = load_json(CONTRACT / "manifest.json")
    assert isinstance(schema, dict)
    assert isinstance(manifest, dict)
    return schema, manifest


def http_cases() -> list[object]:
    cases = load_json(CONTRACT / "fixtures/http/cases.json")
    assert isinstance(cases, list)
    return cases


def auth_cases() -> list[object]:
    cases = load_json(CONTRACT / "fixtures/http/auth-cases.json")
    assert isinstance(cases, list)
    return cases


def test_vendored_contract_matches_its_canonical_lock_and_checksums() -> None:
    lock = load_json(ROOT / "LOCK.json")
    assert isinstance(lock, dict)
    sums = (CONTRACT / "SHA256SUMS").read_bytes()
    assert hashlib.sha256(sums).hexdigest() == lock["sha256sums"]

    for line in sums.decode().splitlines():
        expected, relative = line.split("  ./", maxsplit=1)
        assert hashlib.sha256((CONTRACT / relative).read_bytes()).hexdigest() == expected


def test_schema_and_every_manifest_fixture_have_the_expected_validity() -> None:
    schema, manifest = schema_and_manifest()
    Draft202012Validator.check_schema(schema)
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    fixtures = manifest["fixtures"]
    assert isinstance(fixtures, list)

    for fixture in fixtures:
        assert isinstance(fixture, dict)
        definition = fixture["definition"]
        assert isinstance(definition, str)
        validator = Draft202012Validator(
            {"$ref": f"#/$defs/{definition}", "$defs": definitions},
            format_checker=FormatChecker(),
        )
        path = fixture["path"]
        assert isinstance(path, str)
        errors = list(validator.iter_errors(load_json(CONTRACT / path)))
        assert bool(errors) is (not fixture["valid"]), path


def test_python_builders_match_the_golden_message_log_and_metric_payloads() -> None:
    configuration = Configuration(environment="production", release="v0.1.0")
    builder = EventBuilder(configuration, clock=lambda: NOW, id_factory=lambda: IDENTIFIER)

    expected_message = load_json(CONTRACT / "fixtures/valid/events/message-python.json")
    assert isinstance(expected_message, dict)
    expected_message["contexts"]["runtime"]["version"] = platform.python_version()
    pin_sdk_version(expected_message)
    assert builder.message("cache miss", level="warning") == expected_message

    expected_log = load_json(CONTRACT / "fixtures/valid/logs/single-python.json")
    assert isinstance(expected_log, dict)
    pin_sdk_version(expected_log)
    actual_log = builder.log("order charged", logger="billing", attributes={"order_id": 7})
    assert actual_log == expected_log

    expected_metric = load_json(CONTRACT / "fixtures/valid/metrics/slow-method-python.json")
    assert isinstance(expected_metric, dict)
    pin_sdk_version(expected_metric)
    assert builder.slow_method("checkout.total", 12.345) == expected_metric


def test_python_slow_request_builder_matches_the_golden_performance_issue_schema() -> None:
    schema, _manifest = schema_and_manifest()
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    validator = Draft202012Validator(
        {"$ref": "#/$defs/performance_issue", "$defs": definitions},
        format_checker=FormatChecker(),
    )
    payload = EventBuilder(
        Configuration(environment="production"),
        clock=lambda: NOW,
        id_factory=lambda: IDENTIFIER,
    ).slow_request(
        route="/orders/{id}",
        path="/orders/42",
        url="https://user:secret@example.test:443/orders/42?token=private#fragment",
        duration_ms=12.345,
    )

    assert list(validator.iter_errors(payload)) == []
    assert payload["http_url"] == "https://example.test/orders/42"


def test_python_sql_metric_builders_produce_schema_valid_private_payloads() -> None:
    schema, _manifest = schema_and_manifest()
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    configuration = Configuration(environment="production")
    builder = EventBuilder(configuration, clock=lambda: NOW, id_factory=lambda: IDENTIFIER)
    slow_query = builder.slow_query(
        "SELECT * FROM users WHERE email = 'private@example.test' AND id = 42 -- hidden",
        8.5,
    )
    performance = builder.performance_issue(
        "repeated_http",
        http_host="api.example.test",
        http_url="/users/<n>",
        query_count=5,
    )

    for definition, payload in (
        ("slow_query", slow_query),
        ("performance_issue", performance),
    ):
        validator = Draft202012Validator(
            {"$ref": f"#/$defs/{definition}", "$defs": definitions},
            format_checker=FormatChecker(),
        )
        assert list(validator.iter_errors(payload)) == []

    assert slow_query["sql"] == "SELECT * FROM users WHERE email = ? AND id = ?"
    assert performance["duration_ms"] == 0.0


class ContractRequester:
    def __init__(self, outcomes: list[Outcome]) -> None:
        self.outcomes = deque(outcomes)
        self.calls = 0

    def send(
        self, url: str, body: bytes, headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        del url, body, headers, timeout
        self.calls += 1
        outcome = self.outcomes.popleft()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class MetricSink:
    def __init__(self) -> None:
        self.items: list[tuple[Channel, EventPayload]] = []

    def enqueue(self, channel: Channel, payload: EventPayload) -> bool:
        self.items.append((channel, payload))
        return True


def enabled_configuration() -> Configuration:
    return Configuration(
        endpoint_url="https://ingest.example.com",
        token="cyi_contract",
        project_id="12345678-1234-1234-1234-123456789abc",
        environment="production",
    )


def test_capture_duration_enrichment_matches_the_golden_slow_method_schema() -> None:
    schema, _manifest = schema_and_manifest()
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    validator = Draft202012Validator(
        {"$ref": "#/$defs/slow_method", "$defs": definitions},
        format_checker=FormatChecker(),
    )
    sink = MetricSink()
    client = Client(enabled_configuration(), sink=sink)

    assert client.capture_duration(
        "celery.task.duration.billing.charge",
        12.5,
        contexts={"celery": {"task_name": "billing.charge", "retry_count": 1}},
    )

    channel, payload = sink.items[0]
    assert channel == "metrics"
    assert list(validator.iter_errors(payload)) == []


@pytest.mark.parametrize("case", http_cases())
def test_transport_matches_every_golden_http_case(case: object) -> None:
    assert isinstance(case, dict)
    status = case["status"]
    first: Outcome
    if status is None:
        first = OSError("network")
    else:
        assert isinstance(status, int)
        headers = case.get("headers", {})
        assert isinstance(headers, dict)
        first = HttpResponse(status, headers=headers)
    outcomes: list[Outcome] = [first, HttpResponse(202)] if case["retry"] else [first]
    requester = ContractRequester(outcomes)
    transport = Transport(
        enabled_configuration(),
        requester=requester,
        sleep=lambda _delay: None,
        random_value=lambda: 0.0,
    )

    assert transport.enqueue("events", {"message": "contract"})
    assert transport.flush(1)

    assert requester.calls == (2 if case["retry"] else 1)
    if isinstance(status, int) and 200 <= status < 300:
        assert transport.stats.sent == 1
    elif case["retry"]:
        assert transport.stats.sent == 1
        assert transport.stats.retried == 1
    else:
        assert transport.stats.failed == 1
    transport.close()


def test_auth_contract_separates_bearer_public_dsn_and_trusted_gateway() -> None:
    cases = {case["id"]: case for case in auth_cases() if isinstance(case, dict)}

    assert cases["public-store"]["credential"] == "public_dsn"
    assert cases["public-pageviews-rejected"]["status"] == 401
    assert cases["gateway-pageviews"]["forward_to"].startswith("/api/v1/projects/")
    assert cases["gateway-replays"]["forward_to"].startswith("/api/v1/projects/")
