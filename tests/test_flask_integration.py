from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterable, Iterator
from typing import cast

import pytest

from closeyourit import Client, Configuration, FlaskIntegration, current_scope
from closeyourit.client import Channel

flask = pytest.importorskip("flask")
from flask import Flask, Response  # noqa: E402


class RecordingSink:
    def __init__(self) -> None:
        self.items: list[tuple[Channel, dict[str, object]]] = []

    def enqueue(self, channel: Channel, payload: dict[str, object]) -> bool:
        self.items.append((channel, payload))
        return True


def configured_client(*, threshold: float = 1000.0) -> tuple[Client, RecordingSink]:
    sink = RecordingSink()
    configuration = Configuration(
        endpoint_url="https://ingest.example.test",
        token="cyi_server-token",
        project_id="12345678-1234-1234-1234-123456789abc",
        environment="production",
        slow_request_threshold_ms=threshold,
    )
    return Client(configuration, sink=sink), sink


def test_flask_module_does_not_import_framework_at_package_import_time() -> None:
    command = "import sys, closeyourit.flask; assert 'flask' not in sys.modules"
    result = subprocess.run([sys.executable, "-c", command], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_flask_integration_records_templated_route_headers_and_slow_request() -> None:
    client, sink = configured_client(threshold=0.0)
    app = Flask(__name__)
    FlaskIntegration(app, client=client, monotonic=lambda: 1.0)
    seen: list[dict[str, object]] = []

    @app.get("/orders/<int:order_id>")
    def order(order_id: int) -> tuple[str, int]:
        del order_id
        seen.append(current_scope().snapshot(client.configuration).to_event_data())
        return "created", 201

    response = app.test_client().get(
        "/orders/42?token=private",
        headers={"Accept": "application/json", "Authorization": "Bearer private"},
    )

    assert response.status_code == 201
    assert response.data == b"created"
    assert seen[0]["request"]["route"] == "/orders/<int:order_id>"  # type: ignore[index]
    assert "Authorization" not in seen[0]["request"]["headers"]  # type: ignore[index]
    metrics = [payload for channel, payload in sink.items if channel == "metrics"]
    assert len(metrics) == 1
    assert metrics[0]["route"] == "/orders/<int:order_id>"


def test_flask_404_is_preserved_and_unhandled_500_is_captured_once() -> None:
    client, sink = configured_client()
    app = Flask(__name__)
    app.config.update(TESTING=False)
    app.logger.disabled = True
    FlaskIntegration(app, client=client)

    @app.get("/boom")
    def boom() -> str:
        raise RuntimeError("flask exploded")

    test_client = app.test_client()
    assert test_client.get("/missing").status_code == 404
    assert [item for item in sink.items if item[0] == "events"] == []

    response = test_client.get("/boom")

    assert response.status_code == 500
    events = [payload for channel, payload in sink.items if channel == "events"]
    assert len(events) == 1
    assert events[0]["request"]["status_code"] == 500  # type: ignore[index]
    assert events[0]["exception"]["values"][-1]["mechanism"]["handled"] is False  # type: ignore[index]


def test_flask_streaming_keeps_request_scope_and_integration_is_idempotent() -> None:
    client, _sink = configured_client()
    app = Flask(__name__)
    integration = FlaskIntegration(app, client=client)
    integration.init_app(app)
    seen: list[str] = []

    @app.get("/stream/<name>")
    def stream(name: str) -> Response:
        del name

        def generate() -> Iterator[str]:
            seen.append(str(current_scope().snapshot().request["route"]))  # type: ignore[index]
            yield "one"
            seen.append(str(current_scope().snapshot().request["route"]))  # type: ignore[index]
            yield "two"

        return Response(generate())

    response = app.test_client().get("/stream/alice", buffered=False)
    assert b"".join(cast(Iterable[bytes], response.response)) == b"onetwo"
    response.close()
    assert seen == ["/stream/<name>", "/stream/<name>"]
