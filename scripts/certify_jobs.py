"""Private JSONL control for real Celery/Redis jobs; stdout contains metadata only."""

from __future__ import annotations

import hashlib
import json
import logging
import signal
import sys
import threading
import time
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from types import FrameType
from typing import Any
from urllib.parse import urlsplit

from celery import Celery, signals  # type: ignore[import-untyped]
from celery import __version__ as celery_version
from celery.contrib.testing.worker import start_worker  # type: ignore[import-untyped]

import closeyourit
from closeyourit import CeleryIntegration, Client, Configuration
from closeyourit.transport import Transport

PHASES = ("baseline", "disabled", "enabled", "outage", "uninstalled")
PRIVATE_ARGUMENT = "synthetic-private-job-argument"


def emit(value: dict[str, Any]) -> None:
    print(json.dumps(value), flush=True)


def loopback(value: str, scheme: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != scheme or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Loopback endpoint required")
    return value


def run() -> None:
    config = json.loads(sys.stdin.readline())
    run_id = config["run_id"]
    if len(run_id) != 16 or any(char not in "0123456789abcdef" for char in run_id):
        raise ValueError("Invalid run identity")
    broker = loopback(config["broker_url"], "redis")
    queue = f"certification-{run_id}-python"
    app = Celery("certification_jobs", broker=broker, backend=broker)
    app.conf.update(
        task_default_queue=queue,
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        worker_prefetch_multiplier=1,
        broker_connection_retry_on_startup=False,
    )
    lock = threading.Lock()
    executions: list[dict[str, Any]] = []
    observed: list[dict[str, Any]] = []
    finished: list[str] = []

    def completion(sender: Any = None, task_id: str | None = None, **kwargs: Any) -> None:
        del sender, kwargs
        with lock:
            if task_id is not None:
                finished.append(task_id)

    @app.task(bind=True, name="certification.job", max_retries=1)  # type: ignore[untyped-decorator]
    def task(self: Any, mode: str, _private_argument: str) -> int:
        fails = mode == "failure" or (mode == "retry" and self.request.retries == 0)
        with lock:
            executions.append(
                {
                    "framework": "celery",
                    "job_id": self.request.id,
                    "mode": mode,
                    "observed_attempt": self.request.retries + 1,
                    "application_outcome": "error" if fails else "success",
                    "observed_at": datetime.now(UTC).isoformat(),
                }
            )
        if fails:
            error = ValueError("Certification job failed")
            if mode == "retry":
                raise self.retry(exc=error, countdown=1)
            raise error
        return 42

    def observe(payload: dict[str, Any]) -> dict[str, Any]:
        with lock:
            observed.append(
                {
                    "event_id": payload.get("event_id"),
                    "sample_id": payload.get("sample_id"),
                    "kind": payload.get("kind"),
                    "subtype": payload.get("subtype"),
                    "logger": payload.get("logger"),
                    "label": payload.get("label"),
                    "duration_ms": payload.get("duration_ms"),
                    "level": payload.get("level"),
                    "handled": (payload.get("exception", {}).get("values") or [{}])[-1]
                    .get("mechanism", {})
                    .get("handled"),
                    "job": payload.get("contexts", {}).get("job")
                    or payload.get("attributes", {}).get("contexts", {}).get("job"),
                    "error_job": payload.get("contexts", {}).get("celery"),
                    "private_arguments_absent": PRIVATE_ARGUMENT not in json.dumps(payload),
                }
            )
        return payload

    client = None
    integration = None
    try:
        with start_worker(
            app,
            pool="solo",
            concurrency=1,
            perform_ping_check=False,
            loglevel="CRITICAL",
            logfile="/dev/null",
            shutdown_timeout=15,
        ):
            sdk_root = Path(closeyourit.__file__).parent
            emit(
                {
                    "type": "ready",
                    "sdk": {
                        "version": version("closeyourit"),
                        "path": str(sdk_root),
                        "inventory": {
                            str(path.relative_to(sdk_root.parent)): hashlib.sha256(
                                path.read_bytes()
                            ).hexdigest()
                            for path in sdk_root.rglob("*.py")
                        },
                    },
                    "framework": {
                        "python": sys.version.split()[0],
                        "celery": celery_version,
                    },
                }
            )
            for phase in PHASES:
                command = json.loads(sys.stdin.readline())
                if command == {"command": "stop"}:
                    break
                if command != {"command": "next", "phase": phase}:
                    raise ValueError("Unexpected phase")
                endpoint = loopback(
                    config["outage_endpoint"] if phase == "outage" else config["endpoint"], "http"
                )
                client = Client(
                    Configuration(
                        endpoint_url=endpoint,
                        project_id=config["project_id"],
                        token=None if phase in {"baseline", "disabled"} else config["token"],
                        environment=config["environment"],
                        release=f"{config['release']}-{phase}",
                        usage_enabled=False,
                        slow_method_threshold_ms=0,
                        before_send=observe,
                    )
                )
                integration = CeleryIntegration(
                    client, task_threshold_ms=0, queue_threshold_ms=0, lifecycle_logs=True
                )
                if phase != "baseline":
                    integration.install()
                if phase == "uninstalled":
                    integration.uninstall()
                signals.task_postrun.connect(completion, weak=False)
                with lock:
                    finished.clear()
                    executions.clear()
                    observed.clear()
                start = time.monotonic()
                jobs = []
                results = []
                for mode in ("success", "failure", "retry", "scheduled"):
                    result = task.apply_async(
                        args=(mode, PRIVATE_ARGUMENT), countdown=1 if mode == "scheduled" else 0
                    )
                    jobs.append(
                        {
                            "framework": "celery",
                            "job_id": result.id,
                            "mode": mode,
                            "expected_attempts": 2 if mode == "retry" else 1,
                        }
                    )
                    results.append(result)
                for result in results:
                    result.get(timeout=60, propagate=False, disable_sync_subtasks=False)
                # Result state is saved before task_postrun; require all observed attempts to end.
                deadline = time.monotonic() + 10
                while len(finished) < 5:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Task observation did not settle")
                    time.sleep(0.01)
                if phase == "uninstalled":
                    client.log(
                        "Job certification application completed",
                        level="info",
                        logger="certification.application",
                    )
                flushed = client.flush(timeout=15)
                integration.uninstall()
                signals.task_postrun.disconnect(completion)
                closed = client.close(timeout=15)
                if not flushed or not closed:
                    raise RuntimeError("SDK shutdown deadline exceeded")
                diagnostics = None
                if isinstance(client.sink, Transport):
                    stats = client.sink.stats
                    diagnostics = {
                        name: getattr(stats, name)
                        for name in (
                            "enqueued",
                            "dropped",
                            "sent",
                            "failed",
                            "retried",
                            "pending",
                            "queue_size",
                            "worker_alive",
                            "closed",
                        )
                    }
                with lock:
                    captured_executions, captured_observed = list(executions), list(observed)
                emit(
                    {
                        "type": "phase",
                        "phase": phase,
                        "jobs": jobs,
                        "executions": captured_executions,
                        "observed": captured_observed,
                        "diagnostics": diagnostics,
                        "elapsed_ms": (time.monotonic() - start) * 1000,
                        "telemetry_is_sender_observation": True,
                    }
                )
                for result in results:
                    result.forget()
            client = None
            integration = None
        emit({"type": "completed", "cleanup": {"worker_stopped": True}})
    finally:
        signals.task_postrun.disconnect(completion)
        if integration is not None:
            integration.uninstall()
        if client is not None:
            client.close(timeout=15)
        app.close()


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)

    def terminate(_number: int, _frame: FrameType | None) -> None:
        raise SystemExit(143)

    signal.signal(signal.SIGTERM, terminate)
    try:
        run()
    except Exception:
        emit({"type": "failed", "reason": "producer_failed"})
        raise SystemExit(1) from None
