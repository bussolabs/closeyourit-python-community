"""The vendored contract is an offline invariant, not framework support evidence."""

import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from closeyourit.integrations._jobs import bounded_text, timing_context

ROOT = Path(__file__).parents[1] / "contracts/jobs/v1"


def test_contract_snapshot_matches_its_lock() -> None:
    lock = json.loads((ROOT / "LOCK.json").read_text())
    for path, digest in lock["sha256"].items():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == digest


def test_context_is_valid_with_known_missing_and_skewed_clocks() -> None:
    schema = json.loads((ROOT / "schema.json").read_text())
    validator = Draft202012Validator(
        {"$ref": "#/$defs/job_context", "$defs": schema["$defs"]}, format_checker=FormatChecker()
    )
    for enqueue, schedule, start in [(100, 700, 730), (None, 700, 730), (100, None, 90)]:
        context = {
            "schema_version": 1,
            "framework": "celery",
            "name": "ReportJob",
            "attempt_outcome": "unknown",
            "terminal": None,
            **timing_context(enqueued=enqueue, scheduled=schedule, started=start),
        }
        assert not list(validator.iter_errors(context))


def test_descriptive_fields_are_bounded_without_coercing_arbitrary_objects() -> None:
    assert bounded_text("x" * 201, 200) is None
    assert bounded_text("queue\nname", 128) is None
    assert bounded_text({"args": "private"}, 256) is None
    assert bounded_text(" orders ", 128) == " orders "
