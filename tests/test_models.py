from collections.abc import Mapping

from closeyourit.models import CIRCULAR, freeze_json, thaw_json


def test_freeze_json_normalizes_bytes_unknown_values_and_null_bytes() -> None:
    assert freeze_json(b"before\x00after") == "beforeafter"
    assert freeze_json(object()) == "<object>"


def test_freeze_json_creates_a_deeply_immutable_snapshot_that_can_be_thawed() -> None:
    source = {"items": [b"one", {"count": 2}]}

    frozen = freeze_json(source)

    assert isinstance(frozen, Mapping)
    assert isinstance(frozen["items"], tuple)
    assert thaw_json(frozen) == {"items": ["one", {"count": 2}]}


def test_freeze_json_marks_circular_mappings_and_sequences() -> None:
    circular_mapping: dict[str, object] = {}
    circular_mapping["self"] = circular_mapping
    circular_sequence: list[object] = []
    circular_sequence.append(circular_sequence)

    assert thaw_json(freeze_json(circular_mapping)) == {"self": CIRCULAR}
    assert thaw_json(freeze_json(circular_sequence)) == [CIRCULAR]
