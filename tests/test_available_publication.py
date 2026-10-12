"""Evidence for the application-owned atomic publication example."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
from typing import Any

import pytest

from kazeflow.flow import Flow
from kazeflow.results import AttemptStatus, FlowStatus

EXAMPLE_PATH = Path(__file__).parents[1] / "examples" / "available_materializations.py"


def load_example() -> Any:
    spec = importlib.util.spec_from_file_location(
        "kazeflow_available_materializations_example", EXAMPLE_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_publish_snapshot_keeps_rows_and_stable_receipt_in_one_file(tmp_path):
    example = load_example()
    destination = tmp_path / "snapshot.json"
    rows = [{"partition": "a", "value": 1}, {"partition": "b", "value": 2}]
    materialization_ids = ["artifact-a", "artifact-b"]

    first_receipt = example.publish_snapshot(destination, rows, materialization_ids)
    first_payload = json.loads(destination.read_text(encoding="utf-8"))
    second_receipt = example.publish_snapshot(destination, rows, materialization_ids)
    second_payload = json.loads(destination.read_text(encoding="utf-8"))
    different_inputs_receipt = example.publish_snapshot(
        tmp_path / "different-inputs.json", rows, ["artifact-a", "artifact-c"]
    )

    assert first_payload == {"rows": rows, "receipt": first_receipt}
    assert second_payload == {"rows": rows, "receipt": second_receipt}
    assert first_receipt == second_receipt
    assert first_receipt["materialization_ids"] == materialization_ids
    assert first_receipt["row_count"] == len(rows)
    assert isinstance(first_receipt["publication_id"], str)
    assert first_receipt["publication_id"]
    assert different_inputs_receipt["publication_id"] != first_receipt["publication_id"]


def test_replace_failure_preserves_old_complete_file_and_cleans_temp(
    tmp_path, monkeypatch
):
    example = load_example()
    destination = tmp_path / "snapshot.json"
    old_payload = {
        "rows": [{"partition": "old", "value": 9}],
        "receipt": {
            "publication_id": "old-publication",
            "materialization_ids": ["old-artifact"],
            "row_count": 1,
        },
    }
    old_bytes = json.dumps(old_payload, sort_keys=True).encode("utf-8")
    destination.write_bytes(old_bytes)
    replace_calls = []
    reported_receipts = []

    def fail_replace(source, target):
        replace_calls.append((source, target))
        raise OSError("simulated atomic replacement failure")

    monkeypatch.setattr(example.os, "replace", fail_replace)

    with pytest.raises(OSError, match="simulated atomic replacement failure"):
        receipt = example.publish_snapshot(
            destination, [{"partition": "new", "value": 10}], ["new-artifact"]
        )
        reported_receipts.append(receipt)

    assert len(replace_calls) == 1
    assert reported_receipts == []
    assert destination.read_bytes() == old_bytes
    assert list(tmp_path.iterdir()) == [destination]


def test_invalid_json_rows_fail_before_writing_or_replacing(tmp_path, monkeypatch):
    example = load_example()
    destination = tmp_path / "snapshot.json"
    old_bytes = b'{"rows":["old"],"receipt":{"publication_id":"old"}}\n'
    destination.write_bytes(old_bytes)
    replace_calls = []
    monkeypatch.setattr(
        example.os,
        "replace",
        lambda source, target: replace_calls.append((source, target)),
    )

    with pytest.raises((TypeError, ValueError)):
        example.publish_snapshot(destination, [object()], ["artifact"])

    assert replace_calls == []
    assert destination.read_bytes() == old_bytes
    assert list(tmp_path.iterdir()) == [destination]


def test_example_reuses_prior_outputs_and_keeps_producer_failure_on_repeated_run(
    tmp_path, monkeypatch, capsys
):
    example = load_example()
    root = tmp_path / "materialized"
    snapshot = tmp_path / "published" / "snapshot.json"
    results = []
    run_async = Flow.run_async

    async def capture_result(self, *args, **kwargs):
        result = await run_async(self, *args, **kwargs)
        results.append(result)
        return result

    monkeypatch.setattr(Flow, "run_async", capture_result)
    monkeypatch.setattr(
        sys,
        "argv",
        [str(EXAMPLE_PATH), "--root", str(root), "--snapshot", str(snapshot)],
    )

    assert example.main() == 0
    first_output = capsys.readouterr().out
    first_payload = json.loads(snapshot.read_text(encoding="utf-8"))
    assert example.main() == 0
    second_output = capsys.readouterr().out
    second_payload = json.loads(snapshot.read_text(encoding="utf-8"))

    assert len(results) == 2
    for result in results:
        assert result.status is FlowStatus.FAILED
        assert len(result.failed_partition_keys("producer")) == 1
        assert all(
            attempt.status is AttemptStatus.FAILED
            for attempt in result.tasks[0].attempts
            if attempt.attempt.partition_key
            == result.failed_partition_keys("producer")[0]
        )

    second_attempts = results[1].tasks[0].attempts
    assert [attempt.status for attempt in second_attempts] == [
        AttemptStatus.SUCCESS,
        AttemptStatus.FAILED,
        AttemptStatus.SUCCESS,
    ]
    assert [
        attempt.resolution
        for attempt in second_attempts
        if attempt.status is AttemptStatus.SUCCESS
    ] == ["materialized", "materialized"]
    assert first_payload == second_payload
    assert len(first_payload["rows"]) == 2
    assert first_payload["receipt"]["row_count"] == 2
    assert "producer status: failed" in first_output
    assert "producer status: failed" in second_output
    assert "published 2 rows" in first_output
    assert "published 2 rows" in second_output
