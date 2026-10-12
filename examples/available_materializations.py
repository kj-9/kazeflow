#!/usr/bin/env python3
"""Aggregate deliberately selected materializations after a partial failure.

Run repeatedly with the same root to see successful partitions reused while the
producer continues to fail for the selected bad day:

    uv run python examples/available_materializations.py --root ./materialized
    uv run python examples/available_materializations.py --root ./materialized

The example only queries the explicit contexts below. It does not scan stored
history or change the failed producer result when it publishes available rows.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Sequence

from kazeflow.assets import AssetRegistry
from kazeflow import (
    AssetContext,
    DatePartitionDef,
    Flow,
    MaterializationContext,
)
from kazeflow.file_materialization import FileMaterialization, JsonCodec
from kazeflow.results import FlowStatus


def available_independent_publication(
    adapter: FileMaterialization,
    contexts: Sequence[MaterializationContext],
) -> tuple[tuple[object, ...], tuple[str, ...]]:
    """Load explicitly selected available records as rows and their IDs."""
    records = adapter.available(contexts)
    rows: list[object] = []
    materialization_ids: list[str] = []
    for record in records:
        output = adapter.load(record)
        if not isinstance(output, dict) or not isinstance(output.get("rows"), list):
            raise TypeError("available output must be a JSON object with a rows list")
        rows.extend(output["rows"])
        materialization_ids.append(record.materialization_id)
    return tuple(rows), tuple(materialization_ids)


def publish_snapshot(
    path: Path,
    rows: Sequence[object],
    materialization_ids: Sequence[str],
) -> dict[str, object]:
    """Atomically publish JSON rows and a deterministic receipt in one file."""
    if not isinstance(path, Path):
        raise TypeError("path must be a pathlib.Path")
    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise TypeError("rows must be a non-text sequence")
    if isinstance(materialization_ids, (str, bytes, bytearray)) or not isinstance(
        materialization_ids, Sequence
    ):
        raise TypeError("materialization_ids must be a non-text sequence")
    if any(
        type(materialization_id) is not str or not materialization_id
        for materialization_id in materialization_ids
    ):
        raise ValueError("materialization_ids must contain non-empty strings")

    # Encode and validate all caller data before creating or replacing a file.
    normalized_rows = list(rows)
    normalized_ids = list(materialization_ids)
    strict_json = JsonCodec()
    strict_json.encode(normalized_rows)
    strict_json.encode(normalized_ids)
    identity_payload = json.dumps(
        {"materialization_ids": normalized_ids, "rows": normalized_rows},
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    publication_id = hashlib.sha256(identity_payload).hexdigest()
    receipt: dict[str, object] = {
        "publication_id": publication_id,
        "materialization_ids": normalized_ids,
        "row_count": len(normalized_rows),
    }
    encoded = (
        json.dumps(
            {"rows": normalized_rows, "receipt": receipt},
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(encoded)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--snapshot", type=Path)
    args = parser.parse_args()

    store = FileMaterialization(args.root, codec=JsonCodec())
    partition_def = DatePartitionDef()
    selected = ("2026-10-10", "2026-10-11", "2026-10-12")

    def producer(context: AssetContext) -> dict[str, object]:
        day = partition_def.normalize_key(context.partition_key).isoformat()
        if day == "2026-10-11":
            raise RuntimeError("demonstration producer failure for 2026-10-11")
        return {"partition": day, "rows": [{"day": day, "value": 1}]}

    registry = AssetRegistry()
    registry.register(
        producer,
        partition_def=partition_def,
        materialization=store,
        version="available-aggregation-v1",
    )
    result = asyncio.run(
        Flow(["producer"], registry=registry).run_async({"partition_keys": selected})
    )
    failed_keys = result.failed_partition_keys("producer")
    print(f"producer status: {result.status.value}; failed keys: {failed_keys!r}")
    assert result.status is FlowStatus.FAILED

    contexts = tuple(
        MaterializationContext(
            asset_name="producer",
            version="available-aggregation-v1",
            partition_domain=partition_def.domain,
            partition_key_present=True,
            partition_key=partition_def.normalize_key(key),
        )
        for key in selected
    )
    rows, materialization_ids = available_independent_publication(store, contexts)
    snapshot_path = args.snapshot or args.root.parent / "available-snapshot.json"
    receipt = publish_snapshot(snapshot_path, rows, materialization_ids)
    print(f"published {len(rows)} rows to {snapshot_path}")
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
