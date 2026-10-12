# JSON automation contract

Use `--format json` when another program consumes kazeflow output. Human-readable
text is for review and can change in layout; automation must not parse it. A
successful JSON command writes exactly one typed document to standard output.

Version note: plan v5 and run/history v4 are unreleased changes on `main`. Use the
schema matching the installed package, not necessarily the newest source tree.

## Common envelope

Every completed JSON response has exactly these top-level fields:

```json
{
  "document_type": "kazeflow.plan",
  "schema_version": 5,
  "data": {}
}
```

`document_type` selects the meaning of `data`; `schema_version` applies only to
that document type. Reject a document whose type is unknown or whose version is not
one your consumer explicitly supports.

| Command outcome | Document type | Schema | Representative document |
| --- | --- | --- | --- |
| `assets --format json` | `kazeflow.assets` | [`assets.schema.json`](../schemas/cli/v7/assets.schema.json) | [`assets`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/assets.json) |
| `partitions --format json` | `kazeflow.partitions` | [`partitions.schema.json`](../schemas/cli/v7/partitions.schema.json) | [`partitions`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/partitions.json) |
| `plan --format json` | `kazeflow.plan` v5 | [`plan.schema.json`](../schemas/cli/v7/plan.schema.json) | [`key selection`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/plan-keys.json), [`range`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/plan-range.json), [`empty`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/plan-empty.json), [`retry policy`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/plan-retry.json), [`partition group`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/plan-group.json) |
| completed `run --format json` | `kazeflow.run-result` v4 | [`run-result.schema.json`](../schemas/cli/v7/run-result.schema.json) | [`success`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/run-success.json), [`asset failure`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/run-failed.json), [`circuit open`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/run-circuit-open.json) |
| declined interactive JSON run | `kazeflow.run-declined` | [`run-declined.schema.json`](../schemas/cli/v7/run-declined.schema.json) | [`declined`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/run-declined.json) |
| `runs list --format json` | `kazeflow.runs-list` v4 | [`runs-list.schema.json`](../schemas/cli/v7/runs-list.schema.json) | [`runs list`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/runs-list.json) |
| `runs show --format json` | `kazeflow.runs-show` v4 | [`runs-show.schema.json`](../schemas/cli/v7/runs-show.schema.json) | [`runs show`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/runs-show.json) |
| `runs compare --format json` | `kazeflow.runs-compare` v4 | [`runs-compare.schema.json`](../schemas/cli/v7/runs-compare.schema.json) | [`runs compare`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v7/runs-compare.json) |

The schemas are normative Draft 2020-12 documents. Assets, partitions, and declined
run documents remain at version 1. The v1 through v4 plan schemas and goldens, and
the v1 through v6 collections, remain available for historical consumers. The shared
v7 schema collection documents current versions: plan v5, run-result and history
v4, and other document types v1.
The checked-in examples are normalized only where values necessarily vary between
runs, such as run IDs, timestamps, durations, and traceback locations.

## Version namespaces

Do not treat these version values as interchangeable:

| Field or value | Scope |
| --- | --- |
| envelope `schema_version` | The selected CLI `document_type` only. |
| `record_schema_version` | The nested portable `RunResult.to_record()` contract. |
| `store_schema_version` | The SQLite database format. |
| package version | The installed kazeflow distribution. |

For example, a completed run uses a wrapper so its portable record remains a
separate Python and storage contract:

```json
{
  "document_type": "kazeflow.run-result",
  "schema_version": 4,
  "data": {
    "record_schema_version": 4,
    "record": {"run_id": "…", "status": "success", "tasks": []}
  }
}
```

## Data fields

The JSON Schemas above are the complete machine-readable definitions. This table is
the command-oriented field reference.

| Document type | `data` fields |
| --- | --- |
| `kazeflow.assets` | `declared_flow` (boolean) and `assets` (ordered asset records). Each asset has `name`, `dependencies`, and `partitioned`. |
| `kazeflow.partitions` | `targets` and `partitions`. Each partition record has `asset`, `definition_kind`, `domain`, `key_format`, and `supports_range`. |
| `kazeflow.plan` v5 | `targets`, selected `resources`, `partition_groups`, `config`, and dependency-first `tasks`. Each group has ordered `assets` and `max_active_keys`; selected raw keys and callback code are never projected. |
| `kazeflow.run-result` v4 | `record_schema_version: 4` and a portable record with invocation history, circuit skip reasons, and successful materialization provenance. |
| `kazeflow.run-declined` | `reason`, currently `not_approved`. It is not a `RunResult`. |
| `kazeflow.runs-list` v4 | `runs`, with each summary reporting `record_schema_version` (1, 2, 3, or 4), `status`, and `saved_at`. |
| `kazeflow.runs-show` v4 | `run_id`, per-record `record_schema_version`, `status`, `saved_at`, `store_schema_version`, and a v1, v2, v3, or v4 portable record. |
| `kazeflow.runs-compare` v4 | `left`, `right`, and `comparison`. Each stored side retains its own record version and full record; comparison has flow deltas and ordered task aggregate comparisons. |

`partition_selection.kind` is one of `omitted`, `keys`, `range`, `empty`, or
`unpartitioned`; `domain` and `count` are nullable only where that kind has no
partitioned selection. `max_concurrency` is a positive integer or `null`.
Resource summaries are name-sorted and include only policies used by selected
tasks. `partition_groups` is empty when no declared group is selected. A non-null
`rate_limit` bounds body invocation starts in a rolling window;
`max_attempts` is the operation budget and `classifier_enabled` reveals whether a
callback is configured without serializing its code. A v1 plan consumer can migrate
through v2/v3/v4 or directly to v5, adding the resource array, nullable task resource,
rate limit, retry metadata, and selected partition groups.

### Portable record shape

The nested `record` in `run-result` and `runs-show` retains `run_id`, flow `status`,
UTC `started_at`/`ended_at`, `duration_seconds`, and ordered `tasks`. A task retains
`task.task_name`, `is_partitioned`, terminal `status`, timestamps, duration,
nullable `reason`, `blocked_by`, and ordered `attempts`. Reasons include
`resource_circuit_open` and `mixed_blocked` in record v3; `mixed_blocked` describes
a task aggregate whose skipped partitions have different reasons. Attempt reasons
are limited to `dependency_blocked` and `resource_circuit_open`. Record v1 and v2
reject both new enum values. An attempt retains its `attempt.task.task_name`,
structural `attempt.partition.present` boolean, status, timestamps, duration,
nullable reason, blockers, nullable portable `failure`, and `invocations`. Each
invocation has a contiguous one-based number, success/failed
status, start/end timestamps, duration, nullable failure, and nullable retry and
resource-cooldown seconds. Invocation retries do not create extra logical attempts.
Failure metadata contains `exception_type`, `message`, and `traceback`.

CLI-produced run records use record v4. `RunResult.to_record()` still defaults to
v1 for Python callers unless they request `schema_version=2`, `3`, or `4`. V4 adds
nullable `resolution` and `materialization_id` fields to every attempt. Successful
ordinary work reports `executed` with no ID, committed work reports `executed` with
its committed ID, and reused work reports `materialized` with its ID. Non-successful
attempts have null values. SQLite layout remains store schema v1; it can contain
record v1 through v4 rows, reported independently by each history envelope. Circuit
`blocked_by` references retain the source task name and partition-presence flag in
portable records, but omit raw keys; partition identities can therefore be lossy
after serialization.

`comparison` contains `status_changed`, `duration_seconds_delta`,
`task_count_delta`, and ordered `tasks`. Each task comparison has `task_name`,
nullable `left`/`right` task summaries, and `changed`; a present summary has
`is_partitioned`, `status`, `reason`, attempt counts, status counts,
`failure_present`, and `failure_exception_types`.

For exact nullability, required fields, enum values, and nesting, validate against
the linked schema rather than reimplementing this prose.

## Ordering is part of version 1

Object-member order is not meaningful. Array order is:

- assets: asset-name order;
- plan tasks and dependencies: resolved dependency-first plan order;
- partition attempts: normalized selected-key order;
- run-result tasks: plan order;
- history list: saved time, then run ID;
- comparison `left` and `right`: command argument order.

## Streams, trust, and privacy

During JSON mode, kazeflow routes diagnostics, preflight, confirmation prompts,
progress, and ordinary `sys.stdout` writes from trusted entry modules, explicit
factories, and approved asset bodies to standard error. Standard output therefore
contains only the completed typed JSON document.

This is Python-level stream routing, not a sandbox, redaction facility, or guarantee
about direct file-descriptor writes or user code that replaces global streams. Entry
loading and asset execution still run trusted Python. User output redirected to
stderr can contain sensitive values.

Portable documents structurally omit arbitrary task outputs, raw exception objects,
and dedicated raw partition-key fields. Materialization IDs are opaque,
adapter-defined diagnostic strings; they can contain application information and
are not automatically redacted. Inspection does not resolve IDs or load artifacts.
Documents also do **not** redact application-controlled failure metadata: an
exception message or traceback can repeat a partition key, credential fragment, or
other sensitive value. See the [trust boundary](../concepts/trust-boundary.md).

## Exit and document matrix

| Outcome | Exit | JSON stdout |
| --- | ---: | --- |
| Successful inspection, history command, or completed successful run | `0` | One command-specific typed document. |
| Interactive JSON run declined | `0` | One `kazeflow.run-declined` document. |
| Completed run with asset failure | `1` | One `kazeflow.run-result` document. |
| Usage or configuration error | `2` | No successful document. |
| Entry load or resolution error | `3` | No successful document. |
| Infrastructure or requested adapter failure | `4` | No successful document. |

An adapter failure takes precedence over an otherwise completed asset failure and
suppresses the final run-result document. See [exit codes](exit-codes.md) for the
human-facing status reference.
