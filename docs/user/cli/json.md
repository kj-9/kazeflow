# JSON automation contract

Use `--format json` when another program consumes kazeflow output. Human-readable
text is for review and can change in layout; automation must not parse it. A
successful JSON command writes exactly one typed document to standard output.

## Common envelope

Every completed JSON response has exactly these top-level fields:

```json
{
  "document_type": "kazeflow.plan",
  "schema_version": 1,
  "data": {}
}
```

`document_type` selects the meaning of `data`; `schema_version` applies only to
that document type. Reject a document whose type is unknown or whose version is not
one your consumer explicitly supports.

| Command outcome | Document type | Schema | Representative document |
| --- | --- | --- | --- |
| `assets --format json` | `kazeflow.assets` | [`assets.schema.json`](../schemas/cli/v1/assets.schema.json) | [`assets`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/assets.json) |
| `partitions --format json` | `kazeflow.partitions` | [`partitions.schema.json`](../schemas/cli/v1/partitions.schema.json) | [`partitions`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/partitions.json) |
| `plan --format json` | `kazeflow.plan` | [`plan.schema.json`](../schemas/cli/v1/plan.schema.json) | [`key selection`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/plan-keys.json), [`range`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/plan-range.json), [`empty`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/plan-empty.json) |
| completed `run --format json` | `kazeflow.run-result` | [`run-result.schema.json`](../schemas/cli/v1/run-result.schema.json) | [`success`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/run-success.json), [`asset failure`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/run-failed.json) |
| declined interactive JSON run | `kazeflow.run-declined` | [`run-declined.schema.json`](../schemas/cli/v1/run-declined.schema.json) | [`declined`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/run-declined.json) |
| `runs list --format json` | `kazeflow.runs-list` | [`runs-list.schema.json`](../schemas/cli/v1/runs-list.schema.json) | [`runs list`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/runs-list.json) |
| `runs show --format json` | `kazeflow.runs-show` | [`runs-show.schema.json`](../schemas/cli/v1/runs-show.schema.json) | [`runs show`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/runs-show.json) |
| `runs compare --format json` | `kazeflow.runs-compare` | [`runs-compare.schema.json`](../schemas/cli/v1/runs-compare.schema.json) | [`runs compare`](https://github.com/kj-9/kazeflow/blob/main/tests/fixtures/cli-json/v1/runs-compare.json) |

The schemas are normative Draft 2020-12 documents. The combined schema and its
shared definitions are also available as [`schema.json`](../schemas/cli/v1/schema.json).
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
  "schema_version": 1,
  "data": {
    "record_schema_version": 1,
    "record": {"run_id": "…", "status": "success", "tasks": []}
  }
}
```

## Version-1 data fields

The JSON Schemas above are the complete machine-readable definitions. This table is
the command-oriented field reference.

| Document type | `data` fields |
| --- | --- |
| `kazeflow.assets` | `declared_flow` (boolean) and `assets` (ordered asset records). Each asset has `name`, `dependencies`, and `partitioned`. |
| `kazeflow.partitions` | `targets` and `partitions`. Each partition record has `asset`, `definition_kind`, `domain`, `key_format`, and `supports_range`. |
| `kazeflow.plan` | `targets`, `config`, and dependency-first `tasks`. `config` has `max_concurrency` and `partition_selection`; each task has `name`, `dependencies`, and `partition_selection`. A selection has `kind`, `domain`, and `count`. |
| `kazeflow.run-result` | `record_schema_version` and one portable `record`. |
| `kazeflow.run-declined` | `reason`, currently `not_approved`. It is not a `RunResult`. |
| `kazeflow.runs-list` | `runs`, whose summaries have `run_id`, `record_schema_version`, `status`, and `saved_at`. |
| `kazeflow.runs-show` | `run_id`, `record_schema_version`, `status`, `saved_at`, `store_schema_version`, and portable `record`. |
| `kazeflow.runs-compare` | `left`, `right`, and `comparison`. `left` and `right` use the stored-run shape; `comparison` has flow deltas and ordered task aggregate comparisons. |

### Portable record shape

The nested `record` in `run-result` and `runs-show` retains `run_id`, flow `status`,
UTC `started_at`/`ended_at`, `duration_seconds`, and ordered `tasks`. A task retains
its task reference, terminal `status`, `reason`, `blocked_by`, and ordered `attempts`.
An attempt retains its reference, status, timestamps, duration, reason, blockers,
and nullable portable `failure`. Failure metadata contains `exception_type`,
`message`, and `traceback`.

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
and dedicated raw partition-key fields. They do **not** redact application-controlled
failure metadata: an exception message or traceback can repeat a partition key,
credential fragment, or other sensitive value. See the [trust boundary](../concepts/trust-boundary.md).

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
