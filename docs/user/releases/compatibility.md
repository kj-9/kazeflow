# Release and compatibility

<span class="release-badge">Latest alpha · 0.1.0a5</span>

kazeflow is currently an alpha package. The public CLI review workflow and structured
plan/result model are available, but incompatible changes can still occur before a
stable release.

## Supported environment

- Python 3.10 through 3.13
- Zero required third-party runtime dependencies
- Optional Rich TUI through `kazeflow[tui]`
- Local SQLite persistence through the standard-library adapter

## Public compatibility surface

Documented Python exports, CLI commands/options, exit statuses, and typed JSON
schemas are treated as public interfaces. Compatible releases should deprecate a
public name or option and document its migration before removal.

Human-oriented CLI text is stable in meaning, not whitespace. Automation should use
JSON rather than parse terminal formatting.

## JSON alpha compatibility

Each `(document_type, schema_version)` pair is an independently versioned CLI
contract. Within a published alpha version, required fields, types, nullability,
enum meanings, and documented array order are append-closed. An incompatible change
uses a new schema version for that document type and has a migration note in release
documentation.

Release `0.1.0a5` emits plan schema v5, run-result/history schema v4, and schema
collection v7. All other CLI document types remain v1. Plan v5 includes selected
resource limits, retry and classifier metadata, and partition groups. Plan schemas
v1 through v4 and CLI collections v1 through v6 remain available for historical
consumers. Run-result and history v4 include invocation history, circuit skip
reasons, and successful materialization provenance. Stored rows retain their
individual record version in the unchanged SQLite layout v1.

Portable result records support versions 1 through 4. Python
`RunResult.to_record()` defaults to v1; request v2, v3, or v4 explicitly to include
the corresponding newer fields. Event projections support versions 1 through 4
and default to v4. SQLite layout v1 can retain record versions 1 through 4 without
rewriting older rows. These version numbers belong to separate contracts; check
each field rather than inferring it from another version.

The local file materialization adapter writes reference and manifest format v2.
Updated readers also accept legacy format v1 when no invalidation ledger exists.
Older releases cannot read v2: stop old writers and quiesce store users before
adopting it. Copy the complete store, including its invalidation ledger, to retain
revocation decisions. Invalidation is explicit, does not coordinate processes or
delete artifacts, and cannot protect against restoring the entire root without
its ledger.

Resource statistics and partition outcome queries are available on live results
only. They are not reconstructed from portable history; selecting partitions for
a new run remains explicit application policy; there is no automatic partition
selection or run resume. Materialization collection uses an explicit
identity selection and does not scan history. Invalidation is never automatic.
Neither materialization reuse nor partition grouping provides exactly-once
execution across runs.

Portable records and CLI plan projections omit raw partition keys. Plan output
also omits callback code. Materialization IDs, invalidation reasons, stored keys,
and outputs are application data and are not automatically redacted. Review them
before storing or publishing.
See the [JSON migration notes](../cli/json.md#data-fields).

Each published alpha binary emits the schemas documented for its release. Consumers
should pin kazeflow or explicitly allow only the document versions they support. A
removed documented alpha version is announced in release documentation.

CLI envelope `schema_version`, nested portable `record_schema_version`, SQLite
`store_schema_version`, and package version are separate namespaces. See the
[JSON automation contract](../cli/json.md#version-namespaces) for their field-level
meaning and migration-safe parsing guidance.

## Documentation versions

This site documents the latest alpha. Multi-version documentation switching is
deferred until stable releases make parallel maintained versions useful.

See the repository
[`docs/release-notes.md`](https://github.com/kj-9/kazeflow/blob/main/docs/release-notes.md)
for release-specific maintainer records.
