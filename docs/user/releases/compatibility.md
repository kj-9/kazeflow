# Release and compatibility

<span class="release-badge">Latest alpha · 0.1.0a4</span>

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

Plan schema v4 and run/history schema v3 are unreleased changes on `main`. Plan v4
includes selected resource concurrency, rate limit, retry budget, and
classifier-enabled metadata. Plan schemas v1 through v3 and CLI collection v4 remain
available for historical consumers. Run-result and history v3 add circuit skip
reasons; stored rows retain their individual record version in the unchanged SQLite
layout v1. Check the installed package's emitted versions before choosing a schema.
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
