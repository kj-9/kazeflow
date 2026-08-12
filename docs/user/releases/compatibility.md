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

The current alpha binary emits the currently documented schemas. It does not promise
to parse or emit obsolete versions indefinitely; consumers should pin kazeflow or
explicitly allow only the document versions they support. A removed documented alpha
version is announced in release documentation.

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
