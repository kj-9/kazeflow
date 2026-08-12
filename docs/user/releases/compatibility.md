# Release and compatibility

<span class="release-badge">Latest alpha · 0.1.0a3</span>

kazeflow is currently an alpha package. The public CLI review workflow and structured
plan/result model are available, but incompatible changes can still occur before a
stable release.

## Supported environment

- Python 3.10 through 3.13
- Zero required third-party runtime dependencies
- Optional Rich TUI through `kazeflow[tui]`
- Local SQLite persistence through the standard-library adapter

## Public compatibility surface

Documented Python exports, CLI commands/options, exit statuses, and JSON schemas are
treated as public interfaces. Compatible releases should deprecate a public name or
option and document its migration before removal.

Human-oriented CLI text is stable in meaning, not whitespace. Automation should use
JSON rather than parse terminal formatting.

## Documentation versions

This site documents the latest alpha. Multi-version documentation switching is
deferred until stable releases make parallel maintained versions useful.

See the repository
[`docs/release-notes.md`](https://github.com/kj-9/kazeflow/blob/main/docs/release-notes.md)
for release-specific maintainer records.
