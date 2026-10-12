## Context

Current project, GitHub prerelease and official PyPI JSON all report0.1.0a4. Version0.1.0a5 is unused. Existing publish workflow checks tag/project version and runs Python3.10–3.13 checks plus core/TUI wheel smoke before trusted publication.

## Decisions

- Use uv version to update pyproject.toml/uv.lock coherently with no dependency changes.
- Root owns version/lock and OpenSpec/review/Git integration. Documentation owner consolidates notes and badges without changing old release history.
- Publish feature/release preparation via codex branch and PR. Merge only after checks succeed. PR closes1/2; verify their final state. Create prerelease v0.1.0a5 targeting the exact merged SHA; existing release workflow performs tests and PyPI publication.
- Preserve unrelated local generated files. No source or publisher edits for version preparation.

## Risks / Trade-offs

Alpha-breaking formats → coherent final-version migration notes instead of intermediate unreleased version descriptions.
Remote CI/release failures → inspect concrete failures and fix before claiming publication; no bypass of environment controls.
