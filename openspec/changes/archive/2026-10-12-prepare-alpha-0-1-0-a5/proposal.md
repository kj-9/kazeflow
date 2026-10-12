## Why

The user authorized GitHub integration of Issues #1/#2 and the next alpha publication. Current GitHub/PyPI version is 0.1.0a4; completed features and their compatibility changes require a cohesive 0.1.0a5 release.

## What Changes

- Advance project and lock metadata to0.1.0a5.
- Consolidate unreleased entries into the alpha5 release notes and update the latest-alpha badge.
- Verify release metadata, supported CI and core/TUI wheel installation before publication.
- Scope is packaging/documentation; no new runtime behavior or public contract. Non-goals: stable release, additional features, package-publisher redesign.

## Capabilities

No spec changes; skip_specs:true marks this metadata-only preparation.

## Impact

Package version and release guidance only. Stdlib-only core and existing runtime/schema contracts are preserved. GitHub/PyPI publication follows successful PR CI and existing trusted publisher workflow, at the exact merged commit.
