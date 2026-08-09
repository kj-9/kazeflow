## Why

User guidance is duplicated across the README, repository Markdown guides, and the
GitHub Pages site, so details can drift and readers have to guess which version is
authoritative. M13 should finish by making the hosted site the single source of
truth for installation and usage while keeping repository-only documents focused
on maintenance and project governance.

## What Changes

- Expand GitHub Pages so installation, the Python flow model, CLI commands, graph
  formats, partitions, result review, storage, history, exit statuses, and the
  Python-loading trust boundary are documented there without repository-guide
  fallbacks.
- Reduce the README to a concise package landing page with installation, one minimal
  example, and links into the hosted task-oriented documentation.
- Remove the duplicated `docs/cli.md`, `docs/reviewable-flows.md`, and
  `docs/sqlite-run-store.md` user guides after their unique information is covered
  by Pages.
- Keep `docs/GOAL.md`, `docs/ROADMAP.md`, and release notes as repository-maintainer
  material; they are not alternate user documentation.
- Strengthen static documentation checks so navigation and the hosted-only source
  of truth cannot silently regress.

Non-goals: changing CLI or Python behavior, introducing a documentation framework,
adding runtime dependencies, or moving maintainer planning records to Pages.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `hosted-user-documentation`: Make GitHub Pages the authoritative and complete
  user documentation surface instead of a task-oriented layer over detailed
  repository Markdown guides.

## Impact

The change affects `README.md`, `docs/site/`, the documentation checker and workflow
inputs, repository Markdown guides, and M13 status in `docs/ROADMAP.md`. It does not
change the Python API, CLI contract, package metadata, core runtime, or optional
adapter semantics, and adds no dependency. Direct links to the removed repository
guides are intentionally replaced by stable hosted documentation URLs.
