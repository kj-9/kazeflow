## Context

M13 introduced a repository-owned GitHub Pages site, but the site was deliberately
positioned as a guided layer over four longer repository documents. As features
were added, the same examples and safety qualifications were copied into both
surfaces. The project now needs one user-facing authority while preserving concise
package metadata and maintainer records in the repository.

## Goals / Non-Goals

**Goals:**

- Make `docs/site/` the complete user-facing source of truth.
- Keep task-based navigation while adding the reference details that currently
  require repository Markdown.
- Leave a short README that works on GitHub and PyPI and sends readers to stable
  hosted URLs.
- Keep project goal, roadmap, release, OpenSpec, and contributor material in the
  repository without presenting them as competing usage guides.
- Detect links back to removed user guides and missing cross-page navigation in CI.

**Non-Goals:**

- Changing package, CLI, Python API, or storage behavior.
- Adding a site generator, JavaScript application, or documentation dependency.
- Publishing maintainer planning and OpenSpec history as end-user documentation.
- Preserving every paragraph or heading from the duplicated guides.

## Decisions

### GitHub Pages is the user-documentation authority

All installation and usage explanations live under `docs/site/`. The README retains
only enough product context and one first-flow example to evaluate the package, then
links to the hosted guides. This avoids a second reference manual while preserving a
useful PyPI landing page.

An alternative was to generate Pages from Markdown. That would reduce HTML editing,
but introduces build tooling or a migration larger than the documentation cleanup.
The current dependency-free static HTML remains suitable at this scale.

### Pages remains task-oriented, with reference detail in the relevant task page

Unique CLI contract details move into `cli.html`; review semantics and Python result
concepts move into `getting-started.html` and `results.html`; SQLite boundaries move
into `results.html`. A new duplicate reference hierarchy is not introduced.

### Duplicated repository user guides are removed

`docs/cli.md`, `docs/reviewable-flows.md`, and `docs/sqlite-run-store.md` are deleted
once their unique user-facing information is represented on Pages. Repository links
are updated to hosted URLs. `GOAL.md`, `ROADMAP.md`, and `release-notes.md` remain
because they serve maintainers and release history rather than teaching product use.

### Static checks enforce the boundary

The existing documentation checker will verify local page links, required topics,
and the absence of links to removed guides. This is intentionally a small structural
guard, not a full HTML validator or prose-linting framework.

## Risks / Trade-offs

- [Risk] Old direct links to deleted Markdown guides return 404. → Replace all
  repository-owned links and provide stable Pages URLs from README and site pages;
  accept the alpha-stage cleanup rather than retain duplicate redirect documents.
- [Risk] Hand-authored HTML can drift across shared navigation. → Extend the static
  checker to require the full navigation set on every user page.
- [Risk] A shorter README may omit details expected on PyPI. → Keep installation,
  the smallest runnable flow, the review/run commands, trust warning, and a clear
  documentation index.
- [Risk] Pages and released package versions are not versioned together. → Describe
  the current public alpha and keep release-specific compatibility notes in the
  repository release notes.

## Migration Plan

1. Move unique guidance into the existing task pages and remove repository-guide
   fallback links.
2. Replace the README with a compact landing document pointing at Pages.
3. Delete the duplicated user guides and update repository references.
4. Run the static site checker, link/reference scans, tests, and OpenSpec validation.
5. Deploy through the existing Pages workflow. Rollback is a normal Git revert of
   the documentation commit; package behavior is unaffected.

## Open Questions

None. The existing Pages information architecture is retained.
