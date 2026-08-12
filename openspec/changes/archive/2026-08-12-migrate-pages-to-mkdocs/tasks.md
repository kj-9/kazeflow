## 1. Framework and navigation

- [x] 1.1 Add isolated MkDocs Material and mkdocstrings documentation dependencies,
  lock them, and configure a strict build without changing package runtime metadata.
- [x] 1.2 Define the `docs/user` navigation hierarchy, stable top-level URLs, search,
  page tables of contents, code-copy controls, repository links, and alpha version
  label in `mkdocs.yml`.
- [x] 1.3 Add a narrow custom theme layer that preserves kazeflow's existing colors,
  typography, content density, and trust callouts while retaining Material's
  responsive navigation and accessibility behavior.

## 2. Documentation content

- [x] 2.1 Convert the landing page and getting-started journey to Markdown with the
  first script, representative plan/run output, and trust boundary.
- [x] 2.2 Add task guides for assets/dependencies, partitions, optional TUI, and
  explicit SQLite persistence.
- [x] 2.3 Add concept pages for planning/results/events, failure semantics, and the
  Python loading and execution trust boundary.
- [x] 2.4 Split the CLI reference into overview, assets, plan, run, history, and exit
  status/automation pages while preserving the existing `cli.html` entry.
- [x] 2.5 Add curated generated Python API reference for public core symbols, events,
  result models, and the explicit SQLite adapter.
- [x] 2.6 Add runnable-pattern examples and release/compatibility guidance, then
  update README links to the new generated pages.

## 3. Build, migration, and verification

- [x] 3.1 Replace the static HTML Pages workflow with frozen strict MkDocs build and
  generated artifact deployment.
- [x] 3.2 Remove superseded `docs/site` sources and the bespoke static HTML checker;
  update M14 and documentation-source records.
- [x] 3.3 Run strict docs build, link and required-content checks, full tests,
  formatting/lint/typecheck, wheel metadata checks, and OpenSpec strict validation.
- [x] 3.4 Visually inspect the built site at desktop and mobile sizes, correct visible
  navigation/content defects, and verify established public URLs in the artifact.
- [x] 3.5 Sync and archive the completed OpenSpec change, commit in reviewable units,
  push, and confirm CI and GitHub Pages deployment.
