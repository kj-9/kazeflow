## Context

The repository has a useful README and focused Markdown guides, but they are
repository-oriented rather than a guided public documentation experience. The
project already uses GitHub Actions for CI and release publishing. No documentation
site generator or Pages deployment is configured.

The site must not add a Python runtime dependency or alter the installed package.
It must make the CLI-first review workflow clear while retaining the documented
boundary: a Python entry is trusted code, and planning does not sandbox its import.

## Goals / Non-Goals

**Goals:**

- Provide a stable Pages entry URL with a task-oriented path through the product.
- Give new users one short, copyable first-flow journey before presenting reference
  material.
- Reuse repository-managed Markdown as the documentation source and preserve links
  to the repository and release information.
- Deploy through GitHub Actions with the least additional tooling possible.

**Non-Goals:**

- Adding a documentation framework, JavaScript application, search service, or
  third-party Python dependency.
- Changing CLI, Python APIs, runtime semantics, or package contents.
- Treating the site as a security guarantee for arbitrary Python entry scripts.
- Localizing all content or creating an API reference generator in this change.

## Decisions

### Deploy a static `docs/` site with the GitHub Pages Actions workflow

The repository SHALL use the official Pages configure, upload-artifact, and deploy
actions to publish an already static `docs/` directory on pushes to `main` that
change documentation or its workflow. This keeps deployment explicit, auditable,
and separate from the Python package/release workflow.

GitHub's branch-source Pages publishing was considered, but an Actions workflow
provides a single deployment definition in version control and does not require a
special publishing branch. MkDocs, Sphinx, and a JavaScript static-site generator
were rejected because they introduce a new build toolchain for a small
documentation set.

### Maintain a small HTML/CSS documentation shell and authored Markdown pages

The Pages artifact will contain a hand-maintained static shell and task-oriented
HTML pages whose code examples are copied from, and link back to, repository
Markdown. It can use no build-time site generator. Shared navigation and visual
style live beside the site pages, while authoritative long-form material remains in
the existing Markdown guides.

Pure GitHub/Jekyll Markdown rendering was considered. It would reduce authored
HTML but makes navigation and presentation dependent on Pages' implicit build
environment and theme behavior. A small static artifact makes preview and
deployment behavior deterministic without adding a development dependency.

### Make the first-run journey the primary information architecture

The landing page leads with: install core or TUI; author an exported module-level
`flow`; inspect with `assets`/`plan`; execute with explicit approval; then inspect
history. Separate pages cover CLI reference, partitions, graph output, results and
history, and Python API/review boundaries. Every page presents the no-sandbox trust
boundary near commands that load a user script.

This favors how someone adopts the tool over mirroring source-file organization.

## Risks / Trade-offs

- [Documentation examples drift from CLI behavior] → Link to and retain the
  authoritative repository guides; add a lightweight link/content check in the
  Pages workflow.
- [Pages are not enabled in repository settings] → The deployment job reports the
  missing configuration; document the one-time selection of GitHub Actions as the
  Pages source.
- [Hand-authored static pages duplicate content] → Keep long, exhaustive material
  in Markdown and restrict site pages to onboarding and concise navigation.
- [A user interprets `plan` as safe loading] → Repeat the import-side-effect and
  asset-body distinction in the first-flow and security-boundary sections.

## Migration Plan

1. Add the static documentation shell and source pages under `docs/`.
2. Add a pull-request-safe build/link validation job and a main-branch Pages deploy
   workflow.
3. Enable GitHub Pages with GitHub Actions as its source in repository settings.
4. Deploy on `main`, verify the public URL and representative navigation manually.

Rollback consists of disabling the Pages workflow or selecting no Pages source;
repository Markdown and package behavior remain unaffected.

## Open Questions

- The final public hostname is GitHub's default project Pages URL unless a custom
  domain is explicitly requested later.
