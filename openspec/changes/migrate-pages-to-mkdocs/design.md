## Context

M13 established GitHub Pages as the authoritative user-documentation surface and
removed duplicate repository guides. The source is currently five manually shared
HTML pages with a custom stylesheet and a bespoke structural checker. The content
is accurate, but long reference pages have no search, hierarchical navigation,
page-level table of contents, reusable Markdown authoring, or generated API surface.

## Goals / Non-Goals

**Goals:**

- Adopt a conventional software-documentation build while preserving kazeflow's
  current visual identity and Pages URL.
- Separate tutorial, how-to, explanation, CLI reference, Python API reference,
  examples, and compatibility content in the navigation hierarchy.
- Provide responsive global navigation, local sidebar navigation, site search,
  heading permalinks, and code-copy affordances.
- Build selected public API documentation from source and fail CI on broken
  documentation structure or unresolved references.
- Keep all documentation dependencies outside the package runtime dependency set.

**Non-Goals:**

- Changing runtime behavior, public APIs, CLI semantics, or package extras.
- Versioned documentation switching during the alpha series.
- Automatically documenting private modules or treating generated signatures as a
  substitute for task-oriented guidance.
- Recreating the site as a JavaScript application.

## Decisions

### Use MkDocs Material with Markdown sources

MkDocs Material supplies navigation, search, responsive behavior, page tables of
contents, code-copy controls, and a strong Python documentation ecosystem with a
small configuration surface. Sphinx is stronger when exhaustive API generation is
the primary product, while VitePress and Docusaurus introduce a Node toolchain that
is disproportionate for this project. Hand-authored HTML cannot provide the desired
authoring and navigation behavior without recreating a documentation framework.

### Keep documentation dependencies in a dedicated uv group

`mkdocs-material` and `mkdocstrings[python]` live in a `docs` dependency group.
They are installed only for authoring and CI. `[project].dependencies` remains empty,
and wheel metadata stays unchanged.

### Use `docs/user` as the user-documentation source root

`docs/GOAL.md`, `docs/ROADMAP.md`, and release notes remain maintainer records.
Putting MkDocs sources under `docs/user` makes the source-of-truth boundary explicit
and prevents project-planning documents from appearing in user navigation.

### Preserve established entry URLs

MkDocs uses `use_directory_urls: false`, retaining `getting-started.html`,
`cli.html`, `partitions.html`, and `results.html` for existing README and external
links. New nested reference pages use stable `.html` URLs as well.

### Curate public API reference

mkdocstrings renders only the public names and SQLite adapter documented in the
navigation. Task guides explain behavior and boundaries; generated reference
supplies signatures and source docstrings. Private executor and CLI implementation
symbols are excluded.

### Treat documentation build as the validation boundary

CI installs the frozen docs dependency group and runs `mkdocs build --strict`.
The generated site artifact is uploaded only after a successful strict build. A
small source-policy check may remain only for project-specific invariants that
MkDocs cannot express, such as required trust-boundary wording.

## Risks / Trade-offs

- [Risk] Build dependencies increase lockfile size. → Isolate them in the `docs`
  group and retain wheel metadata tests proving no runtime dependencies.
- [Risk] Generated API pages expose weak docstrings. → Limit generation to public
  symbols and pair it with curated overview text; improve source docstrings through
  later API-specific changes rather than silently inventing semantics.
- [Risk] URL changes break existing links. → Preserve current top-level filenames
  and validate README targets against the built artifact.
- [Risk] Material's default appearance loses kazeflow identity. → Apply a narrow
  custom palette, typography, content width, and landing-page treatment instead of
  overriding the framework layout.
- [Risk] Documentation content drifts from CLI behavior. → Keep command examples
  explicit and add source/build checks; automatic CLI help generation is deferred
  until its output contract is deliberately designed.

## Migration Plan

1. Add docs-only dependencies and MkDocs configuration.
2. Convert and restructure current hosted content under `docs/user`.
3. Add curated CLI, concept, guide, example, and generated API pages.
4. Replace the static Pages workflow with strict MkDocs build and artifact upload.
5. Delete superseded hand-authored HTML and its checker.
6. Validate locally, visually inspect desktop/mobile builds, then deploy through the
   existing Pages environment. A rollback restores the previous static artifact;
   package distribution is unaffected.

## Open Questions

None. Documentation version switching remains deferred until after stable releases.
