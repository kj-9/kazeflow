## Why

The hosted documentation is complete but still behaves like five hand-authored
landing pages: it has no search, hierarchical navigation, durable page-level
reference structure, or generated Python API reference. M14 turns the Pages site
into a maintainable software documentation system without changing kazeflow's
runtime or duplicating user guidance elsewhere.

## What Changes

- Add M14 to the roadmap for a searchable, structured software documentation site.
- Replace hand-authored `docs/site/*.html` with Markdown sources built by MkDocs
  Material.
- Organize content into tutorial, task guides, concepts, CLI reference, Python API
  reference, examples, and release compatibility sections.
- Split CLI reference by command and generate selected public Python API reference
  from source docstrings with mkdocstrings.
- Preserve the current brand direction and existing public top-level `.html` URLs
  where practical while adding responsive navigation, search, page tables of
  contents, permalinks, and code-copy controls.
- Build the site strictly in CI and deploy the generated artifact through the
  existing GitHub Pages workflow.
- Remove the obsolete static HTML checker and validate the MkDocs build instead.

Non-goals: changing the Python or CLI contract, adding package runtime dependencies,
shipping a Web application, introducing documentation version switching during the
alpha series, or publishing maintainer planning documents as user guides.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `hosted-user-documentation`: Require a searchable, responsive, hierarchically
  navigable documentation build with task/reference separation, selected generated
  API reference, strict CI validation, and stable public entry URLs.

## Impact

The change affects `docs/user/`, `mkdocs.yml`, documentation-only dependency groups
in `pyproject.toml` and `uv.lock`, the Pages workflow, README documentation links,
and M14 status in `docs/ROADMAP.md`. MkDocs Material and mkdocstrings are build-time
dependencies only; `[project].dependencies` remains empty. No public Python API,
CLI behavior, JSON schema, or optional adapter semantics change.
