## Why

The README and repository Markdown now describe a complete CLI workflow, but a new
user has to choose between several long documents without a clear reading order.
A hosted, task-oriented documentation site makes installation, planning, deliberate
execution, partition selection, and history review discoverable from one public URL.

This advances a proposed post-M12 documentation milestone. It does not change the
runtime roadmap or any execution semantics.

## What Changes

- Publish a GitHub Pages site from repository-maintained documentation using the
  GitHub Pages deployment workflow.
- Add a concise landing page and an ordered getting-started journey that takes a
  user from installation through a first reviewed CLI run.
- Organize existing CLI, partition, run-history, and Python API guidance into
  clear, cross-linked reference pages with a visible trust boundary.
- Preserve repository Markdown as the source of user-facing documentation, with
  the Pages site adding navigation and presentation rather than a second product
  manual.

## Capabilities

### New Capabilities

- `hosted-user-documentation`: A publicly deployable, task-oriented GitHub Pages
  documentation site for installing and using kazeflow.

### Modified Capabilities

- None.

## Impact

- Affects `docs/`, a GitHub Actions Pages deployment workflow, and repository
  documentation navigation.
- Adds no Python runtime dependency, does not affect the wheel, and does not alter
  the public Python or CLI API.
- Requires GitHub Pages to be configured for GitHub Actions in the repository's
  Pages settings before the first deployment can become publicly reachable.
