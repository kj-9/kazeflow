## 1. Documentation information architecture

- [x] 1.1 Create the static Pages shell, shared navigation, and accessible visual
  styling under `docs/`; own only the new site files.
- [x] 1.2 Create the landing and getting-started pages with an install, declared
  `flow`, plan, explicit-run, and trust-boundary journey; own only new site pages.
- [x] 1.3 Add concise task-oriented pages for CLI commands, plan graphs,
  partitions, results, and stored-run history, with links to authoritative
  repository Markdown; own only new site pages.

## 2. Repository integration

- [x] 2.1 Add a GitHub Pages Actions workflow that validates the static artifact on
  pull requests and deploys it from eligible `main` changes; own only the new
  Pages workflow.
- [x] 2.2 Update README and documentation navigation to point users to the hosted
  documentation entry without replacing repository-local Markdown; coordinate this
  shared documentation edit serially.
- [x] 2.3 Add the proposed post-M12 documentation milestone to `docs/ROADMAP.md`;
  own this roadmap update serially.

## 3. Verification and publication

- [x] 3.1 Add or run a deterministic local validation for site links and required
  pages, and verify that the artifact contains no package-runtime dependency.
- [x] 3.2 Run the applicable documentation validation, `make test`, `make
  ci-check`, `openspec validate publish-github-pages-docs --strict`, and `git diff
  --check`.
- [ ] 3.3 Enable GitHub Pages with GitHub Actions as the source, deploy from
  `main`, and manually verify the published navigation and first-flow examples.
- [ ] 3.4 Sync the approved delta spec into `openspec/specs/`, verify the change,
  and archive it serially after the deployed site is confirmed.
