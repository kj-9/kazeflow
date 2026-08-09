## 1. Hosted documentation

- [x] 1.1 Expand `docs/site/getting-started.html` with the complete Python flow,
  review, result, and trust-boundary concepts needed by a new user.
- [x] 1.2 Expand `docs/site/cli.html`, `partitions.html`, and `results.html` with the
  complete command, graph, partition, result, persistence, history, and exit-status
  reference while keeping task-oriented navigation.
- [x] 1.3 Remove hosted fallback links to repository user guides and ensure every
  hosted user journey stays within Pages.

## 2. Repository consolidation

- [x] 2.1 Reduce `README.md` to a package landing page with installation, a minimal
  reviewed flow, trust warning, and hosted documentation links.
- [x] 2.2 Remove duplicated repository user guides and update all repository-owned
  references to their hosted replacements.
- [x] 2.3 Mark M13 complete and document the Pages-versus-maintainer source-of-truth
  split in `docs/ROADMAP.md` and `docs/GOAL.md`.

## 3. Validation and completion

- [x] 3.1 Extend `scripts/check_docs_site.py` to validate complete hosted navigation,
  required reference topics, and the absence of removed-guide links.
- [x] 3.2 Run the documentation checker, reference scan, `make test`, `make ci-check`,
  and `openspec validate --all --strict`.
- [x] 3.3 Verify the completed change and prepare its delta spec for sync and archive;
  commit the work in reviewable documentation and OpenSpec-history commits.
