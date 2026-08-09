## 1. Representative transcripts

- [x] 1.1 Add plan, approval, and successful text run transcripts to
  `docs/site/getting-started.html`; own that file.
- [x] 1.2 Add a Mermaid graph transcript to `docs/site/cli.html` and stored-run
  list/show transcripts to `docs/site/results.html`; own those two files.

## 2. Validation and publication

- [x] 2.1 Extend `scripts/check_docs_site.py` to require the representative-output
  labels and run the static-site validation.
- [x] 2.2 Run `make test`, `make ci-check`, OpenSpec strict validation, and diff
  checks; deploy the documented change through the existing Pages workflow.
- [ ] 2.3 Sync the hosted-documentation spec and archive the change after the
  deployed site is verified.
