## 1. Reviewed execution adapter

- [x] 1.1 Single CLI owner: extend `src/kazeflow/cli.py` with `run`, shared preflight selection, TTY/`--yes` confirmation, text/JSON result output, and exit classification.
- [x] 1.2 Add focused CLI tests for confirmation, non-interactive refusal, JSON separation, asset failure, and same resolved-entry/options behavior.

## 2. Explicit optional adapters

- [x] 2.1 Add `--tui` with lazy optional import, safe renderer lifetime, and adapter-failure tests.
- [x] 2.2 Add `--store PATH` with post-terminal SQLite save, failure precedence, and portable-record round-trip tests.

## 3. Documentation and verification

- [x] 3.1 Document `run`, confirmation, CI `--yes`, exit statuses, TUI/store opt-in, and the unchanged loading trust boundary.
- [x] 3.2 Extend installed-wheel smoke tests for approved execution and explicit adapter behavior where applicable.
- [x] 3.3 Run focused/full tests, `make ci-check`, lock/package checks, OpenSpec strict validation, and review the integrated diff.

## 4. Integration and archive

- [x] 4.1 Sync/archival remains serial with the active CLI contract and M8 changes; archive only once their living specifications can be coherent.
