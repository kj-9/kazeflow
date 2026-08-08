## 1. CLI foundation

- [x] 1.1 Assign a single CLI owner for `src/kazeflow/cli.py`; implement the stdlib `argparse` command group and `main(argv) -> int` process boundary.
- [x] 1.2 Assign the `pyproject.toml` console-script metadata to its single hotspot owner; add `kazeflow = "kazeflow.cli:main"` without a runtime dependency.
- [x] 1.3 Implement and test entry loading for bare files and explicit entries, including load-local asset discovery and the declared `flow` precedence rule.

## 2. Inspection commands

- [x] 2.1 Implement `assets` text and JSON projections with deterministic ordering, stdout/stderr separation, and inherited failure classification.
- [x] 2.2 Implement `plan` target/config parsing, terminal-target derivation, text/JSON projections, and the no-asset-body-invocation boundary.
- [x] 2.3 Add focused CLI integration tests for declared flows, undeclared scripts, multiple terminal candidates, invalid entries/configuration, JSON mode, and import-versus-asset side effects.

## 3. Documentation and package verification

- [x] 3.1 Update README and CLI documentation with the `assets`/`plan` workflow, bare-script convention, and loading trust boundary.
- [x] 3.2 Extend the core-only installed-wheel smoke test to invoke `kazeflow assets` and `kazeflow plan`; retain the no-Rich/no-SQLite assertions.
- [x] 3.3 Run `make test`, `make ci-check`, `uv lock --check`, the relevant wheel smoke tests, `openspec validate --all --strict`, and change verification.

## 4. Integration and archive

- [x] 4.1 Have the single integration owner reconcile shared package metadata and documentation changes, then review the complete diff.
- [x] 4.2 Sync the accepted `flow-plan-cli` delta into living specs and archive this change only after implementation and verification complete.
