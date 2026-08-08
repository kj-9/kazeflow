## 1. Contract review

- [x] 1.1 Review the CLI-first boundary: `kazeflow` is the primary plan/run interface while Python APIs remain supported escape hatches.
- [x] 1.2 Review bare-file asset discovery, the optional `flow` default, and explicit-target behavior for multiple terminal candidates.
- [x] 1.3 Review explicit-only factory support and the import/factory side-effect boundary with maintainers.
- [x] 1.4 Review the five exit-status classes, including adapter-failure precedence.
- [x] 1.5 Review the lossy JSON boundary for plan and run output; retain raw partition-key values outside the public JSON contract.

## 2. Implementation planning

- [x] 2.1 Create the M8 `add-flow-plan-cli` change with asset discovery, concrete file/module loading behavior, plan projection fields, and CLI integration tests.
- [x] 2.2 Create the M9 `add-reviewed-run-cli` change with confirmation UX, execution output, optional TUI, explicit SQLite persistence, and failure-path tests.
- [x] 2.3 Create the M10 `add-run-history-cli` change after M9 fixes the persisted-run command boundary.

## 3. Validation

- [x] 3.1 Validate this change with `openspec validate define-cli-contracts --strict`.
- [x] 3.2 Before archiving, verify M8--M10 proposals preserve all requirements in `flow-cli-contract`.
