## 1. Core contract and planning (one Luna owner; serial hotspot wave)

- [x] 1.1 Add the frozen public `ResourcePolicy` and optional asset resource declaration in `src/kazeflow/resources.py`, `assets.py`, and `__init__.py`; verify unit tests cover valid metadata and invalid declarations.
- [x] 1.2 Add flow-owned resources and immutable selected-resource metadata in `flow.py` and `plan.py`; verify planning tests cover undefined references, invalid limits, unselected assets, deterministic order, no-resource compatibility, and no asset invocation.

## 2. Executor (same Luna owner after section 1)

- [x] 2.1 Enforce per-resource and global invocation bounds in `flow.py`; verify asynchronous tests show shared limits across assets and partition keys while unrelated work uses free global slots.
- [x] 2.2 Verify failure and external-cancellation paths release or discard run-local capacity without duplicate/unterminated results; test falsey partition keys, exactly-once invocation, result order, and current synchronous-asset cancellation caveat.

## 3. CLI and published contract (different Luna owner, after plan shape is fixed)

- [x] 3.1 Show selected resource summaries and task associations in `src/kazeflow/cli.py`; verify text plan tests for resource and no-resource flows.
- [x] 3.2 Publish `kazeflow.plan` v2 schema and golden documents, update the CLI JSON projection without changing other document versions, and retain v1 artifacts; verify schema/golden and JSON stream tests pass.
- [x] 3.3 Update CLI/Python user Docs and release migration guidance for invocation limits and plan JSON v2; verify `make docs-check` and documentation contract tests pass.

## 4. Serial integration and verification (Sol reviews; Luna repairs code if needed)

- [x] 4.1 Review public API, scheduler fairness/terminal behavior, and schema compatibility against this change; verify with focused tests and resolve any findings before integration.
- [x] 4.2 Run `make test`, `make ci-check`, `make package-check`, `openspec doctor`, and `openspec validate --all --strict`; verify every command succeeds and the core-only wheel has no required third-party runtime dependency.
- [x] 4.3 After implementation verification, sync/archive this change serially and verify living specs reflect the new behavior; do not commit or push without a separate user request.
