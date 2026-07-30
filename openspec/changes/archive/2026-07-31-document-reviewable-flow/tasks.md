## 1. Review workflow documentation

- [x] 1.1 **README owner only:** update `README.md` with a minimal core-only
  plan → run → result example that uses public APIs and does not import the TUI.
- [x] 1.2 **README owner only:** add a review-oriented section showing how to inspect
  targets, dependency-first tasks, partition selection, and run configuration from
  `FlowPlan` before choosing to run, then how to inspect terminal `RunResult`.
- [x] 1.3 **README owner only:** document the distinct roles of FlowPlan, RunResult,
  and logs and state prominently that review support is not a security sandbox,
  safety proof, side-effect prevention mechanism, or automatic approval.

## 2. Supporting examples and release guidance

- [x] 2.1 **Examples/docs owner only:** add or revise a separately owned review
  workflow example under `examples/` or `docs/` only when it adds non-duplicative
  value beyond README; do not edit `README.md`.
- [x] 2.2 **Examples/docs owner only:** document the release-ready core-only smoke
  command: run `uv build --wheel --clear`, then `python3
  scripts/smoke_wheel_install.py --wheel dist --mode core`; state that it uses the
  installed wheel without the TUI extra and identify the Windows-equivalent Python
  launcher where relevant.

## 3. Documentation and smoke verification

- [x] 3.1 **Documentation-test owner only:** add focused validation for documented
  public API imports and the review order, including planning without asset
  invocation; own only dedicated documentation/example tests under `tests/`.
- [x] 3.2 **Smoke owner only:** review or extend `scripts/smoke_wheel_install.py`
  and its focused tests as needed to prove the documented core-only command exercises
  public plan/run/result behavior outside the checkout without optional imports.
- [x] 3.3 **Integration owner only:** run `uv build --wheel --clear`, the documented
  core-only smoke command, `make test`, and `make ci-check`; confirm the resulting
  documentation makes no packaging or executor semantic claim beyond existing APIs.

## 4. Serial OpenSpec completion

- [x] 4.1 **Root owner only:** verify this documentation-focused implementation
  against the change, run `openspec doctor` and strict change/all validation, then
  mark completed tasks.
- [x] 4.2 **Root owner only:** sync the review-workflow and core-installation-smoke
  delta specs serially and archive the completed change.  Do not overlap other
  OpenSpec capability sync or archive work.
