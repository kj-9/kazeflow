## 1. Package metadata and optional feature boundary

- [x] 1.1 **Packaging owner only:** update `pyproject.toml` to remove mandatory
  runtime dependencies and `netext`, declare Rich under the `tui` extra, and retain
  Python 3.10--3.13 compatibility metadata.
- [x] 1.2 **Packaging owner only:** regenerate and inspect `uv.lock` so it matches
  the new base and optional dependency metadata; no other agent edits either
  `pyproject.toml` or `uv.lock` in this wave.
- [x] 1.3 **TUI owner only (if needed):** verify `src/kazeflow/tui.py` imports Rich
  only at the optional adapter boundary and adjust its dedicated tests without
  editing core executor files.

## 2. Installed-artifact verification and CI

- [x] 2.1 **Packaging/CI owner only:** add an isolated built-wheel metadata check
  proving no unconditional third-party `Requires-Dist` requirement and no `netext`.
- [x] 2.2 **Packaging/CI owner only:** add a clean core-only installed-wheel smoke
  using `pip install --no-deps` that verifies public import, non-executing plan,
  run, and structured successful result outside the checkout.
- [x] 2.3 **Packaging/CI owner only:** add a separate clean TUI-enabled
  installed-wheel smoke that installs the `tui` extra and imports/constructs the
  documented Rich renderer outside the checkout.
- [x] 2.4 **Packaging/CI owner only:** integrate both installation modes into CI
  while preserving the Python 3.10--3.13 test matrix.

## 3. Public API and documentation

- [x] 3.1 **Public-API owner only:** review `src/kazeflow/__init__.py` and its
  focused export tests; make only necessary export adjustments so core imports
  remain Rich-free.  Do not overlap another owner's `__init__.py` work.
- [x] 3.2 **Documentation owner only:** update `README.md` and owned examples to
  show base installation, plan-before-run/result-after-run, and the explicit
  `kazeflow[tui]` renderer installation path.
- [x] 3.3 **Documentation owner only:** verify documented examples use public APIs
  and do not imply that core execution automatically renders a terminal UI.

## 4. Serial integration and validation

- [x] 4.1 **Root integration owner only:** review all parallel changes together;
  resolve cross-boundary issues without editing `flow.py` or `assets.py`, which are
  out of scope for M4.
- [x] 4.2 Build the package with `uv build`; execute installed-wheel core and TUI
  smokes, `make test`, and `make ci-check` on the integrated tree.
- [x] 4.3 Run `openspec doctor`, `openspec validate make-core-zero-dependency --strict`,
  and `openspec validate --all --strict`; record any validation failure with the
  command and condition that produced it.
- [x] 4.4 **Root owner only, after implementation:** mark completed tasks, verify
  implementation against this change, then sync/archive the OpenSpec change
  serially.  This specification owner does not perform sync or archive.
