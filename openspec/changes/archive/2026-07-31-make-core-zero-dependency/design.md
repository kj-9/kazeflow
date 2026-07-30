## Context

M3 established that core execution emits neutral events and that the Rich renderer is
an explicitly selected consumer.  The package currently still lists `netext` and
Rich as mandatory runtime dependencies, so installing the distribution contradicts
the standard-library-only core boundary.  M4 changes distribution and verification
boundaries without changing executor semantics.

Supported interpreters remain Python 3.10 through 3.13.  The build backend and
developer tooling are build-time or development dependencies, not core runtime
dependencies.

## Goals / Non-Goals

**Goals:**

- Ship a wheel whose base runtime metadata has no third-party `Requires-Dist`.
- Make Rich available through an explicit `tui` extra and retain the explicit
  `kazeflow.tui` renderer workflow for users who install it.
- Prove core import, planning, execution, and structured-result retrieval from an
  installed wheel in an isolated environment without dependency resolution.
- Prove that an isolated TUI-enabled installation can import and use the renderer.
- Keep packaging, documentation, public exports, and CI aligned with plan/run/result.

**Non-Goals:**

- Changing FlowPlan, RunResult, event, scheduling, partition, or failure semantics.
- Adding persistence, a scheduler, daemon, control plane, remote worker, sandbox, or
  another presentation backend.
- Guaranteeing that a manually forced `kazeflow.tui` import succeeds when its extra
  was deliberately omitted.

## Decisions

### Base distribution and TUI extra are separate installation contracts

The base project dependency list will be empty; `netext` will be removed rather than
moved because it has no supported runtime use.  Rich will be declared only in the
`tui` optional-dependency group.  This makes `pip install kazeflow` sufficient for
the core and `pip install "kazeflow[tui]"` the supported renderer installation.

Keeping Rich mandatory would retain a packaging contract that conflicts with the
core boundary.  Splitting a separate `kazeflow-tui` distribution is deferred because
an extra preserves the existing import and public API while satisfying M4.

### Validate installed artifacts rather than the checkout

CI will build a wheel once and exercise it in fresh virtual environments.  The
core smoke will install that wheel using `pip install --no-deps`, then run a minimal
asset flow through import, plan, run, and result assertion.  A separate TUI smoke
will install the wheel with its `tui` extra (or the corresponding isolated package
set) and import/construct the renderer.  Wheel metadata inspection will assert that
base `Requires-Dist` entries are only optional-extra requirements, with no
unconditional third-party runtime requirement.

Tests that execute from the source tree cannot prove installed metadata or import
isolation; they remain useful but do not replace these smokes.

### Keep optional-import failure conventional and documented

`kazeflow.tui` remains an opt-in module and is not imported by the core package or
public core exports.  In a core-only install, importing it may raise the normal
missing-dependency `ImportError`/`ModuleNotFoundError`; documentation will direct
users to install `kazeflow[tui]`.  Core `import kazeflow`, planning, and execution
must never depend on Rich being installed.

### Parallel ownership is explicit

The packaging owner exclusively edits `pyproject.toml`, `uv.lock`, package-build
and isolated-install CI files, and packaging smoke tests.  The documentation owner
edits `README.md` and example files only.  The public-API owner alone edits
`src/kazeflow/__init__.py` and its dedicated export tests if an export adjustment is
required.  The TUI owner alone edits `src/kazeflow/tui.py` and `tests/test_tui.py`
only if the extra boundary exposes an issue.  No M4 task edits `flow.py` or
`assets.py`; those remain executor hotspots owned elsewhere.  The packaging owner
must finish metadata before the clean-install tests are integrated, and OpenSpec
sync/archive remains serial under the root owner.

## Risks / Trade-offs

- [A source checkout masks an installed-wheel import error] → Run smokes from fresh
  environments outside the checkout and install the built wheel explicitly.
- [A metadata assertion mistakes optional dependencies for base dependencies] →
  inspect requirement markers and assert that every third-party requirement is gated
  by `extra == "tui"`.
- [Existing users import `kazeflow.tui` after a base install] → preserve the module
  and API when the extra is installed; document the one-command migration.
- [Concurrent edits conflict in metadata hotspots] → assign `pyproject.toml` and
  `uv.lock` to one packaging owner and keep docs/public exports separate.

## Migration Plan

1. Release the base wheel with no mandatory third-party runtime dependencies and a
   `tui` extra containing Rich.
2. Update installation and renderer examples to use `pip install "kazeflow[tui]"`
   before importing `kazeflow.tui`.
3. Retain core plan/run/result imports and behavior on Python 3.10--3.13; callers
   that do not use TUI need no code migration.
4. If a packaging regression is discovered before release, restore the prior package
   metadata and lock together, rebuild the wheel, and rerun both isolated smokes.

## Open Questions

None.  The CI implementation may choose `venv`/`pip` shell steps or an equivalent
isolated installer, provided it verifies the exact built wheel and the required
installation modes.
