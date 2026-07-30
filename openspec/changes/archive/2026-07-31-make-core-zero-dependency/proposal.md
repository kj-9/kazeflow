## Why

M3 made presentation an explicit optional consumer, but the published package still
declares third-party runtime dependencies.  M4 now makes the installation boundary
match the core contract: a user who installs kazeflow gets a standard-library-only
flow library, while users who choose terminal presentation opt into it explicitly.

## What Changes

- Make the distribution's mandatory runtime dependency set empty and remove the
  unused `netext` dependency.
- Publish the Rich-backed presentation feature behind the `tui` optional extra.
- Verify built-wheel metadata and clean installations for both the core-only and
  TUI-enabled paths in CI.
- Update public exports, README guidance, and examples to demonstrate the
  inspect-plan, run, and inspect-result workflow, including explicit TUI selection.
- Define installation and compatibility expectations for Python 3.10 through 3.13.

## Capabilities

### New Capabilities

- `zero-dependency-packaging`: Distribution metadata, installation modes, and CI
  verification for the standard-library-only core and opt-in TUI extra.
- `core-installation-smoke`: Clean-environment behavioral checks proving that a
  core-only installation can import, plan, run, and return a result.

### Modified Capabilities

- `optional-tui-rendering`: The Rich renderer becomes available only after the
  caller installs the TUI extra; core execution remains independent of it.

## Impact

This M4 change affects package metadata and lock data, release/CI jobs, public
documentation and examples, and the TUI import path.  `pip install kazeflow` will
no longer install Rich or netext; callers that import `kazeflow.tui` must install
`kazeflow[tui]`.  The existing core plan/run/result API remains source-compatible
on supported Python 3.10--3.13, and no daemon, persistence, scheduler, or other
platform capability is introduced.
