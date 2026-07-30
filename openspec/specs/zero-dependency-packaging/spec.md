## Purpose

Define the distribution metadata, installation modes, and release verification that
keep kazeflow's core free of mandatory third-party runtime dependencies.

## Requirements

### Requirement: Base distribution has no mandatory third-party runtime dependency
The kazeflow base distribution SHALL declare no unconditional third-party runtime
dependency in its wheel metadata.  It SHALL remove `netext` from runtime dependency
metadata.  The supported base installation SHALL run on Python 3.10, 3.11, 3.12, and
3.13 using only the Python standard library at runtime.

#### Scenario: Inspecting built base-wheel metadata
- **WHEN** a release wheel is built and its `Requires-Dist` metadata is inspected
- **THEN** it contains no unconditional third-party runtime requirement and no
  requirement for `netext`

#### Scenario: Installing the base distribution
- **WHEN** a user installs the built kazeflow wheel without selecting an extra
- **THEN** the installation does not require Rich, netext, or any other third-party
  runtime package for core import, planning, execution, or result retrieval

### Requirement: Rich rendering is an opt-in TUI extra
The distribution SHALL provide a `tui` optional extra that installs the Rich runtime
needed by `kazeflow.tui`.  The base package and its public core exports SHALL NOT
import Rich or require the `tui` extra.  A user who selects `kazeflow[tui]` SHALL be
able to import and explicitly construct the documented Rich renderer.

#### Scenario: Core-only installation remains independent of Rich
- **WHEN** a user installs only the built base wheel and imports `kazeflow`
- **THEN** core import succeeds without Rich installed and core plan/run/result
  behavior is available

#### Scenario: TUI-enabled installation supports renderer use
- **WHEN** a user installs the built wheel with its `tui` extra
- **THEN** importing and explicitly constructing `kazeflow.tui.FlowTUIRenderer`
  succeeds

### Requirement: Release CI verifies both installation modes
The release CI pipeline SHALL build the distributable wheel and verify it in clean
environments outside the source checkout.  It SHALL cover base and `tui`-enabled
installation modes, and SHALL fail if their installation contracts regress.

#### Scenario: Core-only installed-wheel smoke
- **WHEN** CI installs the built wheel with `pip install --no-deps` in a clean
  environment
- **THEN** a standalone smoke script can import kazeflow, define an asset, obtain a
  FlowPlan without executing it, run the flow, and assert a successful RunResult

#### Scenario: TUI-enabled installed-wheel smoke
- **WHEN** CI prepares a clean environment with the built wheel and its `tui` extra
- **THEN** a standalone smoke script imports and constructs the Rich renderer without
  importing the project source tree
