## Context

kazeflow already provides a structured `FlowPlan` before execution and a structured `RunResult` after execution, but callers must currently write Python glue to reach either from a shell. ROADMAP M7 makes `kazeflow` the CLI-first workflow and its contract a specification gate for M8--M11; this change defines that contract and does not implement a console script.

The core has no required third-party runtime dependencies. Rich presentation is optional, and `SQLiteRunStore` is a caller-selected adapter. A CLI must preserve those boundaries and must not imply that arbitrary Python has been sandboxed.

## Goals / Non-Goals

**Goals:**

- Define `kazeflow assets`, `kazeflow plan`, and `kazeflow run` as the primary review and execution workflow.
- Let a bare Python file work without a required `Flow` variable, while letting an explicit module-level `flow` declare author intent.
- Define externally observable loading, review, output, failure, and optional-feature boundaries before implementation.
- Make the machine-readable contract safe for the same portable-record boundary already used by `RunResult.to_record()`.

**Non-Goals:**

- Implement the `kazeflow` executable, output renderers, or history subcommands.
- Add a DSL, manifest format, scheduler, daemon, remote worker, sandbox, or automatic approval mechanism.
- Promise that separate CLI invocations execute an identical in-memory plan.
- Serialize arbitrary output, exception objects, or raw partition-key values.

## Decisions

### A bare file is the primary entry point; `flow` is an optional default

The public executable is named `kazeflow`. It accepts a bare `path/to/file.py` as the common entry form, with `module:attribute` and `path/to/file.py:attribute` available for explicit selection. A bare entry uses a module-level `flow` when it is a `Flow`; otherwise the CLI discovers the assets registered while loading the entry and derives terminal assets as review candidates.

The optional explicit attribute is a direct name, not a dotted traversal. It can name a `Flow` or a zero-argument callable returning a `Flow`, invoked once. Factories are never selected implicitly, because calling one increases the hidden arbitrary-Python surface. When automatic discovery yields multiple terminal candidates, `run` requires an explicit target rather than choosing or running all of them.

This makes a simple asset script usable from the CLI without a separate manifest, while preserving an escape hatch for authors who want to declare default targets. The Python API remains supported for definition, unit tests, and custom integration; it is not removed or deprecated.

### Entry resolution and asset invocation have distinct safety boundaries

Resolving an entry necessarily imports a module or executes a file; resolving a factory additionally calls arbitrary user Python. The CLI documents both as potentially side-effectful. Once a `Flow` is resolved, a `plan` operation MUST not invoke an asset body. This is the same limited guarantee as `Flow.plan()`; it is not a security boundary.

### Commands share resolved selections only within one invocation

`assets` exposes the discovered assets without invoking their bodies. `plan` reviews explicit or derived targets with normalized run configuration. `run` uses the same resolved entry and options when it displays a pre-run summary and then executes in that same process. Separate processes do not carry a signed or cached `FlowPlan`; user code or external state may change between invocations.

This preserves the present executor, which creates its own plan immediately before running, rather than introducing a new executable-plan API merely for the CLI. Explicit confirmation mechanics belong to M9.

### Text is human-oriented; JSON is a single portable record projection

Text is for interactive inspection. JSON writes exactly one document to stdout; diagnostics and any human interaction use stderr. Run JSON follows the `RunResult.to_record()` information boundary: it excludes raw outputs, exception objects, and raw partition-key values. Plan JSON must use an equally explicit lossy representation; M8 defines its concrete fields and versioning.

### Exit-status classes distinguish user code from CLI failures

The contract reserves these process statuses: `0` success, `1` completed run with an asset failure, `2` command-line usage or supplied configuration failure, `3` entry resolution failure, and `4` execution infrastructure or selected-adapter failure. An asset failure remains a `RunResult`; it is not reclassified as a CLI error. If recording or an event consumer fails after a terminal asset result, the infrastructure failure takes precedence because the requested CLI operation did not complete.

### Optional adapters remain opt-in

The default CLI path does not import Rich, initialize a terminal renderer, create a database, or persist history. Rich presentation requires an explicit option and the installed TUI extra. SQLite persistence requires an explicit store path and is attempted only for a completed result after the run path has been chosen.

## Risks / Trade-offs

- [Import and factory code can cause side effects before review] → State the boundary in CLI help and documentation; retain ordinary Python semantics rather than claiming sandboxing.
- [A `FlowPlan` can contain arbitrary partition keys] → Use a deliberately lossy plan JSON projection and document any inability to represent data rather than serializing arbitrary objects.
- [The flow may differ across separate commands] → Promise only same-process resolved-entry behavior; defer reusable executable-plan APIs until needed.
- [A new CLI can accidentally pull in optional dependencies] → Add core-only wheel smoke coverage before M11 stabilization.

## Migration Plan

There is no existing CLI or console-script compatibility surface. M8 introduces inspection first, M9 adds deliberate execution, and M10 adds optional history. Each implementation change validates the contract against a core-only wheel; removing the future CLI is a normal package rollback because it does not alter existing Python API behavior or stored SQLite schemas.

## Open Questions

- For file entries, M8 must document supported import-path behavior for sibling and package-relative imports without widening the entry grammar.
- M8 must select the lossy plan-JSON fields and a version marker after reviewing the existing plan model's partition representation.
