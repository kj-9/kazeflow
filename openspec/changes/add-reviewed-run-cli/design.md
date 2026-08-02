## Context

M8 supplies a stdlib-only CLI that resolves a script and reviews its plan. M9
adds the execution decision without changing the core executor: the CLI prepares
the same resolved entry and options, shows a preflight summary, then calls the
existing `Flow` execution API only after explicit confirmation.

## Goals / Non-Goals

**Goals:**

- Make `kazeflow run` a deliberate, review-first entry point for existing flow
  semantics.
- Keep JSON consumable as exactly one portable terminal record, and keep TUI and
  SQLite fully explicit.
- Define all user-visible failure and cancellation outcomes before coding.

**Non-Goals:**

- Guarantee that `Flow.run_async()` consumes the exact in-memory `FlowPlan`
  displayed by the CLI; it re-plans under the same resolved options.
- Sandbox loading or assets, add a daemon/scheduler, cache outputs, or make
  persistence implicit.

## Decisions

### Confirmation is a CLI decision, not a flow status

The CLI preflights and displays a summary. With both stdin and stderr attached to
a TTY it prompts `Proceed? [y/N]`; only `y` or `yes` proceeds. Non-TTY callers
must pass `--yes`. Decline or EOF invokes no asset, TUI, or store, produces no
`RunResult`, writes a cancellation diagnostic, and exits 0 as a deliberate
no-op. JSON remains clean because summary and prompt use stderr.

### Reuse resolved selections, not a new executable-plan abstraction

The CLI shares its resolved entry, targets, and normalized options between
preflight and run in one process. The current public executor constructs a plan
internally, so M9 does not falsely promise object identity or introduce a new
core executor API.

### Adapters are lazy and failure precedence is explicit

After confirmation, `--tui` lazily imports and constructs the optional renderer
before execution. After a terminal `RunResult`, `--store PATH` lazily constructs
and saves to SQLite. A TUI/consumer or store failure is status 4 and takes
precedence over status 1 for an asset failure; no successful final result is
emitted after the requested adapter operation fails.

### Output follows the existing portable record boundary

Text summarizes the terminal result. JSON writes only `RunResult.to_record()` to
stdout after successful completion of requested adapters; raw output, exceptions,
and raw partition keys remain absent. Load/config errors remain stderr-only.

## Risks / Trade-offs

- [Preflight and executor planning can observe changed dynamic state] → promise
  same resolved entry/options, not an identical plan object.
- [Prompt handling varies by stream redirection] → require both stdin and stderr
  TTYs; require `--yes` otherwise.
- [TUI consumer failures can interrupt a run] → propagate as infrastructure
  failure and never synthesize a terminal result.
- [SQLite can create a file] → instantiate it only after terminal result exists.

## Migration Plan

`run` is a new command. It adds no core API migration or database schema change;
removing it only removes the console behavior. M10 later adds history commands
against the existing explicit SQLite store.

## Open Questions

- None for the M9 contract; exact text layout and JSON schema are implementation
  details tested before M11 stabilization.
