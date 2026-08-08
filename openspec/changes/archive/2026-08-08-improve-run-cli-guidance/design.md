## Context

`RunResult` already contains ordered task and attempt outcomes, timing, skip
reasons, and portable failure metadata.  The CLI currently reduces a text result
to its ID, overall status, and task count, so a caller must use the Python API or
history record to answer ordinary review questions.  The `--partition-key`
option is also mechanically documented without first explaining the user problem
it solves.

The change advances the next M12 milestone after M0--M11.  The core remains
stdlib-only; all work is confined to the CLI presentation adapter, tests, and
documentation.

## Goals / Non-Goals

**Goals:**

- Make terminal text output sufficient to understand a run at task granularity.
- Make detailed attempt inspection available on demand without leaking raw
  partition keys, outputs, or exception objects.
- Teach a script-first user when and how to select partitions before running.
- Give new users a short, copyable path from installation to review, execution,
  and result inspection.

**Non-Goals:**

- Change `Flow`, `FlowPlan`, execution scheduling, `RunResult`, JSON record
  schemas, or SQLite schemas.
- Display raw partition values, raw asset outputs, or exception objects.
- Infer safe partitions, add caching/retry/scheduling, or sandbox Python.
- Add mandatory dependencies or a new persistent store.

## Decisions

### Keep JSON unchanged; add text-only result detail

`run --format json` remains exactly one `RunResult.to_record()` document on
stdout.  The text renderer receives `--verbose`; default text shows an ordered
task table-like list with status, duration, and a safe note.  Verbose appends
attempt aggregates and individual attempt failure/skip metadata.  This keeps the
stable machine boundary intact and makes the human projection useful without
requiring a second output schema.

Alternative: add a new JSON presentation schema.  Rejected because the existing
portable record is the established automation boundary and a presentation-only
need does not justify a second machine API.

### Use safe summaries, never raw partition identity

Every task shows whether it is partitioned and an attempt count; verbose attempt
rows use ordinal labels such as `attempt 1/3 (partitioned)`.  Failure notes expose
the portable exception type and message; skips expose their portable reason.  Raw
keys, output values, tracebacks, and exception instances are excluded from normal
text output.

Alternative: print partition values for convenience.  Rejected because values may
be secret, non-serializable, or misleadingly represented, and the CLI contract
already defines them as private.

### Teach partitions by selection intent

Docs first establish that an unpartitioned asset runs once, while a partitioned
asset represents independently selectable slices such as dates.  They then show
repeatable `--partition-key`, the plan-first review loop, and the distinction
between omitted selection, explicit selection, and empty selection as defined by
the existing plan contract.  The command parser continues to accept strings; no
new type coercion is introduced.

Alternative: introduce a partition-value parser or implicit current-date default.
Rejected because arbitrary Python partition definitions own their semantics and
implicit selection would make review less clear.

## Risks / Trade-offs

- [Long task names make text less compact] → Preserve plan/result order and use
  one task per line rather than terminal-width-dependent tables.
- [Failure messages may contain sensitive application text] → Limit rendering to
  existing portable metadata and make no claim that terminal output is secret-safe.
- [Docs over-promise what selection means] → Tie examples explicitly to the flow's
  partition definition and retain the loading/trust boundary.

## Migration Plan

This is additive for documented CLI options.  The former three-line text result is
a non-machine-stable human projection and can be replaced directly.  JSON output,
exit codes, core APIs, and stored records need no migration.  Reverting the change
only removes the optional text detail and documentation; it does not alter stored
data or execution behavior.

## Open Questions

None.  The existing partition and result contracts define the required semantics.
