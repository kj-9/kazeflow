## Why

The CLI can plan and execute a flow, but its terminal result is too terse for a
user to quickly understand which task failed, was skipped, or took time.  In
parallel, partitions are a powerful selection mechanism whose purpose and safe
usage are not apparent from the current introduction.

This M12 change makes the post-run review and first-use story as clear as the
existing pre-run plan review.

## What Changes

- Add a deterministic, human-oriented detailed run-result projection to the
  `run` command's text format, including task status, duration, skip reason,
  and safe failure guidance.
- Add a `--verbose` run option for attempt-level diagnostics while preserving
  the current concise default and portable JSON record contract.
- Add command help and documentation that explain partition selection with a
  runnable script-first example, including default, explicit, empty, and
  falsey selection semantics.
- Add a short value proposition and a copyable quick-start workflow to the
  user-facing documentation.

## Capabilities

### New Capabilities

- `run-result-cli-presentation`: Human-facing text summaries and optional
  attempt-level detail for terminal CLI run results.
- `partition-user-guidance`: User-facing CLI help and documentation for
  partition selection and its review workflow.

### Modified Capabilities

- `flow-cli-contract`: Extend the public output-option contract for the run
  command while retaining its JSON and exit-status guarantees.
- `reviewable-flow-workflow`: Include the quick-start and partition-aware
  review workflow in the documented user journey.

## Impact

The change affects the stdlib-only CLI adapter, its tests, and README/CLI
documentation.  It introduces no runtime dependency, no new core execution
semantics, no persistence default, and no change to the portable `RunResult`
JSON schema.  Existing text output remains a human-facing projection rather
than a byte-for-byte compatibility surface.
