## Why

The hosted documentation tells users which commands to run, but it does not show
what successful planning and execution look like. Concrete, representative terminal
transcripts make the review value visible and help a new user recognize the
preflight, graph, result summary, and stored-run follow-up.

This completes the post-M12 documentation milestone; it does not change any CLI
behavior or output contract.

## What Changes

- Add annotated, representative text transcripts for `plan`, `run`, and stored-run
  inspection to the hosted documentation.
- Show Mermaid graph output as a directly readable example alongside its command.
- State which example values are illustrative and which fields vary per run.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `hosted-user-documentation`: Add representative CLI output to the task-oriented
  documentation journey.

## Impact

- Affects only static Pages content and its local validation.
- Adds no dependency and does not affect package, Python API, CLI semantics, JSON
  schemas, or exit statuses.
