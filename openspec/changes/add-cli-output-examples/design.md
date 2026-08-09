## Context

The hosted site presents copyable commands but leaves their successful terminal
output abstract. The existing CLI contract already defines deterministic text
projection shapes, while run IDs, durations, and timestamps naturally vary.

## Goals / Non-Goals

**Goals:**

- Show representative successful text output next to the commands a new user runs.
- Teach which output fields are useful to review and which values are illustrative.
- Keep examples safe: no raw partition keys, outputs, exception objects, or
  tracebacks.

**Non-Goals:**

- Snapshotting or guaranteeing human text layout as a second public API.
- Adding interactive terminal emulation, an image, JavaScript, or a documentation
  build dependency.
- Changing CLI implementation or JSON output.

## Decisions

### Use labelled, representative text transcripts

Each transcript includes the command followed by concise expected output. Stable
names and graph structure use the documented `daily.py` example; values that vary
per run are written as placeholders such as `<run-id>` and `<duration>`.

Live-command capture was considered but rejected: it would make static docs depend
on tool installation and timing, and it would incorrectly suggest byte-for-byte
text stability. A visible note directs automation users to JSON.

### Place output at the moment of decision

The getting-started page receives plan, confirmation, and run-result transcripts.
The CLI page receives Mermaid output; the results page receives history output.
This keeps output close to the command whose purpose it explains instead of
creating an isolated transcript gallery.

## Risks / Trade-offs

- [Text projection evolves] → Mark examples representative and direct automation
  users to `--format json`.
- [Transcripts become stale] → Require the static-site checker to assert their
  key labels and update them alongside CLI presentation changes.
