## Context

M7--M10 made `kazeflow` a script-first command line interface for inspecting,
reviewing, running, and comparing flows. Its text output is factually complete but
largely a linear dump. M11 makes the public command surface dependable and makes a
resolved execution plan legible without adding a runtime dependency.

The existing JSON projection is already a portable machine interface. Entry loading
remains a trust boundary, and plan rendering starts only after the existing loader
has resolved a `FlowPlan`.

## Goals / Non-Goals

**Goals:**

- Establish compatibility and deprecation rules for public commands, options,
  exit statuses, and JSON schemas.
- Make default text plans readable at a glance with a deterministic ASCII DAG.
- Export the same resolved DAG in Mermaid and DOT without importing Mermaid,
  Graphviz, Rich, or SQLite.
- Keep detailed plan information available without overwhelming default output.
- Make an explicitly selected interactive TUI useful for observing a live run.
- Verify the installed-wheel CLI on supported Python versions and relevant optional
  feature combinations.

**Non-Goals:**

- Static analysis, sandboxing, or graphing code that has not been loaded.
- A graphical UI, automatic opening/rendering of Graphviz or Mermaid output, or a
  new required dependency.
- Changes to flow planning, execution, result semantics, or the existing JSON
  projection schema.
- A default progress stream, log aggregation facility, or terminal UI dependency
  for unattended and machine-readable runs.
- Backporting obsolete text layouts as a compatibility format.

## Decisions

### Treat JSON as the stable automation boundary

`--format json` remains one portable JSON document on stdout, with no schema
renames, field removal, or addition of raw Python values in a compatible release.
Human-facing text is stable in meaning and command shape, but its whitespace and
layout can evolve within the documented M11 rendering contract. This avoids
freezing an accidental early textual dump while giving CI and agents a reliable
interface.

An alternative was to promise byte-for-byte text stability. It was rejected because
the primary purpose of M11 is to improve that text and terminal widths make such a
promise brittle.

### Add plan formats rather than a separate graph command

`kazeflow plan ENTRY` remains the review entry point. Its default text includes a
summary and graph; `--format mermaid` and `--format dot` export the same resolved
plan. A separate `graph` command was rejected because it duplicates entry loading,
target selection, and plan semantics, and splits a single review decision across
commands.

### Keep graph projection pure and deterministic

The CLI renderer consumes `FlowPlan` only. Node and edge order derive from the
plan's dependency-first task order; graphs contain only selected task closure,
declared dependency edges, and safe labels. The text renderer presents an ASCII DAG
with a concise summary. Mermaid and DOT are emitted as one stdout document and do
not invoke external renderers. `--verbose` expands text-only task, partition, and
configuration detail; it is rejected with non-text formats to avoid ambiguous
machine documents.

An optional Graphviz Python binding was rejected: invoking `dot` directly would
also introduce platform-dependent behavior, and neither is necessary to export DOT.

### Standardize CLI diagnostics and compatibility

Help and successful output use stdout; errors use stderr. The published exit-status
mapping 0--4 remains stable. Invalid format/option combinations are usage errors
(2). Any intentional command/option removal requires a documented deprecation in a
prior compatible release and a migration path; breaking changes require the next
documented pre-1.0 compatibility boundary and release notes.

### Keep live progress inside the explicit Rich adapter

`run --tui` retains its lazy Rich import and renders to stderr so JSON stdout remains
one final document. The renderer receives the resolved plan's safe task descriptors
at construction and lifecycle events while executing. It presents a run summary,
overall completion, and stable task states: waiting, running, succeeded, skipped,
or failed. It does not consume asset output, inspect executor internals, or change
the result returned by the executor.

The alternative of printing a default line for every event was rejected: it makes
CI logs noisy, cannot be updated in place, and would blur the boundary between
human-facing TUI and machine output.

## Risks / Trade-offs

- [Dense graphs are hard to render in a terminal] → preserve every edge in a
  deterministic ASCII form, provide Mermaid/DOT for richer rendering, and retain
  `--verbose` rather than attempting a full terminal layout engine.
- [Text snapshots become unnecessarily brittle] → test semantic fixtures and exact
  small DAG renderings; do not promise arbitrary whitespace compatibility.
- [Graph labels can accidentally expose unsafe values] → use task names and safe
  partition metadata only, keeping the existing portable-record boundary.
- [A progress renderer can lag or fail] → it is a non-owning optional event
  consumer; consumer failure retains the existing infrastructure-failure policy and
  cannot be reclassified as an asset failure.
- [New formats drift from execution selection] → render all formats from the same
  resolved `FlowPlan` and cover target/partition/branch fixtures.

## Migration Plan

1. Implement and document the M11 text layout and `--format` choices.
2. Keep JSON behavior byte-for-byte compatible for existing fixtures.
3. Note the former linear text plan in release notes; users needing automation move
   to JSON rather than parsing text.
4. If a rendering defect is found, users can retain `--format json`; rollback is a
   code release because no persisted schema or flow definition changes.

## Open Questions

- Whether the default ASCII renderer needs a configurable maximum width, or whether
  Mermaid/DOT plus `--verbose` sufficiently cover dense graphs.
