# Documentation adversarial review — 2026-08-12

## Scope

The review compared the published MkDocs documentation, its build and deployment
checks, and the documented CLI/API behavior against the implementation and tests.
It considered first-time users, experienced Python and CLI users, automation
consumers, security-conscious users, and maintainers.

There were no P0 findings. The issues below are ordered by their potential to cause
incorrect use or misplaced trust.

## Findings

### P1: Partition selection is described incorrectly

The partition guide says that omitting `--partition-key` leaves selection to the
partition definition and implies that the definition validates CLI key text. The
current planner instead requires an explicit selection whenever any selected task is
partitioned. `DatePartitionDef.range()` is a Python helper; the CLI does not call it
to derive or validate keys.

Acceptance evidence for a correction:

- omitted CLI selection for partitioned work exits `2` with the documented message;
- an explicitly selected textual value is passed through unchanged;
- `DatePartitionDef.range()` is demonstrated separately as a Python API helper.

### P1: Portable records do not guarantee confidentiality of partition values

The structural partition-key field is omitted from portable JSON and SQLite records,
but user-controlled values may still appear in exception messages and tracebacks.
The current documentation can be read as a stronger confidentiality guarantee than
the implementation provides.

Acceptance evidence for a correction:

- every JSON/storage boundary states that failure metadata may contain application
  values and records must be treated as sensitive;
- a regression test demonstrates the intended behavior when an exception repeats a
  partition key;
- stronger redaction, if ever added, is an explicit option with a separate contract.

### P1: Public source changes can leave generated documentation stale

The Pages workflow builds generated API documentation from `src/kazeflow`, but its
path filters do not include source, examples, package metadata beyond
`pyproject.toml`, or release records. A changed signature, docstring, CLI option, or
example can merge without rebuilding and redeploying the site.

Acceptance evidence for a correction:

- public API, CLI, canonical example, and relevant release changes trigger the docs
  validation job;
- a main-branch change to one of those inputs redeploys the generated artifact;
- ordinary CI also exposes documentation validation as a required, visible check.

### P1: External cancellation is oversimplified

The failure guide says that external cancellation produces a `cancelled` terminal
outcome. The executor contract instead re-raises `asyncio.CancelledError` without a
synthetic terminal `RunResult`. Cancelled values remain valid result-model/storage
states, but they are not the current public cancellation control flow.

Acceptance evidence for a correction:

- the concepts page separates representable statuses from the executor's current
  cancellation behavior;
- the statement is covered by the existing cancellation tests or a documentation
  contract test derived from them.

### P2: The public JSON compatibility promise is not actionable

The site calls JSON schemas public interfaces but publishes neither normative schema
files nor complete field tables and golden examples. Machine-readable projections
also do not use a uniform versioned envelope.

Acceptance evidence for a correction:

- assets, plan, run, list, show, and compare publish normative JSON examples or JSON
  Schema documents;
- checked-in fixtures validate representative output from a built wheel;
- envelope/versioning and compatibility rules are explicit and consistent, or the
  current compatibility promise is narrowed to match reality.

### P2: Documentation checks prove presence, not correctness

The generated-site checker looks for files and literal phrases. It cannot detect an
invalid command, stale output, contradictory explanation, or a copied example that
no longer runs.

Acceptance evidence for a correction:

- canonical getting-started, partition, persistence, and automation journeys run in
  isolated temporary directories against a built wheel;
- intentionally changing a documented option or expected exit status fails docs CI;
- arbitrary third-party snippets are not executed.

### P3: Release labels and font delivery are fragile

The release value is duplicated across package metadata, MkDocs configuration, and
multiple pages. The generated site also requests Google-hosted fonts despite its
security-conscious positioning.

Acceptance evidence for a correction:

- one package version source drives or validates every published release label;
- the project explicitly chooses system/self-hosted fonts or documents the external
  request;
- changing the package version cannot silently leave the site label stale.

## Roadmap rationale

Correctness and trust-boundary repairs come before richer content. A site that is
searchable but inaccurate is worse than a smaller honest reference. Normative
automation contracts must be decided before executable examples can assert their
shape, and reliable CI/deployment must exist before release metadata is automated.
