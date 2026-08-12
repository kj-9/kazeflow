## Why

The M14 documentation is searchable and navigable, but adversarial review found
material claims that do not match the current partition, cancellation, and portable
record contracts. M15 must correct these trust-sensitive explanations before the
project builds richer Partition behavior or promises stronger automation contracts.

## What Changes

- Correct Partition guidance: a selected partitioned task currently requires an
  explicit CLI/Python selection; `DatePartitionDef.range()` neither supplies an
  implicit selection nor validates CLI strings.
- Distinguish omission of the structural partition-key field from confidentiality:
  application-controlled exception messages and tracebacks may still contain keys
  or other sensitive values.
- Explain that externally cancelling `Flow.run_async()` re-raises
  `asyncio.CancelledError` without manufacturing a terminal `RunResult`, while
  `cancelled` remains a representable result-model state.
- Add a first-run environment diagnostic path covering supported Python versions,
  virtual environments, the zero-dependency core, and the quoted TUI extra.
- Add focused documentation contract tests for the claims above and link the
  affected guides to one consistent trust/privacy boundary.
- Keep future Partition parsing/range behavior, JSON schema versioning, executable
  end-to-end Docs CI, and release automation in later milestones.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `hosted-user-documentation`: require the published Partition, cancellation, and
  portable-record safety guidance to match current observable behavior and provide
  a verifiable first-run diagnostics path.

## Impact

This M15 change affects `docs/user/**`, the documentation contract checker/tests,
and the living hosted-documentation specification. It may add test fixtures but
does not change core execution, public Python/CLI behavior, package metadata, or
runtime dependencies. The zero-dependency core and all existing alpha compatibility
surfaces remain unchanged.

Non-goals are implicit Partition discovery, new Partition APIs, sandboxing,
automatic failure-data redaction, normative CLI JSON schemas, arbitrary Markdown
snippet execution, multi-version Docs, and release/version automation.
