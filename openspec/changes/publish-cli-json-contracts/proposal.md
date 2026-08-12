## Why

M17 advances the roadmap by turning the CLI's loosely documented JSON projections
into documents an automation consumer can identify and validate without knowing which
command produced them. Current outputs lack a uniform discriminator, overload
`schema_version` with SQLite meanings, and can be corrupted by user `print()` calls.

## What Changes

- Wrap every completed JSON outcome in `{document_type, schema_version, data}` with
  a document-specific stable type and version.
- Publish normative Draft 2020-12 JSON Schema plus representative golden documents
  for assets, partitions, plan, run result, declined run, and run-history commands.
- Separate CLI document, portable run-record, and SQLite store schema versions by
  name and contract.
- Preserve one-document stdout by routing entry/factory/asset stdout to stderr while
  JSON mode is active; keep diagnostics, preflight, prompts, and TUI on stderr.
- Emit a typed declined-run document for an interactive JSON run declined by the user.
- Define the exit/document matrix, deterministic array ordering, sensitive failure
  metadata boundary, and alpha compatibility policy.
- Validate live CLI and installed-wheel output against schemas and normalized golden
  fixtures without adding a runtime dependency.
- **BREAKING (alpha):** all existing CLI JSON shapes gain the uniform envelope; run
  results are nested under a versioned portable-record wrapper, and declined JSON runs
  now emit a document instead of empty stdout.

Scope excludes JSON error envelopes, byte-for-byte stability, arbitrary asset-output
serialization, automatic redaction, and indefinite support for every alpha schema.

## Capabilities

### New Capabilities

- `cli-json-contracts`: Typed envelopes, normative schemas, version policy, golden
  compatibility evidence, and stream-integrity behavior for machine-readable CLI use.

### Modified Capabilities

- `public-cli-stability`: Replace unspecified JSON schema promises with typed,
  independently versioned document compatibility.
- `flow-cli-contract`: Tighten stdout isolation and define declined JSON execution.
- `flow-plan-cli`: Apply the uniform envelope to assets, partitions, and plan output.
- `reviewed-run-cli`: Apply typed run-result/declined documents and exact exit behavior.
- `run-history-cli`: Separate CLI, portable-record, stored-record, and store versions.
- `hosted-user-documentation`: Publish schemas, field reference, golden examples, and
  automation/privacy guidance.

## Impact

The stdlib CLI projection layer, CLI and packaging tests, checked-in documentation
schemas/fixtures, wheel smoke tests, and hosted reference change. `RunResult.to_record()`
and SQLite storage remain separate existing contracts. Schema validation tooling is
development-only; the installed core retains zero required third-party dependencies.
