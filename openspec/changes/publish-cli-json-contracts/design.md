## Context

M16 leaves the CLI with several useful but independently shaped JSON projections.
`assets`, `partitions`, `plan`, `runs list`, and `runs compare` put a
`schema_version` at their top level; `run` emits the portable
`RunResult.to_record()` directly; and `runs show` combines a stored record version
with a SQLite store version.  Consequently, a consumer that receives one document
cannot always identify its producer or determine which version it must interpret.

The CLI already separates its own diagnostics, preflight, prompts, and optional TUI
presentation onto stderr.  It does not, however, isolate stdout written by trusted
entry modules, factories, or asset functions from a JSON document.  That can make an
otherwise valid automation response unparsable.  Entry loading and asset execution
remain trusted-Python operations; stream isolation is an output-routing contract, not
a sandbox or a confidentiality mechanism.

The project supports Python 3.10--3.13 and keeps its installed core limited to the
standard library.  Existing portable run records and SQLite store migrations already
have their own independent versioning and must remain usable from the Python API.

## Goals / Non-Goals

**Goals:**

- Make every successful `--format json` response self-identifying and independently
  versionable.
- Publish normative, machine-validatable schemas and representative golden output for
  all public JSON command forms.
- Preserve a single parseable stdout document even when trusted user Python writes to
  standard output.
- Make the completed-run, declined-run, usage/entry/infrastructure, and terminal
  asset-failure matrix precise.
- Keep CLI document versions, portable record versions, and SQLite store schema
  versions distinct.
- Exercise the public contract from an installed wheel without adding a runtime
  dependency.

**Non-Goals:**

- JSON error envelopes, streaming JSON, byte-for-byte stability for text or stderr,
  arbitrary Python-output serialization, or automatic redaction.
- Changing the Python `RunResult.to_record()` or SQLite persistence contract merely
  to match a CLI envelope.
- Supporting every historical alpha schema indefinitely, accepting untrusted code,
  or introducing a third-party core runtime dependency.

## Decisions

### Every successful JSON response uses one typed envelope

The CLI projection layer will construct every successful JSON response as:

```json
{
  "document_type": "kazeflow.plan",
  "schema_version": 1,
  "data": {}
}
```

`document_type` identifies the semantic shape and `schema_version` versions that
specific document shape.  The initial types are:

| Command outcome | `document_type` |
| --- | --- |
| `assets --format json` | `kazeflow.assets` |
| `partitions --format json` | `kazeflow.partitions` |
| `plan --format json` | `kazeflow.plan` |
| approved `run --format json` | `kazeflow.run-result` |
| interactive JSON run declined | `kazeflow.run-declined` |
| `runs list --format json` | `kazeflow.runs-list` |
| `runs show --format json` | `kazeflow.runs-show` |
| `runs compare --format json` | `kazeflow.runs-compare` |

`data` holds the command-specific projection.  This avoids both command inference
from field names and a monolithic union schema that would make consumers branch on
unrelated optional fields.  A root-level untyped `schema_version` was rejected
because it has already acquired several meanings and cannot identify a document on
its own.

The CLI will use one internal envelope/projection boundary rather than individual
commands serializing dictionaries themselves.  Command-specific record builders stay
responsible for their data shape; the common boundary owns document type/version and
single-document emission.

### Version namespaces remain deliberately separate

The following values have distinct names and meanings:

| Name | Scope | Owner |
| --- | --- | --- |
| `schema_version` | one typed CLI envelope | CLI JSON contract |
| `record_schema_version` | portable `RunResult.to_record()` nested in CLI data | result-record contract |
| `store_schema_version` | SQLite database format | SQLite adapter |
| package version | distribution/release identity | package metadata |

`kazeflow.run-result` data wraps the existing portable record rather than changing
the Python method:

```json
{
  "record_schema_version": 1,
  "record": { "run_id": "...", "status": "success", "tasks": [] }
}
```

`kazeflow.runs-show` similarly exposes the stored run metadata, its
`store_schema_version`, and the nested `record_schema_version`.  `runs list` summary
fields must name any record version explicitly rather than calling it the generic
CLI schema version.  This preserves `RunResult.to_record()` and stored JSON as
separate, reusable API contracts while making the CLI document unambiguous.

### JSON mode redirects user stdout to stderr at trusted-code boundaries

For a command that selected `--format json`, the adapter will temporarily redirect
stdout written while it loads an entry, invokes a flow factory, or executes an asset
body to stderr.  CLI JSON emission itself uses the original stdout stream after the
trusted-code section completes.  Text, Mermaid, and DOT behavior remains unchanged.

This routing is intentionally scoped to user-controlled execution rather than the
whole process: it preserves argparse help and the adapter's own output behavior, and
it avoids treating stderr as structured data.  Nested user redirection and direct
file-descriptor writes are outside this Python-level guarantee; the documented
promise is that ordinary Python writes through `sys.stdout` cannot corrupt the one
CLI JSON document.  User output redirected to stderr can still contain sensitive
values and is not a redaction facility.

Redirecting user stdout into the JSON `data` object was rejected because arbitrary
output is not portable, can be unbounded, and would blur the command's review/result
model.  Suppressing it was rejected because it silently changes normal trusted script
diagnostics; stderr maintains observability while protecting parsability.

### Declining a JSON run is successful and observable

An interactive `run --format json` declined at confirmation remains exit `0`, runs
no asset, initializes no adapter, and creates no result or store record.  Unlike the
current empty stdout behavior, it emits exactly one `kazeflow.run-declined` envelope
with a stable reason such as `not_approved`; it never includes the typed response or
raw input.  Preflight and the prompt remain on stderr.

This makes JSON pipelines able to distinguish a deliberate no-op from a missing
response without misrepresenting it as a completed `RunResult`.  A declined document
was preferred over a synthetic run result because no execution lifecycle exists.

### Normative Draft 2020-12 schemas and normalized golden examples are checked in

Schemas will live in a versioned, checked-in directory grouped by document type and
CLI schema version.  They use JSON Schema Draft 2020-12 with local `$ref` values and
shared definitions for envelope fields, timestamps, statuses, partition selection,
portable result records, stored records, and comparison summaries.  Each schema is
strict about documented object fields (`additionalProperties: false`) and required
fields, while arrays explicitly preserve their producer-defined order.

One representative golden document per command outcome will be checked in beside the
schemas.  Golden cases cover at least assets, partition inspection, key/range/empty
plans, successful and terminally failed runs, a declined JSON run, history list/show,
and compare.  Tests validate the raw generated document against its schema, then
normalize only intentionally volatile fields (run IDs, timestamps, monotonic
durations, and formatted traceback location detail) before golden comparison.

The `jsonschema` package is a development/test-only dependency used by contract and
wheel tests.  It is not imported by `kazeflow` runtime modules, is absent from base
wheel metadata, and does not alter the zero-dependency core.  Handwritten structural
checks were rejected because they would duplicate a standards-based schema validator
and leave the published schema non-normative.

### Error output is intentionally not enveloped

Only successful command outcomes emit an envelope.  Usage/configuration errors exit
`2`, entry-resolution failures exit `3`, and infrastructure/explicit-adapter failures
exit `4`, all with no successful stdout document.  A terminal asset failure exits
`1` and still emits `kazeflow.run-result`, because it is a completed and reviewable
execution result.  This retains shell-friendly exit handling and avoids exposing
application-controlled failure data in a second, competing error schema.

The portable boundary remains structural: CLI documents omit raw task output,
exception objects, and raw partition key values.  Portable failure type, message, and
traceback remain nested result metadata and may contain application-controlled
sensitive values.  Schemas document field presence and type only; they do not make a
confidentiality claim.

### Alpha compatibility is explicit and bounded

Within an alpha release line, a published `(document_type, schema_version)` is
append-closed: required fields, their types, nullability, enum meaning, and documented
array ordering do not change.  A breaking shape or semantic change creates a new
schema version and migration note.  Newly released alpha binaries need not continue
emitting every prior schema version; automation should pin the package or explicitly
allow the document versions it accepts.

Before a stable release, retained old schemas, golden fixtures, and migration notes
are evidence for the supported compatibility window rather than a promise of
indefinite decoding.  A stable-release compatibility policy can tighten this later
without retroactively making alpha output permanent.

## Risks / Trade-offs

- [Trusted code writes ordinary stdout in JSON mode] → Redirect it to stderr during
  entry/factory/asset execution and test all three boundaries.
- [A user writes directly to a file descriptor or changes global streams] → Document
  this as outside adapter control; kazeflow remains a trusted-Python tool, not a
  sandbox.
- [Schema, golden fixture, and implementation drift] → Validate every golden against
  its schema and every live CLI response against the same schema in CI.
- [Volatile timing and traceback values make goldens flaky] → Normalize only listed
  volatile fields and retain exact checks for all structural and semantic data.
- [`jsonschema` accidentally becomes a product dependency] → Keep it in the dev
  group and retain wheel-metadata/core-only smoke checks.
- [Envelope migration breaks alpha scripts] → Treat it as the proposal's documented
  alpha-breaking change; publish field tables, migration examples, and version notes.
- [Failure metadata is mistaken for sanitized data] → Repeat the portable boundary in
  schema docs and automation guidance; no schema marks failure strings as secret-safe.

## Migration Plan

1. Define the CLI envelope and version namespace in capability specs before changing
   emitters.
2. Add checked-in Draft 2020-12 schemas, representative goldens, and a dev-only
   schema validator helper.
3. Convert CLI JSON projection sites to the common envelope and apply JSON-mode
   trusted-stdout redirection around loading, factories, and execution.
4. Add declined-run emission, exact exit/stream tests, schema/golden tests, and
   documentation field references.
5. Extend the installed-wheel smoke path to validate representative JSON documents
   using the checked-in schemas and fixtures; retain core-only and optional-TUI
   coverage.
6. Publish the alpha migration note.  Rollback before release is a single CLI
   contract change; no SQLite migration or conversion of existing run records occurs.

## Ownership and Parallelization

The CLI projection and stream-routing owner changes `src/kazeflow/cli.py` and its
focused CLI tests.  A separate contract-test owner adds schemas, fixtures, schema
validation helpers, and wheel-level contract coverage without editing CLI behavior.
A documentation owner updates the hosted CLI/automation reference.  The package
dependency owner alone changes `pyproject.toml` and `uv.lock` to add development-only
`jsonschema`; no other wave edits those hotspot files.  `results.py`,
`sqlite_store.py`, `flow.py`, `assets.py`, and `__init__.py` are intentionally
out-of-scope unless a later spec proves a separate portable-record change necessary.

The CLI owner and contract-test owner must agree on the finalized envelope/data field
tables before working in parallel.  OpenSpec capability-spec sync and archive remain
serial work.

## Open Questions

None.  The exact field tables, schema IDs, and fixture paths belong in the capability
specification and tasks that follow this design.
