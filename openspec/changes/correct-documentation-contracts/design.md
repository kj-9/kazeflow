## Context

M14 made `docs/user` the searchable source for public usage guidance. An adversarial
review then compared that guidance with `plan.py`, `flow.py`, `results.py`, CLI
behavior, and living OpenSpec contracts. It found that several concise explanations
had crossed from simplification into false observable claims.

This change is documentation-led, but the affected statements are safety and
execution contracts rather than editorial preferences. The implementation must
therefore use executable behavioral evidence where practical while leaving the
runtime unchanged. M16 will separately design new Partition capabilities; M15 only
describes the current behavior truthfully.

## Goals / Non-Goals

**Goals:**

- Align Partition selection, cancellation, and portable-record explanations with
  current public behavior and living specs.
- Make the three trust boundaries—Python entry loading, asset-body execution, and
  portable failure metadata—easy to distinguish from every affected user journey.
- Give first-time users a deterministic environment/install diagnostic path.
- Add narrow regression evidence that fails when these claims drift.
- Preserve the current CLI, Python API, result model, package metadata, and
  zero-runtime-dependency core.

**Non-Goals:**

- Change or extend `PartitionDef`, `FlowPlan`, `RunResult`, CLI options, or executor
  cancellation behavior.
- Promise that portable records are secret-safe, add redaction, or sanitize
  application failures.
- Define normative JSON schemas or execute arbitrary Markdown snippets.
- Change Pages framework, deployment architecture, release versioning, or fonts.

## Decisions

### Treat living execution contracts as the source for behavioral prose

Partition requirements come from `flow-planning`; external cancellation comes from
`core-executor-integration`; portable record contents come from `run-results` and
`sqlite-run-store`. User pages will translate those contracts and link to one
central trust/privacy explanation rather than inventing a parallel model.

Alternative considered: correct only the three reported sentences. Rejected because
the same overstatement appears across Partition, persistence, results, and concepts,
and would drift again without a shared vocabulary.

### Correct current behavior without pre-documenting M16

Partition guidance will state that explicit keys are required today, CLI values are
passed through as strings, and `DatePartitionDef.range()` is a Python helper. It
will not describe future parse, validation, range-selection, or discovery APIs.

Alternative considered: implement validation while fixing the guide. Rejected
because that is a public behavior/API change requiring the separate M16 contract and
would make a correctness repair depend on a larger product decision.

### Describe structural omission and content sensitivity separately

Pages will say that portable projections omit the dedicated raw partition-key field,
arbitrary outputs, and exception objects. They will also say that portable failure
metadata retains exception type, message, and traceback and can therefore repeat
application values. JSON and SQLite records must be handled as potentially sensitive.

Alternative considered: remove all privacy language. Rejected because users still
need the precise portable boundary to choose between in-memory and stored results.

### Separate model-valid cancellation states from executor cancellation flow

The concepts page will distinguish values accepted by result/storage models from
what `Flow.run_async()` currently returns. External asyncio cancellation propagates
`CancelledError` and produces no synthetic result. The docs will not suggest a
public cancellation-result API.

### Add targeted contract evidence now; defer full executable Docs CI to M18

M15 tests will assert the high-risk facts through existing public/core behavior and
verify that generated pages retain the required caveats. M18 will later run complete
built-wheel journeys and golden command output. This keeps M15 small enough to fix
published misinformation promptly.

### Keep file ownership parallelizable

- Partition and trust wording owner: `docs/user/partitions.md`, Partition examples,
  and the central trust boundary.
- Result/cancellation owner: result, persistence, failure, and planning concepts.
- First-run/test owner: getting-started environment guidance and documentation
  contract tests/checker.
- One integration owner reconciles repeated wording, runs strict MkDocs/OpenSpec
  validation, and archives the change.

No M15 task owns core hotspot files.

## Risks / Trade-offs

- **[Risk] Repeating a caveat on every page makes the site noisy.** → Put the full
  explanation on one concepts page and use short, adjacent warnings plus links where
  users export or store data.
- **[Risk] A phrase-presence test can give false confidence.** → Pair generated-page
  assertions with focused behavior tests; keep the stronger end-to-end harness
  explicitly assigned to M18.
- **[Risk] Docs become stale as M16 changes Partition behavior.** → M16 must modify
  the same hosted-documentation requirement and its acceptance evidence alongside
  the public behavior.
- **[Trade-off] The guide may feel stricter after correction.** → Accuracy is
  preferred over implying convenient behavior that does not exist; M16 addresses
  the usability gap deliberately.

## Migration Plan

1. Add focused regression evidence for current Partition, record, and cancellation
   semantics.
2. Update the central trust/privacy explanation and affected task guides.
3. Add first-run environment diagnostics and cross-links.
4. Build strictly, run the full suite, validate OpenSpec, and visually inspect the
   affected pages.
5. Deploy the corrected site, verify the public pages, then sync/archive M15.

Rollback is documentation-only: revert the M15 commit and redeploy the prior Pages
artifact. Runtime and stored data require no migration.

## Open Questions

None for M15. Partition parsing, bounded ranges, and definition compatibility are
owned by the planned M16 change.
