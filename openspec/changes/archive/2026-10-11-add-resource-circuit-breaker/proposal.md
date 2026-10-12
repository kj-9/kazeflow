## Why

Issue #1's prioritized follow-up needs a way to stop further work on an unusable resource while retaining the originating failure and outcomes of work already running. Retry/cooldown provides the classifier boundary needed for this explicit decision.

## What Changes

- Add explicit `OpenCircuit()` failure action; opening is run-local and permanent for that run.
- Skip pending same-resource body invocations, including retries, with source attempt references; finish admitted operations normally.
- Preserve dependency-blocked precedence and allow materialization hits without invoking the unavailable resource.
- Add circuit and mixed-blocked skip reasons with versioned portable/event and CLI/history contracts.
- Scope: classifier, executor, outcomes/events, storage, projections, tests and Docs. Non-goals: thresholds, half-open probes, automatic reopening, cross-run persistence, cancellation of running work and partition outcome query API.

## Capabilities

### New Capabilities
- `resource-circuit-breaker`: explicit run-local circuit opening and attributable skip semantics.

### Modified Capabilities
- `resource-retry-cooldown`: classifier also accepts explicit circuit opening.
- `run-results`: circuit/mixed skip reasons and portable record v3.
- `execution-events`: attributable circuit skip lifecycle and event record v3.
- `sqlite-run-store`: portable record v3 alongside existing rows.
- `cli-json-contracts`: run/history envelope v3 while plan remains v4.

## Impact

Stdlib-only core remains unchanged in dependency footprint. Existing `Fail`/`Retry`, positional APIs and default record v1 remain compatible for representable outcomes. New skip reasons require explicit record/event v3; CLI run/history move to v3. No database layout migration. Root owns executor/shared integration; isolated model and projection owners work against a frozen contract.
