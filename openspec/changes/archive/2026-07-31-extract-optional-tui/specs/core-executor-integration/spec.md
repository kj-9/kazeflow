## MODIFIED Requirements

### Requirement: Executor emits a complete passive event stream with a no-op default
`run()` and `Flow.run_async()` SHALL accept an optional `ExecutionEventConsumer`; when
none is supplied, execution SHALL use a no-op consumer and SHALL not print, configure
global logging, create files, create a database, import a third-party presentation
library, or render terminal UI.  For each normally completed run, a supplied consumer
that does not raise SHALL receive one complete stream that conforms to
`validate_event_sequence`, with sequence numbers beginning at one and increasing in
observed emission order.  The stream SHALL include flow start and finish,
starts/finishes for scheduled tasks and attempts, terminal finishes without starts for
dependency-blocked/no-work work as permitted by the event contract, and failure
metadata on failed attempt finishes.

Core executor code SHALL dispatch the selected consumer synchronously but SHALL NOT
construct, enter, import, configure, or otherwise select a renderer.  If a selected
consumer raises, its exception SHALL propagate under the execution-events contract;
the executor SHALL not promise a complete stream, terminal result, or synthetic
terminal events after that point.  This observer failure is distinct from normal asset
failure handling.

#### Scenario: Default execution has no presentation side effect
- **WHEN** a caller executes a flow without an event consumer
- **THEN** it receives its `RunResult` without terminal UI output, global logging
  configuration, persistence, or a third-party presentation import

#### Scenario: A consumer receives causally valid failure events
- **WHEN** a planned attempt fails and a consumer records all events without raising
- **THEN** the recorded stream validates, includes the failed attempt finish with
  failure metadata, and ends with the failed flow finish

#### Scenario: An explicit renderer is outside executor construction
- **WHEN** a caller constructs an optional renderer and passes it as the event consumer
- **THEN** the executor dispatches its events without importing or managing that
  renderer and returns the same result semantics as a renderer-free run

#### Scenario: A consumer interrupts observation
- **WHEN** a selected consumer raises while the executor is emitting an event
- **THEN** its exception propagates and no asset failure or synthetic terminal result is
  created for the consumer error

### Requirement: Automatic Rich presentation is removed from core execution
Core executor code SHALL not construct or enter a Rich renderer, call automatic flow
tree/progress display, use renderer progress identifiers as scheduling state, or use a
Rich logger to execute an asset.  Asset context logging supplied by core SHALL be
standard-library-only and silent unless a caller configures its own logger.  Existing
decorator registration, dependency inference, direct execution of asset functions,
and explicit event-consumer selection remain available; callers that relied on
automatic Rich display SHALL migrate to an explicitly constructed optional renderer.

#### Scenario: Core executes with only the standard library path
- **WHEN** a caller runs a valid core flow without choosing presentation
- **THEN** execution semantics, results, and events are available without automatic
  Rich presentation behavior

#### Scenario: Core modules do not load Rich
- **WHEN** application code imports the core plan, result, event, asset, and executor
  modules in an environment where Rich is unavailable
- **THEN** those imports succeed without attempting to import Rich
