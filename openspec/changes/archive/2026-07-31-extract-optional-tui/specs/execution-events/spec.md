## MODIFIED Requirements

### Requirement: Event values and consumers are passive model-layer constructs
`ExecutionEvent` values and `validate_event_sequence` SHALL not print, configure
logging, render a UI, create files or databases, persist records, dispatch events, or
schedule work.  `ExecutionEventConsumer` SHALL remain a synchronous protocol with
`on_event(event: ExecutionEvent) -> None`; a consumer implementation MAY present,
record, or otherwise process the event only through explicitly selected application
code.  The core runtime SHALL provide a no-op consumer for omitted selection and MAY
provide explicitly selectable standard-library-only text or logging consumers; neither
the protocol nor the event values SHALL import Rich or a persistence library.

For each event emitted by a normally completed execution, core dispatch SHALL invoke
the selected consumer synchronously once in event sequence order.  If an explicitly
selected consumer raises, the executor SHALL stop further dispatch and propagate that
exception; it SHALL NOT convert it to `FailureInfo`, an asset failure, a synthetic
terminal `RunResult`, or synthetic terminal events.  The consumer is responsible for
handling any failure it intends to recover from.

#### Scenario: A passive consumer uses no presentation dependency
- **WHEN** an object implements `ExecutionEventConsumer` to append events to memory
- **THEN** it can do so without importing Rich or a persistence library

#### Scenario: Omitted observation performs no work
- **WHEN** a caller does not select an event consumer
- **THEN** core uses a no-op consumer and does not print, configure logging, render,
  persist, or import a third-party presentation library

#### Scenario: Dispatch preserves observed event order
- **WHEN** a normally completed concurrent run emits events to a recording consumer
- **THEN** the consumer receives every emitted event once in strictly increasing
  sequence-number order

#### Scenario: An observer failure remains distinct from an asset failure
- **WHEN** a selected consumer raises from `on_event`
- **THEN** that exception propagates to the caller and is not represented as an asset
  failure or synthetic completed run result
