## ADDED Requirements

### Requirement: Numbered invocation history preserves logical result semantics
A logical attempt SHALL optionally retain an immutable ordered invocation history. Each entry SHALL carry a contiguous one-based number, success/failed status, UTC start/end and non-negative duration, failure metadata exactly for failed invocations, and nullable applied retry delay and cooldown seconds. Enabled policies SHALL record each body-plus-commit operation; materialization hits and failures before a body SHALL have empty history. A failed entry SHALL not force the logical aggregate to fail when a later invocation succeeds. Constructor validation SHALL reject malformed history and delay fields. For a normal success/failed logical outcome with nonempty history, its final status SHALL match the final invocation and its final invocation SHALL NOT retain a scheduled retry delay.

#### Scenario: Recover after an initial failure
- **WHEN** a logical partition attempt fails once and then succeeds
- **THEN** its final status is success and its ordered history contains failed invocation 1 followed by successful invocation 2

### Requirement: Result projections support explicit portable record versions
Existing zero-argument result projections SHALL preserve portable record version 1. Explicit version 2 projections SHALL add an ordered `invocations` array to each attempt, with number, status, start/end/duration, nullable failure, retry-delay and cooldown seconds. Other record fields SHALL remain identical. Unknown or boolean versions SHALL be rejected. Both projections SHALL omit raw outputs, raw exceptions, raw partition keys, classifier code and materialization provenance.

#### Scenario: Export retry history safely
- **WHEN** a recovered result is projected explicitly as portable record version 2
- **THEN** its invocation history is included without raw output/exception/key values and version 1 remains available with its original shape
