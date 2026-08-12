## ADDED Requirements

### Requirement: Partition definitions own canonical key normalization
Every partition definition SHALL expose a stable domain identifier and SHALL either
normalize an input to a non-`None`, hashable canonical key or reject it before asset
execution. The base definition SHALL retain identity normalization for compatible
existing custom definitions. `DatePartitionDef` SHALL accept strict `YYYY-MM-DD`
strings and `datetime.date` values other than `datetime.datetime`, normalize them to
`datetime.date`, and reject all other values without echoing a rejected value in its
diagnostic.

#### Scenario: A date key is normalized
- **WHEN** a caller plans a date-partitioned flow with the string `2026-08-11`
- **THEN** the in-memory plan and asset context use `date(2026, 8, 11)`

#### Scenario: An invalid date key is rejected
- **WHEN** a caller supplies an invalid, non-canonical, or non-date key to a date definition
- **THEN** planning fails before any asset body or execution event and the diagnostic does not repeat the rejected value

#### Scenario: An existing custom definition uses the compatibility default
- **WHEN** a custom definition implements the existing range contract but does not override normalization or domain
- **THEN** its valid non-`None` hashable keys retain their values and the definition remains constructible

### Requirement: Date ranges are explicit, bounded, and inclusive
`DatePartitionDef` SHALL expand an explicitly supplied start and end into canonical
date keys including both bounds. It SHALL reject invalid bounds and a start later
than the end before execution. Neither the core nor CLI SHALL implicitly select
today, all history, or an unbounded range.

#### Scenario: A bounded date range is selected
- **WHEN** a caller selects `2026-08-11` through `2026-08-13`
- **THEN** the plan contains those three normalized date keys in ascending order

#### Scenario: A reversed date range is rejected
- **WHEN** a caller selects a range whose start is later than its end
- **THEN** planning fails instead of interpreting it as empty work

### Requirement: One selected flow has one compatible partition domain
All partitioned assets in one selected dependency closure SHALL have equal stable
domain identifiers because the flow applies one explicit selection to all of them.
Planning SHALL reject a mismatch with asset names and domain identifiers before an
asset body or execution event, without exposing raw key values.

#### Scenario: Compatible partition dependencies share canonical keys
- **WHEN** partitioned upstream and downstream assets share a domain
- **THEN** each downstream attempt matches exactly one upstream attempt with the same normalized key

#### Scenario: Selected partition domains differ
- **WHEN** any selected partitioned assets, including independent selected branches, have different domains
- **THEN** planning rejects the flow before execution and identifies the incompatible assets and domains

### Requirement: Partition definitions are inspectable without work enumeration
The CLI SHALL inspect the selected closure's partitioned assets and report each asset,
definition kind, stable domain, accepted key form, and bounded-range support without
invoking an asset body. Inspection SHALL preserve the trusted-entry loading boundary
and SHALL NOT enumerate an unbounded or dynamic catalog.

#### Scenario: Inspect a date definition
- **WHEN** a caller runs `kazeflow partitions` for a date-partitioned target
- **THEN** output identifies its date domain, strict key format, and inclusive bounded-range support without listing arbitrary dates or invoking the asset

#### Scenario: Inspect an unpartitioned closure
- **WHEN** the selected closure contains no partitioned assets
- **THEN** inspection succeeds with an explicit empty result

