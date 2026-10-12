## MODIFIED Requirements

### Requirement: Flow owns explicit targets and a registry
`Flow(targets, *, registry=default_registry, resources=None, partition_groups=None)` SHALL accept direct targets only as a non-empty `list[str]` or `tuple[str, ...]`, and SHALL retain their canonical target tuple, the exact supplied `AssetRegistry`, and its optional named resource policies and immutable partition group declarations as its execution-definition source. `Flow.plan(run_config=None)` SHALL resolve those owned targets only against that owned registry and SHALL return its `FlowPlan` without invoking an asset. Module-level `run(asset_names, ..., *, registry=default_registry)` SHALL construct `Flow` with those exact values and no resource policies unless explicitly added to that API in a later change.

For source compatibility, `Flow(graph)` SHALL temporarily accept only a legacy `dict[str, set[str]]`. It SHALL derive direct targets as the lexical graph keys that are not a dependency of another graph key, retain a deprecated copy at `flow.graph`, and reject planning unless the graph's closure and direct dependency sets exactly equal the plan resolved from those targets in its registry. The legacy graph SHALL NOT be an alternative asset/dependency source; new code SHALL use direct targets.

#### Scenario: A flow uses its explicitly supplied registry
- **WHEN** a caller constructs `Flow(["target"], registry=custom_registry)` while the default registry contains different assets
- **THEN** planning and execution resolve `target` only from `custom_registry`

#### Scenario: A legacy graph must match its registry plan
- **WHEN** a caller constructs `Flow(graph)` whose dependencies differ from the registry closure inferred from its direct targets
- **THEN** planning raises `ValueError` before an asset or event is invoked

#### Scenario: A flow owns resource policies
- **WHEN** a caller constructs a flow with a named resource policy and a selected asset declares that name
- **THEN** its plan resolves the declared policy from that flow rather than from process-global state


#### Scenario: A flow owns explicit partition groups
- **WHEN** a caller supplies group declarations
- **THEN** planning validates and exposes selected groups before execution
