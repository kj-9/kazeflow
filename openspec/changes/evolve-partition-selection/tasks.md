## 1. Core definition and plan model

- [ ] 1.1 Add backward-compatible partition normalization, stable domain metadata, and strict inclusive `DatePartitionDef` behavior.
- [ ] 1.2 Extend plan/run configuration with mutually exclusive explicit key/range selection, normalize through selected definitions, and reject domain or normalized-key disagreement.
- [ ] 1.3 Preserve executor semantics for normalized keys, falsey custom keys, explicit empty selection, reducers, dependency blocking, and exactly-once attempts with regression tests.

## 2. CLI inspection and selection

- [ ] 2.1 Add `kazeflow partitions` text/JSON inspection with shared entry/target resolution and no asset invocation or unbounded enumeration.
- [ ] 2.2 Add `--partition-range` and `--empty-partitions` to plan/run while preserving repeatable key aliases and status/stdout/stderr contracts.
- [ ] 2.3 Show selection kind, domain, and safe counts consistently in text, JSON, graph, and run preflight without raw key exposure.

## 3. Public guidance and compatibility

- [ ] 3.1 Update Partition task/concept/reference/example pages for definition-owned validation, inspection, range, empty selection, and privacy boundaries.
- [ ] 3.2 Update API exports/reference and the documentation contract checker without adding a required runtime dependency.

## 4. Verification and completion

- [ ] 4.1 Run focused and full tests, formatting, lint, type checks, strict docs build, wheel metadata/core smoke, and OpenSpec strict validation.
- [ ] 4.2 Verify implementation against every requirement/scenario, update the M16 roadmap status, sync living specs, and archive the change.
