## Context

The planner currently validates one global sequence only for `None`, hashability, and
duplicates, then copies it to every partitioned task. `PartitionDef` is only a marker
plus a range helper; the CLI forwards raw strings unchanged. The executor joins
partitioned dependencies by normalized-key equality, so validation must happen before
attempt creation. M16 changes core models, CLI presentation, and documentation while
preserving the zero-dependency and trusted-Python boundaries.

## Goals / Non-Goals

**Goals:**

- Make each definition responsible for canonical key normalization and domain identity.
- Represent key, range, and explicit-empty selection deterministically in `FlowPlan`.
- Diagnose invalid dates, reversed ranges, irrelevant selectors, and incompatible
  selected domains before events or assets.
- Inspect definition metadata and range capability through the CLI without enumerating
  work.
- Preserve existing execution behavior after planning, including falsey generic keys,
  empty reducers, partition order, matching-key dependencies, and portable omission.

**Non-Goals:**

- Dynamic catalogs, implicit dates, unbounded ranges, partition mapping, multiple
  domains in one run, multidimensional selection, scheduling, persistence, or sandboxing.
- Raw partition values in CLI JSON, graph labels, storage, or portable run records.
- Third-party runtime dependencies.

## Decisions

### Definition contract has safe compatibility defaults

`PartitionDef` gains concrete `domain`, `key_format`, `normalize_key`, and range
metadata behavior. The default domain is the subclass's qualified class identity and
the default normalizer preserves an already valid Python key. Existing subclasses that
only implement `range()` therefore remain constructible. Custom definitions can
override the stable domain and normalizer. `DatePartitionDef` uses domain `date`,
strictly accepts canonical `YYYY-MM-DD` text or a non-datetime `date`, and returns a
`date`.

Adding new abstract methods was rejected because it would break every existing custom
definition at instantiation time. Treating class instance identity as compatibility was
rejected because separate equivalent instances would never match.

### Planning owns selection normalization

`PlanConfig` accepts either `partition_keys` or one two-bound `partition_range`.
Planning first resolves the selected dependency closure and its definitions, requires
one equal domain across the closure, then asks every definition to normalize/expand
the same raw selection. Their canonical tuples must compare equal. It performs generic
`None`, hashability, and post-normalization duplicate checks afterward.

The normalized `PlanConfig` retains canonical `partition_keys`, normalized range bounds
when range-selected, and a stable `partition_domain`; a computed selection-kind view
distinguishes omitted, keys, range, and empty. Each partitioned `TaskPlan` retains the
same canonical key tuple and domain. Existing executor code can therefore keep matching
keys by equality without accepting per-task ambiguity.

Allowing independent selected branches to use separate domains was rejected for M16:
the public run configuration supplies only one selection, so there is no unambiguous
way to associate raw inputs with branches without a mapping DSL.

### CLI selection is explicit and mutually exclusive

`plan` and `run` retain repeatable `--partition-key`/`--partition`, add exactly one
`--partition-range START END`, and add `--empty-partitions`. Argparse mutual exclusion
prevents mixed forms; omitted selection remains an error for partitioned work, and any
selector is an error for an unpartitioned closure. `run` reuses the normalized preflight
configuration for execution as today.

`kazeflow partitions ENTRY [--target ...] [--format text|json]` selects the same closure
as planning but inspects only definition metadata. A small planning-layer inspection
projection reuses closure validation without constructing attempts or invoking assets.
It reports no dynamic candidates. Loading the entry remains trusted Python execution.

### Presentation exposes provenance, never raw keys

Plan text and JSON expose selection kind, domain, total count, and task counts. The
partition command exposes asset name, definition kind, domain, key format, and bounded
range support. They do not emit selected values or rejected input. This keeps the
portable boundary compatible with later M17 schema versioning and does not imply
redaction of exception messages or tracebacks.

## Risks / Trade-offs

- [Alpha callers used arbitrary strings with `DatePartitionDef`] → document the
  intentional preflight break and direct non-date domains to a custom definition.
- [Custom definitions with equal domain identifiers normalize differently] → normalize
  through every selected definition and reject unequal canonical tuples.
- [Range expansion can allocate many keys] → require finite explicit bounds; limits are
  deferred because choosing a universal threshold would be arbitrary.
- [Definition metadata can be misleading if custom overrides lie] → treat it as an
  author-owned contract, not a safety guarantee or catalog.
- [Import-time effects still occur during inspection] → keep the existing trust-boundary
  diagnostic and tests; no sandbox claim.

## Migration Plan

1. Add compatible definition methods and normalized planning fields.
2. Update planner and executor-facing tests before wiring CLI selectors.
3. Add CLI inspection/presentation and black-box tests.
4. Update public docs and their behavioral checks, then validate/build the wheel.
5. Sync and archive the OpenSpec change after verification. Rollback is one feature
   commit while the project is alpha; no persisted schema migration is involved.

## Open Questions

None for M16. Multiple domains and selection mappings remain an explicit future design.

