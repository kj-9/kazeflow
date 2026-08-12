## Why

M16 advances the roadmap by making partition selection understandable and invalid
work rejectable before asset execution. Today `DatePartitionDef` does not participate
in planning: arbitrary CLI strings reach every partitioned task unchanged, reversed
ranges silently become empty work, and incompatible partition definitions can share a
selection without diagnosis.

## What Changes

- Give partition definitions a public, backward-compatible key normalization and
  stable domain contract; make `DatePartitionDef` normalize strict ISO dates.
- Normalize and validate explicit keys in the planner and reject incompatible domains
  in one selected flow before events or asset invocation.
- Add explicit inclusive bounded date-range selection while preserving repeatable keys,
  falsey generic keys, and explicit empty selection semantics.
- Add `kazeflow partitions` to inspect selected definitions and supported selection
  forms without invoking asset bodies.
- Make plan text and portable JSON expose selection kind, domain, and counts while
  continuing to omit raw partition values.
- Update the hosted partition guidance and behavioral documentation evidence.
- **BREAKING (alpha):** a `DatePartitionDef` no longer accepts arbitrary strings or a
  reversed range; those inputs fail during preflight instead of reaching an asset or
  becoming implicit empty work.

Scope is limited to explicit local selection and inspection. It does not add implicit
today/all selection, an unbounded catalog, multidimensional partitions, mapping DSLs,
scheduling, persistence, or automatic secret classification/redaction.

## Capabilities

### New Capabilities

- `validated-partition-selection`: Definition-owned normalization, bounded ranges,
  domain compatibility, and non-executing partition inspection.

### Modified Capabilities

- `execution-contracts`: Planning normalizes partition keys and preserves the existing
  exactly-once, falsey-key, empty-selection, and dependency semantics.
- `flow-plan-cli`: Plan and run accept validated bounded selection and expose safe
  selection metadata; the CLI gains non-executing partition inspection.
- `hosted-user-documentation`: Public guidance changes from pass-through CLI strings to
  validated definition-owned selection.
- `partition-user-guidance`: Examples and edge-case guidance cover strict date keys,
  bounded ranges, explicit empty work, and preflight diagnostics.

## Impact

The core partition and planning models, CLI adapter, public exports, tests, and hosted
documentation change. Existing custom `PartitionDef` subclasses retain identity
normalization defaults; they may opt into stricter normalization and a stable custom
domain. The core remains Python-standard-library-only, infrastructure-free, and does
not claim to sandbox entry loading or asset execution.
