## 1. Plan model and builder

- [x] 1.1 **Plan owner — `src/kazeflow/plan.py` only:** add the frozen
  `PlanConfig`, `TaskPlan`, and `FlowPlan` public value models, including tuple
  normalization and the `None` versus empty-tuple partition-selection invariant.
- [x] 1.2 **Plan owner — `src/kazeflow/plan.py` only:** implement
  `build_flow_plan()` as a read-only adapter over existing registry metadata;
  accept only the specified `list[str]` or `tuple[str, ...]` target shape,
  normalize targets to the lexical `FlowPlan.targets` tuple, resolve target closures,
  canonicalize direct dependencies, and create the deterministic
  dependency-first task sequence without invoking asset functions.
- [x] 1.3 **Plan owner — `src/kazeflow/plan.py` only:** validate empty or
  duplicate target selections, empty names, unknown assets/dependencies,
  cycles, and invalid target types (including a lone string),
  `max_concurrency`, required partition-key selection, `None`/unhashable keys,
  and equality-duplicate partition keys before returning a plan.

## 2. Focused plan tests

- [x] 2.1 **Plan owner — `tests/test_plan.py` only:** add isolated registry
  tests that prove the three model values are immutable, tuple-based planning
  data and that planning does not invoke asset functions or create execution
  side effects.
- [x] 2.2 **Plan owner — `tests/test_plan.py` only:** add closure and ordering
  tests for target input normalization, rejected lone strings and invalid target
  shapes, empty/duplicate/unknown names, canonical lexical `FlowPlan.targets`
  and equality across input order, transitive dependencies, lexically stable
  independent work, and unordered dependency metadata.
- [x] 2.3 **Plan owner — `tests/test_plan.py` only:** add validation tests for
  missing definitions, cycles, invalid targets/configuration, omitted
  partition keys, empty partition selections, falsey keys, and duplicate keys
  including `0`/`False`.

## 3. Verification and integration boundary

- [x] 3.1 Run `uv run pytest tests/test_plan.py`, then `make test` and
  `make ci-check`; report each command and any failure conditions.  No package
  build check is required because this change does not alter package metadata
  or dependencies.
- [x] 3.2 Confirm that this implementation changed only
  `src/kazeflow/plan.py` and `tests/test_plan.py`; do not edit `flow.py`,
  `assets.py`, `__init__.py`, package metadata, or executor/TUI code in this
  Wave 1 change.
- [x] 3.3 **Serial OpenSpec work after implementation:** the root/integration
  owner validates this change, then performs any living-spec sync and archive
  only when the M1 contract is accepted; do not perform those operations in
  parallel with other capability changes.
