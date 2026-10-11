# Verification: add-partition-outcome-queries

## Completeness

6/6 tasks complete. Four requirements and all six scenarios are covered by
`tests/test_partition_outcome_queries.py` and the installed-wheel smoke.

## Correctness

- Terminal failure and circuit reason filters inspect individual logical attempts,
  including recovered retries, mixed outcomes and cancellation exclusions.
- Falsey, opaque and unhashable keys retain identity, order and repeated references.
- Missing/empty names, non-string names and unpartitioned tasks have specified errors.
- Empty partition selections return empty tuples; live flow tests explicitly select
  a new run and verify circuit state does not carry over.
- Portable record v1/v2/v3 projections are unchanged after inspection.

## Coherence

Implementation follows the two additive RunResult methods and private lookup design.
No executor, dependency, field, record schema, event or storage changes. Documentation
and runnable example explain live object ownership and explicit rerun selection.

## Validation

- Focused query/result tests: 49 passed.
- `make test`: 409 passed on local Python 3.13.
- `make ci-check`: format (52 files), Ruff and ty passed.
- `make docs-check`: strict build and 13-page/search-index check passed.
- Partition outcome example passed, including a successful explicit second run.
- `uv build --offline`, wheel metadata and isolated core install smoke passed.
- All 14 packaged Python modules match current source.
- Python 3.10–3.13 CI matrix retained; other versions were not run locally.
- `openspec doctor` and strict change validation passed.

No critical issues, warnings or suggestions remain. Ready for archive.
