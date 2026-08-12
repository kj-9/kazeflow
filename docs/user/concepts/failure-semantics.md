# Failure semantics

A failed asset makes its dependent work ineligible, but independent branches can
continue. Every planned task reaches a terminal classification.

## Terminal outcomes

- `success`: the attempt completed normally.
- `failed`: the asset body raised and portable failure metadata was captured.
- `skipped`: the attempt did not run, commonly because a dependency failed or no
  partition keys were selected.
- `cancelled`: a terminal status representable by the result and storage models.

If any task fails, the flow result is failed. Cancellation is distinguished from an
asset failure. A dependency-blocked task records its blockers rather than pretending
to have executed.

## External asyncio cancellation

The current executor does not manufacture a cancelled `RunResult` when the caller
cancels the task awaiting `Flow.run_async()`. It stops scheduling pending work,
requests cancellation of executor-created async tasks, and re-raises
`asyncio.CancelledError`. There is no synthetic terminal result or promised complete
terminal event sequence to inspect in that control flow.

The `cancelled` enum values remain valid model/storage states; their presence does
not imply a public cancellation-result API.

## Partitioned work

Each selected partition produces one attempt. A failed partition can coexist with
successful independent partitions. Inspect attempt results when aggregate task
status is not enough.

## CLI exit status is a separate layer

A confirmed run that completes with an asset failure exits `1` and still has a
structured result. Usage, entry loading, and infrastructure failures use different
exit statuses and may not produce a result document. See
[Exit codes and automation](../cli/exit-codes.md).
