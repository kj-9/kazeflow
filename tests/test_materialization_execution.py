import asyncio
from dataclasses import replace

import pytest

from kazeflow.assets import AssetContext, AssetRegistry
from kazeflow.flow import Flow
from kazeflow.materialization import MaterializationRecord
from kazeflow.partition import PartitionDef
from kazeflow.results import AttemptStatus, FlowStatus, SkipReason


class Keys(PartitionDef):
    def range(self, start, end):
        raise AssertionError("range is not used")


class MemoryAdapter:
    def __init__(self):
        self.records = {}
        self.values = {}
        self.fail = None
        self.seen = []

    async def resolve(self, context):
        self.seen.append(("resolve", context))
        if self.fail == "resolve":
            raise RuntimeError("resolve failed")
        return self.records.get(
            (context.partition_key, context.input_materialization_ids)
        )

    async def load(self, record):
        self.seen.append(("load", record))
        if self.fail == "load":
            raise RuntimeError("load failed")
        return self.values[record.materialization_id]

    async def commit(self, context, output):
        self.seen.append(("commit", context))
        if self.fail == "commit":
            raise RuntimeError("commit failed")
        identifier = f"{context.asset_name}:{context.version}:{context.partition_key}:{len(self.seen)}"
        record = MaterializationRecord(
            identifier,
            context.asset_name,
            context.version,
            context.partition_domain,
            context.partition_key_present,
            context.partition_key,
            context.input_materialization_ids,
            True,
        )
        self.records[(context.partition_key, context.input_materialization_ids)] = (
            record
        )
        self.values[identifier] = output
        return record


@pytest.mark.asyncio
async def test_reuse_miss_version_and_input_identity_with_falsey_keys():
    registry = AssetRegistry()
    keys = Keys()
    upstream_adapter = MemoryAdapter()
    downstream_adapter = MemoryAdapter()
    calls = []

    async def upstream(context: AssetContext):
        calls.append(("upstream", context.partition_key))
        return f"value:{context.partition_key}"

    async def downstream(upstream, context: AssetContext):
        calls.append(("downstream", context.partition_key))
        return upstream[context.partition_key] + ":done"

    registry.register(
        upstream, partition_def=keys, materialization=upstream_adapter, version="1"
    )
    registry.register(
        downstream, partition_def=keys, materialization=downstream_adapter, version="1"
    )
    flow = Flow(["downstream"], registry=registry)
    first = await flow.run_async({"partition_keys": [0, ""]})
    assert first.status is FlowStatus.SUCCESS
    assert len(calls) == 4
    assert all(
        a.resolution == "executed" and a.materialization_id
        for t in first.tasks
        for a in t.attempts
    )
    assert all(a.attempt.partition_key_present for t in first.tasks for a in t.attempts)

    calls.clear()
    second = await flow.run_async({"partition_keys": [0, ""]})
    assert calls == []
    assert all(a.resolution == "materialized" for t in second.tasks for a in t.attempts)
    assert [a.attempt.partition_key for a in second.tasks[0].attempts] == [0, ""]

    upstream_adapter.records.pop(("", ()))
    calls.clear()
    third = await flow.run_async({"partition_keys": [0, ""]})
    assert calls == [("upstream", ""), ("downstream", "")]
    assert [a.resolution for a in third.tasks[0].attempts] == [
        "materialized",
        "executed",
    ]
    assert [a.resolution for a in third.tasks[1].attempts] == [
        "materialized",
        "executed",
    ]

    registry.get("upstream").version = "2"
    calls.clear()
    changed = await flow.run_async({"partition_keys": [0]})
    assert calls == [("upstream", 0), ("downstream", 0)]
    assert changed.status is FlowStatus.SUCCESS


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["resolve", "load", "commit"])
async def test_adapter_failure_blocks_dependents_and_keeps_independent_work(failure):
    registry = AssetRegistry()
    adapter = MemoryAdapter()
    keys = Keys()
    calls = []

    async def source(context: AssetContext):
        calls.append(("source", context.partition_key))
        return context.partition_key

    async def child(source, context: AssetContext):
        calls.append(("child", context.partition_key))
        return source

    async def independent(context: AssetContext):
        calls.append(("independent", context.partition_key))
        return "ok"

    registry.register(source, partition_def=keys, materialization=adapter, version="1")
    registry.register(child, partition_def=keys)
    registry.register(independent, partition_def=keys)
    if failure == "load":
        await Flow(["source"], registry=registry).run_async({"partition_keys": [0]})
        calls.clear()
    adapter.fail = failure
    result = await Flow(["child", "independent"], registry=registry).run_async(
        {"partition_keys": [0]}
    )
    by_name = {task.task.task_name: task for task in result.tasks}
    assert result.status is FlowStatus.FAILED
    assert by_name["source"].attempts[0].status is AttemptStatus.FAILED
    assert by_name["source"].attempts[0].materialization_id is None
    assert by_name["child"].attempts[0].reason is SkipReason.DEPENDENCY_BLOCKED
    assert by_name["independent"].attempts[0].status is AttemptStatus.SUCCESS
    assert ("child", 0) not in calls
    assert ("independent", 0) in calls
    assert len(by_name["source"].attempts) == 1


@pytest.mark.asyncio
async def test_invalid_resolved_record_is_miss_and_invalid_commit_fails():
    registry = AssetRegistry()
    adapter = MemoryAdapter()
    calls = []

    async def source(context: AssetContext):
        calls.append(context.partition_key)
        return "ok"

    registry.register(
        source, partition_def=Keys(), materialization=adapter, version="1"
    )
    flow = Flow(["source"], registry=registry)
    first = await flow.run_async({"partition_keys": [0]})
    assert first.status is FlowStatus.SUCCESS
    adapter.records[(0, ())] = replace(adapter.records[(0, ())], version="stale")
    second = await flow.run_async({"partition_keys": [0]})
    assert second.tasks[0].attempts[0].resolution == "executed"
    assert calls == [0, 0]

    class BadCommit(MemoryAdapter):
        async def commit(self, context, output):
            record = await super().commit(context, output)
            return replace(record, committed=False)

    registry.get("source").materialization = BadCommit()
    failed = await flow.run_async({"partition_keys": [0]})
    assert failed.tasks[0].attempts[0].status is AttemptStatus.FAILED


@pytest.mark.asyncio
async def test_committed_upstream_is_reused_after_downstream_failure():
    registry = AssetRegistry()
    adapter = MemoryAdapter()
    calls = []
    should_fail = True

    async def source(context: AssetContext):
        calls.append("source")
        return "saved"

    async def child(source):
        nonlocal should_fail
        calls.append("child")
        if should_fail:
            raise RuntimeError("retry later")
        return source[0]

    registry.register(
        source, partition_def=Keys(), materialization=adapter, version="1"
    )
    registry.register(child, partition_def=Keys())
    flow = Flow(["child"], registry=registry)
    first = await flow.run_async({"partition_keys": [0]})
    assert first.status is FlowStatus.FAILED
    assert calls == ["source", "child"]
    should_fail = False
    calls.clear()
    second = await flow.run_async({"partition_keys": [0]})
    assert second.status is FlowStatus.SUCCESS
    assert calls == ["child"]
    assert second.tasks[0].attempts[0].resolution == "materialized"
    assert second.tasks[1].attempts[0].output == "saved"


@pytest.mark.asyncio
async def test_cancellation_does_not_fabricate_result_or_commit():
    registry = AssetRegistry()
    adapter = MemoryAdapter()
    started = asyncio.Event()
    release = asyncio.Event()

    async def source(context: AssetContext):
        started.set()
        await release.wait()
        return "ready"

    registry.register(
        source, partition_def=Keys(), materialization=adapter, version="1"
    )
    run = asyncio.create_task(
        Flow(["source"], registry=registry).run_async({"partition_keys": [0]})
    )
    await started.wait()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert not any(kind == "commit" for kind, _ in adapter.seen)
