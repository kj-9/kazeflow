"""The standard-library command line interface for inspecting kazeflow flows.

Loading an entry runs ordinary user Python and can therefore have import-time
side effects.  Inspection never invokes an asset body, but it is not a
sandbox or an approval mechanism.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path
import sys
from types import ModuleType
from typing import Any, Sequence, TextIO
from uuid import uuid4

from .assets import Asset, AssetRegistry, default_registry
from .flow import Flow
from .plan import FlowPlan


EXIT_SUCCESS = 0
EXIT_USAGE = 2
EXIT_ENTRY = 3
EXIT_INFRASTRUCTURE = 4
_DEFAULT_HISTORY_STORE = Path(".kazeflow") / "runs.sqlite3"


class _UsageError(Exception):
    pass


class _EntryError(Exception):
    pass


class _InfrastructureError(Exception):
    pass


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise _UsageError(message)


@dataclass(frozen=True)
class _LoadedEntry:
    flow: Flow | None
    assets: tuple[Asset, ...]


@dataclass(frozen=True)
class _SelectedRun:
    flow: Flow
    config: dict[str, Any] | None
    plan: FlowPlan


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="kazeflow",
        description=(
            "Inspect a trusted Python flow. Loading an entry executes user Python; "
            "inspection does not execute asset bodies."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    assets = commands.add_parser("assets", help="list assets loaded from an entry")
    assets.add_argument("entry", help="a Python file or module:attribute entry")
    assets.add_argument("--format", choices=("text", "json"), default="text")

    plan = commands.add_parser("plan", help="build a plan without executing assets")
    plan.add_argument("entry", help="a Python file or module:attribute entry")
    plan.add_argument("--target", dest="targets", action="append", default=[])
    plan.add_argument(
        "--partition-key", "--partition", dest="partition_keys", action="append"
    )
    plan.add_argument("--max-concurrency", type=int)
    plan.add_argument(
        "--format", choices=("text", "json", "mermaid", "dot"), default="text"
    )
    plan.add_argument(
        "--verbose",
        action="store_true",
        help="show configuration and task metadata in text output",
    )

    run = commands.add_parser("run", help="review and deliberately execute a flow")
    run.add_argument("entry", help="a Python file or module:attribute entry")
    run.add_argument("--target", dest="targets", action="append", default=[])
    run.add_argument(
        "--partition-key", "--partition", dest="partition_keys", action="append"
    )
    run.add_argument("--max-concurrency", type=int)
    run.add_argument("--format", choices=("text", "json"), default="text")
    run.add_argument("--yes", action="store_true", help="approve execution")
    run.add_argument("--tui", action="store_true", help="show optional Rich progress")
    run.add_argument("--store", metavar="PATH", help="save the completed run to SQLite")

    runs = commands.add_parser("runs", help="inspect saved local run history")
    history = runs.add_subparsers(dest="history_command", required=True)
    list_runs = history.add_parser("list", help="list saved run summaries")
    list_runs.add_argument("--store", metavar="PATH")
    list_runs.add_argument("--limit", type=int)
    list_runs.add_argument("--format", choices=("text", "json"), default="text")
    show_run = history.add_parser("show", help="show one saved portable record")
    show_run.add_argument("run_id")
    show_run.add_argument("--store", metavar="PATH")
    show_run.add_argument("--format", choices=("text", "json"), default="text")
    compare_runs = history.add_parser(
        "compare", help="compare two saved portable records"
    )
    compare_runs.add_argument("left_run_id")
    compare_runs.add_argument("right_run_id")
    compare_runs.add_argument("--store", metavar="PATH")
    compare_runs.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return its process status without raising user errors."""
    parser = _parser()
    try:
        args = parser.parse_args(argv)
        if args.command == "runs":
            return _run_history(args)
        _validate_output_selection(args)
        _validate_entry_syntax(args.entry)
        if args.command == "assets":
            loaded = _load_entry(args.entry)
            _emit_assets(loaded, args.format)
        elif args.command == "plan":
            loaded = _load_entry(args.entry)
            _emit_plan(loaded, args)
        else:
            loaded = _load_entry(args.entry)
            return _run_selected(loaded, args)
    except _UsageError as error:
        _diagnostic(str(error))
        return EXIT_USAGE
    except _EntryError as error:
        _diagnostic(str(error))
        return EXIT_ENTRY
    except _InfrastructureError as error:
        _diagnostic(str(error))
        return EXIT_INFRASTRUCTURE
    except (TypeError, ValueError) as error:
        _diagnostic(str(error))
        return EXIT_USAGE
    except SystemExit as error:
        # argparse uses SystemExit only for --help in this adapter.
        return int(error.code) if isinstance(error.code, int) else EXIT_USAGE
    except Exception as error:  # pragma: no cover - defensive process boundary
        _diagnostic(f"internal CLI error: {error}")
        return EXIT_INFRASTRUCTURE
    return EXIT_SUCCESS


def _validate_entry_syntax(entry: str) -> None:
    if not entry or entry.count(":") > 1:
        raise _UsageError("entry must be a Python file or module:attribute")
    if ":" in entry:
        source, attribute = entry.split(":", 1)
        if not source or not attribute or "." in attribute:
            raise _UsageError("explicit entries must use source:attribute")
    elif not entry.endswith(".py"):
        raise _UsageError("bare entries must be Python files ending in .py")


def _validate_output_selection(args: argparse.Namespace) -> None:
    if args.command == "plan" and args.verbose and args.format != "text":
        raise _UsageError("--verbose is only available with --format text")


def _load_entry(entry: str) -> _LoadedEntry:
    if ":" not in entry:
        module, discovered = _load_file(entry)
        flow = getattr(module, "flow", None)
        if flow is not None and not isinstance(flow, Flow):
            flow = None
        if isinstance(flow, Flow):
            _localize_default_registry(flow, discovered)
        if flow is None and not discovered:
            raise _EntryError("entry defines neither a Flow named 'flow' nor assets")
        return _LoadedEntry(flow, discovered)

    source, attribute = entry.split(":", 1)
    is_file = source.endswith(".py")
    if is_file:
        module, discovered = _load_file(source)
    else:
        module, discovered = _load_module(source)
    try:
        value = getattr(module, attribute)
    except AttributeError as error:
        raise _EntryError(f"entry attribute not found: {entry}") from error
    flow, discovered = _resolve_explicit_flow(value, entry, discovered, is_file=is_file)
    if is_file:
        _localize_default_registry(flow, discovered)
    return _LoadedEntry(flow, discovered)


def _load_module(name: str) -> tuple[ModuleType, tuple[Asset, ...]]:
    before = dict(default_registry._assets)
    try:
        module = importlib.import_module(name)
    except Exception as error:
        raise _EntryError(f"could not load entry module {name!r}: {error}") from error
    return module, _registry_delta(before)


def _load_file(source: str) -> tuple[ModuleType, tuple[Asset, ...]]:
    path = Path(source)
    if not path.is_file():
        raise _EntryError(f"entry file not found: {source}")
    before = dict(default_registry._assets)
    module_name = f"_kazeflow_entry_{uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise _EntryError(f"could not load entry file: {source}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    sys.path.insert(0, str(path.parent.resolve()))
    discovered: tuple[Asset, ...] = ()
    try:
        spec.loader.exec_module(module)
        discovered = _registry_delta(before)
    except Exception as error:
        raise _EntryError(f"could not load entry file {source!r}: {error}") from error
    finally:
        sys.path.pop(0)
        sys.modules.pop(module_name, None)
        default_registry._assets.clear()
        default_registry._assets.update(before)
    return module, discovered


def _registry_delta(before: dict[str, Asset]) -> tuple[Asset, ...]:
    return tuple(
        default_registry._assets[name]
        for name in sorted(default_registry._assets)
        if before.get(name) is not default_registry._assets[name]
    )


def _resolve_explicit_flow(
    value: Any, entry: str, discovered: tuple[Asset, ...], *, is_file: bool
) -> tuple[Flow, tuple[Asset, ...]]:
    if isinstance(value, Flow):
        return value, discovered
    if not callable(value):
        raise _EntryError(f"entry {entry!r} does not resolve to a Flow")
    before = dict(default_registry._assets)
    if is_file:
        default_registry._assets.clear()
        default_registry._assets.update({asset.name: asset for asset in discovered})
    try:
        resolved = value()
    except Exception as error:
        raise _EntryError(f"Flow factory {entry!r} failed: {error}") from error
    finally:
        if is_file:
            discovered = _registry_delta({})
            default_registry._assets.clear()
            default_registry._assets.update(before)
    if not isinstance(resolved, Flow):
        raise _EntryError(f"Flow factory {entry!r} did not return a Flow")
    return resolved, discovered


def _localize_default_registry(flow: Flow, discovered: tuple[Asset, ...]) -> None:
    """Keep a file-defined default-registry flow valid after global state restores."""
    if flow.registry is not default_registry:
        return
    registry = AssetRegistry()
    registry._assets.update({asset.name: asset for asset in discovered})
    flow.registry = registry


def _emit_assets(loaded: _LoadedEntry, output_format: str) -> None:
    assets = loaded.assets
    if not assets and loaded.flow is not None:
        assets = tuple(
            loaded.flow.registry.get(name)
            for name in sorted(loaded.flow.registry._assets)
        )
    if not assets:
        raise _EntryError("entry defines no inspectable assets")
    records = [
        _asset_record(asset) for asset in sorted(assets, key=lambda asset: asset.name)
    ]
    if output_format == "json":
        _json_output(
            {
                "schema_version": 1,
                "declared_flow": loaded.flow is not None,
                "assets": records,
            }
        )
        return
    print("Assets:")
    for asset in records:
        dependencies = ", ".join(asset["dependencies"]) or "none"
        partitioned = "yes" if asset["partitioned"] else "no"
        print(
            f"- {asset['name']} (dependencies: {dependencies}; partitioned: {partitioned})"
        )


def _emit_plan(loaded: _LoadedEntry, args: argparse.Namespace) -> None:
    selected = _select_run(loaded, args)
    if args.format == "json":
        _json_output(_plan_record(selected.plan))
        return
    if args.format == "mermaid":
        _mermaid_plan(selected.plan)
        return
    if args.format == "dot":
        _dot_plan(selected.plan)
        return
    _text_plan(selected.plan, verbose=args.verbose)


def _select_run(
    loaded: _LoadedEntry, args: argparse.Namespace, *, require_one_target: bool = False
) -> _SelectedRun:
    """Resolve the flow, normalized options, and a side-effect-free preflight plan."""
    targets = list(args.targets)
    if loaded.flow is None:
        if not targets:
            targets = list(_terminal_assets(loaded.assets))
            if require_one_target and len(targets) != 1:
                raise _UsageError(
                    "run requires --target when discovered terminal targets are ambiguous"
                )
        flow = Flow(targets, registry=_registry_for(loaded.assets))
    else:
        flow = (
            loaded.flow if not targets else Flow(targets, registry=loaded.flow.registry)
        )
    config: dict[str, Any] = {}
    if args.max_concurrency is not None:
        config["max_concurrency"] = args.max_concurrency
    if args.partition_keys is not None:
        config["partition_keys"] = args.partition_keys
    plan = flow.plan(config or None)
    return _SelectedRun(flow, config or None, plan)


def _run_selected(loaded: _LoadedEntry, args: argparse.Namespace) -> int:
    selected = _select_run(loaded, args, require_one_target=True)
    _text_plan(selected.plan, file=sys.stderr, heading="Planned run:")

    if not _approved(args.yes):
        return EXIT_SUCCESS

    result = _execute(selected, use_tui=args.tui)
    _save_result(result, args.store)
    _emit_result(result, args.format)
    return EXIT_SUCCESS if result.status.value == "success" else 1


def _approved(approved_by_flag: bool) -> bool:
    if approved_by_flag:
        return True
    if not (sys.stdin.isatty() and sys.stderr.isatty()):
        raise _UsageError("--yes is required when stdin or stderr is not a terminal")
    print("Proceed? [y/N] ", end="", file=sys.stderr, flush=True)
    response = sys.stdin.readline()
    if response.strip().lower() in {"y", "yes"}:
        return True
    _diagnostic("run cancelled")
    return False


def _execute(selected: _SelectedRun, *, use_tui: bool) -> Any:
    """Run only after approval, keeping the optional presenter fully lazy."""
    if not use_tui:
        try:
            return asyncio.run(selected.flow.run_async(selected.config))
        except Exception as error:
            raise _InfrastructureError(f"execution failed: {error}") from error

    try:
        from rich.console import Console

        from .tui import FlowTUIRenderer

        renderer = FlowTUIRenderer(plan=selected.plan, console=Console(stderr=True))
        with renderer:
            return asyncio.run(
                selected.flow.run_async(selected.config, event_consumer=renderer)
            )
    except Exception as error:
        raise _InfrastructureError(f"TUI adapter failed: {error}") from error


def _save_result(result: Any, path: str | None) -> None:
    if path is None:
        return
    try:
        from .sqlite_store import SQLiteRunStore

        with SQLiteRunStore(path) as store:
            store.save(result)
    except Exception as error:
        raise _InfrastructureError(f"SQLite store failed: {error}") from error


def _emit_result(result: Any, output_format: str) -> None:
    if output_format == "json":
        _json_output(result.to_record())
        return
    print("Run result:")
    print(f"- run_id: {result.run_id}")
    print(f"- status: {result.status.value}")
    print(f"- tasks: {len(result.tasks)}")


def _run_history(args: argparse.Namespace) -> int:
    """Read a caller-owned history store without creating or changing it."""
    if args.history_command == "list" and args.limit is not None and args.limit < 0:
        raise _UsageError("--limit must be a non-negative integer")
    if args.history_command == "compare" and args.left_run_id == args.right_run_id:
        raise _UsageError("compare requires two distinct run IDs")

    path = Path(args.store) if args.store is not None else _DEFAULT_HISTORY_STORE
    try:
        if not path.is_file():
            raise _InfrastructureError(f"history store is not an existing file: {path}")
    except OSError as error:
        raise _InfrastructureError(
            f"could not inspect history store {path}: {error}"
        ) from error

    try:
        from .sqlite_store import SQLiteRunStore

        with SQLiteRunStore(path) as store:
            if args.history_command == "list":
                _emit_history_list(store.list_runs(limit=args.limit), args.format)
            elif args.history_command == "show":
                _emit_history_show(
                    store.load(args.run_id), store.schema_version, args.format
                )
            else:
                left = store.load(args.left_run_id)
                right = store.load(args.right_run_id)
                _emit_history_compare(left, right, store.schema_version, args.format)
    except KeyError as error:
        raise _UsageError(f"run not found: {error.args[0]}") from error
    except _UsageError:
        raise
    except _InfrastructureError:
        raise
    except Exception as error:
        raise _InfrastructureError(
            f"could not read history store {path}: {error}"
        ) from error
    return EXIT_SUCCESS


def _history_summary(summary: Any) -> dict[str, Any]:
    return {
        "run_id": summary.run_id,
        "schema_version": summary.schema_version,
        "status": summary.status,
        "saved_at": summary.saved_at.isoformat(),
    }


def _history_envelope(stored: Any, store_schema_version: int) -> dict[str, Any]:
    return {
        "run_id": stored.run_id,
        "schema_version": stored.schema_version,
        "status": stored.status,
        "saved_at": stored.saved_at.isoformat(),
        "store_schema_version": store_schema_version,
        "record": stored.record,
    }


def _emit_history_list(summaries: Sequence[Any], output_format: str) -> None:
    records = [_history_summary(summary) for summary in summaries]
    if output_format == "json":
        _json_output({"schema_version": 1, "runs": records})
        return
    if not records:
        print("No stored runs.")
        return
    print("Stored runs:")
    for record in records:
        print(
            f"- {record['run_id']} ({record['status']}; saved_at: {record['saved_at']}; "
            f"schema: {record['schema_version']})"
        )


def _emit_history_show(
    stored: Any, store_schema_version: int, output_format: str
) -> None:
    envelope = _history_envelope(stored, store_schema_version)
    if output_format == "json":
        _json_output(envelope)
        return
    print("Stored run:")
    print(f"- run_id: {envelope['run_id']}")
    print(f"- status: {envelope['status']}")
    print(f"- saved_at: {envelope['saved_at']}")
    print("Portable record:")
    print(json.dumps(envelope["record"], sort_keys=True, indent=2))


def _task_history_summary(task: dict[str, Any]) -> dict[str, Any]:
    attempts = task["attempts"]
    status_counts: dict[str, int] = {}
    failure_types: set[str] = set()
    partitioned_attempt_count = 0
    for attempt in attempts:
        status = attempt["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
        if attempt["attempt"]["partition"]["present"]:
            partitioned_attempt_count += 1
        failure = attempt["failure"]
        if failure is not None:
            failure_types.add(failure["exception_type"])
    return {
        "is_partitioned": task["is_partitioned"],
        "status": task["status"],
        "reason": task["reason"],
        "attempt_count": len(attempts),
        "partitioned_attempt_count": partitioned_attempt_count,
        "attempt_status_counts": dict(sorted(status_counts.items())),
        "failure_present": bool(failure_types),
        "failure_exception_types": sorted(failure_types),
    }


def _history_comparison(left: Any, right: Any) -> dict[str, Any]:
    left_record = left.record
    right_record = right.record
    left_tasks = {
        task["task"]["task_name"]: _task_history_summary(task)
        for task in left_record["tasks"]
    }
    right_tasks = {
        task["task"]["task_name"]: _task_history_summary(task)
        for task in right_record["tasks"]
    }
    task_comparisons = []
    for name in sorted(set(left_tasks) | set(right_tasks)):
        left_task = left_tasks.get(name)
        right_task = right_tasks.get(name)
        task_comparisons.append(
            {
                "task_name": name,
                "left": left_task,
                "right": right_task,
                "changed": left_task != right_task,
            }
        )
    return {
        "status_changed": left_record["status"] != right_record["status"],
        "duration_seconds_delta": (
            right_record["duration_seconds"] - left_record["duration_seconds"]
        ),
        "task_count_delta": len(right_record["tasks"]) - len(left_record["tasks"]),
        "tasks": task_comparisons,
    }


def _emit_history_compare(
    left: Any, right: Any, store_schema_version: int, output_format: str
) -> None:
    record = {
        "schema_version": 1,
        "left": _history_envelope(left, store_schema_version),
        "right": _history_envelope(right, store_schema_version),
        "comparison": _history_comparison(left, right),
    }
    if output_format == "json":
        _json_output(record)
        return
    print("Run comparison:")
    print(f"- left: {left.run_id} ({left.status})")
    print(f"- right: {right.run_id} ({right.status})")
    print(f"- status_changed: {record['comparison']['status_changed']}")
    print(f"- duration_seconds_delta: {record['comparison']['duration_seconds_delta']}")
    print("Task comparison:")
    for task in record["comparison"]["tasks"]:
        print(f"- {task['task_name']} (changed: {task['changed']})")
        print(f"  left: {_text_task_history_summary(task['left'])}")
        print(f"  right: {_text_task_history_summary(task['right'])}")


def _text_task_history_summary(summary: dict[str, Any] | None) -> str:
    if summary is None:
        return "absent"
    counts = ", ".join(
        f"{status}={count}"
        for status, count in summary["attempt_status_counts"].items()
    )
    failures = ", ".join(summary["failure_exception_types"]) or "none"
    return (
        f"status={summary['status']}; reason={summary['reason']}; "
        f"partitioned={summary['is_partitioned']}; "
        f"partitioned_attempts={summary['partitioned_attempt_count']}; "
        f"attempts={summary['attempt_count']} ({counts}); failures={failures}"
    )


def _registry_for(assets: tuple[Asset, ...]) -> AssetRegistry:
    registry = AssetRegistry()
    registry._assets.update({asset.name: asset for asset in assets})
    return registry


def _terminal_assets(assets: tuple[Asset, ...]) -> tuple[str, ...]:
    names = {asset.name for asset in assets}
    dependencies = {
        dependency
        for asset in assets
        for dependency in asset.deps
        if dependency in names
    }
    return tuple(sorted(names - dependencies))


def _asset_record(asset: Asset) -> dict[str, Any]:
    return {
        "name": asset.name,
        "dependencies": sorted(asset.deps),
        "partitioned": asset.partition_def is not None,
    }


def _plan_record(plan: FlowPlan) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "targets": list(plan.targets),
        "config": {
            "max_concurrency": plan.config.max_concurrency,
            "partition_key_count": (
                None
                if plan.config.partition_keys is None
                else len(plan.config.partition_keys)
            ),
        },
        "tasks": [
            {
                "name": task.name,
                "dependencies": list(task.dependencies),
                "partition_key_count": (
                    None if task.partition_keys is None else len(task.partition_keys)
                ),
            }
            for task in plan.tasks
        ],
    }


def _text_plan(
    plan: FlowPlan,
    *,
    file: TextIO | None = None,
    heading: str | None = None,
    verbose: bool = False,
) -> None:
    stream = file if file is not None else sys.stdout
    if heading is not None:
        print(heading, file=stream)
    targets = ", ".join(plan.targets)
    print(f"Plan: {targets}", file=stream)
    print(
        f"{len(plan.tasks)} assets · "
        f"{_partition_summary(plan)} · {_concurrency_summary(plan)}",
        file=stream,
    )
    print("Graph:", file=stream)
    for task in plan.tasks:
        label = _text_task_label(task.name, task.partition_keys, plan.targets)
        if task.dependencies:
            for dependency in task.dependencies:
                print(f"  {dependency} --> {label}", file=stream)
        else:
            print(f"  {label}", file=stream)
    if not verbose:
        return
    print("Details:", file=stream)
    print(f"- targets: {targets}", file=stream)
    print(f"- max_concurrency: {_concurrency_summary(plan)}", file=stream)
    print(f"- partition_keys: {_partition_summary(plan)}", file=stream)
    for task in plan.tasks:
        dependencies = ", ".join(task.dependencies) or "none"
        partition_detail = (
            "unpartitioned"
            if task.partition_keys is None
            else f"{len(task.partition_keys)} selected"
        )
        print(
            f"- {task.name} (dependencies: {dependencies}; partitions: {partition_detail})",
            file=stream,
        )


def _concurrency_summary(plan: FlowPlan) -> str:
    if plan.config.max_concurrency is None:
        return "default concurrency"
    return f"max concurrency {plan.config.max_concurrency}"


def _partition_summary(plan: FlowPlan) -> str:
    if plan.config.partition_keys is None:
        return "no partition selection"
    return f"{len(plan.config.partition_keys)} partitions selected"


def _text_task_label(
    name: str, partition_keys: Sequence[Any] | None, targets: Sequence[str]
) -> str:
    suffix = " *" if name in targets else ""
    partition = (
        "" if partition_keys is None else f" [partitions: {len(partition_keys)}]"
    )
    return f"{name}{partition}{suffix}"


def _graph_node_ids(plan: FlowPlan) -> dict[str, str]:
    return {task.name: f"task_{index}" for index, task in enumerate(plan.tasks)}


def _graph_label(task: Any, targets: Sequence[str]) -> str:
    label = task.name
    if task.partition_keys is not None:
        label += f" [partitions: {len(task.partition_keys)}]"
    if task.name in targets:
        label += " (target)"
    return label


def _mermaid_plan(plan: FlowPlan) -> None:
    node_ids = _graph_node_ids(plan)
    print("flowchart LR")
    for task in plan.tasks:
        print(
            f"    {node_ids[task.name]}[{json.dumps(_graph_label(task, plan.targets))}]"
        )
    for task in plan.tasks:
        for dependency in task.dependencies:
            print(f"    {node_ids[dependency]} --> {node_ids[task.name]}")


def _dot_plan(plan: FlowPlan) -> None:
    node_ids = _graph_node_ids(plan)
    print("digraph kazeflow {")
    print("  rankdir=LR;")
    for task in plan.tasks:
        print(
            f"  {node_ids[task.name]} [label={json.dumps(_graph_label(task, plan.targets))}];"
        )
    for task in plan.tasks:
        for dependency in task.dependencies:
            print(f"  {node_ids[dependency]} -> {node_ids[task.name]};")
    print("}")


def _json_output(value: dict[str, Any]) -> None:
    print(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _diagnostic(message: str) -> None:
    print(f"kazeflow: {message}", file=sys.stderr)
