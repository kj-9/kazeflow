"""Contracts for explicitly reusable partitioned asset outputs.

The adapter owns storage, output encoding, and atomic publication.  The core
only passes identity metadata and verifies that a returned record agrees with
the requested identity.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol, TypeVar


MaterializationInputs = tuple[tuple[str, str], ...]
_T = TypeVar("_T")


def _validate_identity_fields(
    *,
    asset_name: object,
    version: object,
    partition_domain: object,
    partition_key_present: object,
    input_materialization_ids: object,
) -> MaterializationInputs:
    for field_name, value in (
        ("asset_name", asset_name),
        ("version", version),
        ("partition_domain", partition_domain),
    ):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{field_name} must be a non-empty string")
    if not isinstance(partition_key_present, bool):
        raise ValueError("partition_key_present must be a bool")
    if not isinstance(input_materialization_ids, (tuple, list)):
        raise ValueError("input_materialization_ids must be a sequence of pairs")

    normalized: list[tuple[str, str]] = []
    seen_dependencies: set[str] = set()
    for item in input_materialization_ids:
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            raise ValueError("input_materialization_ids must contain pairs")
        dependency_name, materialization_id = item
        if not isinstance(dependency_name, str) or not dependency_name:
            raise ValueError("input dependency names must be non-empty strings")
        if not isinstance(materialization_id, str) or not materialization_id:
            raise ValueError("input materialization IDs must be non-empty strings")
        if dependency_name in seen_dependencies:
            raise ValueError("input dependency names must be unique")
        seen_dependencies.add(dependency_name)
        normalized.append((dependency_name, materialization_id))
    return tuple(normalized)


def _validate_partition_presence(
    partition_key_present: bool, partition_key: object | None
) -> None:
    if partition_key_present != (partition_key is not None):
        raise ValueError(
            "partition_key_present and partition_key must agree about presence"
        )


@dataclass(frozen=True, slots=True)
class MaterializationContext:
    """Identity used to resolve or commit one asset partition output."""

    asset_name: str
    version: str
    partition_domain: str
    partition_key_present: bool
    partition_key: object | None
    input_materialization_ids: MaterializationInputs = ()

    def __post_init__(self) -> None:
        inputs = _validate_identity_fields(
            asset_name=self.asset_name,
            version=self.version,
            partition_domain=self.partition_domain,
            partition_key_present=self.partition_key_present,
            input_materialization_ids=self.input_materialization_ids,
        )
        _validate_partition_presence(self.partition_key_present, self.partition_key)
        object.__setattr__(self, "input_materialization_ids", inputs)


@dataclass(frozen=True, slots=True)
class MaterializationRecord:
    """Opaque adapter record describing a possibly committed output."""

    materialization_id: str
    asset_name: str
    version: str
    partition_domain: str
    partition_key_present: bool
    partition_key: object | None
    input_materialization_ids: MaterializationInputs = ()
    committed: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.materialization_id, str) or not self.materialization_id:
            raise ValueError("materialization_id must be a non-empty string")
        inputs = _validate_identity_fields(
            asset_name=self.asset_name,
            version=self.version,
            partition_domain=self.partition_domain,
            partition_key_present=self.partition_key_present,
            input_materialization_ids=self.input_materialization_ids,
        )
        _validate_partition_presence(self.partition_key_present, self.partition_key)
        object.__setattr__(self, "input_materialization_ids", inputs)
        if not isinstance(self.committed, bool):
            raise ValueError("committed must be a bool")


class Materialization(Protocol):
    """User supplied storage adapter for materialized asset outputs."""

    def resolve(
        self, context: MaterializationContext
    ) -> MaterializationRecord | None | Awaitable[MaterializationRecord | None]:
        """Return a candidate record for this identity, if one exists."""
        ...

    def load(self, record: MaterializationRecord) -> Any | Awaitable[Any]:
        """Load the output represented by a compatible committed record."""
        ...

    def commit(
        self, context: MaterializationContext, output: Any
    ) -> MaterializationRecord | Awaitable[MaterializationRecord]:
        """Atomically publish output and return its committed identity."""
        ...


def record_matches_context(record: object, context: MaterializationContext) -> bool:
    """Return whether ``record`` is a valid committed record for ``context``.

    Invalid or uncommitted records are ordinary cache misses.  The defensive
    checks also handle adapter objects that bypassed dataclass construction.
    """

    if not isinstance(record, MaterializationRecord) or not isinstance(
        context, MaterializationContext
    ):
        return False
    try:
        if (
            not isinstance(record.materialization_id, str)
            or not record.materialization_id
        ):
            return False
        if record.committed is not True:
            return False
        _validate_identity_fields(
            asset_name=record.asset_name,
            version=record.version,
            partition_domain=record.partition_domain,
            partition_key_present=record.partition_key_present,
            input_materialization_ids=record.input_materialization_ids,
        )
        _validate_partition_presence(record.partition_key_present, record.partition_key)
        _validate_identity_fields(
            asset_name=context.asset_name,
            version=context.version,
            partition_domain=context.partition_domain,
            partition_key_present=context.partition_key_present,
            input_materialization_ids=context.input_materialization_ids,
        )
        _validate_partition_presence(
            context.partition_key_present, context.partition_key
        )
        return (
            record.asset_name == context.asset_name
            and record.version == context.version
            and record.partition_domain == context.partition_domain
            and record.partition_key_present == context.partition_key_present
            and type(record.partition_key) is type(context.partition_key)
            and record.partition_key == context.partition_key
            and record.input_materialization_ids == context.input_materialization_ids
        )
    except (AttributeError, TypeError, ValueError):
        return False


async def call_materialization(operation: Callable[..., _T], *args: Any) -> _T:
    """Call a sync or async adapter operation without blocking the event loop."""

    if not callable(operation):
        raise TypeError("materialization operation must be callable")
    loop = asyncio.get_running_loop()
    is_async = inspect.iscoroutinefunction(operation) or inspect.iscoroutinefunction(
        getattr(operation, "__call__", None)
    )
    if is_async:
        result = operation(*args)
    else:
        result = await loop.run_in_executor(None, functools.partial(operation, *args))
    if inspect.isawaitable(result):
        return await result
    return result
