"""Validation tests for immutable resource statistics values."""

from dataclasses import FrozenInstanceError, fields
from datetime import timedelta
import inspect

import pytest

from kazeflow.resource_statistics import ResourceStatistics


def test_resource_statistics_has_exact_frozen_slots_fields_and_defaults() -> None:
    assert [field.name for field in fields(ResourceStatistics)] == [
        "name",
        "invocations",
        "succeeded",
        "failed",
        "retries",
        "wait_count",
        "wait_duration",
        "invocation_duration",
    ]
    signature = inspect.signature(ResourceStatistics)
    assert signature.parameters["name"].default is inspect.Parameter.empty
    for name in ("invocations", "succeeded", "failed", "retries", "wait_count"):
        assert signature.parameters[name].default == 0
    assert signature.parameters["wait_duration"].default == timedelta()
    assert signature.parameters["invocation_duration"].default == timedelta()

    stats = ResourceStatistics("db")
    assert getattr(ResourceStatistics, "__slots__") == (
        "name",
        "invocations",
        "succeeded",
        "failed",
        "retries",
        "wait_count",
        "wait_duration",
        "invocation_duration",
    )
    with pytest.raises(FrozenInstanceError):
        stats.invocations = 1  # type: ignore[misc]


@pytest.mark.parametrize("name", ["", None, 1])
def test_resource_statistics_requires_non_empty_string_name(name) -> None:
    with pytest.raises(ValueError, match="name"):
        ResourceStatistics(name)


@pytest.mark.parametrize(
    "field,value",
    [
        (field, value)
        for field in ("invocations", "succeeded", "failed", "retries", "wait_count")
        for value in (-1, True, 1.5, "1")
    ],
)
def test_resource_statistics_rejects_invalid_counts(field, value) -> None:
    with pytest.raises(ValueError, match=field):
        ResourceStatistics("db", **{field: value})


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"invocations": 1}, "equal invocations"),
        ({"invocations": 2, "succeeded": 1}, "equal invocations"),
        ({"retries": 1}, "retries"),
        ({"invocations": 1, "succeeded": 1, "retries": 2}, "retries"),
    ],
)
def test_resource_statistics_enforces_operation_invariants(kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        ResourceStatistics("db", **kwargs)


@pytest.mark.parametrize("field", ["wait_duration", "invocation_duration"])
@pytest.mark.parametrize("duration", [-timedelta(seconds=1), 1, "1", None])
def test_resource_statistics_rejects_invalid_durations(field, duration) -> None:
    with pytest.raises(ValueError, match=field):
        ResourceStatistics("db", **{field: duration})


@pytest.mark.parametrize(
    "kwargs,message",
    [
        ({"wait_duration": timedelta(seconds=1)}, "wait_duration"),
        ({"invocation_duration": timedelta(seconds=1)}, "invocation_duration"),
        (
            {"wait_count": 1, "wait_duration": timedelta(seconds=1)},
            None,
        ),
        (
            {
                "invocations": 1,
                "succeeded": 1,
                "invocation_duration": timedelta(seconds=1),
            },
            None,
        ),
    ],
)
def test_resource_statistics_requires_zero_duration_when_count_is_zero(
    kwargs, message
) -> None:
    if message is None:
        assert ResourceStatistics("db", **kwargs)
    else:
        with pytest.raises(ValueError, match=message):
            ResourceStatistics("db", **kwargs)
