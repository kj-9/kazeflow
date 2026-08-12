from datetime import date, datetime

import pytest

from kazeflow.partition import DatePartitionDef, PartitionDef


class ExistingDefinition(PartitionDef):
    def range(self, start, end):
        return (start, end)


def test_existing_partition_definition_keeps_identity_normalization_and_metadata() -> (
    None
):
    definition = ExistingDefinition()

    assert definition.normalize_key(0) == 0
    assert definition.domain.endswith("ExistingDefinition")
    assert definition.key_format == "custom"
    assert definition.supports_range is False


@pytest.mark.parametrize(
    "value",
    [
        "2026-2-03",
        "2026-02-3",
        "2026-02-30",
        "2026-02-03T00:00:00",
        datetime(2026, 2, 3),
        0,
    ],
)
def test_date_partition_definition_rejects_noncanonical_keys(value: object) -> None:
    with pytest.raises(ValueError, match="canonical YYYY-MM-DD"):
        DatePartitionDef().normalize_key(value)


def test_date_partition_definition_normalizes_and_expands_inclusively() -> None:
    definition = DatePartitionDef()

    assert definition.domain == "date"
    assert definition.key_format == "YYYY-MM-DD"
    assert definition.supports_range is True
    assert definition.normalize_key("2026-02-03") == date(2026, 2, 3)
    assert definition.normalize_key(date(2026, 2, 3)) == date(2026, 2, 3)
    assert definition.range("2026-02-03", date(2026, 2, 5)) == [
        date(2026, 2, 3),
        date(2026, 2, 4),
        date(2026, 2, 5),
    ]


def test_date_partition_definition_rejects_reversed_range() -> None:
    with pytest.raises(ValueError, match="must not be later"):
        DatePartitionDef().range("2026-02-05", "2026-02-03")
