from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
from typing import Hashable, Sequence, TypeAlias

PartitionKey: TypeAlias = Hashable
PartitionKeys: TypeAlias = Sequence[PartitionKey]


@dataclass(frozen=True)
class PartitionDef(ABC):
    @abstractmethod
    def range(self, start, end) -> PartitionKeys:
        raise NotImplementedError


class DatePartitionDef(PartitionDef):
    """Represents a definition for a date-based partition."""

    def range(self, start_date: str, end_date: str) -> list[datetime.date]:
        """Creates a configuration object representing a date range."""
        start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        delta = end - start
        return [start + datetime.timedelta(days=i) for i in range(delta.days + 1)]
