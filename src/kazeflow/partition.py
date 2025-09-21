from dataclasses import dataclass
import datetime
from typing import Sequence


@dataclass(frozen=True)
class PartitionKey:
    def range(self, start, end) -> Sequence:
        raise NotImplementedError


class DatePartitionKey(PartitionKey):
    """Represents a definition for a date-based partition."""

    def __init__(self, key: str):
        self.key = key

    def range(self, start_date: str, end_date: str) -> list[datetime.date]:
        """Creates a configuration object representing a date range."""
        start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
        delta = end - start
        return [start + datetime.timedelta(days=i) for i in range(delta.days + 1)]
