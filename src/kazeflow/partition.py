import datetime
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Hashable, Sequence, TypeAlias

PartitionKey: TypeAlias = Hashable
PartitionKeys: TypeAlias = Sequence[PartitionKey]


@dataclass(frozen=True)
class PartitionDef(ABC):
    """Defines one compatible domain of partition keys.

    Existing definitions that only implement :meth:`range` retain identity key
    normalization.  Definitions that need a stronger contract can override the
    metadata and :meth:`normalize_key` methods without changing the executor.
    """

    @property
    def kind(self) -> str:
        """A human-readable definition kind for inspection output."""
        return type(self).__name__

    @property
    def domain(self) -> str:
        """A stable identifier for keys that may share one selection."""
        definition_type = type(self)
        return f"{definition_type.__module__}.{definition_type.__qualname__}"

    @property
    def key_format(self) -> str:
        """Describe the accepted key form without enumerating keys."""
        return "custom"

    @property
    def supports_range(self) -> bool:
        """Whether this definition accepts explicit bounded range selection."""
        return False

    def normalize_key(self, key: object) -> PartitionKey:
        """Return the canonical in-memory representation of one selected key."""
        return key  # type: ignore[return-value]

    @abstractmethod
    def range(self, start, end) -> PartitionKeys:
        raise NotImplementedError


class DatePartitionDef(PartitionDef):
    """Represents a definition for a date-based partition."""

    @property
    def domain(self) -> str:
        return "date"

    @property
    def key_format(self) -> str:
        return "YYYY-MM-DD"

    @property
    def supports_range(self) -> bool:
        return True

    def normalize_key(self, key: object) -> datetime.date:
        """Normalize a canonical ISO date string or plain :class:`date`."""
        if isinstance(key, datetime.datetime):
            raise ValueError("date partition keys must be canonical YYYY-MM-DD dates")
        if isinstance(key, datetime.date):
            return key
        if isinstance(key, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", key):
            try:
                return datetime.date.fromisoformat(key)
            except ValueError as error:
                raise ValueError(
                    "date partition keys must be canonical YYYY-MM-DD dates"
                ) from error
        raise ValueError("date partition keys must be canonical YYYY-MM-DD dates")

    def range(
        self, start_date: str | datetime.date, end_date: str | datetime.date
    ) -> list[datetime.date]:
        """Expand an explicit inclusive bounded date range."""
        start = self.normalize_key(start_date)
        end = self.normalize_key(end_date)
        if start > end:
            raise ValueError("date partition range start must not be later than end")
        delta = end - start
        return [start + datetime.timedelta(days=i) for i in range(delta.days + 1)]
