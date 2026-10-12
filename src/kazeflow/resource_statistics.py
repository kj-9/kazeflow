"""Immutable run-local summaries for named resource execution."""

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class ResourceStatistics:
    """Counts and elapsed durations for one named resource in a single run."""

    name: str
    invocations: int = 0
    succeeded: int = 0
    failed: int = 0
    retries: int = 0
    wait_count: int = 0
    wait_duration: timedelta = timedelta()
    invocation_duration: timedelta = timedelta()

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("name must be a non-empty string")

        counts = (
            ("invocations", self.invocations),
            ("succeeded", self.succeeded),
            ("failed", self.failed),
            ("retries", self.retries),
            ("wait_count", self.wait_count),
        )
        for name, value in counts:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")

        if self.succeeded + self.failed != self.invocations:
            raise ValueError("succeeded + failed must equal invocations")
        if self.retries > self.invocations:
            raise ValueError("retries must not exceed invocations")

        durations = (
            ("wait_duration", self.wait_duration, self.wait_count),
            ("invocation_duration", self.invocation_duration, self.invocations),
        )
        for name, value, count in durations:
            if not isinstance(value, timedelta) or value < timedelta(0):
                raise ValueError(f"{name} must be a non-negative timedelta")
            if count == 0 and value != timedelta(0):
                count_name = "wait_count" if name == "wait_duration" else "invocations"
                raise ValueError(f"{name} must be zero when {count_name} is zero")
