"""Flow-scoped resource concurrency and invocation rate policies."""

from collections import deque
from dataclasses import dataclass
import math
from typing import Callable, TypeAlias


@dataclass(frozen=True)
class Fail:
    """Finish this logical attempt without another invocation."""


@dataclass(frozen=True)
class Retry:
    """Request another invocation after an explicit, run-local delay."""

    after_seconds: float = 0.0
    cooldown_resource: bool = False

    def __post_init__(self) -> None:
        value = self.after_seconds
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("retry after_seconds must be non-negative and finite")
        try:
            seconds = float(value)
        except OverflowError as error:
            raise ValueError(
                "retry after_seconds must be non-negative and finite"
            ) from error
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("retry after_seconds must be non-negative and finite")
        if type(self.cooldown_resource) is not bool:
            raise ValueError("retry cooldown_resource must be a bool")
        object.__setattr__(self, "after_seconds", seconds)


@dataclass(frozen=True)
class OpenCircuit:
    """Fail this attempt and stop pending body invocations on its resource."""


FailureAction: TypeAlias = Fail | Retry | OpenCircuit


@dataclass(frozen=True)
class RateLimit:
    """Maximum asset invocation starts in a rolling window within one run."""

    requests: int
    per_seconds: float

    def __post_init__(self) -> None:
        if (
            isinstance(self.requests, bool)
            or not isinstance(self.requests, int)
            or self.requests <= 0
        ):
            raise ValueError("rate limit requests must be a positive integer")
        duration = self.per_seconds
        if isinstance(duration, bool) or not isinstance(duration, (int, float)):
            raise ValueError("rate limit per_seconds must be positive and finite")
        try:
            seconds = float(duration)
        except OverflowError as error:
            raise ValueError(
                "rate limit per_seconds must be positive and finite"
            ) from error
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("rate limit per_seconds must be positive and finite")
        object.__setattr__(self, "per_seconds", seconds)


@dataclass(frozen=True)
class ResourcePolicy:
    """Maximum simultaneous asset invocations for one named resource."""

    max_concurrency: int
    rate_limit: RateLimit | None = None
    max_attempts: int = 1
    classify_failure: Callable[[Exception], FailureAction] | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_concurrency, bool)
            or not isinstance(self.max_concurrency, int)
            or self.max_concurrency <= 0
        ):
            raise ValueError("resource max_concurrency must be a positive integer")
        if self.rate_limit is not None:
            if not isinstance(self.rate_limit, RateLimit):
                raise TypeError("resource rate_limit must use RateLimit or None")
            # Revalidate even objects whose dataclass initializer was bypassed.
            RateLimit(self.rate_limit.requests, self.rate_limit.per_seconds)
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts <= 0
        ):
            raise ValueError("resource max_attempts must be a positive integer")
        if self.classify_failure is not None and not callable(self.classify_failure):
            raise TypeError("resource classify_failure must be callable or None")


class _RateWindow:
    """Run-local admission state. Launch reservations prevent scheduling races."""

    def __init__(self, limit: RateLimit) -> None:
        self.limit = limit
        self.starts: deque[float] = deque()
        self.reserved = 0

    def _expire(self, now: float) -> None:
        while self.starts and self.starts[0] + self.limit.per_seconds <= now:
            self.starts.popleft()

    def ready(self, now: float) -> bool:
        self._expire(now)
        return len(self.starts) + self.reserved < self.limit.requests

    def deadline(self, now: float) -> float | None:
        if self.ready(now) or not self.starts:
            return None
        return self.starts[0] + self.limit.per_seconds

    def reserve(self, now: float) -> None:
        if not self.ready(now):
            raise RuntimeError("resource rate permit is not available")
        self.reserved += 1

    def record_start(self, now: float) -> None:
        if self.reserved <= 0:
            raise RuntimeError("resource start requires a launch reservation")
        self.reserved -= 1
        self._expire(now)
        self.starts.append(now)
