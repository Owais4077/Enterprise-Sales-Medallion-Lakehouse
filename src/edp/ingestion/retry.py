"""Retry with exponential backoff and jitter.

Why backoff: if a service is struggling, hammering it every second makes things worse.
Why jitter: if 50 workers fail together and all retry after exactly 2 s, they fail together
again; random spread breaks that lock-step ("thundering herd").
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 30.0
    jitter: float = 0.2  # fraction of the delay, applied as +/-

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < self.base_delay:
            raise ValueError("Require 0 <= base_delay <= max_delay")
        if not 0 <= self.jitter < 1:
            raise ValueError("jitter must be in [0, 1)")

    def delay_for(self, attempt: int) -> float:
        """Seconds to wait after failed attempt number ``attempt`` (1-based)."""
        delay = min(self.max_delay, self.base_delay * 2 ** (attempt - 1))
        spread = delay * self.jitter
        return max(0.0, delay + random.uniform(-spread, spread))  # noqa: S311  (not security)


def call_with_retry(
    func: Callable[[], T],
    policy: RetryPolicy,
    *,
    retry_on: tuple[type[BaseException], ...],
    description: str = "operation",
    sleep: Callable[[float], None] = time.sleep,
    retry_after: Callable[[BaseException], float | None] | None = None,
) -> T:
    """Call ``func``; on an exception in ``retry_on`` wait and try again.

    Anything not in ``retry_on`` propagates immediately (retrying a 401 or a bug is pointless).
    After the last attempt the original exception is re-raised, never swallowed.
    ``retry_after`` lets a server's own hint (HTTP Retry-After) lengthen the wait, capped at
    ``max_delay``.
    """
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return func()
        except retry_on as exc:
            if attempt == policy.max_attempts:
                logger.error("%s failed after %d attempts: %s", description, attempt, exc)
                raise
            delay = policy.delay_for(attempt)
            hint = retry_after(exc) if retry_after else None
            if hint:
                delay = min(max(delay, hint), policy.max_delay)
            logger.warning(
                "%s failed (attempt %d/%d): %s. Retrying in %.1fs",
                description,
                attempt,
                policy.max_attempts,
                exc,
                delay,
            )
            sleep(delay)
    raise AssertionError("unreachable")  # loop always returns or raises
