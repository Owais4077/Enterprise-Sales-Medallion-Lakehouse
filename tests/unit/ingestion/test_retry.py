import pytest

from edp.ingestion.retry import RetryPolicy, call_with_retry


class Flaky:
    """Fails ``failures`` times with ``error``, then returns 'ok'."""

    def __init__(self, failures: int, error: Exception | None = None) -> None:
        self.failures, self.error, self.calls = failures, error or ConnectionError("boom"), 0

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return "ok"


POLICY = RetryPolicy(max_attempts=4, base_delay=1.0, max_delay=5.0, jitter=0.0)


def test_succeeds_after_transient_failures_with_exponential_backoff():
    sleeps: list[float] = []
    func = Flaky(failures=3)
    assert call_with_retry(func, POLICY, retry_on=(ConnectionError,), sleep=sleeps.append) == "ok"
    assert func.calls == 4
    assert sleeps == [1.0, 2.0, 4.0]


def test_delay_is_capped_at_max_delay():
    policy = RetryPolicy(max_attempts=6, base_delay=1.0, max_delay=3.0, jitter=0.0)
    assert [policy.delay_for(n) for n in range(1, 6)] == [1.0, 2.0, 3.0, 3.0, 3.0]


def test_gives_up_and_reraises_the_original_error():
    sleeps: list[float] = []
    func = Flaky(failures=99, error=ConnectionError("still down"))
    with pytest.raises(ConnectionError, match="still down"):
        call_with_retry(func, POLICY, retry_on=(ConnectionError,), sleep=sleeps.append)
    assert func.calls == POLICY.max_attempts
    assert len(sleeps) == POLICY.max_attempts - 1  # no sleep after the final failure


def test_non_retryable_error_is_raised_immediately():
    func = Flaky(failures=1, error=ValueError("bug"))
    with pytest.raises(ValueError):
        call_with_retry(func, POLICY, retry_on=(ConnectionError,), sleep=lambda s: None)
    assert func.calls == 1


def test_server_retry_after_hint_lengthens_wait_but_respects_cap():
    sleeps: list[float] = []
    call_with_retry(
        Flaky(failures=1),
        POLICY,
        retry_on=(ConnectionError,),
        sleep=sleeps.append,
        retry_after=lambda exc: 4.0,
    )
    assert sleeps == [4.0]
    sleeps.clear()
    call_with_retry(
        Flaky(failures=1),
        POLICY,
        retry_on=(ConnectionError,),
        sleep=sleeps.append,
        retry_after=lambda exc: 999.0,
    )
    assert sleeps == [POLICY.max_delay]


def test_jitter_stays_within_bounds():
    policy = RetryPolicy(max_attempts=3, base_delay=10.0, max_delay=100.0, jitter=0.2)
    delays = {policy.delay_for(1) for _ in range(200)}
    assert len(delays) > 1  # actually random
    assert all(8.0 <= d <= 12.0 for d in delays)


@pytest.mark.parametrize(
    "kwargs",
    [{"max_attempts": 0}, {"base_delay": 5, "max_delay": 1}, {"jitter": 1.0}, {"base_delay": -1}],
)
def test_invalid_policy_is_rejected(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)
