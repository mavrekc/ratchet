import pytest

from ratchet.errors import FlakyStepError
from ratchet.steps import STEP_REGISTRY, echo, flaky, sleep


def test_echo_returns_exactly_its_args() -> None:
    args = {"a": 1, "b": [2, {"c": "x"}]}
    assert echo(args) == args


def test_sleep_returns_none() -> None:
    assert sleep({"seconds": 0}) is None


def test_flaky_raises_at_fail_rate_one() -> None:
    with pytest.raises(FlakyStepError):
        flaky({"fail_rate": 1.0})


def test_flaky_succeeds_at_fail_rate_zero() -> None:
    assert flaky({"fail_rate": 0.0}) == {"attempted": True}


def test_step_registry_has_exactly_three_names() -> None:
    assert set(STEP_REGISTRY) == {"echo", "sleep", "flaky"}
