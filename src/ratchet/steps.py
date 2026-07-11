"""Stub step registry: no-LLM tools the R1 executor can call by name."""

import random
import time
from collections.abc import Callable, Mapping

from ratchet.errors import FlakyStepError
from ratchet.events import JsonValue

type StepFn = Callable[[Mapping[str, JsonValue]], JsonValue]


def _numeric_arg(args: Mapping[str, JsonValue], key: str, default: float) -> float:
    value = args.get(key)
    if value is None:
        return default
    if isinstance(value, int | float | str):
        return float(value)
    raise ValueError(f"{key} must be numeric, got {type(value).__name__}")


def echo(args: Mapping[str, JsonValue]) -> JsonValue:
    """Return the args unchanged, as a plain dict."""
    return dict(args)


def sleep(args: Mapping[str, JsonValue]) -> JsonValue:
    """Sleep for args['seconds'] (default 0) and return None."""
    time.sleep(_numeric_arg(args, "seconds", 0.0))
    return None


def flaky(args: Mapping[str, JsonValue]) -> JsonValue:
    """Raise FlakyStepError with probability args['fail_rate'] (default 0.5)."""
    fail_rate = _numeric_arg(args, "fail_rate", 0.5)
    if random.random() < fail_rate:
        raise FlakyStepError(f"injected failure at fail_rate={fail_rate}")
    return {"attempted": True}


STEP_REGISTRY: Mapping[str, StepFn] = {"echo": echo, "sleep": sleep, "flaky": flaky}
