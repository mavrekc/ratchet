"""Typed exception hierarchy for ratchet."""


class RatchetError(Exception):
    """Base class for all ratchet errors."""


class ChainError(RatchetError):
    """Base class for event-log hash chain errors."""


class ChainVerificationError(ChainError):
    """Full-chain verification failed at a specific event index."""

    def __init__(self, index: int, reason: str) -> None:
        super().__init__(f"chain invalid at event {index}: {reason}")
        self.index = index
        self.reason = reason


class ChainForkError(ChainError):
    """Append rejected because the log tail no longer matches the expected prev_hash."""


class BrokerError(RatchetError):
    """Broker connectivity or protocol failure."""


class StepError(RatchetError):
    """A step implementation failed."""


class FlakyStepError(StepError):
    """Injected failure raised by the flaky stub step."""


class UnknownStepError(StepError):
    """Step message referenced a tool that is not in the registry."""
