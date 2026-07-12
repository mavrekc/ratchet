"""Generic broker contract: thin, transport-agnostic message passing."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Message:
    """A single delivered broker message: an opaque id and string fields."""

    id: str
    fields: dict[str, str]


class Broker(Protocol):
    """Minimal contract a queue-native broker adapter must satisfy."""

    def ensure_group(self) -> None: ...
    def publish(self, fields: Mapping[str, str]) -> str: ...
    def consume(self, consumer: str, count: int = 10, block_ms: int = 5000) -> list[Message]: ...
    def ack(self, message_id: str) -> int: ...
    def claim(self, consumer: str, min_idle_ms: int, count: int = 10) -> list[Message]: ...
