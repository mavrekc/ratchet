"""Dead-letter queue: failed steps parked with event-slice context, inspectable and requeueable."""

import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from redis import Redis
from redis.exceptions import ConnectionError, TimeoutError
from redis.typing import EncodableT, FieldT

from ratchet.broker import Broker
from ratchet.errors import BrokerError
from ratchet.eventlog import EventLog
from ratchet.events import Event, EventType

DEFAULT_DLQ_STREAM = "ratchet:dlq"

logger = logging.getLogger("ratchet.dlq")


@dataclass(frozen=True)
class DeadLetterEntry:
    """A dead-lettered step: the failure context and enough to inspect or requeue it."""

    id: str
    session_id: str
    step_id: str
    tool: str
    error: str
    error_type: str
    attempt: int
    events: list[Event]
    original: dict[str, str]


class DeadLetterQueue:
    """Plain Redis stream (no consumer group) holding failed steps for review."""

    def __init__(self, redis: Redis, stream: str = DEFAULT_DLQ_STREAM) -> None:
        if not redis.get_connection_kwargs().get("decode_responses"):
            raise ValueError("DeadLetterQueue requires a Redis client with decode_responses=True")
        self._redis = redis
        self._stream = stream

    @property
    def stream_key(self) -> str:
        return self._stream

    def push(
        self,
        *,
        session_id: str,
        step_id: str,
        tool: str,
        error: str,
        error_type: str,
        attempt: int,
        events: Sequence[Event],
        original: Mapping[str, str],
    ) -> str:
        fields: dict[str, str] = {
            "session_id": session_id,
            "step_id": step_id,
            "tool": tool,
            "error": error,
            "error_type": error_type,
            "attempt": str(attempt),
            "events": json.dumps(
                [e.model_dump(mode="json") for e in events], separators=(",", ":")
            ),
            "original": json.dumps(dict(original), separators=(",", ":")),
        }
        xadd_fields = cast(dict[FieldT, EncodableT], fields)
        try:
            entry_id = self._redis.xadd(self._stream, xadd_fields)
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e
        return str(entry_id)

    def entries(self, count: int = 100) -> list[DeadLetterEntry]:
        try:
            raw = self._redis.xrange(self._stream, count=count)
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e
        parsed: list[DeadLetterEntry] = []
        for entry_id, fields in raw or []:
            # One malformed record must not block listing; it stays in the stream.
            try:
                parsed.append(self._parse_entry(entry_id, fields))
            except ValueError as e:
                logger.warning("skipping malformed dead-letter entry: %s", e)
        return parsed

    def requeue(self, entry_id: str, broker: Broker) -> str:
        try:
            raw = self._redis.xrange(self._stream, entry_id, entry_id)
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e
        if not raw:
            raise ValueError(f"no dead-letter entry with id {entry_id!r}")
        found_id, fields = raw[0]
        entry = self._parse_entry(found_id, fields)
        # The executor acks terminal sessions without running anything, so a requeue
        # would only destroy the failure record; refuse it (retry policies land in R3).
        session_events = EventLog(self._redis, entry.session_id).read()
        if any(e.type in (EventType.TASK_DONE, EventType.STEP_FAILED) for e in session_events):
            raise ValueError(
                f"session {entry.session_id!r} is already terminal; requeue would change "
                f"nothing, dead-letter entry {entry_id!r} kept"
            )
        new_id = broker.publish(entry.original)
        try:
            self._redis.xdel(self._stream, entry_id)
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e
        return new_id

    def _parse_entry(
        self, entry_id: object, fields: Mapping[bytes | str, bytes | str] | None
    ) -> DeadLetterEntry:
        raw_id = str(entry_id)
        str_fields = cast(dict[str, str], fields)
        try:
            events_raw = json.loads(str_fields["events"])
            events = [Event.model_validate(item) for item in events_raw]
            original = cast(dict[str, str], json.loads(str_fields["original"]))
            return DeadLetterEntry(
                id=raw_id,
                session_id=str_fields["session_id"],
                step_id=str_fields["step_id"],
                tool=str_fields["tool"],
                error=str_fields["error"],
                error_type=str_fields["error_type"],
                attempt=int(str_fields["attempt"]),
                events=events,
                original=original,
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"malformed dead-letter entry {raw_id!r}: {e}") from e
