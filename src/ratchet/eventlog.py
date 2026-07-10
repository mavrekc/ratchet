"""Hash-chained, append-only session event log backed by one Redis stream."""

from collections.abc import Mapping
from datetime import datetime
from typing import cast

from redis import Redis
from redis.exceptions import ConnectionError, ResponseError, TimeoutError

from ratchet.errors import BrokerError, ChainForkError
from ratchet.events import (
    GENESIS_PREV_HASH,
    Event,
    EventType,
    JsonValue,
    link,
    verify_chain,
)

DEFAULT_LOG_PREFIX = "ratchet:log"

# Atomically compares the stream tail hash against the candidate's prev_hash
# before XADD, so two writers racing on the same tail cannot both succeed.
_APPEND_SCRIPT = """
local key = KEYS[1]
local prev_hash = ARGV[1]
local genesis = ARGV[2]
local data = ARGV[3]
local hash = ARGV[4]
local seq = ARGV[5]

local tail = redis.call('XREVRANGE', key, '+', '-', 'COUNT', 1)
if #tail == 0 then
    if prev_hash ~= genesis then
        return redis.error_reply('RATCHET_FORK: expected genesis prev_hash on empty log')
    end
else
    local fields = tail[1][2]
    local tail_hash = nil
    for i = 1, #fields, 2 do
        if fields[i] == 'hash' then
            tail_hash = fields[i + 1]
        end
    end
    if tail_hash ~= prev_hash then
        return redis.error_reply('RATCHET_FORK: tail hash does not match prev_hash')
    end
end

return redis.call('XADD', key, '*', 'data', data, 'hash', hash, 'seq', seq)
"""


class EventLog:
    """Append-only, hash-chained event log for a single session."""

    def __init__(self, redis: Redis, session_id: str, prefix: str = DEFAULT_LOG_PREFIX) -> None:
        self._redis = redis
        self._session_id = session_id
        self._prefix = prefix
        self._append_script = redis.register_script(_APPEND_SCRIPT)

    @property
    def stream_key(self) -> str:
        return f"{self._prefix}:{self._session_id}"

    def tail(self) -> Event | None:
        try:
            entries = self._redis.xrevrange(self.stream_key, count=1)
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e
        if not entries:
            return None
        _entry_id, fields = entries[0]
        data = cast(dict[str, str], fields)["data"]
        return Event.model_validate_json(data)

    def append(
        self,
        type: EventType,
        payload: Mapping[str, JsonValue],
        ts: datetime | None = None,
    ) -> Event:
        candidate = link(self.tail(), self._session_id, type, payload, ts)
        return self.append_event(candidate)

    def append_event(self, event: Event) -> Event:
        if event.session_id != self._session_id:
            raise ValueError(
                f"event.session_id {event.session_id!r} does not match "
                f"log session_id {self._session_id!r}"
            )
        args = [
            event.prev_hash,
            GENESIS_PREV_HASH,
            event.model_dump_json(),
            event.hash,
            str(event.seq),
        ]
        try:
            self._append_script(keys=[self.stream_key], args=args)
        except ResponseError as e:
            if "RATCHET_FORK" in str(e):
                raise ChainForkError(
                    f"fork detected for session {self._session_id!r}: "
                    f"expected prev_hash {event.prev_hash!r}"
                ) from e
            raise
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e
        return event

    def read(self) -> list[Event]:
        try:
            entries = self._redis.xrange(self.stream_key)
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e
        events: list[Event] = []
        for _entry_id, fields in entries or []:
            data = cast(dict[str, str], fields)["data"]
            events.append(Event.model_validate_json(data))
        return events

    def verify(self) -> None:
        verify_chain(self.read())
