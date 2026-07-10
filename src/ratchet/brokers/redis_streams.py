"""Redis Streams consumer-group adapter: the concrete Broker implementation."""

from collections.abc import Mapping
from typing import cast

from redis import Redis
from redis.exceptions import ConnectionError, ResponseError, TimeoutError
from redis.typing import EncodableT, FieldT, XReadGroupResponse

from ratchet.broker import Message
from ratchet.errors import BrokerError

DEFAULT_STEP_STREAM = "ratchet:steps"
DEFAULT_WORKER_GROUP = "ratchet:workers"

# Shape XREADGROUP actually returns for a single stream, decoded to str.
_RawStreamReply = list[tuple[str, list[tuple[str, dict[str, str]]]]]


def _parse_stream_response(response: XReadGroupResponse) -> list[Message]:
    if not response:
        return []
    entries = cast(_RawStreamReply, response)
    messages: list[Message] = []
    for _stream_name, records in entries:
        for entry_id, fields in records:
            str_fields = {str(k): str(v) for k, v in fields.items()}
            messages.append(Message(id=str(entry_id), fields=str_fields))
    return messages


class RedisStreamsBroker:
    """Broker adapter over a single Redis stream and one consumer group."""

    def __init__(
        self,
        redis: Redis,
        stream: str = DEFAULT_STEP_STREAM,
        group: str = DEFAULT_WORKER_GROUP,
    ) -> None:
        self._redis = redis
        self._stream = stream
        self._group = group

    def ensure_group(self) -> None:
        try:
            self._redis.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except ResponseError as e:
            if not str(e).startswith("BUSYGROUP"):
                raise
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e

    def publish(self, fields: Mapping[str, str]) -> str:
        xadd_fields = cast(dict[FieldT, EncodableT], dict(fields))
        try:
            message_id = self._redis.xadd(self._stream, xadd_fields)
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e
        return str(message_id)

    def consume(self, consumer: str, count: int = 10, block_ms: int = 5000) -> list[Message]:
        try:
            response = self._redis.xreadgroup(
                groupname=self._group,
                consumername=consumer,
                streams={self._stream: ">"},
                count=count,
                block=block_ms,
            )
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e
        return _parse_stream_response(response)

    def ack(self, message_id: str) -> int:
        try:
            return self._redis.xack(self._stream, self._group, message_id)
        except (ConnectionError, TimeoutError) as e:
            raise BrokerError(f"broker unreachable: {e}") from e
