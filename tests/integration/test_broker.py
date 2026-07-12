"""Integration tests for RedisStreamsBroker against a real Redis."""

import time
import uuid

import pytest
from redis import Redis

from ratchet.brokers import RedisStreamsBroker
from ratchet.errors import BrokerError


def _unique_names() -> tuple[str, str]:
    suffix = uuid.uuid4().hex
    return f"test:steps:{suffix}", f"test:workers:{suffix}"


@pytest.mark.integration
def test_publish_then_consume_delivers(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()

    message_id = broker.publish({"tool": "noop", "arg": "1"})
    messages = broker.consume("c1", block_ms=1000)

    assert len(messages) == 1
    assert messages[0].id == message_id
    assert messages[0].fields == {"tool": "noop", "arg": "1"}


@pytest.mark.integration
def test_consume_returns_empty_on_timeout(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()

    messages = broker.consume("c1", block_ms=100)

    assert messages == []


@pytest.mark.integration
def test_unacked_message_in_pending(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish({"tool": "noop"})

    messages = broker.consume("c1", block_ms=1000)
    assert len(messages) == 1

    pending = redis_client.xpending(stream, group)

    assert pending["pending"] == 1
    assert len(pending["consumers"]) == 1
    assert pending["consumers"][0]["name"] == "c1"


@pytest.mark.integration
def test_ack_removes_from_pending(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish({"tool": "noop"})

    messages = broker.consume("c1", block_ms=1000)
    assert len(messages) == 1

    acked = broker.ack(messages[0].id)

    assert acked == 1
    pending = redis_client.xpending(stream, group)
    assert pending["pending"] == 0


@pytest.mark.integration
def test_second_consumer_does_not_receive_delivered_message(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish({"tool": "noop"})

    first = broker.consume("c1", block_ms=1000)
    assert len(first) == 1

    second = broker.consume("c2", block_ms=100)

    assert second == []


@pytest.mark.integration
def test_ensure_group_idempotent(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)

    broker.ensure_group()
    broker.ensure_group()

    groups = redis_client.xinfo_groups(stream)
    assert len(groups) == 1


@pytest.mark.integration
def test_publish_before_group_creation_still_delivered(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)

    message_id = broker.publish({"tool": "noop"})
    broker.ensure_group()

    messages = broker.consume("c1", block_ms=1000)

    assert len(messages) == 1
    assert messages[0].id == message_id


def test_broker_rejects_bytes_client() -> None:
    stream, group = _unique_names()
    client: Redis = Redis.from_url("redis://localhost:6399/0")
    try:
        with pytest.raises(ValueError, match="decode_responses"):
            RedisStreamsBroker(client, stream=stream, group=group)
    finally:
        client.close()


@pytest.mark.integration
def test_connection_failure_raises_broker_error() -> None:
    stream, group = _unique_names()
    bad_client: Redis = Redis.from_url(
        "redis://localhost:6399/0",
        decode_responses=True,
        socket_connect_timeout=0.2,
        socket_timeout=0.2,
    )
    broker = RedisStreamsBroker(bad_client, stream=stream, group=group)

    try:
        with pytest.raises(BrokerError):
            broker.publish({"tool": "noop"})
    finally:
        bad_client.close()


@pytest.mark.integration
def test_claim_reclaims_idle_unacked_message(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish({"tool": "noop"})

    consumed = broker.consume("c1", block_ms=1000)
    assert len(consumed) == 1

    time.sleep(1.2)
    claimed = broker.claim("c2", min_idle_ms=1000)

    assert len(claimed) == 1
    assert claimed[0].id == consumed[0].id
    assert claimed[0].fields == consumed[0].fields

    pending = redis_client.xpending_range(stream, group, min="-", max="+", count=10)
    assert len(pending) == 1
    assert pending[0]["consumer"] == "c2"


@pytest.mark.integration
def test_claim_skips_fresh_entries(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish({"tool": "noop"})

    consumed = broker.consume("c1", block_ms=1000)
    assert len(consumed) == 1

    claimed = broker.claim("c2", min_idle_ms=5000)

    assert claimed == []


@pytest.mark.integration
def test_claim_empty_pel_returns_empty(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()

    claimed = broker.claim("c1", min_idle_ms=0)

    assert claimed == []


@pytest.mark.integration
def test_claim_then_ack_clears_pel(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish({"tool": "noop"})
    broker.consume("c1", block_ms=1000)

    time.sleep(1.2)
    claimed = broker.claim("c2", min_idle_ms=1000)
    assert len(claimed) == 1

    acked = broker.ack(claimed[0].id)
    assert acked == 1

    pending = redis_client.xpending(stream, group)
    assert pending["pending"] == 0


@pytest.mark.integration
def test_claim_resets_idle_time(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish({"tool": "noop"})
    broker.consume("c1", block_ms=1000)

    time.sleep(1.2)
    claimed = broker.claim("c2", min_idle_ms=1000)
    assert len(claimed) == 1

    reclaimed = broker.claim("c3", min_idle_ms=1000)

    assert reclaimed == []


@pytest.mark.integration
def test_claim_connection_failure_raises_broker_error() -> None:
    stream, group = _unique_names()
    bad_client: Redis = Redis.from_url(
        "redis://localhost:6399/0",
        decode_responses=True,
        socket_connect_timeout=0.2,
        socket_timeout=0.2,
    )
    broker = RedisStreamsBroker(bad_client, stream=stream, group=group)

    try:
        with pytest.raises(BrokerError):
            broker.claim("c1", min_idle_ms=1000)
    finally:
        bad_client.close()
