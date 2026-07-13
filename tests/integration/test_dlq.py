"""Integration tests for DeadLetterQueue against a real Redis."""

import uuid

import pytest
from redis import Redis

from ratchet.dlq import DeadLetterQueue
from ratchet.errors import BrokerError
from ratchet.events import EventType, link


def _unique_stream() -> str:
    return f"test:dlq:{uuid.uuid4().hex}"


def _session_id() -> str:
    return f"test-session-{uuid.uuid4().hex}"


@pytest.mark.integration
def test_push_and_entries_round_trip(redis_client: Redis) -> None:
    dlq = DeadLetterQueue(redis_client, stream=_unique_stream())
    session_id = _session_id()
    first = link(None, session_id, EventType.TASK_STARTED, {"goal": "demo"})
    second = link(first, session_id, EventType.STEP_FAILED, {"error": "boom"})
    original = {
        "session_id": session_id,
        "step_id": "step-1",
        "tool": "echo",
        "args": '{"a": 1}',
        "idempotency_key": "idem-1",
        "attempt": "1",
    }

    entry_id = dlq.push(
        session_id=session_id,
        step_id="step-1",
        tool="echo",
        error="boom",
        error_type="RuntimeError",
        attempt=1,
        events=[first, second],
        original=original,
    )
    entries = dlq.entries()

    assert len(entries) == 1
    entry = entries[0]
    assert entry.id == entry_id
    assert entry.session_id == session_id
    assert entry.step_id == "step-1"
    assert entry.tool == "echo"
    assert entry.error == "boom"
    assert entry.error_type == "RuntimeError"
    assert entry.attempt == 1
    assert entry.events == [first, second]
    assert [e.hash for e in entry.events] == [first.hash, second.hash]
    assert entry.original == original


@pytest.mark.integration
def test_entries_multiple_in_order(redis_client: Redis) -> None:
    dlq = DeadLetterQueue(redis_client, stream=_unique_stream())
    session_id = _session_id()
    event = link(None, session_id, EventType.TASK_STARTED, {})

    first_id = dlq.push(
        session_id=session_id,
        step_id="s1",
        tool="echo",
        error="e1",
        error_type="RuntimeError",
        attempt=1,
        events=[event],
        original={"step_id": "s1"},
    )
    second_id = dlq.push(
        session_id=session_id,
        step_id="s2",
        tool="sleep",
        error="e2",
        error_type="TimeoutError",
        attempt=2,
        events=[event],
        original={"step_id": "s2"},
    )

    entries = dlq.entries()

    assert [e.id for e in entries] == [first_id, second_id]
    assert [e.step_id for e in entries] == ["s1", "s2"]


@pytest.mark.integration
def test_requeue_publishes_original_and_deletes(redis_client: Redis) -> None:
    from ratchet.brokers import RedisStreamsBroker

    dlq = DeadLetterQueue(redis_client, stream=_unique_stream())
    stream, group = f"test:steps:{uuid.uuid4().hex}", f"test:workers:{uuid.uuid4().hex}"
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()

    session_id = _session_id()
    event = link(None, session_id, EventType.TASK_STARTED, {})
    original = {
        "session_id": session_id,
        "step_id": "step-1",
        "tool": "echo",
        "args": "{}",
        "idempotency_key": "idem-1",
        "attempt": "1",
    }
    entry_id = dlq.push(
        session_id=session_id,
        step_id="step-1",
        tool="echo",
        error="boom",
        error_type="RuntimeError",
        attempt=1,
        events=[event],
        original=original,
    )

    new_id = dlq.requeue(entry_id, broker)

    messages = broker.consume("c1", block_ms=1000)
    assert len(messages) == 1
    assert messages[0].id == new_id
    assert messages[0].fields == original
    assert dlq.entries() == []


@pytest.mark.integration
def test_requeue_missing_entry_raises(redis_client: Redis) -> None:
    from ratchet.brokers import RedisStreamsBroker

    dlq = DeadLetterQueue(redis_client, stream=_unique_stream())
    stream, group = f"test:steps:{uuid.uuid4().hex}", f"test:workers:{uuid.uuid4().hex}"
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)

    with pytest.raises(ValueError, match="no dead-letter entry"):
        dlq.requeue("0-1", broker)


@pytest.mark.integration
def test_entries_skips_malformed_and_keeps_them(redis_client: Redis) -> None:
    dlq_stream = _unique_stream()
    dlq = DeadLetterQueue(redis_client, stream=dlq_stream)
    redis_client.xadd(dlq_stream, {"garbage": "yes"})
    good_id = dlq.push(
        session_id=_session_id(),
        step_id="s1",
        tool="echo",
        error="boom",
        error_type="RuntimeError",
        attempt=0,
        events=[],
        original={"session_id": "x", "plan": "[]"},
    )

    listed = dlq.entries()
    assert [e.id for e in listed] == [good_id]
    assert redis_client.xlen(dlq_stream) == 2


@pytest.mark.integration
def test_requeue_malformed_entry_raises(redis_client: Redis) -> None:
    from ratchet.brokers import RedisStreamsBroker

    dlq_stream = _unique_stream()
    dlq = DeadLetterQueue(redis_client, stream=dlq_stream)
    entry_id = redis_client.xadd(dlq_stream, {"garbage": "yes"})
    broker = RedisStreamsBroker(redis_client, stream=f"test:steps:{uuid.uuid4().hex}")

    with pytest.raises(ValueError, match=entry_id):
        dlq.requeue(entry_id, broker)


@pytest.mark.integration
def test_push_connection_failure_raises_broker_error() -> None:
    bad_client: Redis = Redis.from_url(
        "redis://localhost:6399/0",
        decode_responses=True,
        socket_connect_timeout=0.2,
        socket_timeout=0.2,
    )
    dlq = DeadLetterQueue(bad_client)
    event = link(None, _session_id(), EventType.TASK_STARTED, {})

    try:
        with pytest.raises(BrokerError):
            dlq.push(
                session_id="session-x",
                step_id="s1",
                tool="echo",
                error="e",
                error_type="RuntimeError",
                attempt=1,
                events=[event],
                original={},
            )
    finally:
        bad_client.close()


def test_dlq_rejects_bytes_client() -> None:
    client: Redis = Redis.from_url("redis://localhost:6399/0")
    try:
        with pytest.raises(ValueError, match="decode_responses"):
            DeadLetterQueue(client)
    finally:
        client.close()
