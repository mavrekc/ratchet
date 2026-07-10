"""Integration tests for EventLog against a real Redis."""

import threading
import uuid

import pytest
from redis import Redis

from ratchet.errors import BrokerError, ChainForkError, ChainVerificationError
from ratchet.eventlog import EventLog
from ratchet.events import GENESIS_PREV_HASH, EventType, link


def _session_id() -> str:
    return f"test-session-{uuid.uuid4().hex}"


@pytest.mark.integration
def test_append_creates_chained_events(redis_client: Redis) -> None:
    log = EventLog(redis_client, _session_id())
    steps = [
        (EventType.TASK_STARTED, {"goal": "demo"}),
        (EventType.STEP_PLANNED, {"step": 1}),
        (EventType.TOOL_CALLED, {"tool": "search"}),
        (EventType.TOOL_RESULT, {"result": "ok"}),
        (EventType.TASK_DONE, {"status": "done"}),
    ]
    for event_type, payload in steps:
        log.append(event_type, payload)

    events = log.read()

    assert [e.seq for e in events] == [0, 1, 2, 3, 4]
    assert [e.type for e in events] == [t for t, _ in steps]
    assert [e.payload for e in events] == [p for _, p in steps]
    log.verify()


@pytest.mark.integration
def test_first_append_uses_genesis(redis_client: Redis) -> None:
    log = EventLog(redis_client, _session_id())

    event = log.append(EventType.TASK_STARTED, {"goal": "demo"})

    assert event.prev_hash == GENESIS_PREV_HASH
    assert event.seq == 0


@pytest.mark.integration
def test_tail_returns_none_on_empty(redis_client: Redis) -> None:
    log = EventLog(redis_client, _session_id())

    assert log.tail() is None


@pytest.mark.integration
def test_tail_returns_last(redis_client: Redis) -> None:
    log = EventLog(redis_client, _session_id())
    log.append(EventType.TASK_STARTED, {"goal": "demo"})
    second = log.append(EventType.STEP_PLANNED, {"step": 1})

    tail = log.tail()

    assert tail is not None
    assert tail.seq == second.seq
    assert tail.hash == second.hash


@pytest.mark.integration
def test_read_empty_returns_empty(redis_client: Redis) -> None:
    log = EventLog(redis_client, _session_id())

    assert log.read() == []


@pytest.mark.integration
def test_stale_append_event_raises_fork(redis_client: Redis) -> None:
    session_id = _session_id()
    log = EventLog(redis_client, session_id)
    log.append(EventType.TASK_STARTED, {"goal": "demo"})

    t = log.tail()
    a = link(t, session_id, EventType.STEP_PLANNED, {"step": "a"})
    b = link(t, session_id, EventType.STEP_PLANNED, {"step": "b"})

    log.append_event(a)
    with pytest.raises(ChainForkError):
        log.append_event(b)

    log.verify()
    assert len(log.read()) == 2


@pytest.mark.integration
def test_concurrent_appends_still_verify(redis_client: Redis) -> None:
    session_id = _session_id()
    log = EventLog(redis_client, session_id)
    errors: list[BaseException] = []

    def worker(label: str) -> None:
        try:
            for i in range(10):
                while True:
                    try:
                        log.append(EventType.STEP_PLANNED, {"worker": label, "i": i})
                        break
                    except ChainForkError:
                        continue
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(label,)) for label in ("w1", "w2")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    events = log.read()
    assert [e.seq for e in events] == list(range(20))
    log.verify()


@pytest.mark.integration
def test_append_event_rejects_session_mismatch(redis_client: Redis) -> None:
    log = EventLog(redis_client, _session_id())
    foreign_event = link(None, _session_id(), EventType.TASK_STARTED, {"goal": "demo"})

    with pytest.raises(ValueError, match="session_id"):
        log.append_event(foreign_event)


@pytest.mark.integration
def test_verify_detects_out_of_band_tamper(redis_client: Redis) -> None:
    session_id = _session_id()
    log = EventLog(redis_client, session_id)
    log.append(EventType.TASK_STARTED, {"goal": "demo"})
    log.append(EventType.STEP_PLANNED, {"step": 1})

    tail = log.tail()
    assert tail is not None
    forged = tail.model_copy(update={"seq": 99})
    redis_client.xadd(
        log.stream_key,
        {"data": forged.model_dump_json(), "hash": forged.hash, "seq": str(forged.seq)},
    )

    with pytest.raises(ChainVerificationError):
        log.verify()


@pytest.mark.integration
def test_connection_failure_raises_broker_error() -> None:
    bad_client: Redis = Redis.from_url(
        "redis://localhost:6399/0",
        decode_responses=True,
        socket_connect_timeout=0.2,
        socket_timeout=0.2,
    )
    log = EventLog(bad_client, _session_id())

    try:
        with pytest.raises(BrokerError):
            log.append(EventType.TASK_STARTED, {"goal": "demo"})
    finally:
        bad_client.close()
