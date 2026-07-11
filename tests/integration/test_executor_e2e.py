"""End-to-end tests for the Worker executor against a real Redis."""

import threading
import time
import uuid

import pytest
from redis import Redis

from ratchet.brokers import RedisStreamsBroker
from ratchet.eventlog import EventLog
from ratchet.events import EventType
from ratchet.executor import Worker, make_step_message


def _unique_names() -> tuple[str, str]:
    suffix = uuid.uuid4().hex
    return f"test:steps:{suffix}", f"test:workers:{suffix}"


def _session_id() -> str:
    return f"test-session-{uuid.uuid4().hex}"


def _session_done(redis_client: Redis, session_id: str) -> bool:
    tail = EventLog(redis_client, session_id).tail()
    return tail is not None and tail.type == EventType.TASK_DONE


@pytest.mark.integration
def test_executor_processes_echo(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish(make_step_message(session_id, "step-1", "echo", {"x": 1}).to_fields())

    worker = Worker(broker, redis_client, "w1")
    processed = worker.run_once(block_ms=1000)

    assert processed == 1
    log = EventLog(redis_client, session_id)
    events = log.read()
    assert [e.type for e in events] == [
        EventType.TASK_STARTED,
        EventType.STEP_PLANNED,
        EventType.TOOL_CALLED,
        EventType.TOOL_RESULT,
        EventType.TASK_DONE,
    ]
    log.verify()
    tool_result = next(e for e in events if e.type == EventType.TOOL_RESULT)
    assert tool_result.payload["result"] == {"x": 1}
    pending = redis_client.xpending(stream, group)
    assert pending["pending"] == 0


@pytest.mark.integration
def test_executor_failure_path(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish(make_step_message(session_id, "step-1", "flaky", {"fail_rate": 1.0}).to_fields())

    worker = Worker(broker, redis_client, "w1")
    processed = worker.run_once(block_ms=1000)

    assert processed == 1
    log = EventLog(redis_client, session_id)
    events = log.read()
    assert [e.type for e in events] == [
        EventType.TASK_STARTED,
        EventType.STEP_PLANNED,
        EventType.TOOL_CALLED,
        EventType.STEP_FAILED,
    ]
    log.verify()
    assert events[-1].payload["error_type"] == "FlakyStepError"
    pending = redis_client.xpending(stream, group)
    assert pending["pending"] == 0


@pytest.mark.integration
def test_executor_unknown_tool(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish(make_step_message(session_id, "step-1", "nope", {}).to_fields())

    worker = Worker(broker, redis_client, "w1")
    processed = worker.run_once(block_ms=1000)

    assert processed == 1
    log = EventLog(redis_client, session_id)
    events = log.read()
    assert events[-1].type == EventType.STEP_FAILED
    assert events[-1].payload["error_type"] == "UnknownStepError"
    log.verify()
    pending = redis_client.xpending(stream, group)
    assert pending["pending"] == 0


@pytest.mark.integration
def test_executor_malformed_message(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish({"session_id": session_id, "garbage": "x"})

    worker = Worker(broker, redis_client, "w1")
    processed = worker.run_once(block_ms=1000)

    assert processed == 1
    log = EventLog(redis_client, session_id)
    events = log.read()
    assert len(events) == 1
    assert events[0].type == EventType.STEP_FAILED
    assert events[0].payload["error_type"] == "validation"
    pending = redis_client.xpending(stream, group)
    assert pending["pending"] == 0


@pytest.mark.integration
def test_executor_poison_message_no_session(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish({"garbage": "x"})

    worker = Worker(broker, redis_client, "w1")
    processed = worker.run_once(block_ms=1000)

    assert processed == 1
    pending = redis_client.xpending(stream, group)
    assert pending["pending"] == 0
    assert redis_client.keys("ratchet:log:*") == []


@pytest.mark.integration
def test_end_to_end_two_workers(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()

    session_ids = [_session_id() for _ in range(20)]
    for session_id in session_ids:
        broker.publish(make_step_message(session_id, "step-1", "echo", {}).to_fields())

    worker1 = Worker(broker, redis_client, "w1")
    worker2 = Worker(broker, redis_client, "w2")
    counts: dict[str, int] = {"w1": 0, "w2": 0}

    def drive(name: str, worker: Worker) -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            counts[name] += worker.run_once(block_ms=200)
            if all(_session_done(redis_client, s) for s in session_ids):
                return

    threads = [
        threading.Thread(target=drive, args=("w1", worker1), daemon=True),
        threading.Thread(target=drive, args=("w2", worker2), daemon=True),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=35)
        assert not t.is_alive()

    for session_id in session_ids:
        log = EventLog(redis_client, session_id)
        events = log.read()
        assert sum(1 for e in events if e.type == EventType.TASK_DONE) == 1
        log.verify()

    pending = redis_client.xpending(stream, group)
    assert pending["pending"] == 0
    assert counts["w1"] + counts["w2"] == 20


@pytest.mark.integration
def test_graceful_shutdown(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    worker = Worker(broker, redis_client, "w1")

    thread = threading.Thread(target=worker.run_forever, kwargs={"block_ms": 200}, daemon=True)
    thread.start()

    session_ids = [_session_id() for _ in range(3)]
    for session_id in session_ids:
        broker.publish(make_step_message(session_id, "step-1", "echo", {}).to_fields())

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if all(_session_done(redis_client, s) for s in session_ids):
            break
        time.sleep(0.05)
    else:
        pytest.fail("steps did not complete before deadline")

    worker.stop()
    thread.join(timeout=3)

    assert not thread.is_alive()
    pending = redis_client.xpending(stream, group)
    assert pending["pending"] == 0
