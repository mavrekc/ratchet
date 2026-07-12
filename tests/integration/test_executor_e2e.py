"""End-to-end tests for the multi-step Worker executor against a real Redis."""

import threading
import time
import uuid
from collections.abc import Mapping, Sequence

import pytest
from redis import Redis

from ratchet.broker import Message
from ratchet.brokers import RedisStreamsBroker
from ratchet.dlq import DeadLetterQueue
from ratchet.errors import BrokerError
from ratchet.eventlog import EventLog
from ratchet.events import Event, EventType
from ratchet.executor import Worker, make_task_message


def _unique_names() -> tuple[str, str]:
    suffix = uuid.uuid4().hex
    return f"test:steps:{suffix}", f"test:workers:{suffix}"


def _session_id() -> str:
    return f"test-session-{uuid.uuid4().hex}"


def _dlq(redis_client: Redis) -> DeadLetterQueue:
    return DeadLetterQueue(redis_client, stream=f"test:dlq:{uuid.uuid4().hex}")


def _session_done(redis_client: Redis, session_id: str) -> bool:
    tail = EventLog(redis_client, session_id).tail()
    return tail is not None and tail.type == EventType.TASK_DONE


def _count(events: Sequence[Event], etype: EventType, step_id: str | None = None) -> int:
    return sum(
        1
        for e in events
        if e.type == etype and (step_id is None or e.payload.get("step_id") == step_id)
    )


@pytest.mark.integration
def test_executor_processes_single_step_plan(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish(make_task_message(session_id, [("step-0", "echo", {"x": 1})]).to_fields())

    worker = Worker(broker, redis_client, "w1", dlq=_dlq(redis_client))
    processed = worker.run_once(block_ms=1000)

    assert processed == 1
    log = EventLog(redis_client, session_id)
    events = log.read()
    assert [e.type for e in events] == [
        EventType.TASK_STARTED,
        EventType.STEP_PLANNED,
        EventType.TOOL_CALLED,
        EventType.TOOL_RESULT,
        EventType.CHECKPOINT,
        EventType.TASK_DONE,
    ]
    log.verify()
    assert next(e for e in events if e.type == EventType.TOOL_RESULT).payload["result"] == {"x": 1}
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.integration
def test_executor_processes_three_step_plan(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    plan = [
        ("step-0", "echo", {"n": 0}),
        ("step-1", "echo", {"n": 1}),
        ("step-2", "echo", {"n": 2}),
    ]
    broker.publish(make_task_message(session_id, plan).to_fields())

    worker = Worker(broker, redis_client, "w1", dlq=_dlq(redis_client))
    worker.run_once(block_ms=1000)

    events = EventLog(redis_client, session_id).read()
    block = [
        EventType.STEP_PLANNED,
        EventType.TOOL_CALLED,
        EventType.TOOL_RESULT,
        EventType.CHECKPOINT,
    ]
    assert [e.type for e in events] == [EventType.TASK_STARTED, *block * 3, EventType.TASK_DONE]
    checkpoints = [e for e in events if e.type == EventType.CHECKPOINT]
    assert [c.payload["cursor"] for c in checkpoints] == [1, 2, 3]
    assert events[-1].payload == {"steps": 3}
    EventLog(redis_client, session_id).verify()
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.integration
def test_executor_failure_mid_plan_dead_letters(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    plan = [
        ("step-0", "echo", {"n": 0}),
        ("step-1", "flaky", {"fail_rate": 1.0}),
        ("step-2", "echo", {"n": 2}),
    ]
    published = make_task_message(session_id, plan).to_fields()
    broker.publish(published)

    dlq = _dlq(redis_client)
    worker = Worker(broker, redis_client, "w1", dlq=dlq)
    worker.run_once(block_ms=1000)

    events = EventLog(redis_client, session_id).read()
    assert [e.type for e in events] == [
        EventType.TASK_STARTED,
        EventType.STEP_PLANNED,
        EventType.TOOL_CALLED,
        EventType.TOOL_RESULT,
        EventType.CHECKPOINT,
        EventType.STEP_PLANNED,
        EventType.TOOL_CALLED,
        EventType.STEP_FAILED,
    ]
    assert _count(events, EventType.TASK_DONE) == 0
    assert _count(events, EventType.STEP_PLANNED, "step-2") == 0
    EventLog(redis_client, session_id).verify()

    entries = dlq.entries()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.session_id == session_id
    assert entry.step_id == "step-1"
    assert entry.tool == "flaky"
    assert entry.error_type == "FlakyStepError"
    assert entry.original == published
    assert len(entry.events) > 0
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.integration
def test_executor_unknown_tool_dead_letters(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish(make_task_message(session_id, [("step-0", "nope", {})]).to_fields())

    dlq = _dlq(redis_client)
    worker = Worker(broker, redis_client, "w1", dlq=dlq)
    worker.run_once(block_ms=1000)

    events = EventLog(redis_client, session_id).read()
    assert events[-1].type == EventType.STEP_FAILED
    assert events[-1].payload["error_type"] == "UnknownStepError"
    entries = dlq.entries()
    assert len(entries) == 1
    assert entries[0].error_type == "UnknownStepError"
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.integration
def test_poison_message_to_dlq(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    with_session = {"session_id": session_id, "garbage": "x"}
    no_session = {"garbage": "x"}
    broker.publish(with_session)
    broker.publish(no_session)

    dlq = _dlq(redis_client)
    worker = Worker(broker, redis_client, "w1", dlq=dlq)
    worker.run_once(block_ms=1000, count=10)

    entries = dlq.entries()
    assert len(entries) == 2
    assert all(e.error_type == "validation" for e in entries)
    by_session = {e.session_id: e for e in entries}
    assert by_session[session_id].original == with_session
    assert by_session[""].original == no_session

    session_events = EventLog(redis_client, session_id).read()
    assert [e.type for e in session_events] == [EventType.STEP_FAILED]
    assert session_events[0].payload["error_type"] == "validation"
    assert not redis_client.exists("ratchet:log:")
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.integration
def test_end_to_end_two_workers(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()

    session_ids = [_session_id() for _ in range(10)]
    for session_id in session_ids:
        plan = [(f"step-{i}", "echo", {"n": i}) for i in range(3)]
        broker.publish(make_task_message(session_id, plan).to_fields())

    workers = [Worker(broker, redis_client, f"w{i}", dlq=_dlq(redis_client)) for i in range(2)]

    def drive(worker: Worker) -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            worker.run_once(block_ms=200)
            if all(_session_done(redis_client, s) for s in session_ids):
                return

    threads = [threading.Thread(target=drive, args=(w,), daemon=True) for w in workers]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=35)
        assert not t.is_alive()

    for session_id in session_ids:
        log = EventLog(redis_client, session_id)
        events = log.read()
        assert _count(events, EventType.TASK_DONE) == 1
        log.verify()
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.integration
def test_graceful_shutdown(redis_client: Redis) -> None:
    stream, group = _unique_names()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    worker = Worker(broker, redis_client, "w1", dlq=_dlq(redis_client))

    thread = threading.Thread(target=worker.run_forever, kwargs={"block_ms": 200}, daemon=True)
    thread.start()

    session_ids = [_session_id() for _ in range(3)]
    for session_id in session_ids:
        plan = [(f"step-{i}", "echo", {"n": i}) for i in range(2)]
        broker.publish(make_task_message(session_id, plan).to_fields())

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
    assert redis_client.xpending(stream, group)["pending"] == 0


class _ResettingBroker:
    """Raises fail_per_batch BrokerErrors, then a success that resets, repeated batches times."""

    def __init__(self, fail_per_batch: int, batches: int) -> None:
        self._fail_per_batch = fail_per_batch
        self._batches = batches
        self._fails_in_batch = 0
        self.batches_done = 0
        self.worker: Worker | None = None

    def claim(self, consumer: str, min_idle_ms: int, count: int = 10) -> list[Message]:
        if self._fails_in_batch < self._fail_per_batch:
            self._fails_in_batch += 1
            raise BrokerError("transient claim failure")
        self._fails_in_batch = 0
        self.batches_done += 1
        if self.batches_done >= self._batches and self.worker is not None:
            self.worker.stop()
        return []

    def consume(self, consumer: str, count: int = 10, block_ms: int = 5000) -> list[Message]:
        return []

    def ack(self, message_id: str) -> int:
        return 1

    def ensure_group(self) -> None:
        return None

    def publish(self, fields: Mapping[str, str]) -> str:
        return "0-0"


class _DeadBroker:
    def __init__(self) -> None:
        self.claim_calls = 0

    def claim(self, consumer: str, min_idle_ms: int, count: int = 10) -> list[Message]:
        self.claim_calls += 1
        raise BrokerError("broker down")

    def consume(self, consumer: str, count: int = 10, block_ms: int = 5000) -> list[Message]:
        return []

    def ack(self, message_id: str) -> int:
        return 1

    def ensure_group(self) -> None:
        return None

    def publish(self, fields: Mapping[str, str]) -> str:
        return "0-0"


@pytest.mark.integration
def test_run_forever_survives_transient_broker_errors(redis_client: Redis) -> None:
    # Two batches of 4 failures with a reset between: 8 total but never 4+1 consecutive,
    # so it survives only if the counter resets after each success.
    resetting = _ResettingBroker(fail_per_batch=4, batches=2)
    survivor = Worker(resetting, redis_client, "w1", dlq=_dlq(redis_client))
    resetting.worker = survivor
    survivor.run_forever(block_ms=10, max_consecutive_errors=5, error_backoff_s=0.001)
    assert resetting.batches_done == 2

    dead = _DeadBroker()
    doomed = Worker(dead, redis_client, "w2", dlq=_dlq(redis_client))
    with pytest.raises(BrokerError):
        doomed.run_forever(block_ms=10, max_consecutive_errors=5, error_backoff_s=0.001)
    assert dead.claim_calls == 5
