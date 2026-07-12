"""Deterministic checkpoint/resume proofs: hand-built log states then claim-driven recovery."""

import threading
import time
import uuid
from collections.abc import Mapping, Sequence

import pytest
from redis import Redis

from ratchet.broker import Broker, Message
from ratchet.brokers import RedisStreamsBroker
from ratchet.dlq import DeadLetterQueue
from ratchet.errors import BrokerError
from ratchet.eventlog import EventLog
from ratchet.events import Event, EventType
from ratchet.executor import PlanStep, TaskMessage, Worker, make_task_message


def _unique_names() -> tuple[str, str]:
    suffix = uuid.uuid4().hex
    return f"test:steps:{suffix}", f"test:workers:{suffix}"


def _session_id() -> str:
    return f"test-session-{uuid.uuid4().hex}"


def _dlq(redis_client: Redis) -> DeadLetterQueue:
    return DeadLetterQueue(redis_client, stream=f"test:dlq:{uuid.uuid4().hex}")


def _echo_plan(n: int) -> list[tuple[str, str, dict[str, object]]]:
    return [(f"step-{i}", "echo", {"n": i}) for i in range(n)]


def _count(events: Sequence[Event], etype: EventType, step_id: str | None = None) -> int:
    return sum(
        1
        for e in events
        if e.type == etype and (step_id is None or e.payload.get("step_id") == step_id)
    )


def _planned(log: EventLog, step: PlanStep) -> None:
    log.append(EventType.STEP_PLANNED, {"step_id": step.step_id, "tool": step.tool})


def _called(log: EventLog, step: PlanStep) -> None:
    log.append(
        EventType.TOOL_CALLED,
        {
            "step_id": step.step_id,
            "tool": step.tool,
            "args": step.args,
            "idempotency_key": step.idempotency_key,
        },
    )


def _result(log: EventLog, step: PlanStep) -> None:
    log.append(EventType.TOOL_RESULT, {"step_id": step.step_id, "result": dict(step.args)})


def _checkpoint(log: EventLog, cursor: int, step: PlanStep) -> None:
    log.append(EventType.CHECKPOINT, {"cursor": cursor, "completed_step_id": step.step_id})


def _seed_full_success(log: EventLog, msg: TaskMessage) -> None:
    log.append(EventType.TASK_STARTED, {})
    for i, step in enumerate(msg.plan):
        _planned(log, step)
        _called(log, step)
        _result(log, step)
        _checkpoint(log, i + 1, step)
    log.append(EventType.TASK_DONE, {"steps": len(msg.plan)})


def _deliver_pending(broker: Broker, msg: TaskMessage) -> tuple[Message, dict[str, str]]:
    fields = msg.to_fields()
    broker.publish(fields)
    delivered = broker.consume("dead", block_ms=1000)
    assert len(delivered) == 1
    return delivered[0], fields


@pytest.mark.integration
def test_resume_from_checkpoint_midstep(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    msg = make_task_message(session_id, _echo_plan(3))
    _deliver_pending(broker, msg)

    log = EventLog(redis_client, session_id)
    log.append(EventType.TASK_STARTED, {})
    _planned(log, msg.plan[0])
    _called(log, msg.plan[0])
    _result(log, msg.plan[0])
    _checkpoint(log, 1, msg.plan[0])
    _planned(log, msg.plan[1])
    _called(log, msg.plan[1])

    time.sleep(0.2)
    worker = Worker(broker, redis_client, "claimant", min_idle_ms=100, dlq=_dlq(redis_client))
    assert worker.run_once(block_ms=100) == 1

    events = log.read()
    resumed = next(e for e in events if e.type == EventType.RESUMED)
    assert resumed.payload["cursor"] == 1
    assert _count(events, EventType.TOOL_CALLED, "step-0") == 1
    assert _count(events, EventType.TOOL_CALLED, "step-1") == 2
    assert _count(events, EventType.TOOL_RESULT, "step-1") == 1
    assert _count(events, EventType.TASK_DONE) == 1
    log.verify()
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.integration
def test_resume_fast_forwards_completed_step(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    msg = make_task_message(session_id, _echo_plan(3))
    _deliver_pending(broker, msg)

    log = EventLog(redis_client, session_id)
    log.append(EventType.TASK_STARTED, {})
    _planned(log, msg.plan[0])
    _called(log, msg.plan[0])
    _result(log, msg.plan[0])
    _checkpoint(log, 1, msg.plan[0])
    _planned(log, msg.plan[1])
    _called(log, msg.plan[1])
    _result(log, msg.plan[1])

    time.sleep(0.2)
    worker = Worker(broker, redis_client, "claimant", min_idle_ms=100, dlq=_dlq(redis_client))
    assert worker.run_once(block_ms=100) == 1

    events = log.read()
    resumed = next(e for e in events if e.type == EventType.RESUMED)
    assert resumed.payload["cursor"] == 2
    assert _count(events, EventType.TOOL_CALLED, "step-1") == 1
    assert 2 in [e.payload["cursor"] for e in events if e.type == EventType.CHECKPOINT]
    assert _count(events, EventType.TASK_DONE) == 1
    log.verify()
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.integration
def test_resume_runs_next_step(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    msg = make_task_message(session_id, _echo_plan(3))
    _deliver_pending(broker, msg)

    log = EventLog(redis_client, session_id)
    log.append(EventType.TASK_STARTED, {})
    _planned(log, msg.plan[0])
    _called(log, msg.plan[0])
    _result(log, msg.plan[0])
    _checkpoint(log, 1, msg.plan[0])

    time.sleep(0.2)
    worker = Worker(broker, redis_client, "claimant", min_idle_ms=100, dlq=_dlq(redis_client))
    assert worker.run_once(block_ms=100) == 1

    events = log.read()
    resumed = next(e for e in events if e.type == EventType.RESUMED)
    assert resumed.payload["cursor"] == 1
    assert _count(events, EventType.TOOL_CALLED, "step-0") == 1
    assert _count(events, EventType.TOOL_CALLED, "step-1") == 1
    assert _count(events, EventType.TOOL_CALLED, "step-2") == 1
    assert _count(events, EventType.TASK_DONE) == 1
    log.verify()
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.integration
def test_reclaim_terminal_task_just_acks(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    msg = make_task_message(session_id, _echo_plan(3))
    _deliver_pending(broker, msg)

    log = EventLog(redis_client, session_id)
    _seed_full_success(log, msg)
    length_before = len(log.read())

    time.sleep(0.2)
    worker = Worker(broker, redis_client, "claimant", min_idle_ms=100, dlq=_dlq(redis_client))
    assert worker.run_once(block_ms=100) == 1

    assert len(log.read()) == length_before
    assert _count(log.read(), EventType.TASK_DONE) == 1
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.integration
def test_requeue_then_resume_skips_completed(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    plan = [
        ("step-0", "echo", {"n": 0}),
        ("step-1", "flaky", {"fail_rate": 1.0}),
        ("step-2", "echo", {"n": 2}),
    ]
    broker.publish(make_task_message(session_id, plan).to_fields())

    dlq = _dlq(redis_client)
    worker = Worker(broker, redis_client, "w1", dlq=dlq)
    worker.run_once(block_ms=1000)

    log = EventLog(redis_client, session_id)
    assert _count(log.read(), EventType.STEP_FAILED) == 1
    entries = dlq.entries()
    assert len(entries) == 1

    length_before = len(log.read())
    dlq.requeue(entries[0].id, broker)
    assert dlq.entries() == []

    assert worker.run_once(block_ms=1000) == 1

    assert len(log.read()) == length_before
    assert _count(log.read(), EventType.TASK_DONE) == 0
    assert redis_client.xpending(stream, group)["pending"] == 0


class _PushFailsDLQ(DeadLetterQueue):
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
        raise BrokerError("dead-letter push failed")


@pytest.mark.integration
def test_dlq_push_failure_leaves_pending(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    broker.publish(
        make_task_message(session_id, [("step-0", "flaky", {"fail_rate": 1.0})]).to_fields()
    )

    failing_dlq = _PushFailsDLQ(redis_client, stream=f"test:dlq:{uuid.uuid4().hex}")
    worker = Worker(broker, redis_client, "w1", dlq=failing_dlq)

    with pytest.raises(BrokerError):
        worker.run_once(block_ms=1000)

    events = EventLog(redis_client, session_id).read()
    assert _count(events, EventType.STEP_FAILED) == 0
    assert redis_client.xpending(stream, group)["pending"] == 1


@pytest.mark.integration
def test_double_claim_chain_stays_linear(redis_client: Redis) -> None:
    stream, group = _unique_names()
    session_id = _session_id()
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    msg = make_task_message(session_id, _echo_plan(3))
    _deliver_pending(broker, msg)

    time.sleep(0.15)
    workers = [
        Worker(broker, redis_client, f"w{i}", min_idle_ms=100, dlq=_dlq(redis_client))
        for i in range(2)
    ]
    threads = [
        threading.Thread(
            target=w.run_forever,
            kwargs={"block_ms": 50, "error_backoff_s": 0.01},
            daemon=True,
        )
        for w in workers
    ]
    for t in threads:
        t.start()

    log = EventLog(redis_client, session_id)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        events = log.read()
        done = _count(events, EventType.TASK_DONE) == 1
        if done and redis_client.xpending(stream, group)["pending"] == 0:
            break
        time.sleep(0.02)

    for w in workers:
        w.stop()
    for t in threads:
        t.join(timeout=3)
        assert not t.is_alive()

    events = log.read()
    assert _count(events, EventType.TASK_DONE) == 1
    log.verify()
    assert redis_client.xpending(stream, group)["pending"] == 0
