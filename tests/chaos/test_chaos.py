"""Kill -9 chaos suite: real worker subprocesses, real SIGKILL, log-derived recovery proofs."""

import os
import signal
import subprocess
import time
import uuid
from collections.abc import Callable, Sequence

import pytest
from redis import Redis

from ratchet.brokers import RedisStreamsBroker
from ratchet.dlq import DeadLetterQueue
from ratchet.eventlog import EventLog
from ratchet.events import Event, EventType
from ratchet.executor import make_task_message

SpawnWorker = Callable[..., "subprocess.Popen[bytes]"]


def _names(prefix: str) -> tuple[str, str, str]:
    suffix = uuid.uuid4().hex
    return (
        f"chaos:{prefix}:steps:{suffix}",
        f"chaos:{prefix}:workers:{suffix}",
        f"chaos-{prefix}-session-{suffix}",
    )


def _count(events: Sequence[Event], etype: EventType, step_id: str | None = None) -> int:
    return sum(
        1
        for e in events
        if e.type == etype and (step_id is None or e.payload.get("step_id") == step_id)
    )


@pytest.mark.chaos
def test_kill_mid_plan_resumes_and_completes(
    redis_client: Redis,
    spawn_worker: SpawnWorker,
    wait_until: Callable[..., bool],
) -> None:
    stream, group, session_id = _names("mid_plan")
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    plan = [("step-0", "echo", {}), ("step-1", "sleep", {"seconds": 2.0}), ("step-2", "echo", {})]
    broker.publish(make_task_message(session_id, plan).to_fields())
    log = EventLog(redis_client, session_id)

    proc_w1 = spawn_worker("w1", stream, group, min_idle_ms=1000, block_ms=200)
    assert wait_until(lambda: _count(log.read(), EventType.TOOL_CALLED, "step-1") == 1, 10.0)

    os.kill(proc_w1.pid, signal.SIGKILL)
    t_kill = time.monotonic()

    spawn_worker("w2", stream, group, min_idle_ms=1000, block_ms=200)

    resumed_at: list[float] = []
    done_at: list[float] = []

    def _observe() -> bool:
        events = log.read()
        now = time.monotonic()
        if not resumed_at and _count(events, EventType.RESUMED) >= 1:
            resumed_at.append(now)
        if not done_at and _count(events, EventType.TASK_DONE) == 1:
            done_at.append(now)
        return bool(done_at)

    assert wait_until(_observe, 20.0), "task_done not observed after recovery"
    recovery_seconds = done_at[0] - t_kill
    resume_latency_seconds = (resumed_at[0] - t_kill) if resumed_at else recovery_seconds

    events = log.read()
    assert _count(events, EventType.RESUMED) == 1
    assert _count(events, EventType.TOOL_CALLED, "step-0") == 1
    for step_id in ("step-0", "step-1", "step-2"):
        assert _count(events, EventType.TOOL_RESULT, step_id) == 1
    assert _count(events, EventType.TASK_DONE) == 1
    log.verify()
    assert redis_client.xpending(stream, group)["pending"] == 0
    assert 0 < recovery_seconds < 15

    print(
        f"RATCHET_CHAOS test=kill_mid_plan min_idle_ms=1000 "
        f"recovery_seconds={recovery_seconds:.2f} "
        f"resume_latency_seconds={resume_latency_seconds:.2f}"
    )


@pytest.mark.chaos
def test_kill_with_no_standby_worker(
    redis_client: Redis,
    spawn_worker: SpawnWorker,
    wait_until: Callable[..., bool],
) -> None:
    stream, group, session_id = _names("no_standby")
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    plan = [("step-0", "echo", {}), ("step-1", "sleep", {"seconds": 2.0}), ("step-2", "echo", {})]
    broker.publish(make_task_message(session_id, plan).to_fields())
    log = EventLog(redis_client, session_id)

    proc_w1 = spawn_worker("w1", stream, group, min_idle_ms=1000, block_ms=200)
    assert wait_until(lambda: _count(log.read(), EventType.TOOL_CALLED, "step-1") == 1, 10.0)

    os.kill(proc_w1.pid, signal.SIGKILL)
    time.sleep(1.5)  # past min_idle_ms, with no standby worker running

    spawn_worker("w2", stream, group, min_idle_ms=1000, block_ms=200)
    assert wait_until(lambda: _count(log.read(), EventType.TASK_DONE) == 1, 20.0)

    events = log.read()
    assert _count(events, EventType.RESUMED) == 1
    log.verify()
    assert redis_client.xpending(stream, group)["pending"] == 0


@pytest.mark.chaos
def test_worker_survives_connection_drop(
    redis_client: Redis,
    spawn_worker: SpawnWorker,
    wait_until: Callable[..., bool],
) -> None:
    stream, group, session_id = _names("conn_drop")
    proc = spawn_worker("w1", stream, group, min_idle_ms=1000, block_ms=200)
    time.sleep(0.3)  # let the worker settle into its blocking XREADGROUP poll

    redis_client.execute_command("CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes")

    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    plan = [("step-0", "sleep", {"seconds": 1.0})]
    broker.publish(make_task_message(session_id, plan).to_fields())

    log = EventLog(redis_client, session_id)
    assert wait_until(lambda: _count(log.read(), EventType.TASK_DONE) == 1, 15.0)
    assert proc.poll() is None
    log.verify()


@pytest.mark.chaos
def test_flaky_step_dead_letters_without_hang(
    redis_client: Redis,
    spawn_worker: SpawnWorker,
    wait_until: Callable[..., bool],
) -> None:
    stream, group, session_id = _names("flaky")
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    plan = [("s0", "echo", {}), ("s1", "flaky", {"fail_rate": 1.0}), ("s2", "echo", {})]
    broker.publish(make_task_message(session_id, plan).to_fields())
    log = EventLog(redis_client, session_id)

    proc = spawn_worker("w1", stream, group, min_idle_ms=1000, block_ms=200)
    assert wait_until(lambda: _count(log.read(), EventType.STEP_FAILED) == 1, 15.0)

    events = log.read()
    assert _count(events, EventType.TOOL_CALLED, "s2") == 0
    log.verify()

    dlq = DeadLetterQueue(redis_client)
    entries = dlq.entries()
    assert len(entries) == 1
    entry = entries[0]
    assert entry.step_id == "s1"
    assert entry.error_type == "FlakyStepError"
    assert entry.original.get("session_id") == session_id
    assert "plan" in entry.original

    assert redis_client.xpending(stream, group)["pending"] == 0
    assert proc.poll() is None


@pytest.mark.chaos
def test_repeated_kills_no_lost_steps(
    redis_client: Redis,
    spawn_worker: SpawnWorker,
    wait_until: Callable[..., bool],
) -> None:
    test_deadline = time.monotonic() + 40.0
    stream, group, session_id = _names("repeated_kills")
    broker = RedisStreamsBroker(redis_client, stream=stream, group=group)
    broker.ensure_group()
    plan = [
        ("s0", "echo", {}),
        ("s1", "sleep", {"seconds": 1.5}),
        ("s2", "sleep", {"seconds": 1.5}),
        ("s3", "echo", {}),
    ]
    broker.publish(make_task_message(session_id, plan).to_fields())
    log = EventLog(redis_client, session_id)

    proc_w1 = spawn_worker("w1", stream, group, min_idle_ms=1000, block_ms=200)
    assert wait_until(lambda: _count(log.read(), EventType.TOOL_CALLED, "s1") == 1, 10.0)
    os.kill(proc_w1.pid, signal.SIGKILL)

    proc_w2 = spawn_worker("w2", stream, group, min_idle_ms=1000, block_ms=200)
    assert wait_until(lambda: _count(log.read(), EventType.TOOL_CALLED, "s2") == 1, 10.0)
    os.kill(proc_w2.pid, signal.SIGKILL)

    spawn_worker("w3", stream, group, min_idle_ms=1000, block_ms=200)
    assert wait_until(lambda: _count(log.read(), EventType.TASK_DONE) == 1, 30.0)

    events = log.read()
    for step_id in ("s0", "s1", "s2", "s3"):
        assert _count(events, EventType.TOOL_RESULT, step_id) == 1
    assert _count(events, EventType.TASK_DONE) == 1
    assert _count(events, EventType.RESUMED) == 2
    log.verify()
    assert redis_client.xpending(stream, group)["pending"] == 0
    assert time.monotonic() < test_deadline
