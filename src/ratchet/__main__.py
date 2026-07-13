"""ratchet CLI: R1 demo, long-running worker subprocess entrypoint, and the kill -9 chaos suite."""

import argparse
import contextlib
import logging
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from types import FrameType

from redis import Redis

from ratchet.brokers import RedisStreamsBroker
from ratchet.errors import BrokerError
from ratchet.eventlog import EventLog
from ratchet.events import EventType
from ratchet.executor import Worker, make_task_message


def _default_redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def _cmd_demo(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    run_id = uuid.uuid4().hex[:8]
    redis: Redis = Redis.from_url(args.redis_url, decode_responses=True)
    broker = RedisStreamsBroker(redis, stream=f"ratchet:steps:{run_id}")
    broker.ensure_group()

    session_ids = [f"demo-{run_id}-{i}" for i in range(args.sessions)]
    for i, session_id in enumerate(session_ids):
        message = make_task_message(session_id, [(f"step-{i}", "echo", {"n": i})])
        broker.publish(message.to_fields())

    workers = [Worker(broker, redis, consumer=f"w{i}") for i in range(args.workers)]
    threads = [
        threading.Thread(target=worker.run_forever, kwargs={"block_ms": 500}, daemon=True)
        for worker in workers
    ]

    stop_requested = threading.Event()

    def handle_signal(signum: int, frame: FrameType | None) -> None:
        stop_requested.set()
        for worker in workers:
            worker.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    for thread in threads:
        thread.start()

    done: set[str] = set()
    failed: set[str] = set()
    deadline = time.monotonic() + args.timeout
    while (
        time.monotonic() < deadline
        and len(done) + len(failed) < len(session_ids)
        and not stop_requested.is_set()
    ):
        for session_id in session_ids:
            if session_id in done or session_id in failed:
                continue
            types = {event.type for event in EventLog(redis, session_id).read()}
            if EventType.TASK_DONE in types:
                done.add(session_id)
            elif EventType.STEP_FAILED in types:
                failed.add(session_id)
        time.sleep(0.2)

    for worker in workers:
        worker.stop()
    for thread in threads:
        thread.join(timeout=5)

    for session_id in session_ids:
        log = EventLog(redis, session_id)
        log.verify()
        if session_id in done:
            status = "done"
        elif session_id in failed:
            status = "failed"
        else:
            status = "incomplete"
        chain = ",".join(event.type.value for event in log.read())
        print(f"session={session_id} status={status} chain=verified events={chain}")

    incomplete = len(session_ids) - len(done) - len(failed)
    print(
        f"sessions={len(session_ids)} done={len(done)} failed={len(failed)} "
        f"incomplete={incomplete} workers={args.workers}"
    )
    if len(done) != len(session_ids):
        print("demo FAILED: not every session completed successfully", file=sys.stderr)
        return 1
    print("all session event chains verified")
    return 0


def _cmd_worker(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    redis: Redis = Redis.from_url(args.redis_url, decode_responses=True)
    broker = RedisStreamsBroker(redis, stream=args.stream, group=args.group)
    broker.ensure_group()
    worker = Worker(broker, redis, consumer=args.consumer, min_idle_ms=args.min_idle_ms)

    def handle_signal(signum: int, frame: FrameType | None) -> None:
        worker.stop()

    # Graceful stop on SIGTERM/SIGINT only. SIGKILL has no handler by design:
    # that unhandled kill is exactly what the chaos suite exercises.
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    print(f"ratchet worker ready consumer={args.consumer} pid={os.getpid()}", flush=True)
    try:
        worker.run_forever(block_ms=args.block_ms)
    except BrokerError as e:
        print(f"worker error: {e}", file=sys.stderr)
        return 1
    return 0


def _worker_subprocess_cmd(
    redis_url: str, stream: str, group: str, consumer: str, *, min_idle_ms: str, block_ms: str
) -> list[str]:
    return [
        sys.executable,
        "-m",
        "ratchet",
        "worker",
        "--consumer",
        consumer,
        "--stream",
        stream,
        "--group",
        group,
        "--redis-url",
        redis_url,
        "--min-idle-ms",
        min_idle_ms,
        "--block-ms",
        block_ms,
    ]


def _cmd_chaos(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    run_id = uuid.uuid4().hex[:8]
    stream = f"ratchet:chaos:{run_id}"
    group = "ratchet:workers"
    session_id = f"chaos-{run_id}"

    redis: Redis = Redis.from_url(args.redis_url, decode_responses=True)
    broker = RedisStreamsBroker(redis, stream=stream, group=group)
    broker.ensure_group()

    plan = [
        ("fetch", "echo", {}),
        ("transform", "sleep", {"seconds": 2.0}),
        ("save", "echo", {}),
    ]
    broker.publish(make_task_message(session_id, plan).to_fields())
    log = EventLog(redis, session_id)

    proc_a = subprocess.Popen(
        _worker_subprocess_cmd(
            args.redis_url, stream, group, "w1", min_idle_ms="1000", block_ms="200"
        )
    )
    proc_b: subprocess.Popen[bytes] | None = None
    try:
        deadline = time.monotonic() + 15.0
        transform_called = False
        while time.monotonic() < deadline:
            if any(
                e.type == EventType.TOOL_CALLED and e.payload.get("step_id") == "transform"
                for e in log.read()
            ):
                transform_called = True
                break
            time.sleep(0.05)
        if not transform_called:
            print("chaos FAILED: transform step never observed as tool_called", file=sys.stderr)
            return 1

        os.kill(proc_a.pid, signal.SIGKILL)
        t_kill = time.monotonic()
        # flush so the narrative interleaves correctly with inherited worker output when piped
        print(f"killed worker consumer=w1 pid={proc_a.pid} mid-step (transform)", flush=True)

        proc_b = subprocess.Popen(
            _worker_subprocess_cmd(
                args.redis_url, stream, group, "w2", min_idle_ms="1000", block_ms="200"
            )
        )

        deadline = time.monotonic() + 30.0
        recovery_seconds: float | None = None
        while time.monotonic() < deadline:
            if any(e.type == EventType.TASK_DONE for e in log.read()):
                recovery_seconds = time.monotonic() - t_kill
                break
            time.sleep(0.05)
        if recovery_seconds is None:
            print(
                "chaos FAILED: task_done never observed after recovery worker started",
                file=sys.stderr,
            )
            return 1

        events = log.read()
        print("chain=" + ",".join(e.type.value for e in events))
        resumed = next((e for e in events if e.type == EventType.RESUMED), None)
        if resumed is not None:
            print(f"resumed payload={resumed.payload}")
        print(f"recovery_seconds={recovery_seconds:.2f}")

        log.verify()
        print("chain verified")

        pending = redis.xpending(stream, group)["pending"]
        print(f"pending={pending}")
        if pending != 0:
            print("chaos FAILED: PEL not empty after recovery", file=sys.stderr)
            return 1
        return 0
    finally:
        for proc in (proc_a, proc_b):
            if proc is None:
                continue
            if proc.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="ratchet",
        description="Durable queue-native execution: demo, worker subprocess, kill -9 chaos suite",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo_parser = subparsers.add_parser("demo", help="run the multi-session stubbed-step demo")
    demo_parser.add_argument("--sessions", type=int, default=5)
    demo_parser.add_argument("--workers", type=int, default=2)
    demo_parser.add_argument("--redis-url", default=_default_redis_url())
    demo_parser.add_argument("--timeout", type=float, default=30.0)
    demo_parser.set_defaults(func=_cmd_demo)

    worker_parser = subparsers.add_parser("worker", help="run a single long-lived worker process")
    worker_parser.add_argument("--consumer", required=True)
    worker_parser.add_argument("--stream", default="ratchet:steps")
    worker_parser.add_argument("--group", default="ratchet:workers")
    worker_parser.add_argument("--redis-url", default=_default_redis_url())
    worker_parser.add_argument("--min-idle-ms", type=int, default=30000)
    worker_parser.add_argument("--block-ms", type=int, default=5000)
    worker_parser.set_defaults(func=_cmd_worker)

    chaos_parser = subparsers.add_parser(
        "chaos", help="kill -9 a worker mid-step and prove log-derived recovery"
    )
    chaos_parser.add_argument("--redis-url", default=_default_redis_url())
    chaos_parser.set_defaults(func=_cmd_chaos)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
