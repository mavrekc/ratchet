"""Runnable R1 demo: enqueue stubbed steps, run workers, verify every session chain."""

import argparse
import logging
import os
import signal
import sys
import threading
import time
import uuid
from types import FrameType

from redis import Redis

from ratchet.brokers import RedisStreamsBroker
from ratchet.eventlog import EventLog
from ratchet.events import EventType
from ratchet.executor import Worker, make_step_message

TERMINAL_TYPES = {EventType.TASK_DONE, EventType.STEP_FAILED}


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="ratchet", description="R1 demo: durable stubbed step execution over Redis Streams"
    )
    parser.add_argument("--sessions", type=int, default=5)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument(
        "--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    run_id = uuid.uuid4().hex[:8]
    redis: Redis = Redis.from_url(args.redis_url, decode_responses=True)
    broker = RedisStreamsBroker(redis, stream=f"ratchet:steps:{run_id}")
    broker.ensure_group()

    session_ids = [f"demo-{run_id}-{i}" for i in range(args.sessions)]
    for i, session_id in enumerate(session_ids):
        message = make_step_message(session_id, f"step-{i}", "echo", {"n": i})
        broker.publish(message.to_fields())

    workers = [Worker(broker, redis, consumer=f"w{i}") for i in range(args.workers)]
    threads = [
        threading.Thread(target=worker.run_forever, kwargs={"block_ms": 500}, daemon=True)
        for worker in workers
    ]

    def handle_signal(signum: int, frame: FrameType | None) -> None:
        for worker in workers:
            worker.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    for thread in threads:
        thread.start()

    done: set[str] = set()
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline and len(done) < len(session_ids):
        for session_id in session_ids:
            if session_id in done:
                continue
            types = {event.type for event in EventLog(redis, session_id).read()}
            if types & TERMINAL_TYPES:
                done.add(session_id)
        time.sleep(0.2)

    for worker in workers:
        worker.stop()
    for thread in threads:
        thread.join(timeout=5)

    for session_id in session_ids:
        log = EventLog(redis, session_id)
        log.verify()
        chain = ",".join(event.type.value for event in log.read())
        print(f"session={session_id} chain=verified events={chain}")

    print(f"sessions={len(session_ids)} completed={len(done)} workers={args.workers}")
    if len(done) != len(session_ids):
        print("demo FAILED: timed out before all sessions completed", file=sys.stderr)
        return 1
    print("all session event chains verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
