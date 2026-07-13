"""Fixtures for the kill -9 chaos suite: worker subprocess spawn/teardown, wait_until polling."""

import os
import subprocess
import sys
import time
from collections.abc import Callable, Iterator

import pytest
from redis import Redis
from redis.exceptions import ResponseError

from ratchet.brokers import RedisStreamsBroker

DEFAULT_TEST_REDIS_URL = "redis://localhost:6379/15"

SpawnWorker = Callable[..., "subprocess.Popen[bytes]"]


def _wait_until(predicate: Callable[[], bool], timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return predicate()


@pytest.fixture()
def wait_until() -> Callable[..., bool]:
    return _wait_until


@pytest.fixture()
def redis_url() -> str:
    return os.environ.get("REDIS_URL", DEFAULT_TEST_REDIS_URL)


@pytest.fixture()
def spawn_worker(redis_client: Redis, redis_url: str) -> Iterator[SpawnWorker]:
    procs: list[subprocess.Popen[bytes]] = []
    ensured_groups: set[tuple[str, str]] = set()

    def _spawn(
        consumer: str,
        stream: str,
        group: str,
        *,
        min_idle_ms: int = 1000,
        block_ms: int = 200,
    ) -> subprocess.Popen[bytes]:
        if (stream, group) not in ensured_groups:
            RedisStreamsBroker(redis_client, stream=stream, group=group).ensure_group()
            ensured_groups.add((stream, group))

        proc = subprocess.Popen(
            [
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
                str(min_idle_ms),
                "--block-ms",
                str(block_ms),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(proc)

        def _consumer_ready() -> bool:
            try:
                consumers = redis_client.xinfo_consumers(stream, group)
            except ResponseError:
                return False
            return any(c["name"] == consumer for c in consumers)

        if not _wait_until(_consumer_ready, 10.0):
            proc.kill()
            proc.wait(timeout=5)
            pytest.fail(f"worker consumer={consumer} did not register within 10s")
        return proc

    yield _spawn

    for proc in procs:
        try:
            proc.kill()
            proc.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            pass
