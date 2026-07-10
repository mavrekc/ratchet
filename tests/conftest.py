"""Shared pytest fixtures: a real-Redis client for integration tests."""

import os
from collections.abc import Iterator

import pytest
from redis import Redis
from redis.exceptions import RedisError

DEFAULT_TEST_REDIS_URL = "redis://localhost:6379/15"


@pytest.fixture()
def redis_client() -> Iterator[Redis]:
    url = os.environ.get("REDIS_URL", DEFAULT_TEST_REDIS_URL)
    client: Redis = Redis.from_url(url, decode_responses=True)
    try:
        client.ping()
    except RedisError:
        client.close()
        if os.environ.get("RATCHET_REQUIRE_REDIS") == "1":
            pytest.fail(f"RATCHET_REQUIRE_REDIS=1 but no Redis reachable at {url}")
        pytest.skip(f"no Redis reachable at {url}")
    client.flushdb()
    yield client
    client.flushdb()
    client.close()
