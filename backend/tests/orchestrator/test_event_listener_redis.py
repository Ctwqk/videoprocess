from __future__ import annotations

from app.orchestrator import event_listener


def test_event_listener_socket_timeout_exceeds_blocking_read():
    client = event_listener._redis()
    options = client.connection_pool.connection_kwargs

    assert options["socket_timeout"] > event_listener.REDIS_BLOCK_MILLISECONDS / 1_000
