from __future__ import annotations

from worker import main as worker_main


def test_worker_redis_client_sets_read_timeout_above_blocking_read(monkeypatch):
    calls: list[dict] = []

    def fake_from_url(url: str, **kwargs):
        calls.append({"url": url, **kwargs})
        return object()

    monkeypatch.setattr(worker_main.aioredis, "from_url", fake_from_url)
    monkeypatch.setattr(worker_main.settings, "redis_url", "redis://redis.example:6380/0")
    monkeypatch.setenv("WORKER_REDIS_SOCKET_TIMEOUT_SECONDS", "30")

    worker_main._redis()

    assert calls == [
        {
            "url": "redis://redis.example:6380/0",
            "decode_responses": True,
            "socket_timeout": 30.0,
            "socket_connect_timeout": 5.0,
            "health_check_interval": 30,
        }
    ]
