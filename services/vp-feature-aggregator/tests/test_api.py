from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from threading import Event

from fastapi.testclient import TestClient

from feature_aggregator.config import Settings
from feature_aggregator.main import create_app
from feature_aggregator.schemas import PDSDecisionEvent, VPActorActionEvent
from feature_aggregator.store.memory import MemoryFeatureStore


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)


def test_healthz():
    client = TestClient(create_app())
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_is_ready_when_consumer_is_disabled():
    client = TestClient(create_app(app_settings=Settings(enable_consumer=False)))

    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_readyz_is_ready_when_enabled_consumer_task_is_running():
    started = Event()

    async def idle_runner(*args, **kwargs) -> None:
        started.set()
        await asyncio.Event().wait()

    with TestClient(
        create_app(
            app_settings=Settings(enable_consumer=True),
            consumer_runner=idle_runner,
        )
    ) as client:
        assert started.wait(timeout=1)
        response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_readyz_returns_unavailable_when_enabled_consumer_task_fails():
    failed = Event()

    async def failing_runner(*args, **kwargs) -> None:
        failed.set()
        raise RuntimeError("consumer failed")

    with TestClient(
        create_app(
            app_settings=Settings(enable_consumer=True),
            consumer_runner=failing_runner,
        )
    ) as client:
        assert failed.wait(timeout=1)
        response = client.get("/readyz")

    assert response.status_code == 503
    assert "consumer" in response.json()["detail"]


def test_features_returns_zero_defaults_for_unknown_actor():
    client = TestClient(create_app())
    response = client.get("/v1/features/actor-unknown")
    assert response.status_code == 200
    payload = response.json()
    assert payload["actor_id"] == "actor-unknown"
    assert payload["publishes_5m"] == 0
    assert payload["publishes_1h"] == 0
    assert payload["publishes_24h"] == 0
    assert payload["blocks_24h"] == 0
    assert payload["flags_7d"] == 0
    assert payload["comment_burst_1m"] == 0
    assert "flags_24h" not in payload
    assert "promotion_attempts_24h" not in payload
    assert datetime.fromisoformat(payload["as_of"])
    assert payload["from_cache"] is False


def test_features_returns_store_counts_for_actor():
    store = MemoryFeatureStore()
    asyncio.run(
        store.apply_vp_action(
            VPActorActionEvent(
                event_id="event-1",
                topic_version="vp.actor.actions.v1",
                actor_id="actor-1",
                action_type="publication_scheduled",
                platform="youtube",
                occurred_at=datetime.now().astimezone(),
                source="videoprocess.channel_ops",
            )
        )
    )

    client = TestClient(create_app(store))
    response = client.get("/v1/features/actor-1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["actor_id"] == "actor-1"
    assert payload["publishes_5m"] == 1
    assert payload["publishes_1h"] == 1
    assert payload["publishes_24h"] == 1


async def test_memory_store_suppresses_duplicate_same_topic_events():
    store = MemoryFeatureStore(now=lambda: NOW)
    event = VPActorActionEvent(
        event_id="same-id",
        topic_version="vp.actor.actions.v1",
        actor_id="actor-1",
        action_type="publication_scheduled",
        platform="youtube",
        occurred_at=NOW,
        source="videoprocess.channel_ops",
    )

    assert await store.apply_vp_action(event) is True
    assert await store.apply_vp_action(event) is False

    features = await store.features_for("actor-1")
    assert features.publishes_24h == 1


async def test_memory_store_namespaces_dedupe_by_topic():
    store = MemoryFeatureStore(now=lambda: NOW)

    assert await store.apply_vp_action(
        VPActorActionEvent(
            event_id="shared-id",
            topic_version="vp.actor.actions.v1",
            actor_id="actor-1",
            action_type="publication_scheduled",
            platform="youtube",
            occurred_at=NOW,
            source="videoprocess.channel_ops",
        )
    )
    assert await store.apply_pds_decision(
        PDSDecisionEvent(
            event_id="shared-id",
            topic_version="pds.decisions.v1",
            actor_id="actor-1",
            action_type="publish",
            platform="youtube",
            verdict="block",
            score=0.8,
            decision_id="decision-1",
            occurred_at=NOW,
        )
    )

    features = await store.features_for("actor-1")
    assert features.publishes_24h == 1
    assert features.blocks_24h == 1


async def test_memory_store_prunes_dedupe_keys_after_ttl():
    current_time = NOW
    store = MemoryFeatureStore(now=lambda: current_time, dedupe_ttl=timedelta(days=7))
    event = VPActorActionEvent(
        event_id="ttl-id",
        topic_version="vp.actor.actions.v1",
        actor_id="actor-1",
        action_type="publication_scheduled",
        platform="youtube",
        occurred_at=NOW,
        source="videoprocess.channel_ops",
    )

    assert await store.apply_vp_action(event) is True
    current_time = NOW + timedelta(days=7, microseconds=1)
    assert await store.apply_vp_action(event) is True

    features = await store.features_for("actor-1")
    assert features.publishes_24h == 0
