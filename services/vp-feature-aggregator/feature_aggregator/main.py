from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from feature_aggregator.config import Settings, settings
from feature_aggregator.consumer import run_consumer
from feature_aggregator.schemas import FeatureResponse
from feature_aggregator.store import FeatureStore
from feature_aggregator.store.memory import MemoryFeatureStore


logger = logging.getLogger(__name__)
ConsumerRunner = Callable[..., Awaitable[None]]


def create_app(
    feature_store: FeatureStore | None = None,
    app_settings: Settings = settings,
    consumer_runner: ConsumerRunner = run_consumer,
) -> FastAPI:
    store = feature_store or MemoryFeatureStore()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        consumer_task: asyncio.Task[None] | None = None
        app.state.consumer_task = None
        app.state.consumer_error = None

        def record_consumer_completion(task: asyncio.Task[None]) -> None:
            try:
                task.result()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                app.state.consumer_error = exc
                logger.exception("Aggregator consumer task failed")

        if app_settings.enable_consumer:
            consumer_task = asyncio.create_task(
                consumer_runner(
                    store,
                    brokers=app_settings.kafka_brokers,
                    group_id=app_settings.kafka_group_id,
                    vp_actions_topic=app_settings.vp_actions_topic,
                    pds_decisions_topic=app_settings.pds_decisions_topic,
                    dead_letter_topic=app_settings.dead_letter_topic,
                )
            )
            app.state.consumer_task = consumer_task
            consumer_task.add_done_callback(record_consumer_completion)
        try:
            yield
        finally:
            if consumer_task is not None:
                if not consumer_task.done():
                    consumer_task.cancel()
                try:
                    await consumer_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

    app = FastAPI(title="VP Feature Aggregator", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> dict[str, str]:
        if app_settings.enable_consumer:
            task: asyncio.Task[None] | None = getattr(app.state, "consumer_task", None)
            error: BaseException | None = getattr(app.state, "consumer_error", None)
            if task is None:
                raise HTTPException(status_code=503, detail="consumer task missing")
            if error is not None:
                raise HTTPException(status_code=503, detail="consumer task failed")
            if task.done():
                if not task.cancelled():
                    task_error = task.exception()
                    if task_error is not None:
                        app.state.consumer_error = task_error
                        raise HTTPException(
                            status_code=503,
                            detail="consumer task failed",
                        )
                raise HTTPException(status_code=503, detail="consumer task stopped")
        return {"status": "ready"}

    @app.get("/v1/features/{actor_id}", response_model=FeatureResponse)
    async def get_features(actor_id: str) -> FeatureResponse:
        return await store.features_for(actor_id)

    return app


app = create_app()
