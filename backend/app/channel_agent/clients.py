from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.artifact import Artifact, ArtifactKind
from app.models.autoflow import AutoFlowRun as AutoFlowRunModel
from app.models.job import Job, JobStatus, NodeExecution
from app.pds_client import (
    NoopPDSClient as NoopPDSClient,
    PDSDecision as PDSDecision,
    PDSDecisionRequest as PDSDecisionRequest,
    PolicyDecisionClient as PolicyDecisionClient,
)


@dataclass(frozen=True)
class AutoFlowPlanObservation:
    plan_id: str
    pipeline_definition: dict[str, Any]

    @property
    def upload_node_count(self) -> int:
        return sum(1 for node in self.pipeline_definition.get("nodes", []) if node.get("type") == "youtube_upload")


@dataclass(frozen=True)
class AutoFlowExecutionObservation:
    run_id: str
    pipeline_id: str | None
    job_id: str | None
    status: str
    error_message: str | None = None


@dataclass(frozen=True)
class AutoFlowJobObservation:
    run_id: str
    pipeline_id: str | None
    job_id: str | None
    status: str
    error_message: str | None = None
    youtube: dict[str, Any] | None = None


class AutoFlowClient(Protocol):
    async def plan_task(self, task, request: dict[str, Any]) -> AutoFlowPlanObservation:
        ...

    async def execute_task(self, task, request: dict[str, Any]) -> AutoFlowExecutionObservation:
        ...

    async def observe_job(self, db, *, run_id: str, job_id: str) -> AutoFlowJobObservation:
        ...


class YouTubeClient(Protocol):
    async def quota_remaining_fraction(self, account) -> float:
        ...

    async def schedule_publish(self, *, video_id: str, scheduled_at: datetime, privacy: str) -> dict[str, Any]:
        ...

    async def refresh_token(self, account) -> bool:
        ...


class MiniMaxClient(Protocol):
    async def generate_thumbnail(self, *, prompt: str, title: str) -> dict[str, Any]:
        ...


class FakeAutoFlowClient:
    def __init__(
        self,
        *,
        include_upload: bool = True,
        youtube_video_id: str = "yt-fake-1",
        observe_running_once: bool = False,
    ):
        self.include_upload = include_upload
        self.youtube_video_id = youtube_video_id
        self.observe_running_once = observe_running_once
        self.requests: list[dict[str, Any]] = []
        self._running_observed: set[str] = set()

    async def plan_task(self, task, request: dict[str, Any]) -> AutoFlowPlanObservation:
        self.requests.append(dict(request))
        nodes = [{"id": "transcode_1", "type": "transcode"}]
        if self.include_upload:
            nodes.append({"id": "youtube_upload_1", "type": "youtube_upload"})
        return AutoFlowPlanObservation(
            plan_id=str(uuid.uuid4()),
            pipeline_definition={"nodes": nodes, "edges": []},
        )

    async def execute_task(self, task, request: dict[str, Any]) -> AutoFlowExecutionObservation:
        seed = f"channel-agent:{task.id}:{task.autoflow_plan_id}"
        return AutoFlowExecutionObservation(
            run_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{seed}:run")),
            pipeline_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{seed}:pipeline")),
            job_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{seed}:job")),
            status="running",
        )

    async def observe_job(self, db, *, run_id: str, job_id: str) -> AutoFlowJobObservation:
        key = f"{run_id}:{job_id}"
        if self.observe_running_once and key not in self._running_observed:
            self._running_observed.add(key)
            return AutoFlowJobObservation(
                run_id=run_id,
                pipeline_id=None,
                job_id=job_id,
                status="running",
            )
        return AutoFlowJobObservation(
            run_id=run_id,
            pipeline_id=None,
            job_id=job_id,
            status="succeeded",
            youtube={"video_id": self.youtube_video_id},
        )


class LocalAutoFlowClient:
    def __init__(self, *, session_factory=None) -> None:
        self.session_factory = session_factory

    async def plan_task(self, task, request: dict[str, Any]) -> AutoFlowPlanObservation:
        from app.autoflow.service import autoflow_service
        from app.db import async_session
        from app.schemas.autoflow import AutoFlowRequest

        factory = self.session_factory or async_session
        async with factory() as db:
            plan = await autoflow_service.plan(AutoFlowRequest.model_validate(request), db)
        return AutoFlowPlanObservation(
            plan_id=plan.plan_id,
            pipeline_definition=plan.pipeline_definition.model_dump(mode="json"),
        )

    async def execute_task(self, task, request: dict[str, Any]) -> AutoFlowExecutionObservation:
        if not task.autoflow_plan_id:
            return AutoFlowExecutionObservation(
                run_id="",
                pipeline_id=None,
                job_id=None,
                status="failed",
                error_message="Production task has no AutoFlow plan id",
            )

        from app.autoflow.service import autoflow_service
        from app.db import async_session
        from app.schemas.autoflow import AutoFlowExecuteRequest

        factory = self.session_factory or async_session
        try:
            async with factory() as db:
                run = await autoflow_service.execute(
                    AutoFlowExecuteRequest(plan_id=str(task.autoflow_plan_id), execute=True),
                    db,
                )
        except (PermissionError, ValueError) as exc:
            return AutoFlowExecutionObservation(
                run_id="",
                pipeline_id=None,
                job_id=None,
                status="failed",
                error_message=str(exc),
            )
        return AutoFlowExecutionObservation(
            run_id=run.run_id,
            pipeline_id=run.pipeline_id,
            job_id=run.job_id,
            status=_execution_status(run.status),
            error_message=run.error_message,
        )

    async def observe_job(self, db: AsyncSession, *, run_id: str, job_id: str) -> AutoFlowJobObservation:
        run_uuid = _uuid_or_none(run_id)
        if run_uuid is None:
            return AutoFlowJobObservation(
                run_id=run_id,
                pipeline_id=None,
                job_id=job_id,
                status="failed",
                error_message="Invalid AutoFlow run id",
            )

        run = await db.get(AutoFlowRunModel, run_uuid)
        if run is None:
            return AutoFlowJobObservation(
                run_id=run_id,
                pipeline_id=None,
                job_id=job_id,
                status="failed",
                error_message="AutoFlow run not found",
            )

        job_uuid = _uuid_or_none(job_id)
        if job_uuid is None:
            return AutoFlowJobObservation(
                run_id=run_id,
                pipeline_id=None,
                job_id=job_id,
                status="failed",
                error_message="Invalid AutoFlow job id",
            )

        if run.job_id is None:
            return AutoFlowJobObservation(
                run_id=str(run.id),
                pipeline_id=str(run.pipeline_id) if run.pipeline_id else None,
                job_id=job_id,
                status="failed",
                error_message=f"AutoFlow run/job mismatch: run {run.id} has no linked job",
            )

        if run.job_id != job_uuid:
            return AutoFlowJobObservation(
                run_id=str(run.id),
                pipeline_id=str(run.pipeline_id) if run.pipeline_id else None,
                job_id=job_id,
                status="failed",
                error_message=f"AutoFlow run/job mismatch: run {run.id} is linked to job {run.job_id}",
            )

        job = await db.get(Job, job_uuid)
        if job is None:
            return AutoFlowJobObservation(
                run_id=str(run.id),
                pipeline_id=str(run.pipeline_id) if run.pipeline_id else None,
                job_id=job_id,
                status="failed",
                error_message="AutoFlow job not found",
            )

        status = _job_status(job.status)
        youtube = await self._youtube_from_job(db, job) if status == "succeeded" else None
        return AutoFlowJobObservation(
            run_id=str(run.id),
            pipeline_id=str(run.pipeline_id or job.pipeline_id) if (run.pipeline_id or job.pipeline_id) else None,
            job_id=str(job.id),
            status=status,
            error_message=job.error_message if status == "failed" else None,
            youtube=youtube,
        )

    async def _youtube_from_job(self, db: AsyncSession, job: Job) -> dict[str, Any] | None:
        result = await db.execute(
            select(NodeExecution)
            .where(NodeExecution.job_id == job.id)
            .where(NodeExecution.node_type == "youtube_upload")
            .order_by(NodeExecution.node_id.asc())
        )
        node = result.scalars().first()
        if node is None:
            return None

        artifact: Artifact | None = None
        if node.output_artifact_id:
            artifact = await db.get(Artifact, node.output_artifact_id)

        if artifact is None:
            artifact_result = await db.execute(
                select(Artifact)
                .where(Artifact.job_id == job.id)
                .where(Artifact.node_execution_id == node.id)
                .where(Artifact.kind == ArtifactKind.FINAL)
                .order_by(Artifact.created_at.desc())
            )
            artifact = artifact_result.scalars().first()

        if artifact is None:
            return None
        return _youtube_from_media_info(artifact.media_info)


class FakeYouTubeClient:
    def __init__(self, *, quota_remaining_fraction: float = 1.0, token_valid: bool = True):
        self._quota_remaining_fraction = quota_remaining_fraction
        self.token_valid = token_valid
        self.scheduled: list[dict[str, Any]] = []

    async def quota_remaining_fraction(self, account) -> float:
        return self._quota_remaining_fraction

    async def schedule_publish(self, *, video_id: str, scheduled_at: datetime, privacy: str) -> dict[str, Any]:
        payload = {"video_id": video_id, "scheduled_at": scheduled_at.isoformat(), "privacy": privacy}
        self.scheduled.append(payload)
        return payload

    async def refresh_token(self, account) -> bool:
        return self.token_valid


class FakeMiniMaxClient:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    async def generate_thumbnail(self, *, prompt: str, title: str) -> dict[str, Any]:
        self.calls.append({"prompt": prompt, "title": title})
        if self.fail:
            raise RuntimeError("minimax failed")
        return {"storage_path": f"/tmp/{title or 'thumbnail'}.png", "provider": "minimax"}


class MiniMaxImageClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        endpoint: str | None = None,
        model: str | None = None,
        timeout_seconds: float | None = None,
        retry_count: int | None = None,
        max_qps: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.api_key = settings.minimax_api_key if api_key is None else api_key
        self.endpoint = settings.minimax_image_generation_url if endpoint is None else endpoint
        self.model = settings.minimax_model if model is None else model
        self.timeout_seconds = settings.minimax_timeout_seconds if timeout_seconds is None else timeout_seconds
        self.retry_count = settings.minimax_retry_count if retry_count is None else retry_count
        self.max_qps = settings.minimax_max_qps if max_qps is None else max_qps
        self.transport = transport
        self._rate_lock = asyncio.Lock()
        self._last_request_at = 0.0

    async def generate_thumbnail(self, *, prompt: str, title: str) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("MINIMAX_API_KEY is not configured")

        payload = {
            "model": self.model,
            "prompt": _thumbnail_prompt(prompt=prompt, title=title),
            "aspect_ratio": "16:9",
            "response_format": "url",
            "n": 1,
            "prompt_optimizer": True,
            "aigc_watermark": False,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        last_error: Exception | None = None
        for attempt in range(self.retry_count + 1):
            try:
                await self._pace()
                async with httpx.AsyncClient(timeout=self.timeout_seconds, transport=self.transport) as client:
                    response = await client.post(self.endpoint, headers=headers, json=payload)
                    response.raise_for_status()
                body = response.json()
                urls = list(((body.get("data") or {}).get("image_urls") or []))
                if not urls:
                    raise RuntimeError("MiniMax image_generation returned no image_urls")
                return {
                    "provider": "minimax",
                    "request_id": body.get("id"),
                    "image_url": urls[0],
                    "raw": body,
                }
            except Exception as exc:
                last_error = exc
                if attempt >= self.retry_count:
                    break
                await asyncio.sleep(0.5 * (attempt + 1))

        raise RuntimeError(f"MiniMax thumbnail generation failed: {last_error}") from last_error

    async def _pace(self) -> None:
        if self.max_qps <= 0:
            return
        interval = 1.0 / self.max_qps
        loop = asyncio.get_running_loop()
        async with self._rate_lock:
            now = loop.time()
            wait_seconds = interval - (now - self._last_request_at)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._last_request_at = loop.time()


def _thumbnail_prompt(*, prompt: str, title: str) -> str:
    base = title.strip() or prompt.strip()[:80] or "YouTube thumbnail"
    return (
        f"YouTube thumbnail for: {base}. "
        "High contrast, clear subject, readable composition, no text overlay, 16:9."
    )


def _status_value(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower()


def _execution_status(value: Any) -> str:
    status = _status_value(value)
    if status in {"failed", "cancelled", "partially_failed"}:
        return "failed"
    if status == "succeeded":
        return "succeeded"
    return "running"


def _job_status(value: Any) -> str:
    status = _status_value(value)
    if status == _status_value(JobStatus.SUCCEEDED):
        return "succeeded"
    if status in {
        _status_value(JobStatus.FAILED),
        _status_value(JobStatus.CANCELLED),
        _status_value(JobStatus.PARTIALLY_FAILED),
    }:
        return "failed"
    return "running"


def _youtube_from_media_info(media_info: Any) -> dict[str, Any] | None:
    if not isinstance(media_info, dict):
        return None
    youtube = media_info.get("youtube")
    if not isinstance(youtube, dict):
        return None
    video_id = str(youtube.get("video_id") or "").strip()
    if not video_id:
        return None
    return dict(youtube)


def _uuid_or_none(value: Any) -> uuid.UUID | None:
    try:
        return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None
