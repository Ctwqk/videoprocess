from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
import uuid

import httpx
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent.discovery_policy import (
    DiscoveryPolicy,
    DiscoveryPolicyError,
)
from app.channel_agent.trend_ingesters.youtube_search import (
    TrendProviderError,
    YouTubeTrendIngester,
)
from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    DiscoveryIngestionRun,
)


SOURCE_YOUTUBE_SEARCH = "youtube_search"
QUERY_VERSION = "youtube-lane-keyword-v1"
RUN_STALE_AFTER = timedelta(minutes=15)


@dataclass(frozen=True)
class DiscoveryIngestionRequest:
    channel_id: uuid.UUID
    queue_item_id: uuid.UUID
    source: str
    scheduler_bucket: str
    attempt_count: int
    locked_by: str
    locked_at: datetime


@dataclass(frozen=True)
class DiscoveryIngestionResult:
    run_id: uuid.UUID
    channel_id: uuid.UUID
    source: str
    scheduler_bucket: str
    status: str
    query_count: int
    created_count: int
    refreshed_count: int
    expired_count: int
    quota_units_estimated: int


@dataclass(frozen=True)
class _RunClaim:
    run_id: uuid.UUID
    generation: int


class DiscoveryIngestionError(RuntimeError):
    pass


class DiscoveryIngestionConflictError(DiscoveryIngestionError):
    pass


class DiscoveryIngestionInProgressError(DiscoveryIngestionConflictError):
    pass


class DiscoveryIngestionAuthorityError(DiscoveryIngestionConflictError):
    pass


class DiscoveryIngestionPolicyError(DiscoveryIngestionConflictError):
    pass


class DiscoveryIngestionProviderError(DiscoveryIngestionError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class DiscoveryIngestionService:
    def __init__(
        self,
        *,
        youtube_client: Any | None = None,
        youtube_client_factory: Callable[[], Any] | None = None,
    ) -> None:
        if youtube_client is None and youtube_client_factory is None:
            raise ValueError("youtube_client or youtube_client_factory is required")
        if youtube_client is not None and youtube_client_factory is not None:
            raise ValueError("pass either youtube_client or youtube_client_factory")
        self.youtube_client = youtube_client
        self.youtube_client_factory = youtube_client_factory

    async def ingest(
        self,
        db: AsyncSession,
        request: DiscoveryIngestionRequest,
        now: datetime,
    ) -> DiscoveryIngestionResult:
        current = _as_utc(now)
        normalized = _normalize_request(request)
        policy = await self._validated_policy(
            db,
            channel_id=normalized.channel_id,
        )
        claim = await self._claim_run(
            db,
            request=normalized,
            policy=policy,
            now=current,
        )
        if isinstance(claim, DiscoveryIngestionResult):
            return claim
        try:
            youtube_client = self._youtube_client_after_claim()
        except Exception:
            await db.rollback()
            await self._mark_failed(
                db,
                claim=claim,
                now=current,
                error_code="provider_unavailable",
                query_count=0,
            )
            raise DiscoveryIngestionProviderError("provider_unavailable") from None
        ingester = YouTubeTrendIngester(
            youtube_client=youtube_client,
            min_view_count=policy.min_view_count,
            max_results=policy.max_results_per_query,
            max_queries=policy.max_queries_per_run,
            region_code=policy.region_code,
        )
        try:
            ingest_result = await ingester.ingest_channel(
                db,
                channel_id=str(normalized.channel_id),
                now=current,
            )
        except TrendProviderError as exc:
            error_code = _provider_error_code(exc.cause)
            await db.rollback()
            await self._mark_failed(
                db,
                claim=claim,
                now=current,
                error_code=error_code,
                query_count=exc.query_count,
            )
            raise DiscoveryIngestionProviderError(error_code) from exc

        terminal_channel = await self._lock_channel(db, normalized.channel_id)
        terminal_error_code: str | None = None
        if (
            terminal_channel is None
            or not terminal_channel.enabled
            or terminal_channel.halted_at is not None
        ):
            terminal_error_code = "channel_unavailable"
        elif terminal_channel.intake_paused_at is not None:
            terminal_error_code = "channel_intake_paused"
        if terminal_error_code is not None:
            await db.rollback()
            await self._mark_failed(
                db,
                claim=claim,
                now=current,
                error_code=terminal_error_code,
                query_count=ingest_result.query_count,
            )
            raise DiscoveryIngestionAuthorityError(terminal_error_code)

        run = await self._lock_owned_run(db, claim)
        run.status = "succeeded"
        run.query_count = ingest_result.query_count
        run.created_count = ingest_result.created_count
        run.refreshed_count = ingest_result.refreshed_count
        run.expired_count = ingest_result.expired_count
        run.quota_units_estimated = ingest_result.query_count * 100
        run.finished_at = current
        run.last_error_code = None
        await db.flush()
        result = _result_from_run(run)
        await db.commit()
        return result

    def _youtube_client_after_claim(self) -> Any:
        if self.youtube_client_factory is not None:
            return self.youtube_client_factory()
        return self.youtube_client

    async def _validated_policy(
        self,
        db: AsyncSession,
        *,
        channel_id: uuid.UUID,
    ) -> DiscoveryPolicy:
        channel = await db.get(ChannelProfile, channel_id)
        if channel is None:
            await db.rollback()
            raise LookupError("channel_not_found")
        if not channel.enabled or channel.halted_at is not None:
            await db.rollback()
            raise DiscoveryIngestionAuthorityError("channel_unavailable")
        try:
            policy = DiscoveryPolicy.from_content_mix(channel.content_mix_policy_json)
        except DiscoveryPolicyError:
            await db.rollback()
            raise DiscoveryIngestionPolicyError("invalid_discovery_policy") from None
        if not policy.enabled:
            await db.rollback()
            raise DiscoveryIngestionPolicyError("discovery_disabled")
        return policy

    async def _claim_run(
        self,
        db: AsyncSession,
        *,
        request: DiscoveryIngestionRequest,
        policy: DiscoveryPolicy,
        now: datetime,
    ) -> _RunClaim | DiscoveryIngestionResult:
        await self._lock_queue_authority(db, request)
        existing = await self._locked_run(db, request)
        if existing is not None:
            return await self._claim_existing(
                db,
                run=existing,
                request=request,
                policy=policy,
                now=now,
            )

        run = DiscoveryIngestionRun(
            channel_profile_id=request.channel_id,
            queue_item_id=request.queue_item_id,
            source=request.source,
            scheduler_bucket=request.scheduler_bucket,
            query_version=QUERY_VERSION,
            status="running",
            attempt_count=1,
            query_count=0,
            created_count=0,
            refreshed_count=0,
            expired_count=0,
            quota_units_estimated=0,
            policy_snapshot_json=asdict(policy),
            started_at=now,
            finished_at=None,
            last_error_code=None,
        )
        db.add(run)
        try:
            await db.flush()
            run_id = run.id
            generation = run.attempt_count
            await db.commit()
            return _RunClaim(run_id=run_id, generation=generation)
        except IntegrityError as exc:
            await db.rollback()
            await self._lock_queue_authority(db, request)
            existing = await self._locked_run(db, request)
            if existing is None:
                raise exc
            return await self._claim_existing(
                db,
                run=existing,
                request=request,
                policy=policy,
                now=now,
            )

    @staticmethod
    async def _lock_queue_authority(
        db: AsyncSession,
        request: DiscoveryIngestionRequest,
    ) -> None:
        queue_item = (
            await db.execute(
                select(ChannelOpsQueueItem)
                .where(ChannelOpsQueueItem.id == request.queue_item_id)
                .execution_options(populate_existing=True)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if queue_item is None or not _same_queue_authority(queue_item, request):
            await db.rollback()
            raise DiscoveryIngestionAuthorityError("queue_authority_changed")

    async def _claim_existing(
        self,
        db: AsyncSession,
        *,
        run: DiscoveryIngestionRun,
        request: DiscoveryIngestionRequest,
        policy: DiscoveryPolicy,
        now: datetime,
    ) -> _RunClaim | DiscoveryIngestionResult:
        if not _same_identity(run, request):
            await db.rollback()
            raise DiscoveryIngestionConflictError("run_identity_conflict")
        if run.status == "succeeded":
            result = _result_from_run(run)
            await db.rollback()
            return result
        if run.status == "running" and _as_utc(run.started_at) > now - RUN_STALE_AFTER:
            await db.rollback()
            raise DiscoveryIngestionInProgressError("discovery_run_in_progress")
        if run.status not in {"running", "failed"}:
            await db.rollback()
            raise DiscoveryIngestionConflictError("invalid_run_status")

        run.status = "running"
        run.attempt_count += 1
        run.query_count = 0
        run.created_count = 0
        run.refreshed_count = 0
        run.expired_count = 0
        run.quota_units_estimated = 0
        run.policy_snapshot_json = asdict(policy)
        run.started_at = now
        run.finished_at = None
        run.last_error_code = None
        run_id = run.id
        generation = run.attempt_count
        await db.commit()
        return _RunClaim(run_id=run_id, generation=generation)

    @staticmethod
    async def _locked_run(
        db: AsyncSession,
        request: DiscoveryIngestionRequest,
    ) -> DiscoveryIngestionRun | None:
        bucket_run = (
            await db.execute(
                select(DiscoveryIngestionRun)
                .where(DiscoveryIngestionRun.channel_profile_id == request.channel_id)
                .where(DiscoveryIngestionRun.source == request.source)
                .where(
                    DiscoveryIngestionRun.scheduler_bucket
                    == request.scheduler_bucket
                )
                .execution_options(populate_existing=True)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if bucket_run is not None:
            return bucket_run
        return (
            await db.execute(
                select(DiscoveryIngestionRun)
                .where(DiscoveryIngestionRun.queue_item_id == request.queue_item_id)
                .execution_options(populate_existing=True)
                .with_for_update()
            )
        ).scalar_one_or_none()

    @staticmethod
    async def _lock_channel(
        db: AsyncSession,
        channel_id: uuid.UUID,
    ) -> ChannelProfile | None:
        return (
            await db.execute(
                select(ChannelProfile)
                .where(ChannelProfile.id == channel_id)
                .execution_options(populate_existing=True)
                .with_for_update()
            )
        ).scalar_one_or_none()

    @staticmethod
    async def _mark_failed(
        db: AsyncSession,
        *,
        claim: _RunClaim,
        now: datetime,
        error_code: str,
        query_count: int,
    ) -> None:
        run = await DiscoveryIngestionService._lock_owned_run(db, claim)
        run.status = "failed"
        run.query_count = query_count
        run.created_count = 0
        run.refreshed_count = 0
        run.expired_count = 0
        run.quota_units_estimated = query_count * 100
        run.finished_at = now
        run.last_error_code = error_code
        await db.commit()

    @staticmethod
    async def _lock_owned_run(
        db: AsyncSession,
        claim: _RunClaim,
    ) -> DiscoveryIngestionRun:
        run = (
            await db.execute(
                select(DiscoveryIngestionRun)
                .where(DiscoveryIngestionRun.id == claim.run_id)
                .where(DiscoveryIngestionRun.status == "running")
                .where(
                    DiscoveryIngestionRun.attempt_count == claim.generation
                )
                .execution_options(populate_existing=True)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if run is None:
            await db.rollback()
            raise DiscoveryIngestionConflictError("run_authority_changed")
        return run


def _normalize_request(request: DiscoveryIngestionRequest) -> DiscoveryIngestionRequest:
    channel_id = _uuid(request.channel_id)
    queue_item_id = _uuid(request.queue_item_id)
    if request.source != SOURCE_YOUTUBE_SEARCH:
        raise ValueError("source must be youtube_search")
    if (
        type(request.scheduler_bucket) is not str
        or not request.scheduler_bucket
        or len(request.scheduler_bucket) > 64
    ):
        raise ValueError("scheduler_bucket must be a non-empty string of at most 64 characters")
    if type(request.attempt_count) is not int or request.attempt_count < 1:
        raise ValueError("attempt_count must be an integer greater than or equal to 1")
    if (
        type(request.locked_by) is not str
        or not request.locked_by.strip()
        or len(request.locked_by) > 255
    ):
        raise ValueError("locked_by must be a nonblank string of at most 255 characters")
    if (
        type(request.locked_at) is not datetime
        or request.locked_at.tzinfo is None
        or request.locked_at.utcoffset() is None
    ):
        raise ValueError("locked_at must include a timezone")
    locked_at = request.locked_at.astimezone(timezone.utc)
    if locked_at == datetime.min.replace(tzinfo=timezone.utc):
        raise ValueError("locked_at must not be zero")
    return DiscoveryIngestionRequest(
        channel_id=channel_id,
        queue_item_id=queue_item_id,
        source=request.source,
        scheduler_bucket=request.scheduler_bucket,
        attempt_count=request.attempt_count,
        locked_by=request.locked_by,
        locked_at=locked_at,
    )


def _same_identity(
    run: DiscoveryIngestionRun,
    request: DiscoveryIngestionRequest,
) -> bool:
    return (
        run.channel_profile_id == request.channel_id
        and run.queue_item_id == request.queue_item_id
        and run.source == request.source
        and run.scheduler_bucket == request.scheduler_bucket
        and run.query_version == QUERY_VERSION
    )


def _same_queue_authority(
    queue_item: ChannelOpsQueueItem,
    request: DiscoveryIngestionRequest,
) -> bool:
    payload = queue_item.payload_json
    return (
        queue_item.kind == "ingest_discovery"
        and queue_item.status == "running"
        and queue_item.channel_profile_id == request.channel_id
        and queue_item.attempt_count == request.attempt_count
        and queue_item.locked_by == request.locked_by
        and queue_item.locked_at is not None
        and _as_utc(queue_item.locked_at) == request.locked_at
        and isinstance(payload, dict)
        and payload.get("channel_id") == str(request.channel_id)
        and payload.get("source") == request.source
        and payload.get("bucket") == request.scheduler_bucket
        and payload.get("scheduler_bucket") == request.scheduler_bucket
    )


def _result_from_run(run: DiscoveryIngestionRun) -> DiscoveryIngestionResult:
    return DiscoveryIngestionResult(
        run_id=run.id,
        channel_id=run.channel_profile_id,
        source=run.source,
        scheduler_bucket=run.scheduler_bucket,
        status=run.status,
        query_count=run.query_count,
        created_count=run.created_count,
        refreshed_count=run.refreshed_count,
        expired_count=run.expired_count,
        quota_units_estimated=run.quota_units_estimated,
    )


def _provider_error_code(error: Exception) -> str:
    if isinstance(error, (TimeoutError, httpx.TimeoutException)):
        return "provider_timeout"
    if isinstance(error, PermissionError):
        return "provider_auth"
    if isinstance(error, httpx.HTTPStatusError):
        if error.response.status_code in {401, 403}:
            return "provider_auth"
        if error.response.status_code == 429:
            return "provider_quota"
    return "provider_contract"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _uuid(value: object) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
