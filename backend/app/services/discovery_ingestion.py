from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
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
from app.models.channel_agent import ChannelProfile, DiscoveryIngestionRun


SOURCE_YOUTUBE_SEARCH = "youtube_search"
QUERY_VERSION = "youtube-lane-keyword-v1"
RUN_STALE_AFTER = timedelta(minutes=15)


@dataclass(frozen=True)
class DiscoveryIngestionRequest:
    channel_id: uuid.UUID
    queue_item_id: uuid.UUID
    source: str
    scheduler_bucket: str


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
    def __init__(self, *, youtube_client) -> None:
        self.youtube_client = youtube_client

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
        run_id = claim.run_id

        ingester = YouTubeTrendIngester(
            youtube_client=self.youtube_client,
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
                run_id=run_id,
                now=current,
                error_code=error_code,
                query_count=exc.query_count,
            )
            raise DiscoveryIngestionProviderError(error_code) from exc

        terminal_channel = await self._lock_channel(db, normalized.channel_id)
        if (
            terminal_channel is None
            or not terminal_channel.enabled
            or terminal_channel.halted_at is not None
        ):
            await db.rollback()
            await self._mark_failed(
                db,
                run_id=run_id,
                now=current,
                error_code="channel_unavailable",
                query_count=ingest_result.query_count,
            )
            raise DiscoveryIngestionAuthorityError("channel_unavailable")

        run = await db.get(DiscoveryIngestionRun, run_id)
        if run is None or run.status != "running":
            await db.rollback()
            raise DiscoveryIngestionConflictError("run_authority_changed")
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
            await db.commit()
            return _RunClaim(run_id=run_id)
        except IntegrityError as exc:
            await db.rollback()
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
        await db.commit()
        return _RunClaim(run_id=run_id)

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
        run_id: uuid.UUID,
        now: datetime,
        error_code: str,
        query_count: int,
    ) -> None:
        run = (
            await db.execute(
                select(DiscoveryIngestionRun)
                .where(DiscoveryIngestionRun.id == run_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if run is None:
            await db.rollback()
            raise DiscoveryIngestionConflictError("run_not_found")
        if run.status != "running":
            await db.rollback()
            raise DiscoveryIngestionConflictError("run_authority_changed")
        run.status = "failed"
        run.query_count = query_count
        run.created_count = 0
        run.refreshed_count = 0
        run.expired_count = 0
        run.quota_units_estimated = query_count * 100
        run.finished_at = now
        run.last_error_code = error_code
        await db.commit()


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
    return DiscoveryIngestionRequest(
        channel_id=channel_id,
        queue_item_id=queue_item_id,
        source=request.source,
        scheduler_bucket=request.scheduler_bucket,
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
