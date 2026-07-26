from __future__ import annotations

import json
import math
import uuid
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.channel_agent import ProductionTask
from app.models.job import JobStatus, NodeExecution, NodeStatus
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.services.job_execution_authority import (
    JobExecutionAuthorityBlocked,
    NodeExecutionClaim,
    lock_job_execution_authority,
    require_active_execution_authority,
    require_matching_node_execution_claim,
    require_registered_worker_node_claim,
    require_worker_registration_lease,
    require_worker_registration_margin,
)

SUBMISSION_LEASE_MARGIN_SECONDS = 150


def _session_is_postgresql(db: object) -> bool:
    get_bind = getattr(db, "get_bind", None)
    if not callable(get_bind):
        return True
    return getattr(getattr(get_bind(), "dialect", None), "name", None) == (
        "postgresql"
    )


@dataclass(frozen=True)
class UploadOperationContext:
    job_id: uuid.UUID
    node_execution_id: uuid.UUID
    execution_claim: NodeExecutionClaim
    input_artifact_id: uuid.UUID
    content_sha256: str
    title: str
    privacy: str


@dataclass(frozen=True)
class UploadOperationClaim:
    action: str
    operation: YouTubeUploadOperation


class UploadOperationConflictError(RuntimeError):
    pass


class YouTubeUploadOperationStore:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._active_submission_fence: ContextVar[
            UploadOperationContext | None
        ] = ContextVar(
            f"youtube-upload-submission-fence-{id(self)}",
            default=None,
        )

    @asynccontextmanager
    async def submission_fence(
        self,
        context: UploadOperationContext,
    ) -> AsyncIterator[None]:
        """Hold durable execution authority across the irreversible upload POST."""

        async with self._session_factory() as db:
            async with db.begin():
                if (
                    getattr(
                        context.execution_claim,
                        "worker_registration_id",
                        None,
                    )
                    is not None
                    and _session_is_postgresql(db)
                ):
                    await require_registered_worker_node_claim(
                        db,
                        context.execution_claim,
                    )
                    await require_worker_registration_margin(
                        db,
                        context.execution_claim,
                        minimum_margin_seconds=(
                            SUBMISSION_LEASE_MARGIN_SECONDS
                        ),
                    )
                    token = self._active_submission_fence.set(context)
                    try:
                        yield
                    finally:
                        self._active_submission_fence.reset(token)
                    return
                authority = await lock_job_execution_authority(
                    db,
                    context.job_id,
                    node_execution_id=context.node_execution_id,
                )
                require_active_execution_authority(
                    authority,
                    job_statuses={JobStatus.RUNNING},
                    node_statuses={NodeStatus.RUNNING},
                )
                require_matching_node_execution_claim(
                    authority,
                    context.execution_claim,
                )
                token = self._active_submission_fence.set(context)
                try:
                    yield
                finally:
                    self._active_submission_fence.reset(token)

    async def claim(self, context: UploadOperationContext) -> UploadOperationClaim:
        async with self._session_factory() as db:
            if (
                getattr(
                    context.execution_claim,
                    "worker_registration_id",
                    None,
                )
                is not None
                and _session_is_postgresql(db)
            ):
                operation = await self._reserve_registered(db, context)
                await db.commit()
                return UploadOperationClaim(
                    self._action_for(operation),
                    operation,
                )
            authority = await lock_job_execution_authority(
                db,
                context.job_id,
                node_execution_id=context.node_execution_id,
            )
            require_active_execution_authority(
                authority,
                job_statuses={JobStatus.RUNNING},
                node_statuses={NodeStatus.RUNNING},
            )
            require_matching_node_execution_claim(
                authority,
                context.execution_claim,
            )
            if (
                getattr(
                    context.execution_claim,
                    "worker_registration_id",
                    None,
                )
                is not None
            ):
                await require_worker_registration_lease(
                    db,
                    context.execution_claim,
                )
            existing = await self._operation_for_node(
                db,
                context.node_execution_id,
                for_update=True,
            )
            if existing is not None:
                return UploadOperationClaim(self._action_for(existing), existing)

            production_task_id = await self._production_task_id(db, context.job_id)
            operation = YouTubeUploadOperation(
                production_task_id=production_task_id,
                job_id=context.job_id,
                node_execution_id=context.node_execution_id,
                input_artifact_id=context.input_artifact_id,
                content_sha256=context.content_sha256,
                title=context.title,
                privacy=context.privacy,
                status="reserved",
            )
            db.add(operation)
            try:
                await db.commit()
            except IntegrityError:
                await db.rollback()
                existing = await self._operation_for_node(db, context.node_execution_id)
                if existing is not None:
                    return UploadOperationClaim(self._action_for(existing), existing)
                if production_task_id is not None:
                    conflicting = await self._operation_for_production_task(db, production_task_id)
                    if conflicting is not None:
                        raise UploadOperationConflictError(
                            "production task already has a YouTube upload operation"
                        ) from None
                raise

            await db.refresh(operation)
            return UploadOperationClaim("submit", operation)

    async def mark_attempting(
        self,
        operation_id: uuid.UUID,
        *,
        context: UploadOperationContext | None = None,
    ) -> YouTubeUploadOperation:
        async with self._session_factory() as db:
            if (
                context is not None
                and getattr(
                    context.execution_claim,
                    "worker_registration_id",
                    None,
                )
                is not None
                and _session_is_postgresql(db)
            ):
                operation = await self._transition_registered(
                    db,
                    operation_id,
                    context,
                    expected_status="reserved",
                    transition="attempting",
                )
                await db.commit()
                return operation
            await self._require_transition_context(
                db,
                operation_id,
                context,
            )
            result = await db.execute(
                update(YouTubeUploadOperation)
                .where(YouTubeUploadOperation.id == operation_id)
                .where(YouTubeUploadOperation.status == "reserved")
                .where(YouTubeUploadOperation.request_attempted_at.is_(None))
                .values(
                    request_attempted_at=datetime.now(timezone.utc),
                    updated_at=func.now(),
                )
                .returning(YouTubeUploadOperation)
            )
            updated_operation = result.scalar_one_or_none()
            if updated_operation is not None:
                await db.commit()
                return updated_operation

            await db.rollback()
            current = await self._operation(db, operation_id)
            raise ValueError(
                f"cannot begin submission from {current.status} operation"
            )

    async def mark_submitted(
        self,
        operation_id: uuid.UUID,
        manager_task_id: str,
        *,
        context: UploadOperationContext | None = None,
    ) -> YouTubeUploadOperation:
        canonical_manager_task_id = self._canonical_manager_task_id(manager_task_id)
        if canonical_manager_task_id is None:
            raise ValueError("manager task id must be a canonical UUID")

        async with self._session_factory() as db:
            if (
                context is not None
                and getattr(
                    context.execution_claim,
                    "worker_registration_id",
                    None,
                )
                is not None
                and _session_is_postgresql(db)
            ):
                operation = await self._transition_registered(
                    db,
                    operation_id,
                    context,
                    expected_status="reserved",
                    transition="submitted",
                    manager_task_id=canonical_manager_task_id,
                )
                await db.commit()
                return operation
            await self._require_transition_context(
                db,
                operation_id,
                context,
            )
            result = await db.execute(
                update(YouTubeUploadOperation)
                .where(YouTubeUploadOperation.id == operation_id)
                .where(YouTubeUploadOperation.status == "reserved")
                .values(
                    status="submitted",
                    manager_task_id=canonical_manager_task_id,
                    request_attempted_at=datetime.now(timezone.utc),
                    error_message=None,
                    updated_at=func.now(),
                )
                .returning(YouTubeUploadOperation)
            )
            updated_operation = result.scalar_one_or_none()
            if updated_operation is not None:
                await db.commit()
                return updated_operation

            await db.rollback()
            current = await self._operation(db, operation_id)
            if (
                current.status == "submitted"
                and current.manager_task_id == canonical_manager_task_id
            ):
                return current
            raise ValueError(f"cannot mark {current.status} operation submitted")

    async def mark_succeeded(
        self,
        operation_id: uuid.UUID,
        platform_video_id: str,
        receipt: dict[str, Any],
        *,
        context: UploadOperationContext | None = None,
    ) -> YouTubeUploadOperation:
        if not self._is_nonblank_string(platform_video_id):
            raise ValueError("platform video id is required")

        async with self._session_factory() as db:
            if (
                context is not None
                and getattr(
                    context.execution_claim,
                    "worker_registration_id",
                    None,
                )
                is not None
                and _session_is_postgresql(db)
            ):
                operation = await self._operation(db, operation_id)
                receipt_json = self._receipt_for(
                    title=operation.title,
                    privacy=operation.privacy,
                    platform_video_id=platform_video_id,
                    receipt=receipt,
                )
                operation = await self._transition_registered(
                    db,
                    operation_id,
                    context,
                    expected_status="submitted",
                    transition="succeeded",
                    platform_video_id=platform_video_id,
                    receipt_json=receipt_json,
                )
                await db.commit()
                return operation
            await self._require_transition_context(
                db,
                operation_id,
                context,
            )
            metadata_result = await db.execute(
                select(YouTubeUploadOperation.title, YouTubeUploadOperation.privacy).where(
                    YouTubeUploadOperation.id == operation_id
                )
            )
            metadata = metadata_result.one_or_none()
            if metadata is None:
                raise LookupError(f"YouTube upload operation {operation_id} was not found")
            receipt_json = self._receipt_for(
                title=metadata.title,
                privacy=metadata.privacy,
                platform_video_id=platform_video_id,
                receipt=receipt,
            )
            try:
                result = await db.execute(
                    update(YouTubeUploadOperation)
                    .where(YouTubeUploadOperation.id == operation_id)
                    .where(YouTubeUploadOperation.status == "submitted")
                    .values(
                        status="succeeded",
                        platform_video_id=platform_video_id,
                        receipt_json=receipt_json,
                        completed_at=datetime.now(timezone.utc),
                        error_message=None,
                        updated_at=func.now(),
                    )
                    .returning(YouTubeUploadOperation)
                )
                updated_operation = result.scalar_one_or_none()
                if updated_operation is not None:
                    await db.commit()
                    return updated_operation
            except IntegrityError:
                await db.rollback()
                conflicting = await self._operation_for_platform_video(db, platform_video_id)
                if conflicting is not None:
                    raise UploadOperationConflictError(
                        "platform video id already belongs to a YouTube upload operation"
                    ) from None
                raise

            await db.rollback()
            current = await self._operation(db, operation_id)
            if current.status == "succeeded":
                if current.platform_video_id == platform_video_id:
                    return current
                raise UploadOperationConflictError(
                    "operation already belongs to a different platform video id"
                )
            raise ValueError(f"cannot mark {current.status} operation succeeded")

    async def mark_uncertain(
        self,
        operation_id: uuid.UUID,
        error_message: str,
        *,
        context: UploadOperationContext | None = None,
    ) -> YouTubeUploadOperation:
        return await self._mark_terminal(
            operation_id,
            "uncertain",
            error_message,
            context=context,
        )

    async def mark_failed(
        self,
        operation_id: uuid.UUID,
        error_message: str,
        *,
        context: UploadOperationContext | None = None,
    ) -> YouTubeUploadOperation:
        return await self._mark_terminal(
            operation_id,
            "failed",
            error_message,
            context=context,
        )

    async def _mark_terminal(
        self,
        operation_id: uuid.UUID,
        status: str,
        error_message: str,
        *,
        context: UploadOperationContext | None,
    ) -> YouTubeUploadOperation:
        async with self._session_factory() as db:
            if (
                context is not None
                and getattr(
                    context.execution_claim,
                    "worker_registration_id",
                    None,
                )
                is not None
                and _session_is_postgresql(db)
            ):
                current = await self._operation(db, operation_id)
                operation = await self._transition_registered(
                    db,
                    operation_id,
                    context,
                    expected_status=current.status,
                    transition=status,
                    error_message=error_message,
                )
                await db.commit()
                return operation
            await self._require_transition_context(
                db,
                operation_id,
                context,
            )
            result = await db.execute(
                update(YouTubeUploadOperation)
                .where(YouTubeUploadOperation.id == operation_id)
                .where(YouTubeUploadOperation.status.in_(("reserved", "submitted")))
                .values(
                    status=status,
                    error_message=error_message,
                    updated_at=func.now(),
                )
                .returning(YouTubeUploadOperation)
            )
            updated_operation = result.scalar_one_or_none()
            if updated_operation is not None:
                await db.commit()
                return updated_operation

            await db.rollback()
            current = await self._operation(db, operation_id)
            if current.status == status:
                return current
            raise ValueError(f"cannot mark {current.status} operation {status}")

    async def _require_transition_context(
        self,
        db: AsyncSession,
        operation_id: uuid.UUID,
        context: UploadOperationContext | None,
    ) -> None:
        operation = await self._operation(db, operation_id)
        if context is None:
            registration_id = await db.scalar(
                select(NodeExecution.worker_registration_id).where(
                    NodeExecution.id == operation.node_execution_id
                )
            )
            if registration_id is not None:
                raise JobExecutionAuthorityBlocked(
                    "registered upload operation requires execution context"
                )
            return
        if (
            operation.job_id != context.job_id
            or operation.node_execution_id != context.node_execution_id
        ):
            raise JobExecutionAuthorityBlocked(
                "upload operation execution context changed"
            )
        if (
            getattr(
                context.execution_claim,
                "worker_registration_id",
                None,
            )
            is not None
            and _session_is_postgresql(db)
        ):
            await require_registered_worker_node_claim(
                db,
                context.execution_claim,
            )
            return
        active_context = self._active_submission_fence.get()
        if active_context == context:
            return
        authority = await lock_job_execution_authority(
            db,
            context.job_id,
            node_execution_id=context.node_execution_id,
        )
        require_active_execution_authority(
            authority,
            job_statuses={JobStatus.RUNNING},
            node_statuses={NodeStatus.RUNNING},
        )
        require_matching_node_execution_claim(
            authority,
            context.execution_claim,
        )
    async def _reserve_registered(
        self,
        db: AsyncSession,
        context: UploadOperationContext,
    ) -> YouTubeUploadOperation:
        claim = context.execution_claim
        registration_id = claim.worker_registration_id
        lease_epoch = claim.worker_lease_epoch
        if not isinstance(registration_id, uuid.UUID) or not isinstance(
            lease_epoch,
            int,
        ):
            raise JobExecutionAuthorityBlocked(
                "registered upload operation has no lease"
            )
        operation_id = await db.scalar(
            text(
                """
                SELECT public.vp_reserve_worker_youtube_upload(
                    :registration_id,
                    :lease_epoch,
                    :worker_id,
                    :worker_started_at,
                    :job_id,
                    :node_execution_id,
                    :input_artifact_id,
                    :content_sha256,
                    :title,
                    :privacy
                )
                """
            ),
            {
                "registration_id": registration_id,
                "lease_epoch": lease_epoch,
                "worker_id": claim.worker_id,
                "worker_started_at": claim.started_at,
                "job_id": context.job_id,
                "node_execution_id": context.node_execution_id,
                "input_artifact_id": context.input_artifact_id,
                "content_sha256": context.content_sha256,
                "title": context.title,
                "privacy": context.privacy,
            },
        )
        if not isinstance(operation_id, uuid.UUID):
            raise JobExecutionAuthorityBlocked(
                "registered upload operation identity is invalid"
            )
        return await self._operation(db, operation_id)

    async def _transition_registered(
        self,
        db: AsyncSession,
        operation_id: uuid.UUID,
        context: UploadOperationContext,
        *,
        expected_status: str,
        transition: str,
        manager_task_id: str | None = None,
        platform_video_id: str | None = None,
        receipt_json: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> YouTubeUploadOperation:
        claim = context.execution_claim
        registration_id = claim.worker_registration_id
        lease_epoch = claim.worker_lease_epoch
        if not isinstance(registration_id, uuid.UUID) or not isinstance(
            lease_epoch,
            int,
        ):
            raise JobExecutionAuthorityBlocked(
                "registered upload operation has no lease"
            )
        persisted_id = await db.scalar(
            text(
                """
                SELECT public.vp_transition_worker_youtube_upload(
                    :registration_id,
                    :lease_epoch,
                    :worker_id,
                    :worker_started_at,
                    :operation_id,
                    :expected_status,
                    :transition,
                    :manager_task_id,
                    :platform_video_id,
                    CAST(:receipt_json AS jsonb),
                    :error_message
                )
                """
            ),
            {
                "registration_id": registration_id,
                "lease_epoch": lease_epoch,
                "worker_id": claim.worker_id,
                "worker_started_at": claim.started_at,
                "operation_id": operation_id,
                "expected_status": expected_status,
                "transition": transition,
                "manager_task_id": manager_task_id,
                "platform_video_id": platform_video_id,
                "receipt_json": (
                    json.dumps(receipt_json)
                    if receipt_json is not None
                    else None
                ),
                "error_message": error_message,
            },
        )
        if persisted_id != operation_id:
            raise JobExecutionAuthorityBlocked(
                "registered upload transition identity changed"
            )
        db.expire_all()
        return await self._operation(db, operation_id)

    @staticmethod
    async def _production_task_id(db: AsyncSession, job_id: uuid.UUID) -> uuid.UUID | None:
        result = await db.execute(
            select(ProductionTask.id).where(ProductionTask.job_id == job_id).limit(2)
        )
        production_task_ids = list(result.scalars())
        if len(production_task_ids) > 1:
            raise UploadOperationConflictError("job is linked to multiple production tasks")
        return production_task_ids[0] if production_task_ids else None

    @staticmethod
    async def _operation(db: AsyncSession, operation_id: uuid.UUID) -> YouTubeUploadOperation:
        operation = await db.get(YouTubeUploadOperation, operation_id)
        if operation is None:
            raise LookupError(f"YouTube upload operation {operation_id} was not found")
        return operation

    @staticmethod
    async def _operation_for_node(
        db: AsyncSession,
        node_execution_id: uuid.UUID,
        *,
        for_update: bool = False,
    ) -> YouTubeUploadOperation | None:
        stmt = select(YouTubeUploadOperation).where(
            YouTubeUploadOperation.node_execution_id == node_execution_id
        )
        if for_update:
            stmt = stmt.with_for_update()
        result = await db.execute(stmt)
        return result.scalar_one_or_none()

    @staticmethod
    async def _operation_for_production_task(
        db: AsyncSession,
        production_task_id: uuid.UUID,
    ) -> YouTubeUploadOperation | None:
        result = await db.execute(
            select(YouTubeUploadOperation).where(
                YouTubeUploadOperation.production_task_id == production_task_id
            )
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def _operation_for_platform_video(
        db: AsyncSession,
        platform_video_id: str,
    ) -> YouTubeUploadOperation | None:
        result = await db.execute(
            select(YouTubeUploadOperation).where(
                YouTubeUploadOperation.platform_video_id == platform_video_id
            )
        )
        return result.scalar_one_or_none()

    @staticmethod
    def _action_for(operation: YouTubeUploadOperation) -> str:
        manager_task_id = YouTubeUploadOperationStore._canonical_manager_task_id(
            operation.manager_task_id
        )
        if operation.status == "reserved" and operation.request_attempted_at is None:
            return "submit"
        if operation.status == "submitted" and manager_task_id is not None:
            return "resume"
        if operation.status == "succeeded" and manager_task_id is not None:
            return "replay"
        return "block"

    @staticmethod
    def _receipt_for(
        *,
        title: str,
        privacy: str,
        platform_video_id: str,
        receipt: dict[str, Any],
    ) -> dict[str, Any]:
        raw_tags = receipt.get("tags", [])
        tags = [tag for tag in raw_tags if isinstance(tag, str)] if isinstance(raw_tags, list) else []
        url = receipt.get("url")
        if not isinstance(url, str):
            url = receipt.get("video_url")
        return {
            "video_id": platform_video_id,
            "url": url if isinstance(url, str) else "",
            "title": receipt["title"] if isinstance(receipt.get("title"), str) else title,
            "privacy": receipt["privacy"] if isinstance(receipt.get("privacy"), str) else privacy,
            "tags": tags,
            "quota_estimate": YouTubeUploadOperationStore._finite_number_or_none(
                receipt.get("quota_estimate", receipt.get("quota_units_estimated"))
            ),
        }

    @staticmethod
    def _finite_number_or_none(value: Any) -> int | float | None:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return number if math.isfinite(number) else None

    @staticmethod
    def _is_nonblank_string(value: Any) -> bool:
        return isinstance(value, str) and bool(value.strip())

    @staticmethod
    def _canonical_manager_task_id(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        try:
            canonical = str(uuid.UUID(value))
        except (AttributeError, ValueError):
            return None
        return canonical if value == canonical else None
