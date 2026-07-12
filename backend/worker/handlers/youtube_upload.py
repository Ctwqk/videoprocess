from __future__ import annotations

import asyncio
import contextlib
import hashlib
import math
import mimetypes
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.services.youtube_upload_operations import (
    UploadOperationContext,
    YouTubeUploadOperationStore,
)
from worker.handlers.base import BaseHandler


UPLOAD_INSERT_COST = 1_600
DEFAULT_POLL_INTERVAL_SECONDS = 1.0
DEFAULT_TIMEOUT_SECONDS = 600.0


class YouTubeUploadHandler(BaseHandler):
    """Submit private or unlisted uploads through the YouTubeManager."""

    def __init__(
        self,
        operation_store: YouTubeUploadOperationStore | None = None,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        client: httpx.AsyncClient | None = None,
        base_url: str | None = None,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__()
        if operation_store is None:
            if session_factory is None:
                raise ValueError("YouTubeUploadHandler requires an operation store or session factory")
            operation_store = YouTubeUploadOperationStore(session_factory)

        resolved_base_url = settings.youtube_manager_url if base_url is None else base_url
        self._operation_store = operation_store
        self._client = client
        self._base_url = str(resolved_base_url).rstrip("/")
        self._poll_interval_seconds = float(poll_interval_seconds)
        self._timeout_seconds = float(timeout_seconds)
        if not self._base_url:
            raise ValueError("YOUTUBE_MANAGER_URL is required for youtube uploads")
        if self._poll_interval_seconds < 0:
            raise ValueError("poll_interval_seconds must be non-negative")
        if self._timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")

    async def execute(
        self,
        node_config: dict,
        input_paths: dict[str, str],
        output_path: str,
    ) -> dict:
        input_file = input_paths.get("input")
        if not input_file:
            raise RuntimeError("youtube upload requires an input media file")

        title, description, tags, privacy = self._upload_metadata(node_config)
        self._validate_publish_policy(privacy)
        context = self._operation_context(
            node_config=node_config,
            input_file=input_file,
            title=title,
            privacy=privacy,
        )
        claim = await self._operation_store.claim(context)

        if claim.action == "replay":
            return self._copy_durable_receipt(claim.operation, input_file, output_path)
        if claim.action == "block":
            raise RuntimeError(
                "youtube upload operation cannot safely be retried from "
                f"{getattr(claim.operation, 'status', 'unknown')} state"
            )
        if claim.action not in {"submit", "resume"}:
            raise RuntimeError(f"unknown youtube upload operation action: {claim.action}")

        async with self._request_client() as client:
            if claim.action == "submit":
                await self._preflight_submission(claim.operation, client)
                manager_task_id = await self._submit_upload(
                    claim.operation,
                    client,
                    input_file=input_file,
                    title=title,
                    description=description,
                    tags=tags,
                    privacy=privacy,
                )
            else:
                resumed_manager_task_id = self._require_canonical_manager_task_id(
                    getattr(claim.operation, "manager_task_id", None)
                )
                if resumed_manager_task_id is None:
                    await self._mark_uncertain(claim.operation, "submitted operation has no canonical manager task")
                    raise RuntimeError("submitted youtube upload operation has no canonical manager task id")
                manager_task_id = resumed_manager_task_id

            succeeded_operation = await self._poll_for_completion(
                claim.operation,
                manager_task_id,
                client,
            )

        return self._copy_durable_receipt(succeeded_operation, input_file, output_path)

    @contextlib.asynccontextmanager
    async def _request_client(self) -> AsyncIterator[httpx.AsyncClient]:
        if self._client is not None:
            yield self._client
            return
        async with httpx.AsyncClient(timeout=httpx.Timeout(self._timeout_seconds)) as client:
            yield client

    async def _preflight_submission(self, operation: Any, client: httpx.AsyncClient) -> None:
        try:
            response = await client.get(f"{self._base_url}/api/auth/status")
            response.raise_for_status()
            payload = self._object_json(response, "YouTubeManager auth status")
            self._validate_auth_and_quota(payload)
        except Exception as exc:
            await self._operation_store.mark_failed(
                operation.id,
                "YouTubeManager authentication or quota preflight failed",
            )
            if isinstance(exc, RuntimeError):
                raise
            raise RuntimeError("YouTubeManager authentication or quota preflight failed") from exc

    async def _submit_upload(
        self,
        operation: Any,
        client: httpx.AsyncClient,
        *,
        input_file: str,
        title: str,
        description: str,
        tags: list[str],
        privacy: str,
    ) -> str:
        try:
            with open(input_file, "rb") as media_file:
                response = await client.post(
                    f"{self._base_url}/api/upload",
                    data={
                        "title": title,
                        "description": description,
                        "tags": ",".join(tags),
                        "privacy_status": privacy,
                    },
                    files={
                        "file": (
                            Path(input_file).name,
                            media_file,
                            self._mime_type(input_file),
                        )
                    },
                )
            response.raise_for_status()
            payload = self._object_json(response, "YouTubeManager upload submission")
            manager_task_id = self._require_canonical_manager_task_id(payload.get("task_id"))
            if manager_task_id is None:
                raise RuntimeError("YouTubeManager upload submission returned no canonical task id")
            await self._operation_store.mark_submitted(operation.id, manager_task_id)
            return manager_task_id
        except Exception as exc:
            await self._mark_uncertain(operation, "YouTubeManager upload submission outcome is uncertain")
            if isinstance(exc, RuntimeError):
                raise
            raise RuntimeError("YouTubeManager upload submission outcome is uncertain") from exc

    async def _poll_for_completion(
        self,
        operation: Any,
        manager_task_id: str,
        client: httpx.AsyncClient,
    ) -> Any:
        deadline = time.monotonic() + self._timeout_seconds
        while True:
            try:
                response = await client.get(f"{self._base_url}/api/status/{manager_task_id}")
                response.raise_for_status()
                payload = self._object_json(response, "YouTubeManager upload status")
                status = payload.get("status")
            except Exception as exc:
                await self._mark_uncertain(operation, "YouTubeManager upload status is uncertain")
                raise RuntimeError("YouTubeManager upload status is uncertain") from exc

            if status == "completed":
                return await self._record_completion(operation, payload)
            if status == "failed":
                error_message = payload.get("error")
                message = error_message.strip() if isinstance(error_message, str) else "manager reported failure"
                await self._operation_store.mark_failed(operation.id, message[:1_000])
                raise RuntimeError(f"YouTubeManager upload failed: {message}")
            if status not in {"pending", "uploading"}:
                await self._mark_uncertain(operation, "YouTubeManager returned an unknown upload status")
                raise RuntimeError("YouTubeManager returned an unknown upload status")
            if time.monotonic() >= deadline:
                await self._mark_uncertain(operation, "YouTubeManager upload polling timed out")
                raise RuntimeError("YouTubeManager upload polling timed out")
            await asyncio.sleep(self._poll_interval_seconds)

    async def _record_completion(self, operation: Any, payload: dict[str, Any]) -> Any:
        result = payload.get("result")
        if not isinstance(result, dict):
            await self._mark_uncertain(operation, "YouTubeManager completed upload has no result object")
            raise RuntimeError("YouTubeManager completed upload has no result object")
        video_id = result.get("video_id")
        url = result.get("url")
        if not isinstance(video_id, str) or not video_id.strip() or not isinstance(url, str):
            await self._mark_uncertain(operation, "YouTubeManager completed upload has invalid result fields")
            raise RuntimeError("YouTubeManager completed upload has invalid result fields")
        try:
            return await self._operation_store.mark_succeeded(operation.id, video_id.strip(), result)
        except Exception as exc:
            await self._mark_uncertain(operation, "YouTubeManager completion could not be recorded durably")
            raise RuntimeError("YouTubeManager completion could not be recorded durably") from exc

    async def _mark_uncertain(self, operation: Any, message: str) -> None:
        await self._operation_store.mark_uncertain(operation.id, message)

    @staticmethod
    def _upload_metadata(node_config: dict) -> tuple[str, str, list[str], str]:
        title = str(node_config.get("title") or "Untitled").strip() or "Untitled"
        description = str(node_config.get("description") or "")
        raw_tags = node_config.get("tags") or ""
        tags = [tag.strip() for tag in str(raw_tags).split(",") if tag.strip()]
        privacy = str(node_config.get("privacy") or "private").strip().lower()
        return title, description, tags, privacy

    def _operation_context(
        self,
        *,
        node_config: dict,
        input_file: str,
        title: str,
        privacy: str,
    ) -> UploadOperationContext:
        artifact_ids = node_config.get("_input_artifact_ids")
        if not isinstance(artifact_ids, dict) or "input" not in artifact_ids:
            raise RuntimeError("youtube upload requires _input_artifact_ids.input worker context")
        return UploadOperationContext(
            job_id=self._require_uuid(node_config.get("_job_id"), "_job_id"),
            node_execution_id=self._require_uuid(
                node_config.get("_node_execution_id"), "_node_execution_id"
            ),
            input_artifact_id=self._require_uuid(artifact_ids["input"], "_input_artifact_ids.input"),
            content_sha256=self._content_sha256(input_file),
            title=title,
            privacy=privacy,
        )

    @staticmethod
    def _validate_publish_policy(privacy: str) -> None:
        if privacy not in {"private", "unlisted"}:
            raise RuntimeError("youtube upload privacy must be private or unlisted")
        if os.environ.get("YOUTUBE_PUBLISH_ENABLED", "false").strip().lower() != "true":
            raise RuntimeError("YOUTUBE_PUBLISH_ENABLED=true is required for youtube uploads")
        if os.environ.get("PUBLIC_PUBLISH_ENABLED", "false").strip().lower() != "false":
            raise RuntimeError("PUBLIC_PUBLISH_ENABLED=false is required for youtube uploads")

    @staticmethod
    def _validate_auth_and_quota(payload: dict[str, Any]) -> None:
        if payload.get("authenticated") is not True:
            raise RuntimeError("YouTubeManager is not authenticated")
        quota = payload.get("quota_estimate")
        if not isinstance(quota, dict):
            raise RuntimeError("YouTubeManager quota estimate is missing")
        daily_limit = YouTubeUploadHandler._finite_nonnegative(quota.get("daily_limit"))
        estimated_units_used = YouTubeUploadHandler._finite_nonnegative(
            quota.get("estimated_units_used")
        )
        remaining = YouTubeUploadHandler._finite_nonnegative(
            quota.get("estimated_units_remaining")
        )
        upload_cost = YouTubeUploadHandler._finite_nonnegative(
            quota.get("upload_cost_per_request")
        )
        if None in (daily_limit, estimated_units_used, remaining, upload_cost):
            raise RuntimeError("YouTubeManager quota estimate is malformed")
        assert daily_limit is not None
        assert remaining is not None
        assert upload_cost is not None
        if daily_limit < upload_cost or upload_cost < UPLOAD_INSERT_COST or remaining < upload_cost:
            raise RuntimeError("YouTubeManager quota estimate is insufficient for an upload")

    @staticmethod
    def _finite_nonnegative(value: Any) -> float | None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else None

    @staticmethod
    def _require_uuid(value: Any, field_name: str) -> uuid.UUID:
        try:
            return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
        except (AttributeError, TypeError, ValueError) as exc:
            raise RuntimeError(f"youtube upload requires a valid {field_name} worker context") from exc

    @staticmethod
    def _content_sha256(path: str) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as media_file:
            for chunk in iter(lambda: media_file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _object_json(response: httpx.Response, label: str) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"{label} returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"{label} returned a non-object JSON response")
        return payload

    @staticmethod
    def _require_canonical_manager_task_id(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        try:
            canonical = str(uuid.UUID(value))
        except (AttributeError, ValueError):
            return None
        return canonical if value == canonical else None

    @staticmethod
    def _mime_type(path: str) -> str:
        return mimetypes.guess_type(path)[0] or "application/octet-stream"

    @staticmethod
    def _copy_durable_receipt(operation: Any, input_file: str, output_path: str) -> dict:
        receipt = getattr(operation, "receipt_json", None)
        if not isinstance(receipt, dict):
            raise RuntimeError("youtube upload operation has no durable receipt")
        video_id = receipt.get("video_id")
        if not isinstance(video_id, str) or not video_id.strip():
            raise RuntimeError("youtube upload operation has an invalid durable receipt")
        shutil.copy2(input_file, output_path)
        return {"youtube": dict(receipt)}
