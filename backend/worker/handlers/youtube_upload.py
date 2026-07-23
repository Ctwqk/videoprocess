from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import math
import mimetypes
import os
import shutil
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.services.job_execution_authority import NodeExecutionClaim
from app.services.youtube_upload_operations import (
    UploadOperationContext,
    YouTubeUploadOperationStore,
)
from worker.handlers.base import BaseHandler, CancelledError


UPLOAD_INSERT_COST = 1_600
DEFAULT_POLL_INTERVAL_SECONDS = 1.0
DEFAULT_TIMEOUT_SECONDS = 600.0
DEFINITE_UPLOAD_REJECTION_STATUSES = frozenset({400, 401, 403, 404, 413, 415, 422})


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
        if not math.isfinite(self._poll_interval_seconds) or self._poll_interval_seconds < 0:
            raise ValueError("poll_interval_seconds must be finite and non-negative")
        if not math.isfinite(self._timeout_seconds) or self._timeout_seconds < 0:
            raise ValueError("timeout_seconds must be finite and non-negative")

    async def execute(
        self,
        node_config: dict,
        input_paths: dict[str, str],
        output_path: str,
    ) -> dict:
        self._raise_if_cancelled()
        input_file = input_paths.get("input")
        if not input_file:
            raise RuntimeError("youtube upload requires an input media file")

        snapshot_path = self._create_input_snapshot(input_file)
        try:
            title, description, tags, privacy = self._upload_metadata(node_config)
            self._validate_publish_policy(privacy)
            content_sha256 = self._content_sha256(snapshot_path)
            context = self._operation_context(
                node_config=node_config,
                content_sha256=content_sha256,
                title=title,
                privacy=privacy,
            )
            self._raise_if_cancelled()
            claim = await self._operation_store.claim(context)
            operation = claim.operation
            self._raise_if_cancelled()

            if claim.action != "block":
                await self._verify_claim_content_hash(
                    claim.action,
                    operation,
                    content_sha256,
                )

            if claim.action == "replay":
                return self._copy_durable_receipt(operation, snapshot_path, output_path)
            if claim.action == "block":
                raise RuntimeError(
                    "youtube upload operation cannot safely be retried from "
                    f"{getattr(operation, 'status', 'unknown')} state"
                )
            if claim.action not in {"submit", "resume"}:
                raise RuntimeError(f"unknown youtube upload operation action: {claim.action}")

            async with self._request_client() as client:
                if claim.action == "submit":
                    async with self._operation_store.submission_fence(context):
                        await self._preflight_submission(operation, client)
                        self._raise_if_cancelled()
                        operation = await self._operation_store.mark_attempting(
                            operation.id
                        )
                        manager_task_id = await self._submit_upload(
                            operation,
                            client,
                            input_file=snapshot_path,
                            title=title,
                            description=description,
                            tags=tags,
                            privacy=privacy,
                        )
                else:
                    resumed_manager_task_id = self._require_canonical_manager_task_id(
                        getattr(operation, "manager_task_id", None)
                    )
                    if resumed_manager_task_id is None:
                        await self._mark_uncertain(
                            operation,
                            "submitted operation has no canonical manager task",
                        )
                        raise RuntimeError("submitted youtube upload operation has no canonical manager task id")
                    manager_task_id = resumed_manager_task_id

                succeeded_operation = await self._poll_for_completion(
                    operation,
                    manager_task_id,
                    client,
                )

            self._raise_if_cancelled()
            return self._copy_durable_receipt(succeeded_operation, snapshot_path, output_path)
        finally:
            Path(snapshot_path).unlink(missing_ok=True)

    @contextlib.asynccontextmanager
    async def _request_client(self) -> AsyncIterator[httpx.AsyncClient]:
        if self._client is not None:
            yield self._client
            return
        async with httpx.AsyncClient(timeout=httpx.Timeout(self._timeout_seconds)) as client:
            yield client

    async def _preflight_submission(self, operation: Any, client: httpx.AsyncClient) -> None:
        try:
            response = await self._await_request(client.get(f"{self._base_url}/api/auth/status"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._operation_store.mark_failed(
                operation.id,
                "YouTubeManager authentication or quota preflight failed",
            )
            raise RuntimeError("YouTubeManager authentication or quota preflight failed") from exc
        except (KeyboardInterrupt, SystemExit):
            raise

        self._raise_if_cancelled()
        try:
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
        self._raise_if_cancelled()
        try:
            with open(input_file, "rb") as media_file:
                self._raise_if_cancelled()
                response = await self._await_request(
                    client.post(
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
                )
        except asyncio.CancelledError:
            await self._mark_uncertain(operation, "YouTubeManager upload submission was cancelled")
            raise
        except Exception as exc:
            await self._mark_uncertain(operation, "YouTubeManager upload submission outcome is uncertain")
            raise RuntimeError("YouTubeManager upload submission outcome is uncertain") from exc
        except (KeyboardInterrupt, SystemExit):
            await self._mark_uncertain(operation, "YouTubeManager upload submission was interrupted")
            raise

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if self._cancelled:
                await self._mark_uncertain(operation, "YouTubeManager upload submission was cancelled")
                raise CancelledError("youtube upload cancelled after submission") from exc
            if exc.response.status_code in DEFINITE_UPLOAD_REJECTION_STATUSES:
                await self._operation_store.mark_failed(
                    operation.id,
                    f"YouTubeManager rejected upload request with HTTP {exc.response.status_code}",
                )
                raise RuntimeError("YouTubeManager rejected the upload request") from exc
            await self._mark_uncertain(operation, "YouTubeManager upload submission outcome is uncertain")
            raise RuntimeError("YouTubeManager upload submission outcome is uncertain") from exc

        try:
            payload = self._object_json(response, "YouTubeManager upload submission")
            manager_task_id = self._require_canonical_manager_task_id(payload.get("task_id"))
            if manager_task_id is None:
                raise RuntimeError("YouTubeManager upload submission returned no canonical task id")
            await self._persist_manager_task(operation, manager_task_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._mark_uncertain(operation, "YouTubeManager upload submission outcome is uncertain")
            if isinstance(exc, RuntimeError):
                raise
            raise RuntimeError("YouTubeManager upload submission outcome is uncertain") from exc

        if self._cancelled:
            await self._mark_uncertain(operation, "YouTubeManager upload submission was cancelled")
            raise CancelledError("youtube upload cancelled after submission")
        return manager_task_id

    async def _poll_for_completion(
        self,
        operation: Any,
        manager_task_id: str,
        client: httpx.AsyncClient,
    ) -> Any:
        deadline = time.monotonic() + self._timeout_seconds
        while True:
            if self._cancelled:
                await self._mark_uncertain(operation, "YouTubeManager upload polling was cancelled")
                raise CancelledError("youtube upload cancelled during polling")
            try:
                response = await self._await_request(
                    client.get(f"{self._base_url}/api/status/{manager_task_id}")
                )
            except asyncio.CancelledError:
                await self._mark_uncertain(operation, "YouTubeManager upload polling was cancelled")
                raise
            except Exception as exc:
                await self._mark_uncertain(operation, "YouTubeManager upload status is uncertain")
                raise RuntimeError("YouTubeManager upload status is uncertain") from exc
            except (KeyboardInterrupt, SystemExit):
                await self._mark_uncertain(operation, "YouTubeManager upload polling was interrupted")
                raise

            if self._cancelled:
                await self._mark_uncertain(operation, "YouTubeManager upload polling was cancelled")
                raise CancelledError("youtube upload cancelled during polling")
            try:
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
        platform_video_id = video_id.strip()
        if url != f"https://www.youtube.com/watch?v={platform_video_id}":
            await self._mark_uncertain(operation, "YouTubeManager completed upload has invalid result fields")
            raise RuntimeError("YouTubeManager completed upload has invalid result fields")
        try:
            return await self._operation_store.mark_succeeded(operation.id, platform_video_id, result)
        except Exception as exc:
            await self._mark_uncertain(operation, "YouTubeManager completion could not be recorded durably")
            raise RuntimeError("YouTubeManager completion could not be recorded durably") from exc

    async def _await_request(self, request: Awaitable[httpx.Response]) -> httpx.Response:
        async with asyncio.timeout(self._timeout_seconds):
            return await request

    async def _persist_manager_task(self, operation: Any, manager_task_id: str) -> None:
        transition = asyncio.create_task(
            self._operation_store.mark_submitted(operation.id, manager_task_id)
        )
        try:
            await asyncio.shield(transition)
        except asyncio.CancelledError:
            try:
                await asyncio.shield(transition)
            except Exception:
                pass
            await self._mark_uncertain(
                operation,
                "YouTubeManager upload submission was cancelled after manager task creation",
            )
            raise

    async def _mark_uncertain(self, operation: Any, message: str) -> None:
        await self._operation_store.mark_uncertain(operation.id, message)

    async def _verify_claim_content_hash(
        self,
        action: str,
        operation: Any,
        snapshot_sha256: str,
    ) -> None:
        expected_sha256 = getattr(operation, "content_sha256", None)
        matches = isinstance(expected_sha256, str) and hmac.compare_digest(
            expected_sha256,
            snapshot_sha256,
        )
        if matches:
            return
        if action == "resume":
            await self._mark_uncertain(
                operation,
                "youtube upload snapshot content hash does not match the claimed operation",
            )
        raise RuntimeError("youtube upload snapshot content hash does not match the claimed operation")

    def _raise_if_cancelled(self) -> None:
        if self._cancelled:
            raise CancelledError("youtube upload cancelled")

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
        content_sha256: str,
        title: str,
        privacy: str,
    ) -> UploadOperationContext:
        job_id = self._require_uuid(node_config.get("_job_id"), "_job_id")
        node_execution_id = self._require_uuid(
            node_config.get("_node_execution_id"),
            "_node_execution_id",
        )
        artifact_ids = node_config.get("_input_artifact_ids")
        if not isinstance(artifact_ids, dict) or "input" not in artifact_ids:
            raise RuntimeError("youtube upload requires _input_artifact_ids.input worker context")
        raw_claim = node_config.get("_execution_claim")
        if not isinstance(raw_claim, dict):
            raise RuntimeError("youtube upload requires _execution_claim worker context")
        worker_id = raw_claim.get("worker_id")
        raw_started_at = raw_claim.get("started_at")
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise RuntimeError("youtube upload execution claim has an invalid worker id")
        if not isinstance(raw_started_at, str):
            raise RuntimeError("youtube upload execution claim has an invalid start time")
        try:
            started_at = datetime.fromisoformat(raw_started_at)
        except ValueError as exc:
            raise RuntimeError("youtube upload execution claim has an invalid start time") from exc
        if started_at.tzinfo is None:
            raise RuntimeError("youtube upload execution claim start time must include a UTC offset")

        return UploadOperationContext(
            job_id=job_id,
            node_execution_id=node_execution_id,
            execution_claim=NodeExecutionClaim(
                job_id=job_id,
                node_execution_id=node_execution_id,
                worker_id=worker_id.strip(),
                started_at=started_at,
            ),
            input_artifact_id=self._require_uuid(artifact_ids["input"], "_input_artifact_ids.input"),
            content_sha256=content_sha256,
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
    def _create_input_snapshot(input_file: str) -> str:
        suffix = Path(input_file).suffix
        descriptor, snapshot_path = tempfile.mkstemp(prefix="vp_youtube_upload_", suffix=suffix)
        completed = False
        try:
            os.fchmod(descriptor, 0o600)
            snapshot_file = os.fdopen(descriptor, "wb")
            descriptor = -1
            with snapshot_file, open(input_file, "rb") as source_file:
                shutil.copyfileobj(source_file, snapshot_file, length=1024 * 1024)
            os.chmod(snapshot_path, 0o400)
            completed = True
            return snapshot_path
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if not completed:
                Path(snapshot_path).unlink(missing_ok=True)

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
        destination = Path(output_path)
        descriptor, temporary_output = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=destination.suffix,
            dir=destination.parent,
        )
        completed = False
        try:
            os.fchmod(descriptor, 0o600)
            output_file = os.fdopen(descriptor, "wb")
            descriptor = -1
            with output_file, open(input_file, "rb") as source_file:
                shutil.copyfileobj(source_file, output_file, length=1024 * 1024)
            os.replace(temporary_output, destination)
            completed = True
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if not completed:
                Path(temporary_output).unlink(missing_ok=True)
        return {"youtube": dict(receipt)}
