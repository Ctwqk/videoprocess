"""Trusted-code-only pre-receipt drill, not live arming or receiver-audit proof."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import secrets
import stat
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote, urlsplit

import httpx

from app.services.job_execution_authority import NodeExecutionClaim
from app.services.youtube_upload_operations import (
    UploadOperationClaim,
    UploadOperationContext,
    YouTubeUploadOperationStore,
)


def _digest(value: Any) -> str:
    return hashlib.sha256(_encode(value)).hexdigest()


def _encode(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")


@dataclass(frozen=True)
class AckDrillTarget:
    context: UploadOperationContext
    production_task_id: uuid.UUID
    drill_id: uuid.UUID
    expires_at: datetime
    owned_attestation_sha256: str
    manager_origin: str

    def __post_init__(self) -> None:
        context = self.context
        if type(context) is not UploadOperationContext or type(context.execution_claim) is not NodeExecutionClaim:
            raise ValueError("ack drill requires an immutable upload context")
        claim = context.execution_claim
        if any(not isinstance(value, uuid.UUID) or value.int == 0 for value in (
            context.job_id, context.node_execution_id, context.input_artifact_id,
            claim.job_id, claim.node_execution_id, claim.worker_registration_id,
            self.production_task_id, self.drill_id,
        )):
            raise ValueError("ack drill requires exact UUID identities")
        if (
            claim.job_id != context.job_id or claim.node_execution_id != context.node_execution_id
            or type(claim.worker_lease_epoch) is not int or claim.worker_lease_epoch <= 0
            or not isinstance(claim.worker_id, str)
            or re.fullmatch(r"[A-Za-z0-9_.@:-]{1,255}", claim.worker_id) is None
            or not isinstance(claim.started_at, datetime) or claim.started_at.utcoffset() is None
        ):
            raise ValueError("ack drill requires an exact registered execution claim")
        if (
            context.privacy != "unlisted" or not isinstance(context.title, str)
            or not context.title or context.title != context.title.strip() or len(context.title) > 500
            or any(ord(char) < 32 for char in context.title)
        ):
            raise ValueError("ack drill requires exact title and unlisted privacy")
        for digest in (context.content_sha256, self.owned_attestation_sha256):
            if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise ValueError("ack drill requires a SHA-256 digest")
        if (
            not isinstance(self.expires_at, datetime) or self.expires_at.utcoffset() is None
            or self.expires_at.timestamp() <= time.time()
        ):
            raise ValueError("ack drill expiry must be in the future with a UTC offset")
        if not isinstance(self.manager_origin, str):
            raise ValueError("ack drill requires a fixed Manager origin")
        origin = urlsplit(self.manager_origin)
        if (
            origin.scheme not in {"http", "https"} or not origin.hostname
            or origin.username is not None or origin.password is not None
            or origin.path or origin.query or origin.fragment
            or str(httpx.URL(self.manager_origin)).rstrip("/") != self.manager_origin
        ):
            raise ValueError("ack drill requires a canonical credential-free Manager origin")


class InjectedPreReceiptAbort(RuntimeError):
    """Only the originating helper's exact issued object can authorize recovery."""

    def __init__(self) -> None:
        super().__init__("owned unlisted pre-receipt drill abort")


class OwnedUnlistedAckDrill:
    def __init__(self, target: AckDrillTarget, *, state_dir: Path) -> None:
        if type(target) is not AckDrillTarget:
            raise ValueError("ack drill requires typed trusted-code configuration")
        target.__post_init__()
        if (
            not isinstance(state_dir, Path) or not state_dir.is_absolute()
            or len(state_dir.parts) < 2 or ".." in state_dir.parts
        ):
            raise ValueError("ack drill state directory must be an absolute protected path")
        self._target = target
        self._state_dir = state_dir
        self._directory_identity: tuple[int, int] | None = None
        with self._directory() as fd:
            info = os.fstat(fd)
            self._directory_identity = (info.st_dev, info.st_ino)
        context, claim = target.context, target.context.execution_claim
        self._identity = {
            "drill_id": str(target.drill_id), "production_task_id": str(target.production_task_id),
            "job_id": str(context.job_id), "node_execution_id": str(context.node_execution_id),
            "input_artifact_id": str(context.input_artifact_id), "content_sha256": context.content_sha256,
            "title_sha256": _digest(context.title), "privacy": "unlisted",
            "worker_id": claim.worker_id, "started_at": claim.started_at.isoformat(),
            "worker_registration_id": str(claim.worker_registration_id),
            "worker_lease_epoch": claim.worker_lease_epoch,
            "owned_attestation_sha256": target.owned_attestation_sha256,
            "manager_origin": target.manager_origin, "expires_at": target.expires_at.isoformat(),
        }
        self._records: dict[str, bytes] = {}
        self._phase = "new"
        self._operation_id: uuid.UUID | None = None
        self._manager_task_id: str | None = None
        self._video_id: str | None = None
        self._deadline = time.monotonic()
        self._owner = object()
        self._nonce = secrets.token_hex(32)
        self._abort: InjectedPreReceiptAbort | None = None

    @contextmanager
    def _directory(self) -> Iterator[int]:
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
        try:
            for index, part in enumerate(self._state_dir.parts[1:], start=1):
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
                info = os.fstat(fd)
                final = index == len(self._state_dir.parts) - 1
                mode = stat.S_IMODE(info.st_mode)
                if final:
                    valid = info.st_uid == os.geteuid() and mode == 0o700
                else:
                    valid = info.st_uid in {0, os.geteuid()} and (
                        not mode & 0o022 or (info.st_uid == 0 and bool(mode & stat.S_ISVTX))
                    )
                if not valid:
                    raise ValueError("ack drill state path ownership or mode is unsafe")
            info = os.fstat(fd)
            if self._directory_identity is not None and self._directory_identity != (info.st_dev, info.st_ino):
                raise RuntimeError("ack drill state directory was replaced")
            yield fd
        finally:
            os.close(fd)

    def _check_records(self, fd: int) -> None:
        if set(os.listdir(fd)) != set(self._records):
            raise RuntimeError("ack drill journal contains prior, partial or conflicting state")
        for name, expected in self._records.items():
            record_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            try:
                info = os.fstat(record_fd)
                if (
                    not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                    or info.st_size != len(expected)
                ):
                    raise RuntimeError("ack drill journal file is unsafe")
                with os.fdopen(record_fd, "rb", closefd=False) as source:
                    if source.read(len(expected) + 1) != expected:
                        raise RuntimeError("ack drill journal changed")
            finally:
                os.close(record_fd)

    def _write(self, name: str, record: dict[str, Any]) -> None:
        encoded = _encode(record) + b"\n"
        with self._directory() as fd:
            self._check_records(fd)
            output_fd = os.open(
                name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd,
            )
            try:
                with os.fdopen(output_fd, "wb", closefd=False) as output:
                    output.write(encoded)
                    output.flush()
                    os.fsync(output_fd)
                os.fsync(fd)
            finally:
                os.close(output_fd)
        self._records[name] = encoded

    def _record(self, event: str, *, receipt_sha256: str | None = None) -> None:
        sequence = len(self._records) - int("consumed.json" in self._records) + 1
        record = {
            **self._identity, "sequence": sequence, "event": event,
            "utc": datetime.now(timezone.utc).isoformat(), "monotonic": time.monotonic(),
            "operation_id": str(self._operation_id), "manager_task_id": self._manager_task_id,
            "video_id": self._video_id,
        }
        if event.startswith("fresh_submitted"):
            record.update(status="submitted", request_attempted=True, receipt_empty=True,
                          platform_video_id=None, completed_at=None)
        if event.startswith("processed_unlisted"):
            record.update(privacy="unlisted", upload_status="processed")
        if event.startswith("completed_get"):
            record["manager_status"] = "completed"
        if receipt_sha256 is not None:
            record["receipt_sha256"] = receipt_sha256
        self._write(f"{sequence:04d}.json", record)

    def remaining(self) -> float:
        remaining = min(self._deadline - time.monotonic(), self._target.expires_at.timestamp() - time.time())
        if remaining <= 0:
            raise RuntimeError("ack drill deadline expired")
        return remaining

    def _match(self, context: UploadOperationContext, operation: Any) -> None:
        if context != self._target.context or operation.production_task_id != self._target.production_task_id:
            raise RuntimeError("ack drill target or production task changed")
        for field in ("job_id", "node_execution_id", "input_artifact_id", "content_sha256", "title", "privacy"):
            if getattr(operation, field) != getattr(context, field):
                raise RuntimeError("ack drill operation context changed")
        if not isinstance(operation.id, uuid.UUID) or (self._operation_id is not None and operation.id != self._operation_id):
            raise RuntimeError("ack drill operation identity changed")

    def prepare(
        self, context: UploadOperationContext, claim: UploadOperationClaim, *,
        manager_origin: str, timeout_seconds: float,
    ) -> None:
        if self._phase != "new":
            raise RuntimeError("ack drill already started")
        self._match(context, claim.operation)
        operation = claim.operation
        if (
            manager_origin != self._target.manager_origin or claim.action != "submit"
            or operation.status != "reserved" or operation.request_attempted_at is not None
            or operation.manager_task_id is not None or operation.receipt_json != {}
            or operation.platform_video_id is not None or operation.completed_at is not None
            or not math.isfinite(timeout_seconds) or timeout_seconds <= 0
        ):
            raise RuntimeError("ack drill requires an exact fresh unlisted submission")
        self._deadline = time.monotonic() + timeout_seconds
        self.remaining()
        self._operation_id = operation.id
        self._phase = "failed"
        self._record("start")
        self._phase = "prepared"

    def record_post_attempt(self, context: UploadOperationContext, operation: Any) -> None:
        self.remaining()
        self._match(context, operation)
        if self._phase != "prepared" or operation.request_attempted_at is None:
            raise RuntimeError("ack drill cannot repeat the upload attempt")
        self._phase = "failed"
        self._record("upload_post_attempt")
        self._phase = "posted"

    def record_submitted(self, context: UploadOperationContext, operation: Any, manager_task_id: str) -> None:
        self.remaining()
        self._match(context, operation)
        if self._phase != "posted" or str(uuid.UUID(manager_task_id)) != manager_task_id:
            raise RuntimeError("ack drill requires the original canonical Manager task")
        self._phase = "failed"
        self._manager_task_id = manager_task_id
        self._record("submitted_committed")
        self._phase = "submitted"

    @staticmethod
    def _check_cancelled(cancelled: asyncio.Event) -> None:
        if cancelled.is_set():
            # Import lazily: worker.handlers registers the YouTube handler itself.
            from worker.handlers.base import CancelledError

            raise CancelledError("ack drill cancelled")

    async def verify_video(self, client: httpx.AsyncClient, video_id: str, cancelled: asyncio.Event) -> None:
        if not isinstance(video_id, str) or re.fullmatch(r"[A-Za-z0-9_/? .-]{1,128}", video_id) is None or " " in video_id:
            raise RuntimeError("ack drill video identity is malformed")
        deadline = time.monotonic() + min(30.0, self.remaining())
        while True:
            self._check_cancelled(cancelled)
            timeout = min(deadline - time.monotonic(), self.remaining())
            if timeout <= 0:
                raise RuntimeError("ack drill video verification timed out")
            request = asyncio.create_task(client.get(
                f"{self._target.manager_origin}/api/videos/{quote(video_id, safe='')}/status",
                headers={"Cache-Control": "no-cache, no-store", "Pragma": "no-cache"},
                follow_redirects=False,
            ))
            cancellation = asyncio.create_task(cancelled.wait())
            try:
                done, _ = await asyncio.wait((request, cancellation), timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
                self._check_cancelled(cancelled)
                if request not in done:
                    raise RuntimeError("ack drill video verification timed out")
                response = request.result()
            finally:
                for task in (request, cancellation):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(request, cancellation, return_exceptions=True)
            self.remaining()
            if response.status_code != 200:
                raise RuntimeError("ack drill video status HTTP failure")
            try:
                payload = response.json()
            except ValueError as exc:
                raise RuntimeError("ack drill video status is malformed") from exc
            if not isinstance(payload, dict) or payload.get("video_id") != video_id or payload.get("privacy") != "unlisted":
                raise RuntimeError("ack drill requires exact processed unlisted video state")
            upload, processing = payload.get("upload_status"), payload.get("processing_status")
            if upload == "processed" and ("processing_status" not in payload or processing == "succeeded"):
                return
            if upload not in {"uploaded", "processing"} or processing not in {"pending", "processing"}:
                raise RuntimeError("ack drill video state is not explicitly nonterminal")
            try:
                await asyncio.wait_for(cancelled.wait(), timeout=min(0.1, self.remaining(), max(0, deadline - time.monotonic())))
            except TimeoutError:
                pass

    async def _load(self, store: YouTubeUploadOperationStore, context: UploadOperationContext) -> Any:
        assert self._operation_id is not None and self._manager_task_id is not None
        async with asyncio.timeout(self.remaining()):
            claim = await store.load_submitted(
                context, operation_id=self._operation_id, manager_task_id=self._manager_task_id,
            )
        operation = claim.operation
        self._match(context, operation)
        if (
            claim.action != "resume" or operation.status != "submitted"
            or operation.manager_task_id != self._manager_task_id or operation.request_attempted_at is None
            or operation.receipt_json != {} or operation.platform_video_id is not None or operation.completed_at is not None
        ):
            raise RuntimeError("ack drill requires an unchanged pre-receipt submission")
        return operation

    async def before_receipt(
        self, store: YouTubeUploadOperationStore, context: UploadOperationContext,
        operation: Any, manager_task_id: str, video_id: str, client: httpx.AsyncClient,
        cancelled: asyncio.Event,
    ) -> None:
        self._match(context, operation)
        if manager_task_id != self._manager_task_id or self._phase not in {"submitted", "recovering"}:
            raise RuntimeError("ack drill completion is not eligible")
        recovery = self._phase == "recovering"
        if recovery and video_id != self._video_id:
            raise RuntimeError("ack drill recovered video identity changed")
        self._phase = "failed"
        # Validate identity before any payload-derived value enters the journal.
        if re.fullmatch(r"[A-Za-z0-9_/? .-]{1,128}", video_id) is None or " " in video_id:
            raise RuntimeError("ack drill video identity is malformed")
        self._video_id = video_id
        self._record("completed_get_2" if recovery else "completed_get_1")
        await self.verify_video(client, video_id, cancelled)
        self._check_cancelled(cancelled)
        self._record("processed_unlisted_get_2" if recovery else "processed_unlisted_get_1")
        if recovery:
            self._check_cancelled(cancelled)
            self.remaining()
            self._phase = "verified"
            return
        await self._load(store, context)
        self._check_cancelled(cancelled)
        self.remaining()
        self._record("fresh_submitted_empty_receipt")
        self._write("consumed.json", {
            **self._identity, "operation_id": str(self._operation_id),
            "manager_task_id": self._manager_task_id, "video_id": video_id, "nonce": self._nonce,
        })
        self._record("token_consumed")
        self._record("pre_receipt_abort")
        abort = InjectedPreReceiptAbort()
        abort._identity = (self._owner, self._nonce, context, self._operation_id, manager_task_id, video_id)
        self._abort = abort
        self._phase = "aborted"
        raise abort

    def authenticate_abort(
        self, abort: InjectedPreReceiptAbort, context: UploadOperationContext,
        operation_id: uuid.UUID, manager_task_id: str,
    ) -> None:
        if (
            self._phase != "aborted" or abort is not self._abort or type(abort) is not InjectedPreReceiptAbort
            or context != self._target.context or operation_id != self._operation_id
            or manager_task_id != self._manager_task_id
            or getattr(abort, "_identity", None) != (
                self._owner, self._nonce, context, operation_id, manager_task_id, self._video_id,
            )
            or "consumed.json" not in self._records
        ):
            raise RuntimeError("ack drill abort is not authentic")
        self._phase = "failed"
        with self._directory() as fd:
            self._check_records(fd)
        self.remaining()
        self._phase = "resume_pending"

    async def load_for_resume(
        self, store: YouTubeUploadOperationStore, context: UploadOperationContext,
        cancelled: asyncio.Event,
    ) -> Any:
        if self._phase != "resume_pending":
            raise RuntimeError("ack drill recovery was already attempted")
        self._phase = "failed"
        self._check_cancelled(cancelled)
        operation = await self._load(store, context)
        self._check_cancelled(cancelled)
        self.remaining()
        self._record("fresh_submitted_resume")
        self._phase = "recovering"
        return operation

    def record_succeeded(self, context: UploadOperationContext, operation: Any) -> None:
        self._match(context, operation)
        if (
            self._phase != "verified" or operation.status != "succeeded"
            or operation.platform_video_id != self._video_id or operation.completed_at is None
            or operation.manager_task_id != self._manager_task_id
            or operation.receipt_json.get("video_id") != self._video_id
        ):
            raise RuntimeError("ack drill requires the ordinary durable receipt")
        self._phase = "failed"
        self._record("mark_succeeded_commit", receipt_sha256=_digest(operation.receipt_json))
        self._phase = "finished"
