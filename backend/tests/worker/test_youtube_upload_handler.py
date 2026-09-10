from __future__ import annotations

import asyncio
import contextlib
import hashlib
import stat
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select

from app.services.job_execution_authority import JobExecutionAuthorityBlocked
from app.services.youtube_upload_operations import UploadOperationClaim
from worker.handlers import youtube_upload as youtube_upload_module
from worker.handlers.base import CancelledError
from worker.handlers.youtube_upload import YouTubeUploadHandler
from tests.worker.ack_drill_postgres import (
    ack_drill_database as _ack_drill_database,
    ack_drill_runtime as _ack_drill_runtime,
)
from tests.worker.test_youtube_ack_drill import drill_api, journal_records


ack_drill_database = _ack_drill_database
ack_drill_runtime = _ack_drill_runtime


JOB_ID = uuid.UUID("00000000-0000-0000-0000-000000000101")
NODE_EXECUTION_ID = uuid.UUID("00000000-0000-0000-0000-000000000102")
INPUT_ARTIFACT_ID = uuid.UUID("00000000-0000-0000-0000-000000000103")
OPERATION_ID = uuid.UUID("00000000-0000-0000-0000-000000000104")
MANAGER_TASK_ID = "00000000-0000-0000-0000-000000000105"


class FakeOperationStore:
    def __init__(
        self,
        actions: list[str],
        *,
        durable_receipt: dict | None = None,
    ) -> None:
        self._actions = list(actions)
        self.operation = SimpleNamespace(
            id=OPERATION_ID,
            status="reserved",
            manager_task_id=None,
            receipt_json={},
            content_sha256=None,
        )
        self.durable_receipt = durable_receipt or {
            "video_id": "video-123",
            "url": "https://www.youtube.com/watch?v=video-123",
            "title": "Canary upload",
            "privacy": "unlisted",
            "tags": ["canary"],
            "quota_estimate": 1600,
        }
        self.claim_contexts: list[object] = []
        self.attempting: list[uuid.UUID] = []
        self.submitted: list[tuple[uuid.UUID, str]] = []
        self.succeeded: list[tuple[uuid.UUID, str, dict]] = []
        self.failed: list[tuple[uuid.UUID, str]] = []
        self.uncertain: list[tuple[uuid.UUID, str]] = []
        self.mark_submitted_started: asyncio.Event | None = None
        self.mark_submitted_continue: asyncio.Event | None = None
        self.submission_fence_contexts: list[object] = []
        self.submission_fence_active = False

    async def claim(self, context):
        self.claim_contexts.append(context)
        if self.operation.content_sha256 is None:
            self.operation.content_sha256 = context.content_sha256
        action = self._actions.pop(0)
        return UploadOperationClaim(action=action, operation=self.operation)

    @contextlib.asynccontextmanager
    async def submission_fence(self, context):
        self.submission_fence_contexts.append(context)
        assert not self.submission_fence_active
        self.submission_fence_active = True
        try:
            yield
        finally:
            self.submission_fence_active = False

    async def mark_submitted(
        self,
        operation_id: uuid.UUID,
        manager_task_id: str,
        *,
        context=None,
    ):
        assert context in self.claim_contexts
        if self.mark_submitted_started is not None:
            self.mark_submitted_started.set()
            assert self.mark_submitted_continue is not None
            await self.mark_submitted_continue.wait()
        self.submitted.append((operation_id, manager_task_id))
        self.operation.status = "submitted"
        self.operation.manager_task_id = manager_task_id
        return self.operation

    async def mark_attempting(self, operation_id: uuid.UUID, *, context=None):
        assert self.submission_fence_active
        assert context in self.claim_contexts
        self.attempting.append(operation_id)
        self.operation.request_attempted_at = object()
        return self.operation

    async def mark_succeeded(
        self,
        operation_id: uuid.UUID,
        platform_video_id: str,
        receipt: dict,
        *,
        context=None,
    ):
        assert context in self.claim_contexts
        self.succeeded.append((operation_id, platform_video_id, receipt))
        self.operation.status = "succeeded"
        self.operation.receipt_json = dict(self.durable_receipt)
        return self.operation

    async def mark_failed(
        self,
        operation_id: uuid.UUID,
        error_message: str,
        *,
        context=None,
    ):
        assert context in self.claim_contexts
        self.failed.append((operation_id, error_message))
        self.operation.status = "failed"
        return self.operation

    async def mark_uncertain(
        self,
        operation_id: uuid.UUID,
        error_message: str,
        *,
        context=None,
    ):
        assert context in self.claim_contexts
        self.uncertain.append((operation_id, error_message))
        self.operation.status = "uncertain"
        return self.operation


@pytest.fixture(autouse=True)
def enabled_youtube_publishing(monkeypatch):
    monkeypatch.setenv("YOUTUBE_PUBLISH_ENABLED", "true")
    monkeypatch.setenv("PUBLIC_PUBLISH_ENABLED", "false")


@pytest.fixture
def media_paths(tmp_path: Path) -> tuple[dict[str, str], str]:
    input_path = tmp_path / "input.mp4"
    input_path.write_bytes(b"owned unlisted canary media")
    return {"input": str(input_path)}, str(tmp_path / "output.mp4")


def upload_config(**overrides) -> dict:
    config = {
        "title": "Canary upload",
        "description": "A private canary upload",
        "tags": "canary, verification",
        "privacy": "unlisted",
        "_job_id": str(JOB_ID),
        "_node_execution_id": str(NODE_EXECUTION_ID),
        "_input_artifact_ids": {"input": str(INPUT_ARTIFACT_ID)},
        "_execution_claim": {
            "worker_id": "gpu-worker@150:42",
            "started_at": "2026-07-22T12:00:00+00:00",
            "worker_registration_id": "00000000-0000-0000-0000-000000000106",
            "worker_lease_epoch": 7,
        },
    }
    config.update(overrides)
    return config


def auth_payload(*, quota: dict | None = None, authenticated: bool = True) -> dict:
    return {
        "authenticated": authenticated,
        "quota_estimate": quota
        if quota is not None
        else {
            "daily_limit": 10_000,
            "estimated_units_used": 0,
            "estimated_units_remaining": 10_000,
            "upload_cost_per_request": 1_600,
        },
    }


def make_handler(store: FakeOperationStore, client: httpx.AsyncClient, **overrides) -> YouTubeUploadHandler:
    async def refresh_worker_lease(*, minimum_margin_seconds: float):
        assert minimum_margin_seconds == 150

    overrides.setdefault("lease_refresher", refresh_worker_lease)
    return YouTubeUploadHandler(
        store,
        client=client,
        base_url="http://youtube-manager",
        poll_interval_seconds=0,
        **overrides,
    )


@pytest.fixture
async def durable_drill(ack_drill_runtime, tmp_path):
    from app.services import youtube_upload_operations as operations

    runtime = ack_drill_runtime
    context = runtime.context
    api = drill_api()
    target = api.AckDrillTarget(
        context=context, production_task_id=runtime.task_id, drill_id=uuid.uuid4(),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        owned_attestation_sha256="b" * 64, manager_origin="http://youtube-manager",
    )
    state_dir = tmp_path / "ack-drill"
    state_dir.mkdir(mode=0o700)
    return SimpleNamespace(
        store=operations.YouTubeUploadOperationStore(runtime.sessions), runtime=runtime,
        target=target, state_dir=state_dir,
        helper=api.OwnedUnlistedAckDrill(target, state_dir=state_dir),
        config=upload_config(
            title=context.title, _job_id=str(context.job_id),
            _node_execution_id=str(context.node_execution_id),
            _input_artifact_ids={"input": str(context.input_artifact_id)},
            _execution_claim={
                "worker_id": context.execution_claim.worker_id,
                "started_at": context.execution_claim.started_at.isoformat(),
                "worker_registration_id": str(context.execution_claim.worker_registration_id),
                "worker_lease_epoch": context.execution_claim.worker_lease_epoch,
            },
        ),
    )


@pytest.mark.asyncio
async def test_ack_drill_fresh_reader_uses_canonical_runtime_permissions(ack_drill_runtime):
    from app.services.youtube_upload_operations import YouTubeUploadOperationStore

    runtime = ack_drill_runtime
    context = runtime.context
    store = YouTubeUploadOperationStore(runtime.sessions)
    assert not await runtime.owner.fetchval(
        "SELECT has_table_privilege($1,'public.production_tasks','SELECT')",
        runtime.role,
    )
    reserved = await store.claim(context)
    await runtime.refresh(minimum_margin_seconds=150)
    async with store.submission_fence(context):
        attempted = await store.mark_attempting(reserved.operation.id, context=context)
        assert attempted.request_attempted_at is not None
        await store.mark_submitted(attempted.id, MANAGER_TASK_ID, context=context)
    # This is deliberately not an owner session or a widened test-role grant.
    loaded = await store.load_submitted(
        context, operation_id=reserved.operation.id, manager_task_id=MANAGER_TASK_ID,
    )
    assert loaded.operation.id == reserved.operation.id and loaded.action == "resume"


@pytest.mark.asyncio
async def test_ack_drill_discards_first_completion_and_commits_only_fresh_get(
    durable_drill, media_paths, monkeypatch,
):
    from app.models.youtube_upload_operation import YouTubeUploadOperation

    drill = durable_drill
    store = drill.store
    requests, observations, writes, claims = [], [], [], []
    original_load, original_succeed, original_claim = store.load_submitted, store.mark_succeeded, store.claim
    abort_observed = asyncio.Event()
    release_resume = asyncio.Event()

    async def claim(context):
        claims.append(context)
        return await original_claim(context)

    async def observe(context, **kwargs):
        loaded = await original_load(context, **kwargs)
        assert store._active_submission_fence.get() is None
        async with drill.runtime.owner_sessions() as db:
            row = await db.get(YouTubeUploadOperation, loaded.operation.id)
            assert row.status == "submitted"
            assert row.request_attempted_at is not None
            assert row.receipt_json == {}
            assert row.platform_video_id is None and row.completed_at is None
            assert row.manager_task_id == MANAGER_TASK_ID
        assert not writes and not Path(media_paths[1]).exists()
        observations.append(loaded.operation)
        if len(observations) == 2:
            assert (drill.state_dir / "consumed.json").exists()
            assert journal_records(drill.state_dir)[-1]["event"] == "pre_receipt_abort"
            abort_observed.set()
            await release_resume.wait()
        return loaded

    async def succeed(*args, **kwargs):
        assert len(observations) == 2
        writes.append(args)
        return await original_succeed(*args, **kwargs)

    monkeypatch.setattr(store, "claim", claim)
    monkeypatch.setattr(store, "load_submitted", observe)
    monkeypatch.setattr(store, "mark_succeeded", succeed)
    statuses = 0

    def route(request):
        nonlocal statuses
        requests.append((request.method, request.url.path))
        if request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST":
            assert store._active_submission_fence.get() is not None
            assert journal_records(drill.state_dir)[-1]["event"] == "upload_post_attempt"
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        if request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            statuses += 1
            if statuses == 1:
                assert journal_records(drill.state_dir)[-1]["event"] == "submitted_committed"
            return httpx.Response(200, json={"status": "completed", "result": {
                "video_id": "video-123", "url": "https://www.youtube.com/watch?v=video-123",
                "title": "First discarded" if statuses == 1 else "Fresh receipt",
                "secret": "must-not-enter-journal", "signed_url": "https://secret.invalid/?token=secret",
            }})
        assert request.url.path == "/api/videos/video-123/status"
        return httpx.Response(200, json={
            "video_id": "video-123", "privacy": "unlisted",
            "upload_status": "processed", "processing_status": "succeeded",
            "secret": "must-not-enter-journal",
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client, ack_drill=drill.helper, lease_refresher=drill.runtime.refresh)
        running = asyncio.create_task(handler.execute(drill.config, *media_paths))
        boundary = asyncio.create_task(abort_observed.wait())
        try:
            done, _ = await asyncio.wait((running, boundary), timeout=5, return_when=asyncio.FIRST_COMPLETED)
            if running in done:
                await running
            assert boundary in done, "handler never reached the pre-receipt boundary"
            assert not running.done()
            assert not writes and not Path(media_paths[1]).exists()
            release_resume.set()
            result = await running
        finally:
            if not running.done():
                running.cancel()
            boundary.cancel()
            await asyncio.gather(running, boundary, return_exceptions=True)

    assert len(claims) == 1 and len(writes) == 1
    assert observations[0] is not observations[1]
    assert observations[0].id == observations[1].id == writes[0][0]
    assert result["youtube"]["title"] == "Fresh receipt"
    assert Path(media_paths[1]).read_bytes() == Path(media_paths[0]["input"]).read_bytes()
    assert requests == [
        ("GET", "/api/auth/status"), ("POST", "/api/upload"),
        ("GET", f"/api/status/{MANAGER_TASK_ID}"), ("GET", "/api/videos/video-123/status"),
        ("GET", f"/api/status/{MANAGER_TASK_ID}"), ("GET", "/api/videos/video-123/status"),
    ]
    records = journal_records(drill.state_dir)
    assert [record["event"] for record in records] == [
        "start", "upload_post_attempt", "submitted_committed", "completed_get_1",
        "processed_unlisted_get_1", "fresh_submitted_empty_receipt", "token_consumed",
        "pre_receipt_abort", "fresh_submitted_resume", "completed_get_2",
        "processed_unlisted_get_2", "mark_succeeded_commit",
    ]
    assert [record["sequence"] for record in records] == list(range(1, 13))
    journal = "".join(path.read_text() for path in drill.state_dir.iterdir())
    assert "secret" not in journal and "signed_url" not in journal
    assert records[-1]["receipt_sha256"] == hashlib.sha256(
        __import__("json").dumps(result["youtube"], sort_keys=True, separators=(",", ":"), allow_nan=False).encode(),
    ).hexdigest()


@pytest.mark.parametrize("failure", [
    "missing-video", "bad-url", "public", "private", "wrong-video", "malformed",
    "not-processed", "changed-second-video", "second-public", "cancel-first-get",
    "cancel-resume", "authority-loss", "storage", "snapshot", "forged", "ordinary-write",
    "copied-sentinel", "wrong-nonce", "wrong-operation", "wrong-manager", "wrong-context",
    "wrong-sentinel-video", "missing-token", "partial-token", "duplicate-consumer", "reconstructed",
    "token-fsync", "abort-fsync", "expired-lease", "processing-cancel", "processing-timeout",
])
@pytest.mark.asyncio
async def test_ack_drill_failure_never_posts_twice_or_manufactures_receipt(
    durable_drill, media_paths, monkeypatch, failure,
):
    from app.models.job import Job, JobStatus
    from app.models.youtube_upload_operation import YouTubeUploadOperation

    drill = durable_drill
    counts = {"post": 0, "status": 0, "video": 0, "write": 0}
    original_load = drill.store.load_submitted
    original_succeed = drill.store.mark_succeeded
    original_boundary = drill.helper.before_receipt
    reads = 0

    async def load(context, **kwargs):
        nonlocal reads
        reads += 1
        if reads == 2:
            if failure == "cancel-resume":
                handler.cancel()
            if failure == "authority-loss":
                async with drill.runtime.owner_sessions() as db:
                    job = await db.get(Job, context.job_id)
                    job.status = JobStatus.CANCELLED
                    await db.commit()
            if failure == "expired-lease":
                await drill.runtime.owner.execute(
                    "UPDATE public.worker_registrations SET lease_expires_at=clock_timestamp() WHERE id=$1",
                    context.execution_claim.worker_registration_id,
                )
        if failure == "storage":
            raise OSError("storage unavailable: secret")
        return await original_load(context, **kwargs)

    async def succeed(*args, **kwargs):
        counts["write"] += 1
        return await original_succeed(*args, **kwargs)

    async def boundary(*args, **kwargs):
        api = drill_api()
        try:
            await original_boundary(*args, **kwargs)
        except api.InjectedPreReceiptAbort as abort:
            if failure in {"forged", "copied-sentinel"}:
                forged = api.InjectedPreReceiptAbort()
                if failure == "copied-sentinel":
                    forged.__dict__.update(abort.__dict__)
                raise forged from None
            changes = {"wrong-nonce": (1, "wrong"), "wrong-context": (2, replace(drill.target.context, title="other")),
                       "wrong-operation": (3, uuid.uuid4()), "wrong-manager": (4, str(uuid.uuid4())),
                       "wrong-sentinel-video": (5, "other")}
            if failure in changes:
                identity = list(abort._identity)
                index, value = changes[failure]
                identity[index] = value
                abort._identity = tuple(identity)
            if failure == "missing-token":
                (drill.state_dir / "consumed.json").unlink()
            if failure == "partial-token":
                (drill.state_dir / "consumed.json").write_text("{")
            if failure == "duplicate-consumer":
                drill.helper.authenticate_abort(abort, drill.target.context, args[2].id, MANAGER_TASK_ID)
            if failure == "reconstructed":
                handler._ack_drill = api.OwnedUnlistedAckDrill(drill.target, state_dir=drill.state_dir)
            raise

    monkeypatch.setattr(drill.store, "load_submitted", load)
    monkeypatch.setattr(drill.store, "mark_succeeded", succeed)
    monkeypatch.setattr(drill.helper, "before_receipt", boundary)
    if failure == "ordinary-write":
        from sqlalchemy import event

        def fail_completion_sql(conn, cursor, statement, parameters, context, executemany):
            if "vp_transition_worker_youtube_upload" in statement and "succeeded" in parameters:
                raise OSError("test connection failure at ordinary success transition")

        event.listen(drill.runtime.sessions.kw["bind"].sync_engine, "before_cursor_execute", fail_completion_sql)
    if failure in {"token-fsync", "abort-fsync"}:
        original_write = drill.helper._write

        def write_with_disk_failure(name, record):
            if (failure == "token-fsync" and name == "consumed.json") or (
                failure == "abort-fsync" and record.get("event") == "pre_receipt_abort"
            ):
                def fail_fsync(fd):
                    raise OSError("test journal durability failure")
                with monkeypatch.context() as patcher:
                    patcher.setattr(drill_api().os, "fsync", fail_fsync)
                    return original_write(name, record)
            return original_write(name, record)

        monkeypatch.setattr(drill.helper, "_write", write_with_disk_failure)
    if failure == "snapshot":
        original_hash = YouTubeUploadHandler._content_sha256
        hashes = 0

        def changed_hash(path):
            nonlocal hashes
            hashes += 1
            return original_hash(path) if hashes == 1 else "c" * 64

        monkeypatch.setattr(YouTubeUploadHandler, "_content_sha256", staticmethod(changed_hash))

    async def route(request):
        if request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST":
            counts["post"] += 1
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        if request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            counts["status"] += 1
            video = "other" if failure == "changed-second-video" and counts["status"] == 2 else "video-123"
            result = {"video_id": video, "url": f"https://www.youtube.com/watch?v={video}"}
            if failure == "missing-video":
                del result["video_id"]
            if failure == "bad-url":
                result["url"] = "https://untrusted.invalid/"
            return httpx.Response(200, json={"status": "completed", "result": result})
        counts["video"] += 1
        if failure == "cancel-first-get":
            handler.cancel()
            await asyncio.sleep(10)
        payload = {"video_id": "video-123", "privacy": "unlisted", "upload_status": "processed"}
        if failure in {"public", "private"}:
            payload["privacy"] = failure
        if failure == "second-public" and counts["video"] == 2:
            payload["privacy"] = "public"
        if failure == "wrong-video":
            payload["video_id"] = "other"
        if failure == "not-processed":
            payload["upload_status"] = "uploaded"
        if failure in {"processing-cancel", "processing-timeout"}:
            payload.update(upload_status="uploaded", processing_status="processing")
            if failure == "processing-cancel":
                asyncio.get_running_loop().call_soon(handler.cancel)
            else:
                # Shorten only the test's post-submission verification budget.
                drill.helper._deadline = min(drill.helper._deadline, __import__("time").monotonic() + 0.15)
        return httpx.Response(200, json=[] if failure == "malformed" else payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(drill.store, client, ack_drill=drill.helper,
                               lease_refresher=drill.runtime.refresh, timeout_seconds=10)
        with pytest.raises((RuntimeError, ValueError, OSError, CancelledError, asyncio.CancelledError)):
            await handler.execute(drill.config, *media_paths)
    second_completion = failure in {"changed-second-video", "second-public", "ordinary-write"}
    assert counts["post"] == 1 and counts["status"] == (2 if second_completion else 1)
    expected_reads = 1
    if failure in {"missing-video", "bad-url", "public", "private", "wrong-video", "malformed",
                   "not-processed", "cancel-first-get", "processing-cancel", "processing-timeout"}:
        expected_reads = 0
    elif second_completion or failure in {"cancel-resume", "authority-loss", "expired-lease"}:
        expected_reads = 2
    assert reads == expected_reads
    if failure in {"missing-video", "bad-url"}:
        assert counts["video"] == 0
    elif failure == "processing-timeout":
        assert counts["video"] >= 2
    else:
        assert counts["video"] == (2 if failure in {"second-public", "ordinary-write"} else 1)
    assert counts["write"] == (1 if failure == "ordinary-write" else 0)
    assert not Path(media_paths[1]).exists()
    async with drill.runtime.owner_sessions() as db:
        operation = (await db.execute(select(YouTubeUploadOperation).where(
            YouTubeUploadOperation.node_execution_id == drill.target.context.node_execution_id,
        ))).scalar_one()
        assert operation.status != "succeeded"
        assert operation.receipt_json == {} and operation.platform_video_id is None
        if failure == "ordinary-write":
            assert operation.status == "uncertain"
    assert "mark_succeeded_commit" not in [r["event"] for r in journal_records(drill.state_dir)]


@pytest.mark.parametrize("waiting_on", ["first-status", "second-status", "second-video"])
@pytest.mark.asyncio
async def test_ack_drill_cancels_pending_fresh_get_without_waiting_for_response(
    durable_drill, media_paths, waiting_on,
):
    from app.models.youtube_upload_operation import YouTubeUploadOperation

    drill = durable_drill
    reached = asyncio.Event()
    counts = {"post": 0, "status": 0, "video": 0}

    async def route(request):
        if request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST":
            counts["post"] += 1
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        if request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            counts["status"] += 1
            current = "first-status" if counts["status"] == 1 else "second-status"
            payload = {"status": "completed", "result": {
                "video_id": "video-123", "url": "https://www.youtube.com/watch?v=video-123",
            }}
        else:
            assert request.url.path == "/api/videos/video-123/status"
            counts["video"] += 1
            current = "first-video" if counts["video"] == 1 else "second-video"
            payload = {"video_id": "video-123", "privacy": "unlisted", "upload_status": "processed"}
        if current == waiting_on:
            reached.set()
            await asyncio.Event().wait()
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(drill.store, client, ack_drill=drill.helper,
                               lease_refresher=drill.runtime.refresh, timeout_seconds=10)
        running = asyncio.create_task(handler.execute(drill.config, *media_paths))
        try:
            await asyncio.wait_for(reached.wait(), timeout=3)
            handler.cancel()
            done, _ = await asyncio.wait((running,), timeout=1)
            assert running in done, "armed cancellation waited for the remote response"
            with pytest.raises((CancelledError, RuntimeError)):
                await running
        finally:
            if not running.done():
                running.cancel()
            await asyncio.gather(running, return_exceptions=True)
    assert counts["post"] == 1
    assert counts["status"] == (1 if waiting_on == "first-status" else 2)
    assert not Path(media_paths[1]).exists()
    async with drill.runtime.owner_sessions() as db:
        operation = (await db.execute(select(YouTubeUploadOperation).where(
            YouTubeUploadOperation.node_execution_id == drill.target.context.node_execution_id,
        ))).scalar_one()
        assert operation.status == "uncertain" and operation.receipt_json == {}
    assert "mark_succeeded_commit" not in [r["event"] for r in journal_records(drill.state_dir)]


@pytest.mark.parametrize("changed", ["job", "lease", "node", "channel", "schedule"])
@pytest.mark.asyncio
async def test_ack_drill_final_write_rechecks_actual_authority(durable_drill, media_paths, monkeypatch, changed):
    from sqlalchemy.exc import DBAPIError

    from app.models.youtube_upload_operation import YouTubeUploadOperation

    drill = durable_drill
    context = drill.target.context
    counts = {"post": 0, "status": 0, "video": 0, "read": 0, "write": 0}
    original_load, original_write = drill.store.load_submitted, drill.store.mark_succeeded

    async def load(*args, **kwargs):
        result = await original_load(*args, **kwargs)
        counts["read"] += 1
        return result

    async def write(*args, **kwargs):
        counts["write"] += 1
        return await original_write(*args, **kwargs)

    monkeypatch.setattr(drill.store, "load_submitted", load)
    monkeypatch.setattr(drill.store, "mark_succeeded", write)

    async def route(request):
        if request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST":
            counts["post"] += 1
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        if request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            counts["status"] += 1
            return httpx.Response(200, json={"status": "completed", "result": {
                "video_id": "video-123", "url": "https://www.youtube.com/watch?v=video-123",
            }})
        assert request.url.path == "/api/videos/video-123/status"
        counts["video"] += 1
        if counts["video"] == 2:
            assert counts["read"] == 2 and counts["write"] == 0
            if changed == "job":
                await drill.runtime.owner.execute("UPDATE public.jobs SET status='CANCELLED' WHERE id=$1", context.job_id)
            elif changed == "lease":
                await drill.runtime.owner.execute(
                    "UPDATE public.worker_registrations SET lease_expires_at=clock_timestamp() WHERE id=$1",
                    context.execution_claim.worker_registration_id,
                )
            elif changed == "node":
                await drill.runtime.owner.execute(
                    "UPDATE public.node_executions SET worker_id='other-worker' WHERE id=$1", context.node_execution_id,
                )
            elif changed == "channel":
                await drill.runtime.owner.execute(
                    "UPDATE public.channel_profiles SET halted_at=clock_timestamp(),halt_reason='test hold' "
                    "WHERE id=(SELECT channel_profile_id FROM public.production_tasks WHERE id=$1)",
                    drill.target.production_task_id,
                )
            else:
                await drill.runtime.owner.execute("UPDATE public.runtime_schedules SET state='CLOSED' WHERE service_name='videoprocess'")
        return httpx.Response(200, json={"video_id": "video-123", "privacy": "unlisted", "upload_status": "processed"})

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
            handler = make_handler(drill.store, client, ack_drill=drill.helper, lease_refresher=drill.runtime.refresh)
            expected_error = {
                "job": "job_authority_changed", "lease": "lease_fenced", "node": "node_claim_mismatch",
                "channel": "channel_authority_changed", "schedule": "schedule_authority_changed",
            }[changed]
            # Existing terminal transitions surface the actual SQL authority error.
            with pytest.raises(DBAPIError, match=expected_error):
                await handler.execute(drill.config, *media_paths)
    finally:
        if changed == "schedule":
            await drill.runtime.owner.execute("UPDATE public.runtime_schedules SET state='OPEN' WHERE service_name='videoprocess'")
    assert counts == {"post": 1, "status": 2, "video": 2, "read": 2, "write": 1}
    async with drill.runtime.owner_sessions() as db:
        operation = (await db.execute(select(YouTubeUploadOperation).where(
            YouTubeUploadOperation.node_execution_id == context.node_execution_id,
        ))).scalar_one()
        assert operation.status == "submitted" and operation.receipt_json == {}
        assert operation.platform_video_id is None and operation.completed_at is None
    assert not Path(media_paths[1]).exists()
    assert "mark_succeeded_commit" not in [r["event"] for r in journal_records(drill.state_dir)]


@pytest.mark.asyncio
async def test_ack_drill_final_journal_failure_preserves_receipt_but_never_reports_or_reuploads(
    durable_drill, media_paths, monkeypatch,
):
    from app.models.youtube_upload_operation import YouTubeUploadOperation

    drill = durable_drill
    requests = []
    original_write = drill.helper._write

    def fail_final_record(name, record):
        if record.get("event") == "mark_succeeded_commit":
            raise OSError("test journal storage unavailable after receipt commit")
        return original_write(name, record)

    monkeypatch.setattr(drill.helper, "_write", fail_final_record)

    def route(request):
        requests.append((request.method, request.url.path))
        if request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST":
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        if request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            return httpx.Response(200, json={"status": "completed", "result": {
                "video_id": "video-audit-failure", "url": "https://www.youtube.com/watch?v=video-audit-failure",
            }})
        assert request.url.path == "/api/videos/video-audit-failure/status"
        return httpx.Response(200, json={"video_id": "video-audit-failure", "privacy": "unlisted", "upload_status": "processed"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(drill.store, client, ack_drill=drill.helper, lease_refresher=drill.runtime.refresh)
        with pytest.raises(OSError, match="journal storage unavailable"):
            await handler.execute(drill.config, *media_paths)
        assert len(requests) == 6
        reconstructed = drill_api().OwnedUnlistedAckDrill(drill.target, state_dir=drill.state_dir)
        retry = make_handler(drill.store, client, ack_drill=reconstructed, lease_refresher=drill.runtime.refresh)
        with pytest.raises(RuntimeError, match="fresh unlisted submission"):
            await retry.execute(drill.config, *media_paths)
        assert len(requests) == 6
    async with drill.runtime.owner_sessions() as db:
        operation = (await db.execute(select(YouTubeUploadOperation).where(
            YouTubeUploadOperation.node_execution_id == drill.target.context.node_execution_id,
        ))).scalar_one()
        assert operation.status == "succeeded"
        assert operation.receipt_json["video_id"] == "video-audit-failure"
    assert not Path(media_paths[1]).exists()
    assert "mark_succeeded_commit" not in [r["event"] for r in journal_records(drill.state_dir)]


@pytest.mark.parametrize("boundary,change", [
    ("journal-fsync", "wall"), ("journal-fsync", "monotonic"), ("journal-fsync", "cancel"),
    ("helper-return", "wall"), ("helper-return", "monotonic"),
])
@pytest.mark.asyncio
async def test_ack_drill_rechecks_expiry_after_second_processed_journal_before_success(
    durable_drill, media_paths, monkeypatch, boundary, change,
):
    from app.models.youtube_upload_operation import YouTubeUploadOperation

    drill = durable_drill
    api = drill_api()
    real_time = api.time
    clock_offsets = {"wall": 0.0, "monotonic": 0.0}
    monkeypatch.setattr(api, "time", SimpleNamespace(
        time=lambda: real_time.time() + clock_offsets["wall"],
        monotonic=lambda: real_time.monotonic() + clock_offsets["monotonic"],
    ))
    original_journal, original_boundary = drill.helper._write, drill.helper.before_receipt
    original_success = drill.store.mark_succeeded
    video_id = f"expiry-{boundary}-{change}"
    counts = {"post": 0, "status": 0, "video": 0, "success": 0, "crossed": 0}

    def cross_boundary():
        counts["crossed"] += 1
        if change == "cancel":
            handler.cancel()
        elif change == "wall":
            clock_offsets["wall"] = drill.target.expires_at.timestamp() - real_time.time() + 1
        else:
            clock_offsets["monotonic"] = drill.helper._deadline - real_time.monotonic() + 1

    def journal(name, record):
        if boundary != "journal-fsync" or record.get("event") != "processed_unlisted_get_2":
            return original_journal(name, record)
        original_fsync = api.os.fsync

        def slow_directory_fsync(fd):
            original_fsync(fd)
            if stat.S_ISDIR(api.os.fstat(fd).st_mode):
                # Model time spent in successful real journal I/O, not DB authority.
                cross_boundary()

        with monkeypatch.context() as patcher:
            patcher.setattr(api.os, "fsync", slow_directory_fsync)
            return original_journal(name, record)

    async def before_receipt(*args, **kwargs):
        await original_boundary(*args, **kwargs)
        if boundary == "helper-return":
            cross_boundary()

    async def success(*args, **kwargs):
        counts["success"] += 1
        return await original_success(*args, **kwargs)

    monkeypatch.setattr(drill.helper, "_write", journal)
    monkeypatch.setattr(drill.helper, "before_receipt", before_receipt)
    monkeypatch.setattr(drill.store, "mark_succeeded", success)

    def route(request):
        if request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST":
            counts["post"] += 1
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        if request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            counts["status"] += 1
            return httpx.Response(200, json={"status": "completed", "result": {
                "video_id": video_id, "url": f"https://www.youtube.com/watch?v={video_id}",
            }})
        assert request.url.path == f"/api/videos/{video_id}/status"
        counts["video"] += 1
        return httpx.Response(200, json={"video_id": video_id, "privacy": "unlisted", "upload_status": "processed"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(drill.store, client, ack_drill=drill.helper, lease_refresher=drill.runtime.refresh)
        expected_error = CancelledError if change == "cancel" else RuntimeError
        with pytest.raises(expected_error, match="cancelled" if change == "cancel" else "deadline expired"):
            await handler.execute(drill.config, *media_paths)
    assert counts == {"post": 1, "status": 2, "video": 2, "success": 0, "crossed": 1}
    async with drill.runtime.owner_sessions() as db:
        operation = (await db.execute(select(YouTubeUploadOperation).where(
            YouTubeUploadOperation.node_execution_id == drill.target.context.node_execution_id,
        ))).scalar_one()
        assert operation.status in ({"submitted", "uncertain"} if change == "cancel" else {"submitted"})
        assert operation.receipt_json == {} and operation.platform_video_id is None and operation.completed_at is None
    assert not Path(media_paths[1]).exists()
    assert (drill.state_dir / "consumed.json").exists()
    records = journal_records(drill.state_dir)
    assert records[-1]["event"] == "processed_unlisted_get_2"
    assert "mark_succeeded_commit" not in [record["event"] for record in records]


@pytest.mark.asyncio
async def test_node_config_cannot_enable_ack_drill(media_paths):
    store = FakeOperationStore(["submit"])
    routes = []

    def route(request):
        routes.append(request.url.path)
        if request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST":
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        return httpx.Response(200, json={"status": "completed", "result": {
            "video_id": "video-123", "url": "https://www.youtube.com/watch?v=video-123",
        }})

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        await make_handler(store, client).execute(upload_config(
            ack_drill={"enabled": True}, _ack_drill={"enabled": True},
            VP_YOUTUBE_ACK_DRILL_ENABLED=True,
        ), *media_paths)
    assert routes == ["/api/auth/status", "/api/upload", f"/api/status/{MANAGER_TASK_ID}"]


@pytest.mark.asyncio
async def test_fresh_150_second_lease_is_required_before_submission_fence(
    media_paths,
) -> None:
    store = FakeOperationStore(["submit"])
    events: list[str] = []
    original_fence = store.submission_fence

    async def refresh_worker_lease(*, minimum_margin_seconds: float):
        assert minimum_margin_seconds == 150
        events.append("refresh")

    @contextlib.asynccontextmanager
    async def ordered_fence(context):
        assert events == ["refresh"]
        events.append("fence")
        async with original_fence(context):
            yield

    store.submission_fence = ordered_fence

    def route(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "status": "completed",
                    "result": {
                        "video_id": "video-123",
                        "url": "https://www.youtube.com/watch?v=video-123",
                    },
                },
            )
        raise AssertionError("unexpected request")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(route)
    ) as client:
        await make_handler(
            store,
            client,
            lease_refresher=refresh_worker_lease,
        ).execute(upload_config(), input_paths, output_path)

    assert events == ["refresh", "fence"]


@pytest.mark.asyncio
async def test_upload_post_and_submitted_transition_use_fixed_time_bounds(
    monkeypatch,
    media_paths,
) -> None:
    store = FakeOperationStore(["submit"])
    timeout_values: list[float] = []
    original_timeout = asyncio.timeout

    def recording_timeout(delay):
        timeout_values.append(delay)
        return original_timeout(delay)

    monkeypatch.setattr(youtube_upload_module.asyncio, "timeout", recording_timeout)

    def route(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        return httpx.Response(
            200,
            json={
                "status": "completed",
                "result": {
                    "video_id": "video-123",
                    "url": "https://www.youtube.com/watch?v=video-123",
                },
            },
        )

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(route)
    ) as client:
        await make_handler(store, client).execute(
            upload_config(),
            input_paths,
            output_path,
        )

    assert 120 in timeout_values
    assert 15 in timeout_values


@pytest.mark.asyncio
async def test_lease_refresh_denial_prevents_preflight_attempt_and_post(
    media_paths,
) -> None:
    store = FakeOperationStore(["submit"])
    seen: list[httpx.Request] = []

    async def deny_refresh(*, minimum_margin_seconds: float):
        raise JobExecutionAuthorityBlocked("worker lease margin is insufficient")

    def route(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise AssertionError("denied lease refresh must prevent manager calls")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(route)
    ) as client:
        with pytest.raises(JobExecutionAuthorityBlocked, match="margin"):
            await make_handler(
                store,
                client,
                lease_refresher=deny_refresh,
            ).execute(upload_config(), input_paths, output_path)

    assert seen == []
    assert store.attempting == []


@pytest.mark.asyncio
async def test_lease_loss_after_post_attempt_keeps_durable_uncertainty_fence(
    media_paths,
) -> None:
    store = FakeOperationStore(["submit"])

    async def reject_uncertain(
        operation_id,
        error_message,
        *,
        context=None,
    ):
        raise JobExecutionAuthorityBlocked("worker lease was fenced")

    store.mark_uncertain = reject_uncertain

    def route(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=auth_payload())
        raise httpx.ReadError("connection outcome is unknown")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(route)
    ) as client:
        with pytest.raises(JobExecutionAuthorityBlocked, match="fenced"):
            await make_handler(store, client).execute(
                upload_config(),
                input_paths,
                output_path,
            )

    assert store.attempting == [OPERATION_ID]
    assert store.operation.request_attempted_at is not None
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_public_privacy_is_rejected_before_any_http(media_paths):
    store = FakeOperationStore(["submit"])
    seen: list[httpx.Request] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise AssertionError("public uploads must not call YouTubeManager")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError, match="private or unlisted"):
            await make_handler(store, client).execute(
                upload_config(privacy="public"), input_paths, output_path
            )

    assert seen == []
    assert store.claim_contexts == []
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_disabled_publishing_is_rejected_before_any_http(monkeypatch, media_paths):
    monkeypatch.setenv("YOUTUBE_PUBLISH_ENABLED", "false")
    store = FakeOperationStore(["submit"])
    seen: list[httpx.Request] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise AssertionError("disabled publishing must not call YouTubeManager")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError, match="YOUTUBE_PUBLISH_ENABLED"):
            await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert seen == []
    assert store.claim_contexts == []
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_public_publish_switch_must_remain_false_before_any_http(monkeypatch, media_paths):
    monkeypatch.setenv("PUBLIC_PUBLISH_ENABLED", "true")
    store = FakeOperationStore(["submit"])
    seen: list[httpx.Request] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise AssertionError("public publishing switch must prevent manager calls")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError, match="PUBLIC_PUBLISH_ENABLED"):
            await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert seen == []
    assert store.claim_contexts == []


@pytest.mark.asyncio
async def test_missing_internal_execution_context_is_rejected_before_claim_or_http(media_paths):
    store = FakeOperationStore(["submit"])
    seen: list[httpx.Request] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise AssertionError("invalid worker context must not call YouTubeManager")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError, match="_input_artifact_ids"):
            await make_handler(store, client).execute(
                upload_config(_input_artifact_ids={}), input_paths, output_path
            )

    assert seen == []
    assert store.claim_contexts == []


@pytest.mark.asyncio
async def test_execution_claim_is_bound_into_upload_operation_context(
    monkeypatch,
    media_paths,
) -> None:
    store = FakeOperationStore(["replay"])
    store.operation.status = "succeeded"
    store.operation.receipt_json = dict(store.durable_receipt)
    captured_contexts: list[dict] = []

    def record_context(**kwargs):
        captured_contexts.append(dict(kwargs))
        return SimpleNamespace(**kwargs)

    def route(request: httpx.Request) -> httpx.Response:
        raise AssertionError("replay must not call YouTubeManager")

    monkeypatch.setattr(youtube_upload_module, "UploadOperationContext", record_context)
    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        await make_handler(store, client).execute(
            upload_config(
                    _execution_claim={
                        "worker_id": "gpu-worker@150:42",
                        "started_at": "2026-07-22T12:00:00+00:00",
                        "worker_registration_id": (
                            "00000000-0000-0000-0000-000000000106"
                        ),
                        "worker_lease_epoch": 7,
                    }
            ),
            input_paths,
            output_path,
        )

    assert len(captured_contexts) == 1
    assert "execution_claim" in captured_contexts[0]
    execution_claim = captured_contexts[0]["execution_claim"]
    assert execution_claim.job_id == JOB_ID
    assert execution_claim.node_execution_id == NODE_EXECUTION_ID
    assert execution_claim.worker_id == "gpu-worker@150:42"
    assert execution_claim.started_at.isoformat() == "2026-07-22T12:00:00+00:00"


@pytest.mark.asyncio
async def test_unauthenticated_manager_is_marked_failed_without_upload_post(media_paths):
    store = FakeOperationStore(["submit"])
    seen: list[tuple[str, str]] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        assert request.method == "GET"
        assert request.url.path == "/api/auth/status"
        return httpx.Response(200, json=auth_payload(authenticated=False))

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError, match="authenticated"):
            await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert seen == [("GET", "/api/auth/status")]
    assert store.failed and store.failed[0][0] == OPERATION_ID
    assert store.uncertain == []
    assert not Path(output_path).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "quota"),
    [
        (
            "missing cost",
            {
                "daily_limit": 10_000,
                "estimated_units_used": 0,
                "estimated_units_remaining": 10_000,
            },
        ),
        (
            "malformed limit",
            {
                "daily_limit": "10_000",
                "estimated_units_used": 0,
                "estimated_units_remaining": 10_000,
                "upload_cost_per_request": 1_600,
            },
        ),
        (
            "nonfinite remaining",
            {
                "daily_limit": 10_000,
                "estimated_units_used": 0,
                "estimated_units_remaining": float("nan"),
                "upload_cost_per_request": 1_600,
            },
        ),
        (
            "nonfinite cost",
            {
                "daily_limit": 10_000,
                "estimated_units_used": 0,
                "estimated_units_remaining": 10_000,
                "upload_cost_per_request": float("inf"),
            },
        ),
        (
            "insufficient remaining",
            {
                "daily_limit": 10_000,
                "estimated_units_used": 8_401,
                "estimated_units_remaining": 1_599,
                "upload_cost_per_request": 1_600,
            },
        ),
        (
            "manager cost below expected",
            {
                "daily_limit": 10_000,
                "estimated_units_used": 0,
                "estimated_units_remaining": 10_000,
                "upload_cost_per_request": 1_599,
            },
        ),
    ],
)
async def test_invalid_or_insufficient_nested_quota_is_marked_failed_without_upload_post(
    name,
    quota,
    media_paths,
):
    store = FakeOperationStore(["submit"])
    seen: list[tuple[str, str]] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        assert request.method == "GET"
        assert request.url.path == "/api/auth/status"
        return httpx.Response(200, json=auth_payload(quota=quota))

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError, match="quota"):
            await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert seen == [("GET", "/api/auth/status")], name
    assert store.failed and store.failed[0][0] == OPERATION_ID
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_fresh_claim_submits_then_polls_and_returns_only_durable_receipt(media_paths):
    durable_receipt = {
        "video_id": "video-123",
        "url": "https://www.youtube.com/watch?v=video-123",
        "title": "Canary upload",
        "privacy": "unlisted",
        "tags": ["canary"],
        "quota_estimate": 1600,
    }
    store = FakeOperationStore(["submit"], durable_receipt=durable_receipt)
    seen: list[tuple[str, str]] = []
    poll_statuses = [
        {"status": "pending"},
        {
            "status": "completed",
            "result": {
                "video_id": "video-123",
                "url": "https://www.youtube.com/watch?v=video-123",
                "access_token": "manager-secret",
            },
        },
    ]

    def route(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            body = request.content
            assert b'name="file"' in body
            assert b'name="title"' in body and b"Canary upload" in body
            assert b'name="privacy_status"' in body and b"unlisted" in body
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        if request.method == "GET" and request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            return httpx.Response(200, json=poll_statuses.pop(0))
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        result = await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert seen == [
        ("GET", "/api/auth/status"),
        ("POST", "/api/upload"),
        ("GET", f"/api/status/{MANAGER_TASK_ID}"),
        ("GET", f"/api/status/{MANAGER_TASK_ID}"),
    ]
    assert store.claim_contexts[0].job_id == JOB_ID
    assert store.claim_contexts[0].node_execution_id == NODE_EXECUTION_ID
    assert store.claim_contexts[0].input_artifact_id == INPUT_ARTIFACT_ID
    assert store.claim_contexts[0].content_sha256 == hashlib.sha256(
        b"owned unlisted canary media"
    ).hexdigest()
    assert store.claim_contexts[0].privacy == "unlisted"
    assert store.attempting == [OPERATION_ID]
    assert store.submitted == [(OPERATION_ID, MANAGER_TASK_ID)]
    assert store.succeeded == [
        (
            OPERATION_ID,
            "video-123",
            {
                "video_id": "video-123",
                "url": "https://www.youtube.com/watch?v=video-123",
                "access_token": "manager-secret",
            },
        )
    ]
    assert Path(output_path).read_bytes() == b"owned unlisted canary media"
    assert result == {"youtube": durable_receipt}
    assert "manager-secret" not in str(result)


@pytest.mark.asyncio
async def test_submitted_claim_skips_upload_post_and_resumes_polling(media_paths):
    store = FakeOperationStore(["resume"])
    store.operation.status = "submitted"
    store.operation.manager_task_id = MANAGER_TASK_ID
    seen: list[tuple[str, str]] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        assert request.method == "GET"
        assert request.url.path == f"/api/status/{MANAGER_TASK_ID}"
        return httpx.Response(
            200,
            json={
                "status": "completed",
                "result": {
                    "video_id": "video-123",
                    "url": "https://www.youtube.com/watch?v=video-123",
                },
            },
        )

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        result = await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert seen == [("GET", f"/api/status/{MANAGER_TASK_ID}")]
    assert store.submitted == []
    assert store.succeeded and store.succeeded[0][1] == "video-123"
    assert Path(output_path).exists()
    assert result == {"youtube": store.durable_receipt}


@pytest.mark.asyncio
async def test_replay_claim_makes_no_http_request_and_copies_after_durable_success(media_paths):
    durable_receipt = {
        "video_id": "video-replayed",
        "url": "https://www.youtube.com/watch?v=video-replayed",
        "title": "Canary upload",
        "privacy": "private",
        "tags": [],
        "quota_estimate": None,
    }
    store = FakeOperationStore(["replay"], durable_receipt=durable_receipt)
    store.operation.status = "succeeded"
    store.operation.receipt_json = dict(durable_receipt)
    seen: list[httpx.Request] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise AssertionError("replay must not call YouTubeManager")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        result = await make_handler(store, client).execute(upload_config(privacy="private"), input_paths, output_path)

    assert seen == []
    assert store.submitted == []
    assert store.succeeded == []
    assert Path(output_path).read_bytes() == b"owned unlisted canary media"
    assert result == {"youtube": durable_receipt}


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["reserved", "uncertain", "failed"])
async def test_blocked_claim_makes_no_http_request_or_output(status, media_paths):
    store = FakeOperationStore(["block"])
    store.operation.status = status
    seen: list[httpx.Request] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise AssertionError("blocked operation must not call YouTubeManager")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError, match="cannot safely"):
            await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert seen == []
    assert store.submitted == []
    assert store.succeeded == []
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_failed_manager_status_marks_failed_without_copying_output(media_paths):
    store = FakeOperationStore(["submit"])

    def route(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        if request.method == "GET" and request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            return httpx.Response(200, json={"status": "failed", "error": "manager rejected media"})
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError, match="manager rejected media"):
            await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert store.submitted == [(OPERATION_ID, MANAGER_TASK_ID)]
    assert store.failed and store.failed[-1][0] == OPERATION_ID
    assert not Path(output_path).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["missing_task", "post_transport", "poll_timeout"])
async def test_ambiguous_submission_marks_uncertain_and_a_retry_never_posts_again(scenario, media_paths):
    store = FakeOperationStore(["submit", "block"])
    post_count = 0

    def route(request: httpx.Request) -> httpx.Response:
        nonlocal post_count
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            post_count += 1
            if scenario == "missing_task":
                return httpx.Response(200, json={"status": "pending"})
            if scenario == "post_transport":
                raise httpx.ConnectError("connection reset", request=request)
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        if request.method == "GET" and request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            assert scenario == "poll_timeout"
            return httpx.Response(200, json={"status": "uploading"})
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    input_paths, output_path = media_paths
    timeout_seconds = 0 if scenario == "poll_timeout" else 10
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client, timeout_seconds=timeout_seconds)
        with pytest.raises(RuntimeError):
            await handler.execute(upload_config(), input_paths, output_path)
        with pytest.raises(RuntimeError, match="cannot safely"):
            await handler.execute(upload_config(), input_paths, output_path)

    assert post_count == 1
    assert store.uncertain and store.uncertain[0][0] == OPERATION_ID
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_cancelled_handler_does_not_claim_or_call_manager(media_paths):
    store = FakeOperationStore(["submit"])
    seen: list[httpx.Request] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise AssertionError("cancelled handler must not call YouTubeManager")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client)
        handler.cancel()
        with pytest.raises(CancelledError):
            await handler.execute(upload_config(), input_paths, output_path)

    assert store.claim_contexts == []
    assert seen == []
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_cancellation_during_preflight_prevents_upload_post(media_paths):
    store = FakeOperationStore(["submit"])
    seen: list[tuple[str, str]] = []
    handler: YouTubeUploadHandler

    def route(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == "/api/auth/status":
            handler.cancel()
            return httpx.Response(200, json=auth_payload())
        raise AssertionError("cancellation during preflight must prevent upload POST")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client)
        with pytest.raises(CancelledError):
            await handler.execute(upload_config(), input_paths, output_path)

    assert seen == [("GET", "/api/auth/status")]
    assert len(store.claim_contexts) == 1
    assert store.submitted == []
    assert store.uncertain == []
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_submission_fence_wraps_preflight_and_irreversible_post(media_paths):
    store = FakeOperationStore(["submit"])

    def route(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/auth/status":
            assert store.submission_fence_active
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            assert store.submission_fence_active
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        if request.method == "GET" and request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            assert not store.submission_fence_active
            return httpx.Response(
                200,
                json={
                    "status": "completed",
                    "result": {
                        "video_id": "video-123",
                        "url": "https://www.youtube.com/watch?v=video-123",
                    },
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert store.submission_fence_contexts == store.claim_contexts


@pytest.mark.asyncio
async def test_rejected_submission_fence_prevents_preflight_and_state_changes(
    media_paths,
) -> None:
    store = FakeOperationStore(["submit"])
    seen: list[tuple[str, str]] = []

    @contextlib.asynccontextmanager
    async def reject_fence(_context):
        raise JobExecutionAuthorityBlocked("node execution claim changed")
        yield

    store.submission_fence = reject_fence

    def route(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        return httpx.Response(200, json=auth_payload())

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(JobExecutionAuthorityBlocked, match="claim changed"):
            await make_handler(store, client).execute(
                upload_config(),
                input_paths,
                output_path,
            )

    assert seen == []
    assert store.attempting == []
    assert store.failed == []
    assert store.uncertain == []


@pytest.mark.asyncio
async def test_cancellation_during_upload_persists_submitted_then_uncertain_and_never_reposts(media_paths):
    store = FakeOperationStore(["submit", "block"])
    post_count = 0
    handler: YouTubeUploadHandler

    def route(request: httpx.Request) -> httpx.Response:
        nonlocal post_count
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            post_count += 1
            handler.cancel()
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        raise AssertionError("cancelled upload must not begin polling")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client)
        with pytest.raises(CancelledError):
            await handler.execute(upload_config(), input_paths, output_path)
        retry_handler = make_handler(store, client)
        with pytest.raises(RuntimeError, match="cannot safely"):
            await retry_handler.execute(upload_config(), input_paths, output_path)

    assert post_count == 1
    assert store.submitted == [(OPERATION_ID, MANAGER_TASK_ID)]
    assert store.uncertain and store.uncertain[-1][0] == OPERATION_ID
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_cancellation_during_polling_marks_uncertain_without_output(media_paths):
    store = FakeOperationStore(["submit", "block"])
    handler: YouTubeUploadHandler

    def route(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        if request.method == "GET" and request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            handler.cancel()
            return httpx.Response(
                200,
                json={
                    "status": "completed",
                    "result": {
                        "video_id": "video-123",
                        "url": "https://www.youtube.com/watch?v=video-123",
                    },
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client)
        with pytest.raises(CancelledError):
            await handler.execute(upload_config(), input_paths, output_path)
        retry_handler = make_handler(store, client)
        with pytest.raises(RuntimeError, match="cannot safely"):
            await retry_handler.execute(upload_config(), input_paths, output_path)

    assert store.submitted == [(OPERATION_ID, MANAGER_TASK_ID)]
    assert store.uncertain and store.uncertain[-1][0] == OPERATION_ID
    assert store.succeeded == []
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_snapshot_binds_hash_upload_and_output_to_original_bytes(media_paths):
    store = FakeOperationStore(["submit"])
    input_paths, output_path = media_paths
    input_path = Path(input_paths["input"])
    original = input_path.read_bytes()

    def route(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            assert original in request.content
            input_path.write_bytes(b"mutated after durable claim")
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        if request.method == "GET" and request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            return httpx.Response(
                200,
                json={
                    "status": "completed",
                    "result": {
                        "video_id": "video-123",
                        "url": "https://www.youtube.com/watch?v=video-123",
                    },
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert store.claim_contexts[0].content_sha256 == hashlib.sha256(original).hexdigest()
    assert Path(output_path).read_bytes() == original


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "",
        "http://www.youtube.com/watch?v=video-123",
        "https://www.youtube.com/watch?v=other-video",
    ],
)
async def test_completed_result_requires_canonical_watch_url(url, media_paths):
    store = FakeOperationStore(["submit"])

    def route(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        if request.method == "GET" and request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            return httpx.Response(
                200,
                json={"status": "completed", "result": {"video_id": "video-123", "url": url}},
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError, match="invalid result"):
            await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert store.succeeded == []
    assert store.uncertain and store.uncertain[-1][0] == OPERATION_ID
    assert not Path(output_path).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["upload", "status"])
async def test_hanging_manager_request_uses_wall_clock_timeout_and_marks_uncertain(phase, media_paths):
    store = FakeOperationStore(["submit", "block"])
    post_count = 0

    async def route(request: httpx.Request) -> httpx.Response:
        nonlocal post_count
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            post_count += 1
            if phase == "upload":
                await asyncio.Event().wait()
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        if request.method == "GET" and request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            assert phase == "status"
            await asyncio.Event().wait()
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client, timeout_seconds=0.01)
        with pytest.raises(RuntimeError, match="uncertain"):
            await asyncio.wait_for(handler.execute(upload_config(), input_paths, output_path), timeout=0.2)
        with pytest.raises(RuntimeError, match="cannot safely"):
            await handler.execute(upload_config(), input_paths, output_path)

    assert post_count == 1
    assert store.uncertain and store.uncertain[-1][0] == OPERATION_ID
    assert not Path(output_path).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "expected_state"),
    [(404, "failed"), (422, "failed"), (500, "uncertain")],
)
async def test_upload_response_classification_blocks_retry_without_copying_output(
    status_code,
    expected_state,
    media_paths,
):
    store = FakeOperationStore(["submit", "block"])
    post_count = 0

    def route(request: httpx.Request) -> httpx.Response:
        nonlocal post_count
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            post_count += 1
            return httpx.Response(status_code, json={"detail": "rejected"})
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client)
        with pytest.raises(RuntimeError):
            await handler.execute(upload_config(), input_paths, output_path)
        with pytest.raises(RuntimeError, match="cannot safely"):
            await handler.execute(upload_config(), input_paths, output_path)

    assert post_count == 1
    assert bool(store.failed) is (expected_state == "failed")
    assert bool(store.uncertain) is (expected_state == "uncertain")
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_resume_with_snapshot_hash_mismatch_marks_uncertain_without_http_or_output(media_paths):
    store = FakeOperationStore(["resume", "block"])
    input_paths, output_path = media_paths
    original_hash = hashlib.sha256(Path(input_paths["input"]).read_bytes()).hexdigest()
    store.operation.status = "submitted"
    store.operation.manager_task_id = MANAGER_TASK_ID
    store.operation.content_sha256 = original_hash
    Path(input_paths["input"]).write_bytes(b"mutated after prior submission")
    seen: list[httpx.Request] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise AssertionError("hash-mismatched resume must not call YouTubeManager")

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client)
        with pytest.raises(RuntimeError, match="content hash"):
            await handler.execute(upload_config(), input_paths, output_path)
        retry_handler = make_handler(store, client)
        with pytest.raises(RuntimeError, match="cannot safely"):
            await retry_handler.execute(upload_config(), input_paths, output_path)

    assert seen == []
    assert store.uncertain and store.uncertain[-1][0] == OPERATION_ID
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_replay_with_snapshot_hash_mismatch_never_rewrites_terminal_success(media_paths):
    store = FakeOperationStore(["replay"])
    input_paths, output_path = media_paths
    original_hash = hashlib.sha256(Path(input_paths["input"]).read_bytes()).hexdigest()
    store.operation.status = "succeeded"
    store.operation.content_sha256 = original_hash
    store.operation.receipt_json = dict(store.durable_receipt)
    Path(input_paths["input"]).write_bytes(b"mutated after prior success")
    seen: list[httpx.Request] = []

    def route(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        raise AssertionError("hash-mismatched replay must not call YouTubeManager")

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError, match="content hash"):
            await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert seen == []
    assert store.uncertain == []
    assert store.failed == []
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_cancellation_while_persisting_manager_task_reaches_submitted_then_uncertain(media_paths):
    store = FakeOperationStore(["submit", "block"])
    store.mark_submitted_started = asyncio.Event()
    store.mark_submitted_continue = asyncio.Event()
    post_count = 0

    def route(request: httpx.Request) -> httpx.Response:
        nonlocal post_count
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            post_count += 1
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        raise AssertionError("cancellation while persisting must not poll")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client)
        execution = asyncio.create_task(handler.execute(upload_config(), input_paths, output_path))
        await store.mark_submitted_started.wait()
        execution.cancel()
        store.mark_submitted_continue.set()
        with pytest.raises(asyncio.CancelledError):
            await execution
        retry_handler = make_handler(store, client)
        with pytest.raises(RuntimeError, match="cannot safely"):
            await retry_handler.execute(upload_config(), input_paths, output_path)

    assert post_count == 1
    assert store.submitted == [(OPERATION_ID, MANAGER_TASK_ID)]
    assert store.uncertain and store.uncertain[-1][0] == OPERATION_ID
    assert not Path(output_path).exists()


@pytest.mark.asyncio
async def test_replay_replaces_read_only_output_with_normal_worker_permissions(media_paths):
    store = FakeOperationStore(["replay", "replay"])
    store.operation.status = "succeeded"
    store.operation.receipt_json = dict(store.durable_receipt)
    input_paths, output_path = media_paths

    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: None)) as client:
        await make_handler(store, client).execute(upload_config(), input_paths, output_path)
        Path(output_path).chmod(0o400)
        await make_handler(store, client).execute(upload_config(), input_paths, output_path)

    assert Path(output_path).read_bytes() == Path(input_paths["input"]).read_bytes()
    assert stat.S_IMODE(Path(output_path).stat().st_mode) == 0o600


@pytest.mark.asyncio
async def test_polling_404_remains_uncertain(media_paths):
    store = FakeOperationStore(["submit", "block"])

    def route(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST" and request.url.path == "/api/upload":
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID, "status": "pending"})
        if request.method == "GET" and request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            return httpx.Response(404, json={"detail": "missing"})
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    input_paths, output_path = media_paths
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        handler = make_handler(store, client)
        with pytest.raises(RuntimeError, match="status is uncertain"):
            await handler.execute(upload_config(), input_paths, output_path)
        retry_handler = make_handler(store, client)
        with pytest.raises(RuntimeError, match="cannot safely"):
            await retry_handler.execute(upload_config(), input_paths, output_path)

    assert store.failed == []
    assert store.uncertain and store.uncertain[-1][0] == OPERATION_ID
    assert not Path(output_path).exists()
