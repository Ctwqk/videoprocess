from __future__ import annotations

import asyncio
import contextlib
import hashlib
import stat
import uuid
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from app.services.job_execution_authority import JobExecutionAuthorityBlocked
from app.services.youtube_upload_operations import UploadOperationClaim
from worker.handlers import youtube_upload as youtube_upload_module
from worker.handlers.base import CancelledError
from worker.handlers.youtube_upload import YouTubeUploadHandler


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

    async def mark_submitted(self, operation_id: uuid.UUID, manager_task_id: str):
        if self.mark_submitted_started is not None:
            self.mark_submitted_started.set()
            assert self.mark_submitted_continue is not None
            await self.mark_submitted_continue.wait()
        self.submitted.append((operation_id, manager_task_id))
        self.operation.status = "submitted"
        self.operation.manager_task_id = manager_task_id
        return self.operation

    async def mark_attempting(self, operation_id: uuid.UUID):
        assert self.submission_fence_active
        self.attempting.append(operation_id)
        self.operation.request_attempted_at = object()
        return self.operation

    async def mark_succeeded(self, operation_id: uuid.UUID, platform_video_id: str, receipt: dict):
        self.succeeded.append((operation_id, platform_video_id, receipt))
        self.operation.status = "succeeded"
        self.operation.receipt_json = dict(self.durable_receipt)
        return self.operation

    async def mark_failed(self, operation_id: uuid.UUID, error_message: str):
        self.failed.append((operation_id, error_message))
        self.operation.status = "failed"
        return self.operation

    async def mark_uncertain(self, operation_id: uuid.UUID, error_message: str):
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
    return YouTubeUploadHandler(
        store,
        client=client,
        base_url="http://youtube-manager",
        poll_interval_seconds=0,
        **overrides,
    )


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
