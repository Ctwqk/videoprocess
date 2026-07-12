from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from app.services.youtube_upload_operations import UploadOperationClaim
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
        self.submitted: list[tuple[uuid.UUID, str]] = []
        self.succeeded: list[tuple[uuid.UUID, str, dict]] = []
        self.failed: list[tuple[uuid.UUID, str]] = []
        self.uncertain: list[tuple[uuid.UUID, str]] = []

    async def claim(self, context):
        self.claim_contexts.append(context)
        action = self._actions.pop(0)
        return UploadOperationClaim(action=action, operation=self.operation)

    async def mark_submitted(self, operation_id: uuid.UUID, manager_task_id: str):
        self.submitted.append((operation_id, manager_task_id))
        self.operation.status = "submitted"
        self.operation.manager_task_id = manager_task_id
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
