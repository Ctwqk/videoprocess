"""Slow producer validation must not consume the irreversible POST lease budget."""

import asyncio
import uuid
from types import SimpleNamespace

import httpx
import pytest

from app.services import youtube_upload_operations as operations
from app.services.job_execution_authority import JobExecutionAuthorityBlocked
from worker.handlers.youtube_upload import YouTubeUploadHandler
from worker.handlers.base import CancelledError
from tests.worker.test_youtube_upload_handler import auth_payload, upload_config


@pytest.fixture
def slow_submission(monkeypatch, tmp_path):
    monkeypatch.setenv("YOUTUBE_PUBLISH_ENABLED", "true")
    monkeypatch.setenv("PUBLIC_PUBLISH_ENABLED", "false")
    env = SimpleNamespace(
        now=0,
        expires=180,
        events=[],
        posts=0,
        refreshes=0,
        fail_refresh=None,
        pause_refresh=None,
        refresh_waiting=asyncio.Event(),
        lose_claim=False,
        fault=None,
    )
    operation = SimpleNamespace(
        id=uuid.uuid4(),
        status="reserved",
        manager_task_id=None,
        request_attempted_at=None,
        content_sha256=None,
    )

    class Session:
        active = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def begin(self):
            assert not self.active
            self.active = True
            env.events.append("begin")

        def in_transaction(self):
            return self.active

        async def commit(self):
            assert self.active
            self.active = False
            env.events.append("commit")

        async def rollback(self):
            self.active = False
            env.events.append("rollback")

    db = Session()
    store = operations.YouTubeUploadOperationStore(lambda: db)

    async def claim(context):
        operation.content_sha256 = context.content_sha256
        return operations.UploadOperationClaim("submit", operation)

    async def require_claim(session, _claim):
        assert session.active
        env.events.append("claim")
        if env.lose_claim:
            raise JobExecutionAuthorityBlocked("node claim lost")

    async def margin(session, _claim, *, minimum_margin_seconds):
        assert session.active and minimum_margin_seconds == 150
        env.events.append("margin")
        if env.expires - env.now < minimum_margin_seconds:
            raise JobExecutionAuthorityBlocked("lease margin insufficient")

    async def transition(session, _id, _context, *, transition, **kwargs):
        assert session.active
        env.events.append(transition)
        if transition in {"attempting", "fence"}:
            # More than the 30s difference between lease and required margin.
            env.now += 56
        if transition == "attempting":
            operation.request_attempted_at = env.now
        if transition == "submitted":
            operation.status = "submitted"
            operation.manager_task_id = kwargs["manager_task_id"]
        return operation

    async def refresh(*, minimum_margin_seconds):
        assert minimum_margin_seconds == 150
        env.refreshes += 1
        env.events.append("refresh")
        if env.refreshes == env.pause_refresh:
            env.refresh_waiting.set()
            await asyncio.Future()
        if env.refreshes == env.fail_refresh:
            if env.fault == "cancel":
                raise asyncio.CancelledError
            raise JobExecutionAuthorityBlocked("authenticated refresh failed")
        if env.fault == "expired" and env.refreshes == 3:
            env.expires = env.now
            return
        if env.fault == "claim" and env.refreshes == 3:
            env.lose_claim = True
        if env.fault == "handler_cancel" and env.refreshes == 3:
            env.handler.cancel()
        env.expires = env.now + 180

    async def completed(*_args):
        return operation

    monkeypatch.setattr(store, "claim", claim)
    monkeypatch.setattr(store, "_transition_registered", transition)
    monkeypatch.setattr(
        operations, "require_registered_worker_node_claim", require_claim
    )
    monkeypatch.setattr(operations, "require_worker_registration_margin", margin)
    env.store, env.refresh, env.db, env.operation = store, refresh, db, operation
    env.path = tmp_path / "input.mp4"
    env.path.write_bytes(b"owned test media")

    def route(request):
        if request.method == "POST":
            env.posts += 1
            assert db.active and env.expires - env.now >= 150
            env.events.append("post")
            return httpx.Response(200, json={"task_id": str(uuid.uuid4())})
        return httpx.Response(200, json=auth_payload())

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
            env.handler = YouTubeUploadHandler(
                store, client=client, base_url="http://manager", lease_refresher=refresh
            )
            monkeypatch.setattr(env.handler, "_poll_for_completion", completed)
            monkeypatch.setattr(env.handler, "_copy_durable_receipt", lambda *_args: {})
            return await env.handler.execute(
                upload_config(), {"input": str(env.path)}, str(tmp_path / "out.mp4")
            )

    env.run = run
    return env


async def test_slow_producer_checks_refresh_both_sides_of_final_fence(slow_submission):
    env = slow_submission
    await env.run()
    assert env.posts == 1 and env.operation.status == "submitted"
    assert env.events == [
        "refresh",
        "begin",
        "claim",
        "margin",
        "attempting",
        "commit",
        "refresh",
        "begin",
        "claim",
        "margin",
        "fence",
        "refresh",
        "claim",
        "margin",
        "post",
        "submitted",
        "commit",
    ]


@pytest.mark.parametrize("point", [2, 3])
@pytest.mark.parametrize("fault", ["failure", "cancel"])
async def test_refresh_failure_or_cancellation_never_posts(
    slow_submission, point, fault
):
    env = slow_submission
    env.fail_refresh, env.fault = point, fault
    error = (
        asyncio.CancelledError if fault == "cancel" else JobExecutionAuthorityBlocked
    )
    with pytest.raises(error):
        await env.run()
    assert env.refreshes == point
    assert env.posts == 0 and not env.db.active
    assert (
        env.operation.request_attempted_at is not None
        and env.operation.manager_task_id is None
    )
    assert env.operation.status == "reserved"
    assert env.store._action_for(env.operation) == "block"
    assert env.store._active_submission_fence.get() is None


@pytest.mark.parametrize("fault", ["expired", "claim", "handler_cancel"])
async def test_final_refresh_does_not_replace_database_or_cancellation_gates(
    slow_submission, fault
):
    env = slow_submission
    env.fault = fault
    with pytest.raises(CancelledError if fault == "handler_cancel" else RuntimeError):
        await env.run()
    assert env.refreshes == 3
    assert env.posts == 0 and not env.db.active
    assert (
        env.operation.request_attempted_at is not None
        and env.operation.manager_task_id is None
    )


@pytest.mark.parametrize("point", [2, 3])
async def test_task_cancel_during_authenticated_refresh_never_posts(
    slow_submission, point
):
    env = slow_submission
    env.pause_refresh = point
    task = asyncio.create_task(env.run())
    await asyncio.wait_for(env.refresh_waiting.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert env.posts == 0 and not env.db.active
    assert (
        env.operation.request_attempted_at is not None
        and env.operation.manager_task_id is None
    )
    assert env.store._action_for(env.operation) == "block"


async def test_registered_attempt_without_refresher_refuses_before_attempt_write(
    slow_submission,
):
    env = slow_submission
    handler = YouTubeUploadHandler(env.store, base_url="http://manager")
    context = handler._operation_context(
        node_config=upload_config(),
        content_sha256="a" * 64,
        title="Canary upload",
        privacy="unlisted",
    )
    with pytest.raises(JobExecutionAuthorityBlocked, match="lease refresher"):
        async with env.store.submission_fence(context):
            await env.store.mark_attempting(env.operation.id, context=context)
    assert env.posts == 0 and env.operation.request_attempted_at is None
    assert not env.db.active
