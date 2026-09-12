from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from app.services.job_execution_authority import NodeExecutionClaim
from app.services.youtube_upload_operations import UploadOperationClaim, UploadOperationContext


def drill_api():
    assert importlib.util.find_spec("worker.youtube_ack_drill") is not None, (
        "the concrete pre-receipt drill is not implemented"
    )
    return importlib.import_module("worker.youtube_ack_drill")


def test_concrete_drill_rejects_dictionary_configuration(tmp_path):
    api = drill_api()
    with pytest.raises(ValueError):
        api.OwnedUnlistedAckDrill({"enabled": True}, state_dir=tmp_path)


@pytest.fixture
def target():
    api = drill_api()
    job, node = uuid.uuid4(), uuid.uuid4()
    return api.AckDrillTarget(
        context=UploadOperationContext(
            job_id=job, node_execution_id=node, input_artifact_id=uuid.uuid4(),
            content_sha256="a" * 64, title="Owned canary", privacy="unlisted",
            execution_claim=NodeExecutionClaim(
                job_id=job, node_execution_id=node, worker_id="publisher@localhost:1",
                started_at=datetime.now(timezone.utc),
                worker_registration_id=uuid.uuid4(), worker_lease_epoch=7,
            ),
        ),
        production_task_id=uuid.uuid4(), drill_id=uuid.uuid4(),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        owned_attestation_sha256="b" * 64, manager_origin="http://youtube-manager",
    )


@pytest.fixture
def state_dir(tmp_path):
    path = tmp_path / "drill"
    path.mkdir(mode=0o700)
    return path


def reserved(target):
    context = target.context
    return UploadOperationClaim("submit", SimpleNamespace(
        id=uuid.uuid4(), production_task_id=target.production_task_id,
        job_id=context.job_id, node_execution_id=context.node_execution_id,
        input_artifact_id=context.input_artifact_id, content_sha256=context.content_sha256,
        title=context.title, privacy=context.privacy, status="reserved",
        manager_task_id=None, request_attempted_at=None, receipt_json={},
        platform_video_id=None, completed_at=None,
    ))


def prepare(helper, target, claim):
    helper.prepare(target.context, claim, manager_origin=target.manager_origin, timeout_seconds=5)


def test_target_and_operation_accept_actual_postgres_uuid_values(target, state_dir):
    from asyncpg.pgproto.pgproto import UUID as PostgresUUID

    context = target.context
    claim = context.execution_claim
    context = replace(context, execution_claim=replace(
        claim, worker_registration_id=PostgresUUID(str(claim.worker_registration_id)),
    ))
    target = replace(target, context=context)
    operation = reserved(target)
    operation.operation.id = PostgresUUID(str(operation.operation.id))
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    prepare(helper, target, operation)
    assert json.loads((state_dir / "0001.json").read_text())["operation_id"] == str(operation.operation.id)


@pytest.mark.parametrize("field,value", [
    ("privacy", "private"), ("content_sha256", "A" * 64),
    ("content_sha256", "x"), ("title", " title "), ("job_id", "not-a-uuid"),
])
def test_target_rejects_nonexact_context(target, field, value):
    with pytest.raises(ValueError):
        replace(target, context=replace(target.context, **{field: value}))


@pytest.mark.parametrize("field,value", [
    ("worker_id", ""), ("worker_id", "worker\nsecret"),
    ("worker_registration_id", None), ("worker_lease_epoch", True),
    ("worker_lease_epoch", 0), ("started_at", datetime(2026, 1, 1)),
    ("job_id", uuid.UUID(int=1)),
])
def test_target_rejects_invalid_claim(target, field, value):
    with pytest.raises(ValueError):
        replace(target, context=replace(target.context, execution_claim=replace(
            target.context.execution_claim, **{field: value},
        )))


@pytest.mark.parametrize("field,value", [
    ("manager_origin", "http://user:password@youtube-manager"),
    ("manager_origin", "http://youtube-manager/api"),
    ("manager_origin", "http://youtube-manager?token=secret"),
    ("manager_origin", "file:///tmp/manager"),
    ("manager_origin", "http://youtube-manager/"),
    ("expires_at", datetime(2020, 1, 1, tzinfo=timezone.utc)),
    ("expires_at", datetime(2030, 1, 1)),
    ("owned_attestation_sha256", ""), ("production_task_id", "any"),
    ("drill_id", "../another"),
])
def test_target_rejects_invalid_trusted_configuration(target, field, value):
    with pytest.raises(ValueError):
        replace(target, **{field: value})


@pytest.mark.parametrize("change", ["mode", "owner", "symlink", "ancestor-symlink", "missing"])
def test_state_directory_must_be_preexisting_and_protected(target, state_dir, tmp_path, change):
    path = state_dir
    if change == "mode":
        path.chmod(0o770)
    elif change == "owner":
        # Ownership is checked against the effective uid, including root execution.
        from unittest.mock import patch
        with patch("os.geteuid", return_value=os.geteuid() + 1), pytest.raises((OSError, ValueError)):
            drill_api().OwnedUnlistedAckDrill(target, state_dir=path)
        return
    elif change == "symlink":
        path = tmp_path / "link"
        path.symlink_to(state_dir, target_is_directory=True)
    elif change == "ancestor-symlink":
        link = tmp_path / "parent-link"
        link.symlink_to(tmp_path, target_is_directory=True)
        path = link / "drill"
    else:
        path = tmp_path / "missing"
    with pytest.raises((OSError, ValueError)):
        drill_api().OwnedUnlistedAckDrill(target, state_dir=path)


def test_exclusive_start_survives_reconstruction(target, state_dir):
    api = drill_api()
    claim = reserved(target)
    first = api.OwnedUnlistedAckDrill(target, state_dir=state_dir)
    second = api.OwnedUnlistedAckDrill(target, state_dir=state_dir)

    def start(helper):
        try:
            prepare(helper, target, claim)
            return True
        except (OSError, RuntimeError):
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(start, (first, second))) == 1
    with pytest.raises((OSError, RuntimeError)):
        prepare(api.OwnedUnlistedAckDrill(target, state_dir=state_dir), target, claim)
    assert len(list(state_dir.iterdir())) == 1


@pytest.mark.parametrize("change", ["context", "origin", "task", "attempted", "resume", "expired"])
def test_prepare_fails_before_post_on_mismatch(target, state_dir, monkeypatch, change):
    api = drill_api()
    helper = api.OwnedUnlistedAckDrill(target, state_dir=state_dir)
    claim = reserved(target)
    context, origin = target.context, target.manager_origin
    if change == "context":
        context = replace(context, content_sha256="c" * 64)
    elif change == "origin":
        origin = "http://other-manager"
    elif change == "task":
        claim.operation.production_task_id = uuid.uuid4()
    elif change == "attempted":
        claim.operation.request_attempted_at = datetime.now(timezone.utc)
    elif change == "resume":
        claim = UploadOperationClaim("resume", claim.operation)
    else:
        monkeypatch.setattr(api.time, "time", lambda: target.expires_at.timestamp() + 1)
    with pytest.raises((RuntimeError, ValueError)):
        helper.prepare(context, claim, manager_origin=origin, timeout_seconds=5)
    assert not list(state_dir.iterdir())


@pytest.mark.parametrize("corruption", ["partial", "symlink", "hardlink", "mode", "extra"])
def test_partial_or_replaced_journal_blocks_post(target, state_dir, tmp_path, corruption):
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    claim = reserved(target)
    prepare(helper, target, claim)
    record = next(state_dir.iterdir())
    if corruption == "partial":
        record.write_text('{"partial":')
    elif corruption == "mode":
        record.chmod(0o644)
    elif corruption == "extra":
        (state_dir / "unexpected").write_text("partial")
    else:
        external = tmp_path / "external"
        record.rename(external)
        if corruption == "symlink":
            record.symlink_to(external)
        else:
            os.link(external, record)
    claim.operation.request_attempted_at = datetime.now(timezone.utc)
    with pytest.raises((OSError, RuntimeError, ValueError)):
        helper.record_post_attempt(target.context, claim.operation)


@pytest.mark.parametrize("failure_at", [1, 2])
def test_failed_file_or_directory_fsync_preserves_start_and_never_rearms(
    target, state_dir, monkeypatch, failure_at,
):
    api = drill_api()
    helper = api.OwnedUnlistedAckDrill(target, state_dir=state_dir)
    claim = reserved(target)
    original, calls = os.fsync, 0

    def fail(fd):
        nonlocal calls
        calls += 1
        if calls == failure_at:
            raise OSError("disk unavailable")
        original(fd)

    with monkeypatch.context() as patcher:
        patcher.setattr(api.os, "fsync", fail)
        with pytest.raises(OSError):
            prepare(helper, target, claim)
    assert list(state_dir.iterdir())
    with pytest.raises((OSError, RuntimeError)):
        prepare(api.OwnedUnlistedAckDrill(target, state_dir=state_dir), target, claim)


@pytest.mark.asyncio
async def test_video_verifier_polls_only_nonterminal_states_and_escapes_identity(target, state_dir):
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    prepare(helper, target, reserved(target))
    requests = []
    heartbeat = asyncio.Event()

    async def route(request):
        requests.append(request)
        if len(requests) == 1:
            asyncio.get_running_loop().call_soon(heartbeat.set)
            return httpx.Response(200, json={
                "video_id": "video/a?b", "privacy": "unlisted",
                "upload_status": "uploaded", "processing_status": "processing",
            })
        assert heartbeat.is_set()
        return httpx.Response(200, json={
            "video_id": "video/a?b", "privacy": "unlisted",
            "upload_status": "processed", "processing_status": "succeeded",
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        await helper.verify_video(client, "video/a?b", asyncio.Event())
    assert len(requests) == 2
    assert requests[0].url.raw_path == b"/api/videos/video%2Fa%3Fb/status"
    assert all(request.method == "GET" for request in requests)


@pytest.mark.parametrize("payload", [
    {}, [], {"video_id": "other", "privacy": "unlisted", "upload_status": "processed"},
    {"video_id": "video", "privacy": "public", "upload_status": "processed"},
    {"video_id": "video", "privacy": "private", "upload_status": "processed"},
    {"video_id": "video", "privacy": "unlisted", "upload_status": "completed"},
    {"video_id": "video", "privacy": "unlisted", "upload_status": "failed"},
    {"video_id": "video", "privacy": "unlisted", "upload_status": "processed", "processing_status": None},
    {"video_id": "video", "privacy": "unlisted", "upload_status": "processed", "processing_status": "processing"},
])
@pytest.mark.asyncio
async def test_video_verifier_rejects_ambiguous_or_unsafe_states(target, state_dir, payload):
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    prepare(helper, target, reserved(target))
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=payload),
    )) as client:
        with pytest.raises(RuntimeError):
            await helper.verify_video(client, "video", asyncio.Event())
    assert not (state_dir / "consumed.json").exists()


@pytest.mark.parametrize("extra", [
    {}, {"processing_status": "succeeded"}, {"published_at": None},
    {"published_at": "2026-09-10T07:00:00Z"},
    {"processing_status": "succeeded", "published_at": "2026-09-10T07:00:00Z"},
])
@pytest.mark.asyncio
async def test_video_verifier_preserves_deployed_canonical_fields(target, state_dir, extra):
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    prepare(helper, target, reserved(target))
    requests = []

    def route(request):
        requests.append(request)
        return httpx.Response(200, json={
            "video_id": "video", "privacy": "unlisted", "upload_status": "processed", **extra,
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        await helper.verify_video(client, "video", asyncio.Event())
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/videos/video/status"),
    ]


@pytest.fixture
def manager_video_status():
    # Same nine-field/three-raw-section shape as the captured D415 Manager response.
    return {
        "video_id": "video", "privacy": "unlisted", "upload_status": "processed",
        "made_for_kids": False, "public_stats_viewable": True, "title": "Owned canary",
        "published_at": "2026-09-11T01:07:12Z", "processing_status": "succeeded",
        "raw": {
            "status": {
                "uploadStatus": "processed", "privacyStatus": "unlisted", "license": "youtube",
                "embeddable": True, "publicStatsViewable": True, "madeForKids": False,
            },
            "snippet": {
                "publishedAt": "2026-09-11T01:07:12Z", "channelId": "UC" + "a" * 22,
                "title": "Owned canary", "description": "Owned fixture description",
                "thumbnails": {
                    "default": {"url": "https://i.ytimg.com/vi/video/default.jpg", "width": 120, "height": 90},
                    "medium": {"url": "https://i.ytimg.com/vi/video/mqdefault.jpg", "width": 320, "height": 180},
                    "high": {"url": "https://i.ytimg.com/vi/video/hqdefault.jpg", "width": 480, "height": 360},
                    "standard": {"url": "https://i.ytimg.com/vi/video/sddefault.jpg", "width": 640, "height": 480},
                    "maxres": {"url": "https://i.ytimg.com/vi/video/maxresdefault.jpg", "width": 1280, "height": 720},
                },
                "channelTitle": "Owned fixture", "tags": ["AutoFlow", "owned"], "categoryId": "22",
                "liveBroadcastContent": "none", "defaultLanguage": "en",
                "localized": {"title": "Owned canary", "description": "Owned fixture description"},
            },
            "processingDetails": {
                "processingStatus": "succeeded", "fileDetailsAvailability": "available",
                "processingIssuesAvailability": "available", "tagSuggestionsAvailability": "inProgress",
                "editorSuggestionsAvailability": "inProgress", "thumbnailsAvailability": "available",
            },
        },
    }


@pytest.mark.asyncio
async def test_deployed_manager_contract_supports_first_and_resume_receipt_checks(
    target, state_dir, manager_video_status,
):
    api = drill_api()
    helper = api.OwnedUnlistedAckDrill(target, state_dir=state_dir)
    claim = reserved(target)
    prepare(helper, target, claim)
    operation = claim.operation
    operation.request_attempted_at = datetime.now(timezone.utc)
    helper.record_post_attempt(target.context, operation)
    manager_task_id = str(uuid.uuid4())
    operation.status, operation.manager_task_id = "submitted", manager_task_id
    helper.record_submitted(target.context, operation, manager_task_id)

    async def load_submitted(*args, **kwargs):
        return UploadOperationClaim("resume", operation)

    store, requests = SimpleNamespace(load_submitted=load_submitted), []

    def route(request):
        requests.append(request)
        return httpx.Response(200, json=manager_video_status)

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(api.InjectedPreReceiptAbort) as aborted:
            await helper.before_receipt(
                store, target.context, operation, manager_task_id, "video", client, asyncio.Event(),
            )
        helper.authenticate_abort(aborted.value, target.context, operation.id, manager_task_id)
        operation = await helper.load_for_resume(store, target.context, asyncio.Event())
        await helper.before_receipt(
            store, target.context, operation, manager_task_id, "video", client, asyncio.Event(),
        )
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/videos/video/status"), ("GET", "/api/videos/video/status"),
    ]
    events = [record["event"] for record in journal_records(state_dir)]
    assert "processed_unlisted_get_1" in events and "processed_unlisted_get_2" in events
    assert operation.receipt_json == {} and operation.platform_video_id is None


@pytest.mark.parametrize("path,value", [
    (("made_for_kids",), 0), (("public_stats_viewable",), 1), (("title",), []),
    (("raw",), []), (("raw", "status"), None), (("raw", "snippet"), []),
    (("raw", "processingDetails"), "succeeded"), (("raw", "id"), "other"),
    (("raw", "status", "privacyStatus"), "public"),
    (("raw", "status", "uploadStatus"), "failed"),
    (("raw", "status", "madeForKids"), True),
    (("raw", "status", "madeForKids"), 0),
    (("raw", "status", "publicStatsViewable"), False),
    (("raw", "status", "publicStatsViewable"), 1),
    (("raw", "snippet", "title"), "other"),
    (("raw", "snippet", "publishedAt"), "2026-09-11T00:00:00Z"),
    (("raw", "processingDetails", "processingStatus"), "failed"),
    (("video_id",), "other"), (("privacy",), "public"),
    (("upload_status",), "completed"), (("processing_status",), None),
    (("privacyStatus",), "unlisted"), (("unexpected",), True),
])
@pytest.mark.asyncio
async def test_deployed_manager_contract_rejects_malformed_or_conflicting_fields(
    target, state_dir, manager_video_status, path, value,
):
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    prepare(helper, target, reserved(target))
    node = manager_video_status
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    requests = []

    def route(request):
        requests.append(request)
        return httpx.Response(200, json=manager_video_status)

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        with pytest.raises(RuntimeError):
            await helper.verify_video(client, "video", asyncio.Event())
    assert len(requests) == 1 and not (state_dir / "consumed.json").exists()


@pytest.mark.asyncio
async def test_deployed_manager_contract_polls_matching_explicit_nonterminal_state(
    target, state_dir, manager_video_status,
):
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    prepare(helper, target, reserved(target))
    requests = []

    def route(request):
        requests.append(request)
        payload = json.loads(json.dumps(manager_video_status))
        if len(requests) == 1:
            payload["upload_status"] = payload["raw"]["status"]["uploadStatus"] = "uploaded"
            payload["processing_status"] = payload["raw"]["processingDetails"]["processingStatus"] = "processing"
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        await helper.verify_video(client, "video", asyncio.Event())
    assert len(requests) == 2 and all(request.method == "GET" for request in requests)


@pytest.mark.parametrize("canonical", [
    "privacy", "upload_status", "processing_status", "made_for_kids",
    "public_stats_viewable", "title", "published_at",
])
@pytest.mark.asyncio
async def test_deployed_manager_raw_alias_never_replaces_missing_canonical(
    target, state_dir, manager_video_status, canonical,
):
    del manager_video_status[canonical]
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    prepare(helper, target, reserved(target))
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=manager_video_status),
    )) as client:
        with pytest.raises(RuntimeError):
            await helper.verify_video(client, "video", asyncio.Event())
    assert not (state_dir / "consumed.json").exists()


@pytest.mark.parametrize("original,replacement", [
    ('"privacyStatus": "unlisted"', '"privacyStatus": "public", "privacyStatus": "unlisted"'),
    ('"made_for_kids": false', '"made_for_kids": false, "made_for_kids": false'),
    ('"height": 90', '"height": NaN'),
    ('"height": 90', '"height": 1e999'),
])
@pytest.mark.asyncio
async def test_deployed_manager_contract_preserves_strict_recursive_json_parser(
    target, state_dir, manager_video_status, original, replacement,
):
    body = json.dumps(manager_video_status)
    assert body.count(original) == 1
    body = body.replace(original, replacement)
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    prepare(helper, target, reserved(target))
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body),
    )) as client:
        with pytest.raises(RuntimeError, match="video status is malformed"):
            await helper.verify_video(client, "video", asyncio.Event())
    assert not (state_dir / "consumed.json").exists()


@pytest.mark.parametrize("extra", [
    pytest.param('"current_privacy":"public"', id="conflicting-current-privacy"),
    pytest.param('"privacy_status":"public"', id="conflicting-privacy-status"),
    pytest.param('"privacyStatus":"public"', id="conflicting-camel-privacy"),
    pytest.param('"current_privacy":"unlisted"', id="unsupported-agreeing-alias"),
    pytest.param('"id":"other"', id="conflicting-id"),
    pytest.param('"videoId":"other"', id="conflicting-video-id"),
    pytest.param('"account_id":"other"', id="unsupported-account-identity"),
    pytest.param('"platform_channel_id":"other"', id="unsupported-channel-identity"),
    pytest.param('"uploadStatus":"failed"', id="conflicting-upload-state"),
    pytest.param('"processingStatus":"failed"', id="conflicting-processing-state"),
    pytest.param('"status":"failed"', id="conflicting-status"),
    pytest.param('"privacy":"public"', id="duplicate-privacy"),
    pytest.param('"privacy":"unlisted"', id="duplicate-agreeing-privacy"),
    pytest.param('"video_id":"other"', id="duplicate-video-id"),
    pytest.param('"upload_status":"failed"', id="duplicate-upload-state"),
    pytest.param('"processing_status":"failed","processing_status":"succeeded"', id="duplicate-processing-state"),
    pytest.param('"published_at":null,"published_at":"2026-09-10T07:00:00Z"', id="duplicate-published-at"),
    pytest.param('"published_at":{"value":1,"value":2}', id="nested-duplicate-key"),
    pytest.param('"published_at":NaN', id="nonfinite-nan"),
    pytest.param('"published_at":Infinity', id="nonfinite-infinity"),
    pytest.param('"published_at":-Infinity', id="nonfinite-negative-infinity"),
    pytest.param('"published_at":1e999', id="nonfinite-overflow"),
    pytest.param('"published_at":{"value":NaN}', id="nested-nonfinite"),
    pytest.param('"unsupported_field":true', id="unsupported-field"),
])
@pytest.mark.parametrize("stage", ["first", "resume"])
@pytest.mark.asyncio
async def test_receipt_verifier_rejects_noncanonical_json_on_first_and_resume(
    target, state_dir, extra, stage,
):
    api = drill_api()
    helper = api.OwnedUnlistedAckDrill(target, state_dir=state_dir)
    claim = reserved(target)
    prepare(helper, target, claim)
    operation = claim.operation
    operation.request_attempted_at = datetime.now(timezone.utc)
    helper.record_post_attempt(target.context, operation)
    manager_task_id = str(uuid.uuid4())
    operation.status, operation.manager_task_id = "submitted", manager_task_id
    helper.record_submitted(target.context, operation, manager_task_id)
    loads = []

    async def load_submitted(context, *, operation_id, manager_task_id):
        assert context == target.context and operation_id == operation.id
        assert manager_task_id == operation.manager_task_id
        loads.append(operation_id)
        return UploadOperationClaim("resume", operation)

    # Unit-only store boundary; real helper verification, journal and abort authentication.
    store = SimpleNamespace(load_submitted=load_submitted)
    canonical = '"video_id":"video","privacy":"unlisted","upload_status":"processed"'
    invalid_body = "{" + extra + "," + canonical + "}"
    requests = []

    def route(request):
        requests.append(request)
        assert request.method == "GET" and request.url.path == "/api/videos/video/status"
        body = "{" + canonical + "}" if stage == "resume" and len(requests) == 1 else invalid_body
        return httpx.Response(200, content=body)

    cancelled = asyncio.Event()
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        if stage == "resume":
            with pytest.raises(api.InjectedPreReceiptAbort) as aborted:
                await helper.before_receipt(
                    store, target.context, operation, manager_task_id, "video", client, cancelled,
                )
            helper.authenticate_abort(aborted.value, target.context, operation.id, manager_task_id)
            operation = await helper.load_for_resume(store, target.context, cancelled)
        with pytest.raises(RuntimeError) as rejected:
            await helper.before_receipt(
                store, target.context, operation, manager_task_id, "video", client, cancelled,
            )
        assert type(rejected.value) is RuntimeError  # The injected abort must not count as rejection.

    assert len(requests) == (2 if stage == "resume" else 1)
    assert len(loads) == (2 if stage == "resume" else 0)
    events = [record["event"] for record in journal_records(state_dir)]
    assert events[-1] == ("completed_get_2" if stage == "resume" else "completed_get_1")
    assert events.count("token_consumed") == (1 if stage == "resume" else 0)
    assert (state_dir / "consumed.json").exists() == (stage == "resume")
    assert operation.receipt_json == {} and operation.platform_video_id is None


def journal_records(state_dir):
    return [json.loads(path.read_text()) for path in sorted(state_dir.glob("[0-9]*.json"))]


def test_filesystem_root_is_not_a_private_state_directory(target):
    with pytest.raises(ValueError):
        drill_api().OwnedUnlistedAckDrill(target, state_dir=Path("/"))


@pytest.mark.parametrize("name", ["0001.json", "consumed.json"])
@pytest.mark.parametrize("contents", ["", "{", '{"nonce":"forged"}'])
def test_preexisting_partial_or_consumed_records_never_rearm(target, state_dir, name, contents):
    (state_dir / name).write_text(contents)
    (state_dir / name).chmod(0o600)
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    with pytest.raises(RuntimeError):
        prepare(helper, target, reserved(target))
    assert (state_dir / name).read_text() == contents


def test_directory_replacement_cannot_redirect_post_record(target, state_dir):
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    claim = reserved(target)
    prepare(helper, target, claim)
    state_dir.rename(state_dir.with_name("original"))
    state_dir.mkdir(mode=0o700)
    claim.operation.request_attempted_at = datetime.now(timezone.utc)
    with pytest.raises(RuntimeError):
        helper.record_post_attempt(target.context, claim.operation)
    assert not list(state_dir.iterdir())


def test_one_helper_cannot_record_second_post(target, state_dir):
    helper = drill_api().OwnedUnlistedAckDrill(target, state_dir=state_dir)
    claim = reserved(target)
    prepare(helper, target, claim)
    claim.operation.request_attempted_at = datetime.now(timezone.utc)
    helper.record_post_attempt(target.context, claim.operation)
    with pytest.raises(RuntimeError):
        helper.record_post_attempt(target.context, claim.operation)
    assert [r["event"] for r in journal_records(state_dir)] == ["start", "upload_post_attempt"]


def test_journal_token_primitive_is_exclusive_and_blocks_reconstruction(target, state_dir):
    api = drill_api()
    helper = api.OwnedUnlistedAckDrill(target, state_dir=state_dir)
    claim = reserved(target)
    prepare(helper, target, claim)
    # File-primitive coverage only; actual authenticated abort is a PG integration test.
    helper._write("consumed.json", {"drill_id": str(target.drill_id), "operation_id": str(claim.operation.id)})
    original = (state_dir / "consumed.json").read_bytes()
    with pytest.raises(FileExistsError):
        helper._write("consumed.json", {"drill_id": "replacement"})
    assert (state_dir / "consumed.json").read_bytes() == original
    with pytest.raises(RuntimeError):
        prepare(api.OwnedUnlistedAckDrill(target, state_dir=state_dir), target, claim)


@pytest.mark.parametrize("event", ["file-fsync", "directory-fsync"])
def test_partial_token_is_preserved_when_durability_fails(target, state_dir, monkeypatch, event):
    api = drill_api()
    helper = api.OwnedUnlistedAckDrill(target, state_dir=state_dir)
    claim = reserved(target)
    prepare(helper, target, claim)
    original = api.os.fsync
    calls = 0

    def fail(fd):
        nonlocal calls
        calls += 1
        if calls == (1 if event == "file-fsync" else 2):
            raise OSError("test durability failure")
        original(fd)

    with monkeypatch.context() as patcher:
        patcher.setattr(api.os, "fsync", fail)
        with pytest.raises(OSError):
            helper._write("consumed.json", {"drill_id": str(target.drill_id)})
    assert (state_dir / "consumed.json").exists()
    with pytest.raises(RuntimeError):
        prepare(api.OwnedUnlistedAckDrill(target, state_dir=state_dir), target, claim)


@pytest.mark.parametrize("url", [
    "postgresql+asyncpg://postgres:test@127.0.0.1:5432/postgres",
    "postgresql+asyncpg://postgres:test@localhost:55449/postgres",
    "postgresql+asyncpg://postgres:test@127.0.0.1:55449/existing",
    "postgresql+asyncpg://postgres:test@127.0.0.1:55449/postgres?host=elsewhere",
])
def test_postgres_fixture_rejects_any_nonapproved_endpoint(monkeypatch, url):
    from tests.worker.ack_drill_postgres import scratch_url

    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.setenv("CHANNEL_OPS_POSTGRES_TEST_URL", url)
    with pytest.raises(ValueError):
        scratch_url()


def test_postgres_fixture_accepts_exact_repository_ci_endpoint(monkeypatch):
    from tests.worker.ack_drill_postgres import scratch_url

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_REPOSITORY", "Ctwqk/videoprocess")
    monkeypatch.setenv("CHANNEL_OPS_POSTGRES_TEST_URL", "postgresql+asyncpg://postgres:test@127.0.0.1:5432/postgres")
    assert scratch_url().port == 5432


@pytest.mark.parametrize("actions,repository", [
    ("false", "Ctwqk/videoprocess"), ("TRUE", "Ctwqk/videoprocess"),
    ("true", "another/videoprocess"), ("true", ""), ("", "Ctwqk/videoprocess"),
])
def test_postgres_fixture_rejects_ci_port_without_exact_ci_context(monkeypatch, actions, repository):
    from tests.worker.ack_drill_postgres import scratch_url

    monkeypatch.setenv("GITHUB_ACTIONS", actions)
    monkeypatch.setenv("GITHUB_REPOSITORY", repository)
    monkeypatch.setenv("CHANNEL_OPS_POSTGRES_TEST_URL", "postgresql+asyncpg://postgres:test@127.0.0.1:5432/postgres")
    with pytest.raises(ValueError):
        scratch_url()


@pytest.mark.parametrize("change", ["monotonic", "expiry", "cancel", "http-timeout"])
@pytest.mark.asyncio
async def test_video_verification_stops_on_deadline_or_cancellation(target, state_dir, monkeypatch, change):
    api = drill_api()
    helper = api.OwnedUnlistedAckDrill(target, state_dir=state_dir)
    prepare(helper, target, reserved(target))
    cancelled, started = asyncio.Event(), asyncio.Event()
    requests = []

    async def route(request):
        requests.append(request)
        started.set()
        if change == "http-timeout":
            raise httpx.ReadTimeout("test")
        await asyncio.Event().wait()

    if change == "monotonic":
        old = api.time.monotonic()
        monkeypatch.setattr(api.time, "monotonic", lambda: old + 10)
    elif change == "expiry":
        monkeypatch.setattr(api.time, "time", lambda: target.expires_at.timestamp() + 1)
    async with httpx.AsyncClient(transport=httpx.MockTransport(route)) as client:
        task = asyncio.create_task(helper.verify_video(client, "video-123", cancelled))
        if change == "cancel":
            await started.wait()
            cancelled.set()
        from worker.handlers.base import CancelledError
        with pytest.raises((RuntimeError, CancelledError, httpx.ReadTimeout)):
            await task
    assert len(requests) == (1 if change in {"cancel", "http-timeout"} else 0)
    assert not (state_dir / "consumed.json").exists()
