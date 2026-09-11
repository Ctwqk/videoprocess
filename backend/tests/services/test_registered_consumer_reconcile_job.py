from __future__ import annotations

import asyncio
import copy
import importlib
import json
import os
from pathlib import Path
import runpy
import time
from types import SimpleNamespace
from uuid import UUID

import pytest

from app.services.registered_consumer_reconcile import EvalCommand
from tests.services.test_registered_consumer_reconcile import decode, document


def module():
    path = (
        Path(__file__).parents[2] / "app/services/registered_consumer_reconcile_job.py"
    )
    assert path.exists(), "Task1 callback/file protocol is missing"
    return importlib.import_module("app.services.registered_consumer_reconcile_job")


def invocation(**changes):
    values = dict(
        pins=decode(),
        attempt_id=UUID(int=987),
        replay_only=False,
        control_generation="control-1",
        redis_generation="redis-1",
        redis_username="vp_control_1",
        database_secret_id="a" * 25,
        redis_secret_id="b" * 25,
        database_secret_sha256="c" * 64,
        redis_secret_sha256="d" * 64,
    )
    return SimpleNamespace(**(values | changes))


def setup_protocol(tmp_path):
    job = module()
    root = tmp_path / "attempt"
    root.mkdir(mode=0o700)
    files = job.prepare_files(root)
    request = invocation()
    binding = job.make_binding(request, files, descriptor_sha256="e" * 64)
    return job, request, files, binding


def frame(job, binding, sequence=1, action="revalidate", **changes):
    value = dict(
        version=1,
        attempt_id=binding["attempt_id"],
        sequence=sequence,
        nonce="f" * 32,
        action=action,
        binding_sha256=job.digest(binding),
        stream=None,
        command_sha256=None,
        outcome=None,
    )
    return value | changes


def append(job, files, value):
    descriptor = os.open(files["request"]["path"], os.O_WRONLY | os.O_APPEND)
    try:
        os.write(descriptor, job.canonical(value))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def test_prepare_modes_no_overwrite(tmp_path):
    job, _, files, _ = setup_protocol(tmp_path)
    assert Path(files["request"]["path"]).stat().st_mode & 0o777 == 0o602
    assert Path(files["replies"]["path"]).stat().st_mode & 0o777 == 0o755
    with pytest.raises(job.ProtocolError):
        job.prepare_files(tmp_path / "attempt")


def test_fresh_frame_partial_then_complete_and_prefix(tmp_path):
    job, _, files, binding = setup_protocol(tmp_path)
    state = job.new_record(binding)
    payload = job.canonical(frame(job, binding))
    path = Path(files["request"]["path"])
    path.write_bytes(payload[:-1])
    assert job.read_request(state) is None
    path.write_bytes(payload)
    request, prefix = job.read_request(state)
    assert request["sequence"] == 1
    assert prefix == payload
    state = job.advance_record(state, request, prefix)
    assert job.read_request(state) is None
    path.write_bytes(payload.replace(b"revalidate", b"revalidatE"))
    with pytest.raises(job.ProtocolError):
        job.read_request(state)


@pytest.mark.parametrize(
    "change",
    [
        {"sequence": True},
        {"sequence": 2},
        {"nonce": "bad"},
        {"attempt_id": str(UUID(int=999))},
        {"binding_sha256": "0" * 64},
        {"action": "delete"},
        {"stream": "vp:events"},
        {"outcome": "success"},
        {"extra": 1},
    ],
)
def test_bad_requests_refused(tmp_path, change):
    job, _, files, binding = setup_protocol(tmp_path)
    append(job, files, frame(job, binding, **change))
    with pytest.raises(job.ProtocolError):
        job.read_request(job.new_record(binding))


@pytest.mark.parametrize(
    "corruption", ["duplicate_json", "noncanonical", "two", "oversized"]
)
def test_bad_framing_refused(tmp_path, corruption):
    job, _, files, binding = setup_protocol(tmp_path)
    raw = job.canonical(frame(job, binding))
    if corruption == "duplicate_json":
        raw = raw.replace(b'"version":1', b'"version":1,"version":1')
    elif corruption == "noncanonical":
        raw = b" " + raw
    elif corruption == "two":
        raw *= 2
    else:
        raw = b"x" * (job.MAX_BYTES + 1)
    Path(files["request"]["path"]).write_bytes(raw)
    with pytest.raises(job.ProtocolError):
        job.read_request(job.new_record(binding))


@pytest.mark.parametrize("damage", ["mode", "inode", "symlink", "hardlink", "truncate"])
def test_request_inode_refusals(tmp_path, damage):
    job, _, files, binding = setup_protocol(tmp_path)
    path = Path(files["request"]["path"])
    append(job, files, frame(job, binding))
    state = job.new_record(binding)
    value, prefix = job.read_request(state)
    state = job.advance_record(state, value, prefix)
    if damage == "mode":
        path.chmod(0o600)
    elif damage in {"inode", "symlink"}:
        other = tmp_path / "other"
        other.write_bytes(prefix)
        other.chmod(0o602)
        path.unlink()
        if damage == "symlink":
            path.symlink_to(other)
        else:
            other.rename(path)
    elif damage == "hardlink":
        os.link(path, tmp_path / "link")
    else:
        path.write_bytes(b"")
    with pytest.raises(job.ProtocolError):
        job.read_request(state)


def test_consumption_unknown_and_replay_never_reauthorize(tmp_path):
    job, request, _, binding = setup_protocol(tmp_path)
    command = EvalCommand(request.pins, "vp-ffmpeg-worker-go-swarm")
    before = frame(
        job,
        binding,
        action="before_eval",
        stream=command.stream,
        command_sha256=job.digest(list(command.arguments)),
    )
    state = job.advance_record(job.new_record(binding), before, job.canonical(before))
    assert state["streams"][command.stream] == "consumed"
    again = before | {"sequence": 2, "nonce": "1" * 32}
    with pytest.raises(job.ProtocolError):
        job.advance_record(state, again, job.canonical(before) + job.canonical(again))
    after = again | {"action": "after_eval", "outcome": "unknown"}
    state = job.advance_record(
        state, after, job.canonical(before) + job.canonical(after)
    )
    assert state["streams"][command.stream] == "unknown"
    with pytest.raises(job.ProtocolError):
        job.advance_record(state, before | {"sequence": 3}, b"unused")


def test_binding_pins_and_credentials_are_exact(tmp_path):
    job, request, files, binding = setup_protocol(tmp_path)
    assert binding["pin_sha256"] == request.pins.sha256
    assert binding["binding_revision"] == request.pins.revision
    assert "password" not in json.dumps(binding)
    changed = invocation(redis_secret_id="z" * 25)
    assert job.make_binding(changed, files, descriptor_sha256="e" * 64) != binding
    for value in (True, -1):
        damaged = copy.deepcopy(binding)
        damaged["binding_revision"] = value
        with pytest.raises(job.ProtocolError):
            job.validate_binding(damaged)


@pytest.mark.parametrize(
    "uid,gid,groups", [(10001, 22, []), (22, 10001, []), (22, 23, [23])]
)
def test_write_only_permission_class_rejects_owner_or_group(uid, gid, groups):
    job = module()
    with pytest.raises(job.ProtocolError):
        job.require_writer_identity({"uid": uid, "gid": gid}, 10001, 10001, groups)


def test_write_only_other_class_allowed():
    module().require_writer_identity({"uid": 22, "gid": 23}, 10001, 10001, [])


@pytest.mark.parametrize("damage", ["mode", "symlink", "hardlink", "directory"])
def test_reply_permission_and_link_drift_refused(tmp_path, damage):
    job, _, files, binding = setup_protocol(tmp_path)
    request = frame(job, binding)
    job.write_reply(files, request)
    path = Path(files["replies"]["path"]) / "reply.json"
    if damage == "mode":
        path.chmod(0o666)
    elif damage == "symlink":
        target = tmp_path / "target"
        path.rename(target)
        path.symlink_to(target)
    elif damage == "hardlink":
        os.link(path, tmp_path / "alias")
    else:
        path.parent.chmod(0o777)
    with pytest.raises(job.ProtocolError):
        job.read_reply(files, request)


def test_reply_exact_and_atomic(tmp_path):
    job, _, files, binding = setup_protocol(tmp_path)
    request = frame(job, binding)
    job.write_reply(files, request)
    assert job.read_reply(files, request)
    assert (
        Path(files["replies"]["path"]) / "reply.json"
    ).stat().st_mode & 0o777 == 0o644
    assert not job.read_reply(files, request | {"nonce": "1" * 32})


@pytest.mark.parametrize("field", ["version", "sequence"])
def test_reply_echo_requires_exact_scalar_types(tmp_path, field):
    job, _, files, binding = setup_protocol(tmp_path)
    request = frame(job, binding)
    job.write_reply(files, request | {field: True})
    assert not job.read_reply(files, request)


def test_reply_read_may_advance_atime_without_identity_change(tmp_path, monkeypatch):
    job, _, files, binding = setup_protocol(tmp_path)
    request = frame(job, binding)
    job.write_reply(files, request)
    inode = (Path(files["replies"]["path"]) / "reply.json").stat().st_ino
    original = os.fstat
    reads = 0

    def fstat(descriptor):
        nonlocal reads
        result = original(descriptor)
        if result.st_ino == inode:
            reads += 1
            if reads == 2:
                fields = {
                    name: getattr(result, name)
                    for name in dir(result)
                    if name.startswith("st_")
                }
                return SimpleNamespace(**(fields | {"st_atime": result.st_atime + 1}))
        return result

    monkeypatch.setattr(os, "fstat", fstat)
    assert job.read_reply(files, request)


def test_nonce_cannot_be_reused_at_a_later_sequence(tmp_path):
    job, _, _, binding = setup_protocol(tmp_path)
    first = frame(job, binding)
    prefix = job.canonical(first)
    state = job.advance_record(job.new_record(binding), first, prefix)
    second = first | {"sequence": 2}
    with pytest.raises(job.ProtocolError):
        job.advance_record(state, second, prefix + job.canonical(second))


@pytest.mark.parametrize("change", [{"action": []}, {"outcome": {}}, {"stream": []}])
def test_malformed_scalar_is_static_refusal(tmp_path, change):
    job, _, _, binding = setup_protocol(tmp_path)
    with pytest.raises(
        job.ProtocolError, match="^registered_reconcile_protocol_failed$"
    ):
        job.validate_frame(frame(job, binding, **change), binding, 1)


@pytest.mark.asyncio
async def test_process_start_failure_is_static_and_poisoned(tmp_path, monkeypatch):
    job, request, files, binding = setup_protocol(tmp_path)
    monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)

    async def fail(*args, **kwargs):
        raise OSError("private detail must not escape")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fail)
    client = job.FileAuthority(binding, files)
    with pytest.raises(
        job.ProtocolError, match="^registered_reconcile_protocol_failed$"
    ):
        await client.revalidate(request)
    assert client.pending_process is None
    assert client.poisoned


@pytest.mark.asyncio
async def test_cancellation_during_cleanup_is_not_swallowed(tmp_path, monkeypatch):
    job, request, files, binding = setup_protocol(tmp_path)
    monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)
    waiting, release = asyncio.Event(), asyncio.Event()

    class Process:
        returncode = 0

        async def communicate(self, payload):
            return b"ok\n", None

        async def wait(self):
            waiting.set()
            await release.wait()
            return 0

    async def create(*args, **kwargs):
        return Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    client = job.FileAuthority(binding, files)
    running = asyncio.create_task(client.revalidate(request))
    await waiting.wait()
    running.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert client.poisoned and client.pending_process is None


@pytest.mark.asyncio
async def test_client_real_files_and_owned_process_no_resume(tmp_path, monkeypatch):
    job, request, files, binding = setup_protocol(tmp_path)
    # Local tests do not switch UIDs. The permission rule has separate pure tests;
    # parent will qualify the actual UID10001 bind mount on disposable Linux.
    monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)
    client = job.FileAuthority(binding, files)
    state = job.new_record(binding)
    running = asyncio.create_task(client.revalidate(request))
    while not running.done():
        found = job.read_request(state)
        if found:
            value, prefix = found
            state = job.advance_record(state, value, prefix)
            job.write_reply(files, value)
        await asyncio.sleep(0.005)
    await running
    with pytest.raises(job.ProtocolError):
        await job.FileAuthority(binding, files).revalidate(request)


@pytest.mark.asyncio
async def test_cancelled_client_reaps_child_and_cannot_continue(tmp_path, monkeypatch):
    job, request, files, binding = setup_protocol(tmp_path)
    monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)
    client = job.FileAuthority(binding, files)
    running = asyncio.create_task(client.revalidate(request))
    while Path(files["request"]["path"]).stat().st_size == 0 and not running.done():
        await asyncio.sleep(0.005)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert client.pending_process is None
    with pytest.raises(job.ProtocolError):
        await client.revalidate(request)


@pytest.mark.asyncio
async def test_no_reply_times_out_without_an_orphan(tmp_path, monkeypatch):
    job, request, files, binding = setup_protocol(tmp_path)
    monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)
    client = job.FileAuthority(binding, files)
    started = time.monotonic()
    with pytest.raises(job.ProtocolError):
        await client.revalidate(request)
    assert time.monotonic() - started < 2
    assert client.pending_process is None and client.poisoned


def test_replay_only_cannot_consume_a_stream(tmp_path):
    job, request, files, _ = setup_protocol(tmp_path)
    binding = job.make_binding(
        invocation(replay_only=True), files, descriptor_sha256="e" * 64
    )
    command = EvalCommand(request.pins, "vp-ffmpeg-worker-go-swarm")
    value = frame(
        job,
        binding,
        action="before_eval",
        stream=command.stream,
        command_sha256=job.digest(list(command.arguments)),
    )
    with pytest.raises(job.ProtocolError):
        job.advance_record(job.new_record(binding), value, job.canonical(value))


@pytest.mark.asyncio
async def test_duplicate_client_cannot_append_or_reuse_authorization(
    tmp_path, monkeypatch
):
    job, request, files, binding = setup_protocol(tmp_path)
    monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)
    first, second = job.FileAuthority(binding, files), job.FileAuthority(binding, files)
    running = asyncio.create_task(first.revalidate(request))
    state = job.new_record(binding)
    while not (found := job.read_request(state)) and not running.done():
        await asyncio.sleep(0.005)
    assert found is not None
    with pytest.raises(job.ProtocolError):
        await second.revalidate(request)
    assert job.read_request(state) == found
    job.write_reply(files, found[0])
    await running


@pytest.mark.asyncio
async def test_ten_callbacks_use_real_journal_fsync_and_file_subprocesses(monkeypatch):
    root = Path(__file__).resolve().parents[3]
    helpers = runpy.run_path(str(root / "tests/test_worker_admission_transaction.py"))
    fixture = helpers["RegisteredReconcileJournalTests"](methodName="runTest")
    fixture.setUp()
    try:
        job = module()
        pin_document = document()
        pin_document.update(
            transaction_id=fixture.binding["transaction_id"],
            revision=71,
            release_commit="2" * 40,
        )
        for worker in pin_document["workers"]:
            current = worker["current"]
            generation, image = fixture.binding["targets"][current["service_name"]]
            current.update(
                generation=generation, image_identity=image, release_commit="2" * 40
            )
        request = invocation(
            pins=decode(pin_document), **fixture.binding["credentials"]
        )
        files = fixture.binding["files"]
        fixture.binding = job.make_binding(request, files, descriptor_sha256="f" * 64)
        fixture.prepare()
        monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)
        client = job.FileAuthority(fixture.binding, files)

        async def callbacks():
            for service in job.STREAMS:
                command = EvalCommand(request.pins, service)
                await client.revalidate(request)
                await client.before_eval(request, command)
                await client.after_eval(request, command, "retired")
            await client.revalidate(request)

        started = time.monotonic()
        running = asyncio.create_task(callbacks())
        try:
            while not running.done():
                fixture.answer()
                await asyncio.sleep(0.005)
            try:
                await running
            except job.ProtocolError:
                pytest.fail(
                    f"callback failed: durable_sequence={fixture.record()['sequence']}, "
                    f"client_sequence={client.sequence}"
                )
        finally:
            if not running.done():
                running.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await running
        elapsed = time.monotonic() - started
        assert elapsed < 6  # Host-only transport cost, not real PG/Redis qualification.
        assert fixture.record()["sequence"] == 10
        assert set(fixture.record()["streams"].values()) == {"retired"}
        assert client.pending_process is None
    finally:
        fixture.doCleanups()
