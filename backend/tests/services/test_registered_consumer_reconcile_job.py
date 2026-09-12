from __future__ import annotations

import asyncio
from asyncio.base_subprocess import BaseSubprocessTransport
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
from tests.services.test_registered_consumer_reconcile import facts, NOW


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
        redis_generation="eeb8593f43dc5709d0191a06c528a9d35b22785e",
        redis_secret_name="vp-control-redis-eeb8593f43dc",
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


def test_capture_uses_complete_active_registration_and_grant_facts():
    job = module()
    payload = document()
    registrations, grants = facts(payload)
    current_ids = {worker.current.registration_id for worker in decode().workers}
    current = [row for row in registrations if row.id in current_ids]
    current_grants = [row for row in grants if row.id in {r.grant_id for r in current}]
    captured = job.capture_snapshot(current, current_grants, now=NOW)
    assert captured["workers"] == [item["current"] for item in payload["workers"]]
    assert job.capture_snapshot([], [], now=NOW)["workers"] == [None] * 4


@pytest.mark.parametrize("fault", ["extra", "grant", "lease", "binding", "revoked"])
def test_capture_refuses_ambiguous_or_unready_predecessor(fault):
    job = module()
    registrations, grants = facts(document())
    current_ids = {worker.current.registration_id for worker in decode().workers}
    registrations = [row for row in registrations if row.id in current_ids]
    grants = [row for row in grants if row.id in {r.grant_id for r in registrations}]
    if fault == "extra":
        registrations.append(copy.copy(registrations[0]))
    elif fault == "grant":
        grants.pop()
    elif fault == "lease":
        registrations[0].lease_expires_at = NOW
    elif fault == "binding":
        grants[0].database_principal = "other"
    else:
        registrations[0].revoked_at = NOW
    with pytest.raises(job.ProtocolError):
        job.capture_snapshot(registrations, grants, now=NOW)


def managed_spec(job, binding):
    return job.managed_spec(
        binding,
        image="vp-ffmpeg-worker-python:deploy-" + binding["release_commit"][:12],
        network_id="n" * 25,
        manager_node="ccttww-lap",
        manager_node_id="m" * 25,
        pins_secret_id="p" * 25,
    )


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "user",
        "env",
        "mount",
        "secret",
        "network",
        "replicas",
        "restart",
        "command",
        "write_root",
        "group",
        "port",
        "force",
    ],
)
def test_managed_descriptor_has_only_exact_mounts_and_authority(tmp_path, fault):
    job, _, _, binding = setup_protocol(tmp_path)
    expected = managed_spec(job, binding)
    actual = copy.deepcopy(expected)
    container = actual["TaskTemplate"]["ContainerSpec"]
    if fault == "user":
        container["User"] = "0"
    elif fault == "env":
        container["Env"] = ["DATABASE_URL=forbidden"]
    elif fault == "mount":
        container["Mounts"].append({"Source": "/var/run/docker.sock"})
    elif fault == "secret":
        container["Secrets"][0]["SecretID"] = "x" * 25
    elif fault == "network":
        actual["TaskTemplate"]["Networks"][0]["Target"] = "x" * 25
    elif fault == "replicas":
        actual["Mode"]["ReplicatedJob"]["MaxConcurrent"] = 2
    elif fault == "restart":
        actual["TaskTemplate"]["RestartPolicy"]["Condition"] = "any"
    elif fault == "command":
        container["Args"].append("--other")
    elif fault == "write_root":
        container["ReadOnly"] = False
    elif fault == "group":
        container["Groups"] = [str(os.getgid())]
    elif fault == "port":
        actual["EndpointSpec"] = {"Ports": [{"PublishedPort": 8080}]}
    elif fault == "force":
        actual["TaskTemplate"]["ForceUpdate"] = 1
    if fault is None:
        job.validate_managed_spec(actual, expected)
        assert len(container["Mounts"]) == 3 and len(container["Secrets"]) == 3
        assert not any("transactions" in item["Target"] for item in container["Mounts"])
    else:
        with pytest.raises(job.ProtocolError):
            job.validate_managed_spec(actual, expected)


@pytest.mark.asyncio
async def test_fixed_entry_awaits_actual_unit2_and_preserves_result(
    tmp_path, monkeypatch
):
    job, request, files, binding = setup_protocol(tmp_path)
    monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)
    from app.services import registered_consumer_reconcile_runtime as runtime

    entered = []

    async def reconcile(invocation, authority):
        assert type(invocation) is runtime.Invocation
        assert invocation.pins.sha256 == request.pins.sha256
        assert type(authority) is job.FileAuthority
        entered.append(True)
        return runtime.RunResult("already_absent", invocation.pins.sha256, ())

    async def finished(self, invocation, outcome):
        assert entered == [True] and outcome == "already_absent"
        entered.append("finished")

    monkeypatch.setattr(runtime, "reconcile_registered_consumers", reconcile)
    monkeypatch.setattr(job.FileAuthority, "finished", finished)
    result = await job.run_managed(binding, files)
    assert entered == [True, "finished"] and result["pin_sha256"] == request.pins.sha256
    assert result["outcome"] == "already_absent"


@pytest.mark.parametrize(
    "state,allowed",
    [
        ("unused", True),
        ("retired", True),
        ("already_absent", True),
        ("consumed", False),
        ("unknown", False),
    ],
)
def test_finish_is_durable_only_after_known_results_and_never_reopens(
    tmp_path, state, allowed
):
    job, _, _, binding = setup_protocol(tmp_path)
    record = job.new_record(binding)
    if state != "unused":
        first = frame(
            job,
            binding,
            action="before_eval",
            stream="vp:tasks:ffmpeg_go",
            command_sha256=binding["commands"]["vp:tasks:ffmpeg_go"],
        )
        prefix = job.canonical(first)
        record = job.advance_record(record, first, prefix)
        if state != "consumed":
            second = frame(
                job,
                binding,
                2,
                "after_eval",
                stream="vp:tasks:ffmpeg_go",
                command_sha256=binding["commands"]["vp:tasks:ffmpeg_go"],
                outcome=state,
                nonce="e" * 32,
            )
            prefix += job.canonical(second)
            record = job.advance_record(record, second, prefix)
    else:
        prefix = b""
    finish = frame(
        job,
        binding,
        record["sequence"] + 1,
        "finished",
        outcome="already_absent" if state == "unused" else "reconciled",
        nonce="d" * 32,
    )
    if not allowed:
        with pytest.raises(job.ProtocolError):
            job.advance_record(record, finish, prefix + job.canonical(finish))
        return
    prefix += job.canonical(finish)
    record = job.advance_record(record, finish, prefix)
    assert record["last_request"]["action"] == "finished"
    more = frame(job, binding, record["sequence"] + 1, nonce="c" * 32)
    with pytest.raises(job.ProtocolError):
        job.advance_record(record, more, prefix + job.canonical(more))


@pytest.mark.asyncio
async def test_snapshot_reader_is_selective_readonly_rr_and_uses_db_clock():
    job = module()
    calls = []

    class Transaction:
        async def __aenter__(self):
            calls.append("begin")

        async def __aexit__(self, *args):
            calls.append("end")

    class Connection:
        def transaction(self, **kwargs):
            assert kwargs == {"isolation": "repeatable_read", "readonly": True}
            return Transaction()

        async def fetchval(self, sql):
            assert sql == "SELECT transaction_timestamp()"
            return NOW

        async def fetch(self, sql, services):
            assert "token_sha256" not in sql and "*" not in sql
            assert "FOR UPDATE" not in sql and "FOR SHARE" not in sql
            assert set(services) == job.SERVICES
            calls.append(sql)
            return []

    result = await job.read_snapshot(Connection())
    assert result == {"observed_at": NOW.isoformat(), "workers": [None] * 4}
    assert calls[0] == "begin" and calls[-1] == "end" and len(calls) == 4


def test_capture_builds_current_pins_only_from_preserved_baseline():
    job = module()
    payload = document()
    snapshot = {
        "observed_at": NOW.isoformat(),
        "workers": [w["current"] for w in payload["workers"]],
    }
    baseline = {
        "observed_at": NOW.isoformat(),
        "workers": [w["predecessor"] for w in payload["workers"]],
    }
    result = job.build_capture_pins(
        snapshot,
        baseline,
        transaction_id=payload["transaction_id"],
        revision=payload["revision"],
        release_commit=payload["release_commit"],
    )
    assert result["pin_json"] == decode().canonical_json
    assert result["pin_sha256"] == decode().sha256
    assert set(result["commands"]) == set(job.STREAMS.values())
    baseline["workers"][0] = snapshot["workers"][0]
    with pytest.raises(job.ProtocolError):
        job.build_capture_pins(
            snapshot,
            baseline,
            transaction_id=payload["transaction_id"],
            revision=payload["revision"],
            release_commit=payload["release_commit"],
        )


def test_fresh_baseline_absence_does_not_manufacture_retiring_commands():
    job = module()
    payload = document()
    snapshot = {
        "observed_at": NOW.isoformat(),
        "workers": [w["current"] for w in payload["workers"]],
    }
    baseline = {"observed_at": NOW.isoformat(), "workers": [None] * 4}
    result = job.build_capture_pins(
        snapshot,
        baseline,
        transaction_id=payload["transaction_id"],
        revision=payload["revision"],
        release_commit=payload["release_commit"],
    )
    assert result["commands"] == {}


def test_finished_record_cannot_reload_with_unknown_or_consumed_results(tmp_path):
    job, _, _, binding = setup_protocol(tmp_path)
    finish = frame(job, binding, action="finished", outcome="already_absent")
    state = job.advance_record(job.new_record(binding), finish, job.canonical(finish))
    for outcome in ("consumed", "unknown", "retired"):
        altered = copy.deepcopy(state)
        altered["streams"]["vp:tasks:ffmpeg_go"] = outcome
        with pytest.raises(job.ProtocolError):
            job.validate_record(altered)


@pytest.mark.parametrize("name", ["vp-control-redis-eeb8593f43dc", "a" * 255])
def test_capture_descriptor_cannot_execute_reconcile_or_mount_extra_authority(tmp_path, name):
    job, _, files, binding = setup_protocol(tmp_path)
    binding["credentials"]["redis_secret_name"] = name
    spec = job.capture_spec(
        attempt_id=binding["attempt_id"],
        transaction_id=binding["transaction_id"],
        files=files,
        credentials=binding["credentials"],
        image="vp-ffmpeg-worker-python:deploy-" + binding["release_commit"][:12],
        network_id="n" * 25,
        manager_node="ccttww-lap",
        manager_node_id="m" * 25,
        capture_read=dict(
            id="r" * 25,
            name="vp-registered-read-" + binding["transaction_id"],
            sha256="a" * 64,
            principal="vp_deploy_read",
        ),
    )
    container = spec["TaskTemplate"]["ContainerSpec"]
    assert container["Args"][-1] == "--capture"
    assert len(container["Secrets"]) == 3
    expected_redis = dict(
        SecretID=binding["credentials"]["redis_secret_id"],
        SecretName=binding["credentials"]["redis_secret_name"],
        File=dict(Name="registered-reconcile-redis-url", UID="10001", GID="10001", Mode=0o400),
    )
    assert container["Secrets"][1] == expected_redis
    assert managed_spec(job, binding)["TaskTemplate"]["ContainerSpec"]["Secrets"][1] == expected_redis
    assert container["Secrets"][-1]["File"] == dict(
        Name="registered-reconcile-capture-read", UID="10001", GID="10001", Mode=0o400
    )
    assert all(
        secret["File"]["Name"] != "registered-reconcile-capture-read"
        for secret in managed_spec(job, binding)["TaskTemplate"]["ContainerSpec"][
            "Secrets"
        ]
    )
    assert container["User"] == "10001:10001"
    assert len(container["Mounts"]) == 3


def test_input_and_capture_output_are_exact_private_inodes(tmp_path, monkeypatch):
    job, _, files, binding = setup_protocol(tmp_path)
    payload = {"binding": binding, "files": files}
    path = tmp_path / "attempt/input.json"
    metadata = job.write_input(path, payload)
    assert path.stat().st_mode & 0o777 == 0o644
    assert job.read_input(path, metadata) == payload
    with pytest.raises(job.ProtocolError):
        job.write_input(path, payload)
    # Retain the original inode so Linux cannot reuse it for the replacement.
    path.rename(path.with_name("original-input.json"))
    path.write_bytes(job.canonical(payload))
    path.chmod(0o644)
    assert path.stat().st_ino != metadata["inode"]
    with pytest.raises(job.ProtocolError):
        job.read_input(path, metadata)


@pytest.mark.parametrize(
    "state,exit_code,expected",
    [
        ("running", None, None),
        ("complete", 0, 0),
        ("failed", 7, 7),
        ("failed", 0, "error"),
        ("orphaned", None, "error"),
        ("shutdown", None, "error"),
    ],
)
def test_terminal_task_is_evidence_not_waiter_returncode(
    tmp_path, state, exit_code, expected
):
    job, _, _, binding = setup_protocol(tmp_path)
    spec = managed_spec(job, binding)
    task = {
        "ID": "t" * 25,
        "ServiceID": "s" * 25,
        "NodeID": "m" * 25,
        "Spec": spec["TaskTemplate"],
        "Status": {"State": state, "ContainerStatus": {"ExitCode": exit_code}},
    }
    if expected == "error":
        with pytest.raises(job.ProtocolError):
            job.task_exit(task, "s" * 25, spec)
    else:
        assert job.task_exit(task, "s" * 25, spec) == expected
        task["ServiceID"] = "x" * 25
        with pytest.raises(job.ProtocolError):
            job.task_exit(task, "s" * 25, spec)


def test_task_must_actually_run_on_the_captured_manager_node(tmp_path):
    job, _, _, binding = setup_protocol(tmp_path)
    spec = managed_spec(job, binding)
    spec["Labels"]["vp.manager-node-id"] = "m" * 25
    task = dict(
        ID="t" * 25,
        ServiceID="s" * 25,
        NodeID="x" * 25,
        Spec=spec["TaskTemplate"],
        Status={"State": "running"},
    )
    with pytest.raises(job.ProtocolError):
        job.task_exit(task, "s" * 25, spec)


def test_cleanup_preserves_raw_bytes_and_only_removes_bound_names(tmp_path):
    job, _, files, binding = setup_protocol(tmp_path)
    input_file = job.write_input(tmp_path / "attempt/input.json", {"binding": binding})
    raw = b'{"unchanged":"raw"}\n'
    Path(files["request"]["path"]).write_bytes(raw)
    Path(files["replies"]["path"], "reply.json").write_bytes(b"{}\n")
    unrelated = tmp_path / "unrelated"
    unrelated.write_bytes(b"retained")
    job.retain_managed_files(files, input_file)
    assert not Path(files["request"]["path"]).exists()
    assert (tmp_path / "attempt/retained-requests").read_bytes() == raw
    assert unrelated.read_bytes() == b"retained"
    job.retain_managed_files(files, input_file)
    Path(files["request"]["path"]).write_bytes(b"replacement")
    with pytest.raises(job.ProtocolError):
        job.retain_managed_files(files, input_file)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", [None, "", "bad/name", "missing", "a" * 256])
async def test_capture_rejects_missing_or_malformed_name_before_secret_read(tmp_path, monkeypatch, name):
    from app.services import registered_consumer_reconcile_runtime as runtime

    job, _, files, binding = setup_protocol(tmp_path)
    credentials = {key: binding["credentials"][key] for key in (
        "control_generation", "redis_generation", "redis_secret_name",
        "database_secret_id", "redis_secret_id",
    )}
    if name == "missing":
        del credentials["redis_secret_name"]
    else:
        credentials["redis_secret_name"] = name
    reads = []
    monkeypatch.setattr(runtime, "_read_mount", lambda key: reads.append(key))
    with pytest.raises(job.ProtocolError, match="^registered_reconcile_protocol_failed$"):
        await job.capture_managed(dict(files=files, credentials=credentials))
    assert reads == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "hash",
        "url_principal",
        "session_principal",
        "target_host",
        "target_port",
        "target_database",
        "reader_database_name",
        "operator_database_name",
        "query_redirect",
        "fragment",
        "missing_port",
        "normalized_target",
        "name_255",
    ],
)
async def test_capture_reads_secret_bytes_but_returns_only_hashes_and_real_principal(
    tmp_path, monkeypatch, fault
):
    import hashlib
    import asyncpg
    from app.services import registered_consumer_reconcile_runtime as runtime
    from app.services.worker_control_role_cli import role_names_for_generation

    job, _, files, binding = setup_protocol(tmp_path)
    credentials = {
        key: binding["credentials"][key]
        for key in (
            "control_generation",
            "redis_generation",
            "redis_secret_name",
            "database_secret_id",
            "redis_secret_id",
        )
    }
    if fault == "name_255":
        credentials["redis_secret_name"] = "a" * 255
    principal = role_names_for_generation(credentials["control_generation"]).versioned[
        "operator"
    ]
    raw = {
        "database": f"postgresql://{principal}:private@database.invalid:5432/fixture\n",
        "redis": "redis://vp_control:private@redis.invalid:6379/0\n",
    }
    reader = "postgresql://vp_deploy_read:private@database.invalid:5432/fixture\n"
    for case, before, after in (
        ("target_host", "database.invalid", "other.invalid"),
        ("target_port", ":5432", ":5433"),
        ("target_database", "/fixture", "/other"),
        ("query_redirect", "/fixture", "/fixture?host=other.invalid"),
        ("fragment", "/fixture", "/fixture#extra"),
        ("missing_port", ":5432", ""),
        ("normalized_target", "postgresql://", "postgresql+asyncpg://"),
    ):
        if fault == case:
            reader = reader.replace(before, after)
    if fault == "normalized_target":
        reader = reader.replace("database.invalid", "DATABASE.INVALID")
    capture_read = dict(
        id="r" * 25,
        name="vp-registered-read-" + binding["transaction_id"],
        sha256=hashlib.sha256(reader.encode()).hexdigest(),
        principal="vp_deploy_read",
    )
    if fault == "hash":
        capture_read["sha256"] = "f" * 64
    elif fault == "url_principal":
        capture_read["principal"] = "other_reader"
    closed = []
    snapshots = []

    class Connection:
        def __init__(self, user):
            self.user = user

        async def fetchrow(self, query):
            assert query in (
                "SELECT session_user, current_user",
                "SELECT session_user, current_user, pg_catalog.current_database() AS database_name",
            )
            identity = dict(
                session_user=self.user, current_user=self.user, database_name="fixture"
            )
            if fault == "session_principal" and self.user == "vp_deploy_read":
                identity["current_user"] = "other_reader"
            if (
                fault == "reader_database_name"
                and self.user == "vp_deploy_read"
                or fault == "operator_database_name"
                and self.user == principal
            ):
                identity["database_name"] = "other"
            return identity

        async def close(self, **kwargs):
            closed.append(self.user)

    async def connect(*args, **kwargs):
        from urllib.parse import urlsplit

        return Connection(urlsplit(args[0]).username)

    async def snapshot(connection):
        # The actual operator role has no table SELECT grants.
        assert connection.user == "vp_deploy_read"
        snapshots.append(connection.user)
        return {"observed_at": NOW.isoformat(), "workers": [None] * 4}

    monkeypatch.setattr(asyncpg, "connect", connect)
    monkeypatch.setattr(runtime, "_read_mount", raw.__getitem__)
    monkeypatch.setattr(job, "_read_capture_mount", lambda: reader, raising=False)
    monkeypatch.setattr(job, "read_snapshot", snapshot)
    payload = dict(
        files=files, credentials=credentials, baseline=None, capture_read=capture_read
    )
    if fault not in (None, "normalized_target", "name_255"):
        with pytest.raises(
            job.ProtocolError, match="^registered_reconcile_protocol_failed$"
        ):
            await job.capture_managed(payload)
        assert principal in closed
        assert snapshots == []
        return
    result = await job.capture_managed(payload)
    assert sorted(closed) == sorted([principal, "vp_deploy_read"])
    assert (
        result["credentials"]["database_secret_sha256"]
        == hashlib.sha256(raw["database"].encode()).hexdigest()
    )
    assert result["credentials"]["redis_username"] == "vp_control"
    assert result["credentials"]["redis_secret_name"] == credentials["redis_secret_name"]
    assert "private" not in json.dumps(result)
    assert result["snapshot"]["workers"] == [None] * 4


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
    changed = invocation(redis_secret_name="qualified-control.current")
    renamed = job.make_binding(changed, files, descriptor_sha256="e" * 64)
    assert renamed["credentials"]["redis_secret_name"] == "qualified-control.current"
    assert job.digest(renamed) != job.digest(binding)
    longest = job.make_binding(invocation(redis_secret_name="a" * 255), files, descriptor_sha256="e" * 64)
    assert longest["credentials"]["redis_secret_name"] == "a" * 255
    for name in (None, "", "bad/name", "bad\nname", "a" * 256):
        damaged = copy.deepcopy(binding)
        damaged["credentials"]["redis_secret_name"] = name
        with pytest.raises(job.ProtocolError):
            job.validate_binding(damaged)
    del damaged["credentials"]["redis_secret_name"]
    with pytest.raises(job.ProtocolError):
        job.validate_binding(damaged)
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
@pytest.mark.parametrize("interrupt", ["timeout", "cancel", "repeated_cancel"])
async def test_pending_startup_is_cancelled_not_passively_settled(
    tmp_path, monkeypatch, interrupt
):
    job, request, files, binding = setup_protocol(tmp_path)
    monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = asyncio.create_subprocess_exec
    creators, processes = [], []

    async def create(*args, **kwargs):
        creators.append(asyncio.current_task())
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        process = await original(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    client = job.FileAuthority(binding, files)
    started = time.monotonic()
    running = asyncio.create_task(client.revalidate(request))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        if interrupt != "timeout":
            for _ in range(3 if interrupt == "repeated_cancel" else 1):
                running.cancel()
                await asyncio.sleep(0)
        done, _ = await asyncio.wait({running}, timeout=1.9)
        assert done, "pending cooperative creator escaped callback cancellation"
        with pytest.raises(
            job.ProtocolError if interrupt == "timeout" else asyncio.CancelledError
        ):
            await running
        assert time.monotonic() - started < 2
        assert cancelled.is_set() and all(task.done() for task in creators)
        assert processes == [] and client.pending_process is None and client.poisoned
        assert Path(files["request"]["path"]).stat().st_size == 0
        with pytest.raises(job.ProtocolError):
            await client.revalidate(request)
    finally:
        # Release only to clean up the expected RED failure, never to make it pass.
        release.set()
        await asyncio.gather(running, return_exceptions=True)
        assert all(process.returncode is not None for process in processes)


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt", ["timeout", "cancel", "repeated_cancel"])
async def test_child_before_handle_publication_uses_transport_cancel_cleanup(
    tmp_path, monkeypatch, interrupt
):
    job, request, files, binding = setup_protocol(tmp_path)
    monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)
    entered, reaped, release_reap = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original_connect = BaseSubprocessTransport._connect_pipes
    original_wait = BaseSubprocessTransport._wait
    transports, waiters, creators = [], [], []
    original_create = asyncio.create_subprocess_exec

    async def create(*args, **kwargs):
        creators.append(asyncio.current_task())
        return await original_create(*args, **kwargs)

    async def connect(transport, waiter):
        # Finish real pipe setup but withhold the native startup waiter. CPython
        # owns the real child here; no public Process handle exists yet.
        ready = asyncio.get_running_loop().create_future()
        await original_connect(transport, ready)
        ready.result()
        transports.append(transport)
        waiters.append(waiter)
        entered.set()

    async def wait(transport):
        result = await original_wait(transport)
        if transport in transports:
            reaped.set()
            if interrupt == "repeated_cancel":
                await release_reap.wait()
        return result

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(BaseSubprocessTransport, "_connect_pipes", connect)
    monkeypatch.setattr(BaseSubprocessTransport, "_wait", wait)
    client = job.FileAuthority(binding, files)
    started = time.monotonic()
    running = asyncio.create_task(client.revalidate(request))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert client.pending_process is None and transports[0].get_returncode() is None
        pid = transports[0].get_pid()
        if interrupt != "timeout":
            running.cancel()
        if interrupt == "repeated_cancel":
            await asyncio.wait_for(reaped.wait(), 0.5)
            for _ in range(3):
                running.cancel()
                await asyncio.sleep(0)
            assert not running.done(), "callback detached pending transport cleanup"
            release_reap.set()
        done, _ = await asyncio.wait({running}, timeout=1.9)
        assert done, "native startup transport was not cancelled"
        with pytest.raises(
            job.ProtocolError if interrupt == "timeout" else asyncio.CancelledError
        ):
            await running
        assert time.monotonic() - started < 2
        assert reaped.is_set() and all(task.done() for task in creators)
        assert transports[0].get_returncode() is not None
        with pytest.raises(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)
        assert client.pending_process is None and client.poisoned
        assert Path(files["request"]["path"]).stat().st_size == 0
    finally:
        release_reap.set()
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)
        await asyncio.gather(running, return_exceptions=True)
        assert all(transport.get_returncode() is not None for transport in transports)


@pytest.mark.asyncio
async def test_cancel_racing_completed_creator_retains_and_reaps_handle(
    tmp_path, monkeypatch
):
    job, request, files, binding = setup_protocol(tmp_path)
    monkeypatch.setattr(job, "require_writer_identity", lambda *args: None)
    original = asyncio.create_subprocess_exec
    processes = []

    async def create(*args, **kwargs):
        process = await original(*args, **kwargs)
        processes.append(process)
        asyncio.get_running_loop().call_soon(running.cancel)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    client = job.FileAuthority(binding, files)
    running = asyncio.create_task(client.revalidate(request))
    with pytest.raises(asyncio.CancelledError):
        await running
    assert len(processes) == 1 and processes[0].returncode is not None
    assert client.pending_process is None and client.poisoned


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
