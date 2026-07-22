from __future__ import annotations

import importlib.util
import json
import subprocess
import uuid
from contextlib import contextmanager
from datetime import timedelta, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.models.asset import Asset
from app.models.base import Base
from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    LaneFormatMatrix,
    ManualSeed,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TopicLane,
)
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.services.schedule_service import load_video_jobs_for_recovery, release_waiting_video_jobs


TABLES = [
    Asset.__table__,
    ChannelProfile.__table__,
    TopicLane.__table__,
    PublishingAccount.__table__,
    LaneFormatMatrix.__table__,
    ManualSeed.__table__,
    Job.__table__,
    NodeExecution.__table__,
    ProductionTask.__table__,
    PublicationRecord.__table__,
    ChannelOpsQueueItem.__table__,
]


def load_runner() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / "scripts" / "run_vp_unlisted_canary.py"
    spec = importlib.util.spec_from_file_location("vp_unlisted_canary_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("preflight", "live", "expected"),
    ((True, False, "preflight_only"), (False, True, "live_unlisted")),
)
def test_execution_mode_accepts_exactly_one_mode(preflight, live, expected):
    runner = load_runner()
    args = SimpleNamespace(
        preflight_only=preflight,
        confirm_live_unlisted=live,
    )

    assert runner.execution_mode(args) == expected


@pytest.mark.parametrize(("preflight", "live"), ((False, False), (True, True)))
def test_execution_mode_rejects_ambiguous_mode(preflight, live):
    runner = load_runner()
    args = SimpleNamespace(
        preflight_only=preflight,
        confirm_live_unlisted=live,
    )

    with pytest.raises(runner.CanaryError, match="exactly one"):
        runner.execution_mode(args)


def test_parse_args_accepts_shared_services_ssh_host(monkeypatch: pytest.MonkeyPatch):
    runner = load_runner()
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_vp_unlisted_canary.py",
            "--preflight-only",
            "--shared-services-ssh-host",
            "10.0.0.127",
        ],
    )

    args = runner.parse_args()

    assert args.shared_services_ssh_host == "10.0.0.127"


def test_evidence_path_distinguishes_preflight_mode():
    runner = load_runner()
    args = SimpleNamespace(evidence=None)

    assert runner.evidence_path(args, "run-123", runner.MODE_PREFLIGHT).name == (
        "unlisted-canary-preflight-run-123.json"
    )
    assert runner.evidence_path(args, "run-123", runner.MODE_LIVE).name == (
        "unlisted-canary-run-123.json"
    )


def test_ssh_readonly_command_adds_optional_jump():
    runner = load_runner()

    assert runner.ssh_readonly_command("10.0.0.127", "hostname") == [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "10.0.0.127",
        "hostname",
    ]
    assert runner.ssh_readonly_command(
        "10.0.0.150",
        "hostname",
        jump_host="10.0.0.127",
    ) == [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-J",
        "10.0.0.127",
        "10.0.0.150",
        "hostname",
    ]


def test_shared_service_endpoints_preserve_urls_and_forward_original_targets():
    runner = load_runner()

    endpoints = runner.build_shared_service_endpoints(
        database_url=(
            "postgresql+asyncpg://vp:p%40ss@10.0.0.150:5435/videoprocess"
            "?application_name=vp-canary"
        ),
        redis_url="redis://cache:p%40ss@10.0.0.150:6380/4?health_check_interval=5",
        youtube_manager_url="http://10.0.0.150:18999/api?source=canary",
        local_ports=(25435, 26380, 28999),
    )

    assert endpoints.database_url == (
        "postgresql+asyncpg://vp:p%40ss@127.0.0.1:25435/videoprocess"
        "?application_name=vp-canary"
    )
    assert endpoints.redis_url == (
        "redis://cache:p%40ss@127.0.0.1:26380/4?health_check_interval=5"
    )
    assert endpoints.youtube_manager_url == "http://127.0.0.1:28999/api?source=canary"
    assert [
        (forward.name, forward.local_port, forward.target_host, forward.target_port)
        for forward in endpoints.forwards
    ] == [
        ("database", 25435, "10.0.0.150", 5435),
        ("redis", 26380, "10.0.0.150", 6380),
        ("youtube_manager", 28999, "10.0.0.150", 18999),
    ]


def test_shared_service_endpoints_use_defaults_and_omit_empty_redis():
    runner = load_runner()

    endpoints = runner.build_shared_service_endpoints(
        database_url="postgresql://vp:secret@database/videoprocess",
        redis_url="",
        youtube_manager_url="http://youtube-manager/api",
        local_ports=(15432, 18080),
    )

    assert endpoints.database_url == "postgresql://vp:secret@127.0.0.1:15432/videoprocess"
    assert endpoints.redis_url == ""
    assert endpoints.youtube_manager_url == "http://127.0.0.1:18080/api"
    assert [(forward.name, forward.target_port) for forward in endpoints.forwards] == [
        ("database", 5432),
        ("youtube_manager", 80),
    ]


def test_shared_service_endpoints_accept_legacy_postgres_scheme():
    runner = load_runner()

    endpoints = runner.build_shared_service_endpoints(
        database_url="postgres://vp:secret@10.0.0.150:5435/videoprocess",
        redis_url="",
        youtube_manager_url="http://10.0.0.150:18999",
        local_ports=(25435, 28999),
    )

    assert endpoints.database_url == "postgres://vp:secret@127.0.0.1:25435/videoprocess"
    assert endpoints.forwards[0] == runner.TunnelForward(
        "database",
        25435,
        "10.0.0.150",
        5435,
    )


def test_shared_service_endpoints_reject_postgresql_lookalike_scheme():
    runner = load_runner()

    with pytest.raises(runner.CanaryError, match="scheme is unsupported"):
        runner.build_shared_service_endpoints(
            database_url="postgresqlx://vp:secret@10.0.0.150:5435/videoprocess",
            redis_url="",
            youtube_manager_url="http://10.0.0.150:18999",
            local_ports=(25435, 28999),
        )


def test_shared_service_tunnel_command_is_forward_only_and_argument_safe():
    runner = load_runner()
    endpoints = runner.build_shared_service_endpoints(
        database_url="postgresql://vp:secret@10.0.0.150:5435/videoprocess",
        redis_url="redis://10.0.0.150:6380/0",
        youtube_manager_url="http://10.0.0.150:18999",
        local_ports=(25435, 26380, 28999),
    )

    assert runner.ssh_tunnel_command("10.0.0.127", endpoints.forwards) == [
        "ssh",
        "-N",
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ExitOnForwardFailure=yes",
        "-L",
        "127.0.0.1:25435:10.0.0.150:5435",
        "-L",
        "127.0.0.1:26380:10.0.0.150:6380",
        "-L",
        "127.0.0.1:28999:10.0.0.150:18999",
        "10.0.0.127",
    ]


@pytest.mark.parametrize(
    ("redis_url", "youtube_manager_url"),
    (
        ("rediss://10.0.0.150:6380/0", "http://10.0.0.150:18999"),
        ("redis://10.0.0.150:6380/0", "https://10.0.0.150:18999"),
    ),
)
def test_shared_service_endpoints_reject_tls_hostname_rewrite(
    redis_url: str,
    youtube_manager_url: str,
):
    runner = load_runner()

    with pytest.raises(runner.CanaryError, match="scheme is unsupported"):
        runner.build_shared_service_endpoints(
            database_url="postgresql://vp:secret@10.0.0.150:5435/videoprocess",
            redis_url=redis_url,
            youtube_manager_url=youtube_manager_url,
            local_ports=(25435, 26380, 28999),
        )


@pytest.mark.parametrize(
    "forbidden_host",
    ("10.0.0.126", "colima-swarmbridged", "CASPERs-Mac-mini.local"),
)
def test_shared_service_tunnel_rejects_126_class_hosts(forbidden_host: str):
    runner = load_runner()
    forward = runner.TunnelForward("database", 25435, "10.0.0.150", 5435)

    with pytest.raises(runner.CanaryError, match="forbidden"):
        runner.ssh_tunnel_command(forbidden_host, (forward,))


class FakeTunnelProcess:
    def __init__(self, *, returncode: int | None = None, stop_on_terminate: bool = True):
        self.returncode = returncode
        self.stop_on_terminate = stop_on_terminate
        self.terminated = False
        self.killed = False
        self.wait_calls: list[float] = []

    def wait(self, timeout: float):
        self.wait_calls.append(timeout)
        if self.returncode is None:
            raise subprocess.TimeoutExpired(cmd="ssh", timeout=timeout)
        return self.returncode

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        if self.stop_on_terminate:
            self.returncode = 0

    def kill(self):
        self.killed = True
        self.returncode = -9


def test_shared_service_tunnel_always_terminates_and_waits():
    runner = load_runner()
    process = FakeTunnelProcess()
    popen_calls = []

    def popen(command, **kwargs):
        popen_calls.append((command, kwargs))
        return process

    with runner.open_shared_service_tunnel(
        ssh_host="10.0.0.127",
        database_url="postgresql://vp:secret@10.0.0.150:5435/videoprocess",
        redis_url="redis://10.0.0.150:6380/0",
        youtube_manager_url="http://10.0.0.150:18999",
        local_ports=(25435, 26380, 28999),
        popen_factory=popen,
        startup_timeout_seconds=0.01,
    ) as endpoints:
        assert endpoints.database_url.startswith("postgresql://vp:secret@127.0.0.1:25435/")

    assert process.terminated is True
    assert process.killed is False
    assert process.wait_calls == [0.01, 5.0]
    assert len(popen_calls) == 1
    _command, kwargs = popen_calls[0]
    assert kwargs == {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }


def test_shared_service_tunnel_startup_exit_fails_closed_without_diagnostics():
    runner = load_runner()
    process = FakeTunnelProcess(returncode=255)

    with pytest.raises(runner.CanaryError, match="exited during startup") as raised:
        with runner.open_shared_service_tunnel(
            ssh_host="10.0.0.127",
            database_url="postgresql://vp:secret@10.0.0.150:5435/videoprocess",
            redis_url="",
            youtube_manager_url="http://10.0.0.150:18999",
            local_ports=(25435, 28999),
            popen_factory=lambda *_args, **_kwargs: process,
            startup_timeout_seconds=0.01,
        ):
            raise AssertionError("startup failure yielded a tunnel")

    assert "secret" not in str(raised.value)
    assert process.terminated is False
    assert process.killed is False


def test_shared_service_tunnel_kills_process_that_ignores_terminate():
    runner = load_runner()
    process = FakeTunnelProcess(stop_on_terminate=False)

    with runner.open_shared_service_tunnel(
        ssh_host="10.0.0.127",
        database_url="postgresql://vp:secret@10.0.0.150:5435/videoprocess",
        redis_url="",
        youtube_manager_url="http://10.0.0.150:18999",
        local_ports=(25435, 28999),
        popen_factory=lambda *_args, **_kwargs: process,
        startup_timeout_seconds=0.01,
    ):
        pass

    assert process.terminated is True
    assert process.killed is True
    assert process.wait_calls == [0.01, 5.0, 5.0]


@pytest.mark.anyio
async def test_deployment_readiness_routes_manager_ssh_through_jump(monkeypatch):
    runner = load_runner()
    commit = "a" * 40
    responses = iter(
        (
            commit,
            commit,
            "publisher|1/1",
            "vp-worker:deploy-aaaaaaaaaaaa",
            "node.labels.vp.publisher==true\nnode.hostname==ccttww-lap",
            f"{runner.CHANNEL_OPS_RUNNER_SERVICE}|1/1",
            "vp-runner:deploy-aaaaaaaaaaaa",
            "CHANNELOPS_RUNNER_POLL_SECONDS=5\nCHANNELOPS_THROTTLE_ENABLED=false",
        )
    )
    commands: list[list[str]] = []

    def run_readonly(command, **_kwargs):
        commands.append(command)
        return next(responses)

    async def request_json(*_args, **_kwargs):
        return {"status": "ok"}

    monkeypatch.setattr(runner, "run_readonly_command", run_readonly)
    monkeypatch.setattr(runner, "request_json", request_json)
    args = SimpleNamespace(
        api_url="http://api",
        runtime_host="runtime-host",
        manager_host="manager-host",
        manager_ssh_jump="jump-host",
        publisher_service="publisher",
    )

    deployment = await runner.deployment_readiness(args, object())

    runtime_commands = [command for command in commands if "runtime-host" in command]
    manager_commands = [command for command in commands if "manager-host" in command]
    assert len(runtime_commands) == 1
    assert "-J" not in runtime_commands[0]
    assert len(manager_commands) == 6
    assert all(
        command[command.index("-J") + 1] == "jump-host"
        for command in manager_commands
    )
    assert deployment["manager_host"] == "manager-host"
    assert deployment["manager_ssh_jump"] == "jump-host"


@pytest.mark.anyio
async def test_mode_aware_dispatch_and_schedule_close(monkeypatch: pytest.MonkeyPatch):
    runner = load_runner()
    preflight = AsyncMock()
    canary = AsyncMock()
    close = AsyncMock()
    monkeypatch.setattr(runner, "execute_preflight", preflight)
    monkeypatch.setattr(runner, "execute_canary", canary)
    monkeypatch.setattr(runner, "close_schedule", close)
    values = (object(), object(), object(), {}, Path("evidence.json"))

    await runner.execute_selected_mode(runner.MODE_PREFLIGHT, *values)
    await runner.close_schedule_for_mode(
        runner.MODE_PREFLIGHT,
        values[0],
        values[2],
        values[3],
    )

    preflight.assert_awaited_once_with(*values)
    canary.assert_not_awaited()
    close.assert_not_awaited()

    await runner.execute_selected_mode(runner.MODE_LIVE, *values)
    await runner.close_schedule_for_mode(
        runner.MODE_LIVE,
        values[0],
        values[2],
        values[3],
    )

    canary.assert_awaited_once_with(*values)
    close.assert_awaited_once_with(values[0], values[2], values[3])


@pytest.mark.anyio
async def test_run_records_connection_failure_without_schedule_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    runner = load_runner()
    disposed = False

    class FailingConnectionContext:
        async def __aenter__(self):
            raise ConnectionRefusedError("database unavailable")

        async def __aexit__(self, *_args):
            return False

    class FailingEngine:
        def connect(self):
            return FailingConnectionContext()

        async def dispose(self):
            nonlocal disposed
            disposed = True

    close = AsyncMock()
    monkeypatch.setattr(runner, "create_async_engine", lambda *_args, **_kwargs: FailingEngine())
    monkeypatch.setattr(runner, "close_schedule", close)
    path = tmp_path / "connection-failure.json"
    args = SimpleNamespace(
        evidence=path,
        preflight_only=True,
        confirm_live_unlisted=False,
        redis_url="",
    )

    with pytest.raises(ConnectionRefusedError):
        await runner.run(args, "postgresql+asyncpg://unavailable")

    payload = json.loads(path.read_text())
    assert payload["status"] == "failed"
    assert payload["failure"]["type"] == "ConnectionRefusedError"
    assert payload["failure"]["message"] == (
        "unexpected failure; inspect sanitized service logs by exception type"
    )
    assert payload["completed_at"]
    assert payload["schedule"]["final_state"] is None
    close.assert_not_awaited()
    assert disposed is True


@pytest.mark.anyio
async def test_run_routes_shared_services_before_database_and_records_safe_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    runner = load_runner()
    disposed = False
    tunnel_exited = False
    tunnel_arguments = {}
    engine_urls = []

    class FailingConnectionContext:
        async def __aenter__(self):
            raise ConnectionRefusedError("database unavailable")

        async def __aexit__(self, *_args):
            return False

    class FailingEngine:
        def connect(self):
            return FailingConnectionContext()

        async def dispose(self):
            nonlocal disposed
            disposed = True

    @contextmanager
    def fake_tunnel(**kwargs):
        nonlocal tunnel_exited
        tunnel_arguments.update(kwargs)
        try:
            yield runner.SharedServiceEndpoints(
                database_url="postgresql+asyncpg://vp:secret@127.0.0.1:25435/videoprocess",
                redis_url="redis://127.0.0.1:26380/0",
                youtube_manager_url="http://127.0.0.1:28999",
                forwards=(
                    runner.TunnelForward("database", 25435, "10.0.0.150", 5435),
                    runner.TunnelForward("redis", 26380, "10.0.0.150", 6380),
                    runner.TunnelForward("youtube_manager", 28999, "10.0.0.150", 18999),
                ),
            )
        finally:
            tunnel_exited = True

    def create_engine(url, **_kwargs):
        engine_urls.append(url)
        return FailingEngine()

    monkeypatch.setattr(runner, "open_shared_service_tunnel", fake_tunnel)
    monkeypatch.setattr(runner, "create_async_engine", create_engine)
    path = tmp_path / "tunnel-connection-failure.json"
    args = SimpleNamespace(
        evidence=path,
        preflight_only=True,
        confirm_live_unlisted=False,
        redis_url="redis://10.0.0.150:6380/0",
        youtube_manager_url="http://10.0.0.150:18999",
        shared_services_ssh_host="10.0.0.127",
    )
    database_url = "postgresql+asyncpg://vp:secret@10.0.0.150:5435/videoprocess"

    with pytest.raises(ConnectionRefusedError):
        await runner.run(args, database_url)

    assert tunnel_arguments == {
        "ssh_host": "10.0.0.127",
        "database_url": database_url,
        "redis_url": "redis://10.0.0.150:6380/0",
        "youtube_manager_url": "http://10.0.0.150:18999",
    }
    assert engine_urls == [
        "postgresql+asyncpg://vp:secret@127.0.0.1:25435/videoprocess"
    ]
    assert tunnel_exited is True
    assert disposed is True
    payload = json.loads(path.read_text())
    assert payload["shared_services_tunnel"] == {
        "enabled": True,
        "ssh_host": "10.0.0.127",
        "targets": ["database", "redis", "youtube_manager"],
    }
    serialized = json.dumps(payload)
    assert "25435" not in serialized
    assert "26380" not in serialized
    assert "28999" not in serialized
    assert "secret" not in serialized


@pytest.mark.anyio
async def test_run_preserves_failed_preflight_redis_audit_and_root_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    runner = load_runner()
    audit = {
        "available": True,
        "streams": {
            "vp:events": {
                "available": False,
                "group": "orchestrator",
                "reason": "ConnectionError",
            },
            "vp:tasks:youtube_publisher": {
                "group": "youtube_publisher-workers",
                "pending": 0,
            },
        },
    }
    redis_audit = AsyncMock(return_value=audit)

    monkeypatch.setattr(runner, "acquire_advisory_lock", AsyncMock())
    monkeypatch.setattr(runner, "release_advisory_lock", AsyncMock())
    monkeypatch.setattr(
        runner,
        "schedule_status",
        AsyncMock(return_value={"state": "CLOSED", "active_jobs": 0}),
    )
    monkeypatch.setattr(
        runner,
        "active_backlog",
        AsyncMock(
            return_value={
                "runnable_job_ids": [],
                "unsafe_queue_item_ids": [],
                "unsafe_task_ids": [],
            }
        ),
    )
    monkeypatch.setattr(runner, "deployment_readiness", AsyncMock(return_value={"ready": True}))
    monkeypatch.setattr(runner, "manager_readiness", AsyncMock(return_value={"authenticated": True}))
    monkeypatch.setattr(runner, "redis_pending_audit", redis_audit)
    path = tmp_path / "redis-preflight-failure.json"
    args = SimpleNamespace(
        evidence=path,
        preflight_only=True,
        confirm_live_unlisted=False,
        redis_url="redis://cache/0",
        youtube_manager_url="",
        shared_services_ssh_host="",
    )

    with pytest.raises(runner.CanaryError, match="Redis pending audit"):
        await runner.run(args, "sqlite+aiosqlite:///:memory:")

    payload = json.loads(path.read_text())
    assert payload["status"] == "failed"
    assert payload["failure"] == {
        "type": "CanaryError",
        "message": "Redis pending audit is unavailable for vp:events",
        "at": payload["failure"]["at"],
    }
    assert payload["redis_stream_pending_audit"] == audit
    assert payload["completed_at"]
    redis_audit.assert_awaited_once_with("redis://cache/0")


@pytest.fixture
async def db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync_connection: Base.metadata.create_all(sync_connection, tables=TABLES))
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session
    await engine.dispose()


async def add_asset(
    db: AsyncSession,
    *,
    license_value: str = "owned",
    provenance: str = "generated",
    mime_type: str = "video/mp4",
) -> Asset:
    asset = Asset(
        filename="canary.mp4",
        original_name="canary.mp4",
        mime_type=mime_type,
        file_size=42,
        storage_backend="s3",
        storage_path="assets/canary.mp4",
        media_info={"license": license_value, "provenance": provenance},
    )
    db.add(asset)
    await db.commit()
    return asset


@pytest.mark.anyio
async def test_execute_preflight_reads_readiness_without_live_side_effects(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    runner = load_runner()
    calls: list[str] = []

    async def schedule_status(*_args):
        calls.append("schedule_status")
        return {"state": "CLOSED", "active_jobs": 0}

    async def deployment_readiness(*_args):
        calls.append("deployment_readiness")
        return {"ready": True}

    async def manager_readiness(*_args):
        calls.append("manager_readiness")
        return {"authenticated": True}

    async def redis_pending_audit(_redis_url):
        calls.append("redis_pending_audit")
        return {
            "available": True,
            "streams": {
                "vp:events": {"group": "orchestrator", "pending": 0},
                "vp:tasks:youtube_publisher": {
                    "group": "youtube_publisher-workers",
                    "pending": 0,
                },
            },
        }

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("preflight invoked a live mutation")

    monkeypatch.setattr(runner, "schedule_status", schedule_status)
    monkeypatch.setattr(runner, "deployment_readiness", deployment_readiness)
    monkeypatch.setattr(runner, "manager_readiness", manager_readiness)
    monkeypatch.setattr(runner, "redis_pending_audit", redis_pending_audit)
    monkeypatch.setattr(runner, "mutate_schedule", forbidden)
    monkeypatch.setattr(runner, "close_schedule", forbidden)
    monkeypatch.setattr(runner, "execute_canary", forbidden)
    evidence = {
        "mode": "preflight_only",
        "status": "running",
        "schedule": {"transitions": [], "final_state": None},
    }
    path = tmp_path / "preflight.json"

    await runner.execute_preflight(SimpleNamespace(redis_url="redis://cache/0"), db, object(), evidence, path)

    assert calls == [
        "schedule_status",
        "deployment_readiness",
        "manager_readiness",
        "redis_pending_audit",
    ]
    assert evidence["status"] == "succeeded"
    assert evidence["schedule"]["final_state"] == "CLOSED"
    assert evidence["preflight_backlog"]["runnable_job_ids"] == []
    assert evidence["redis_stream_pending_audit"]["streams"]["vp:events"]["pending"] == 0
    assert path.exists()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "audit",
    (
        {"available": False, "reason": "ConnectionError"},
        {
            "available": True,
            "streams": {
                "vp:events": {
                    "available": False,
                    "group": "orchestrator",
                    "reason": "ConnectionError",
                },
                "vp:tasks:youtube_publisher": {
                    "group": "youtube_publisher-workers",
                    "pending": 0,
                },
            },
        },
        {
            "available": True,
            "streams": {
                "vp:events": {"group": "orchestrator", "pending": 1},
                "vp:tasks:youtube_publisher": {
                    "group": "youtube_publisher-workers",
                    "pending": 0,
                },
            },
        },
        {
            "available": True,
            "streams": {
                "vp:events": {"group": "orchestrator", "pending": 0},
            },
        },
        {
            "available": True,
            "streams": {
                "vp:events": {"group": "wrong-group", "pending": 0},
                "vp:tasks:youtube_publisher": {
                    "group": "youtube_publisher-workers",
                    "pending": 0,
                },
            },
        },
        {
            "available": True,
            "streams": {
                "vp:events": {"group": "orchestrator", "pending": "0"},
                "vp:tasks:youtube_publisher": {
                    "group": "youtube_publisher-workers",
                    "pending": 0,
                },
            },
        },
    ),
)
async def test_execute_preflight_fails_closed_on_unsafe_redis_audit(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    audit: dict,
):
    runner = load_runner()

    monkeypatch.setattr(
        runner,
        "schedule_status",
        AsyncMock(return_value={"state": "CLOSED", "active_jobs": 0}),
    )
    monkeypatch.setattr(runner, "deployment_readiness", AsyncMock(return_value={"ready": True}))
    monkeypatch.setattr(runner, "manager_readiness", AsyncMock(return_value={"authenticated": True}))
    monkeypatch.setattr(runner, "redis_pending_audit", AsyncMock(return_value=audit))
    evidence = {
        "mode": runner.MODE_PREFLIGHT,
        "status": "running",
        "schedule": {"transitions": [], "final_state": None},
    }

    with pytest.raises(runner.CanaryError, match="Redis pending audit"):
        await runner.execute_preflight(
            SimpleNamespace(redis_url="redis://cache/0"),
            db,
            object(),
            evidence,
            tmp_path / "preflight.json",
        )

    assert evidence["status"] == "running"
    assert evidence["redis_stream_pending_audit"] == audit


@pytest.mark.anyio
async def test_execute_preflight_fails_closed_without_followup_checks(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    runner = load_runner()

    async def schedule_status(*_args):
        return {"state": "OPEN", "active_jobs": 0}

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("unsafe follow-up check ran")

    monkeypatch.setattr(runner, "schedule_status", schedule_status)
    monkeypatch.setattr(runner, "deployment_readiness", forbidden)
    monkeypatch.setattr(runner, "manager_readiness", forbidden)
    evidence = {
        "mode": runner.MODE_PREFLIGHT,
        "status": "running",
        "schedule": {"transitions": [], "final_state": None},
    }

    with pytest.raises(runner.CanaryError, match="must be CLOSED"):
        await runner.execute_preflight(
            object(),
            db,
            object(),
            evidence,
            tmp_path / "preflight.json",
        )

    assert evidence["schedule"]["final_state"] == "OPEN"


@pytest.mark.anyio
async def test_create_graph_is_atomic_unlisted_and_enqueues_one_tick(db: AsyncSession):
    runner = load_runner()
    asset = await add_asset(db)

    graph = await runner.create_canary_graph(db, "run-123", str(asset.id))

    channel = await db.get(ChannelProfile, uuid.UUID(graph["channel_id"]))
    account = await db.get(PublishingAccount, uuid.UUID(graph["account_id"]))
    lane_format = await db.get(LaneFormatMatrix, uuid.UUID(graph["lane_format_id"]))
    seed = await db.get(ManualSeed, uuid.UUID(graph["manual_seed_id"]))
    ticks = list(
        await db.scalars(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.channel_profile_id == channel.id)
        )
    )

    assert channel.enabled is True
    assert channel.dry_run is False
    assert channel.risk_policy_json["publication_privacy"] == "unlisted"
    assert account.default_privacy == "unlisted"
    assert account.external_asset_auto_publish is False
    assert lane_format.default_publish_visibility == "unlisted"
    assert lane_format.source_platforms_json == []
    assert seed.source_policy == "owned_only"
    assert seed.constraints_json["input_asset_id"] == str(asset.id)
    assert [(row.kind, row.status) for row in ticks] == [("agent_tick", "queued")]
    assert ticks[0].payload_json == {
        "channel_id": str(channel.id),
        "plan_delay_seconds": 300,
    }
    assert graph["agent_tick_id"] == str(ticks[0].id)


@pytest.mark.anyio
async def test_create_graph_rejects_asset_without_owned_generated_video_attestation(db: AsyncSession):
    runner = load_runner()
    asset = await add_asset(db, provenance="external")

    with pytest.raises(runner.CanaryError, match="owned generated video"):
        await runner.create_canary_graph(db, "run-unsafe", str(asset.id))

    assert await db.scalar(select(func.count()).select_from(ChannelProfile)) == 0
    assert await db.scalar(select(func.count()).select_from(ChannelOpsQueueItem)) == 0


async def add_uploaded_publication(db: AsyncSession) -> tuple[ChannelProfile, ProductionTask, PublicationRecord]:
    channel = ChannelProfile(name="canary", dry_run=False)
    db.add(channel)
    await db.flush()
    account_id = uuid.uuid4()
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account_id,
        prompt="owned canary",
        state="uploaded_private",
    )
    db.add(task)
    await db.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        account_id=account_id,
        platform_content_id="video-123",
        desired_privacy="unlisted",
        current_privacy="unlisted",
        publish_status="uploaded",
        compliance_disposition="approved",
    )
    db.add(publication)
    await db.flush()
    db.add(
        ChannelOpsQueueItem(
            kind="promote_publication",
            idempotency_key=f"promote_publication:{publication.id}:unlisted:delayed",
            channel_profile_id=channel.id,
            priority=70,
            payload_json={"publication_id": str(publication.id), "target_visibility": "unlisted"},
        )
    )
    await db.commit()
    return channel, task, publication


@pytest.mark.anyio
async def test_replace_auto_promotion_is_atomic_and_unlisted(db: AsyncSession):
    runner = load_runner()
    channel, _task, publication = await add_uploaded_publication(db)

    cancelled_ids, immediate = await runner.replace_auto_promotion_with_immediate(
        db,
        channel.id,
        publication.id,
    )

    rows = list(
        await db.scalars(
            select(ChannelOpsQueueItem)
            .where(ChannelOpsQueueItem.kind == "promote_publication")
            .order_by(ChannelOpsQueueItem.created_at.asc())
        )
    )
    assert len(cancelled_ids) == 1
    assert [row.status for row in rows] == ["cancelled", "queued"]
    assert immediate.id == rows[1].id
    assert immediate.priority == 70
    assert immediate.payload_json == {
        "publication_id": str(publication.id),
        "target_visibility": "unlisted",
        "channel_profile_id": str(channel.id),
    }
    assert immediate.idempotency_key == f"promote_publication:{publication.id}:unlisted:manual"


@pytest.mark.anyio
async def test_metrics_probe_uses_api_equivalent_hour_bucket_idempotency(db: AsyncSession):
    runner = load_runner()
    channel, _task, publication = await add_uploaded_publication(db)

    first = await runner.enqueue_metrics_probe(db, publication.id)
    second = await runner.enqueue_metrics_probe(db, publication.id)

    assert second.id == first.id
    assert first.kind == "collect_metrics"
    assert first.channel_profile_id == channel.id
    assert first.priority == 90
    assert first.payload_json == {
        "publication_id": str(publication.id),
        "snapshot_stage": "immediate",
    }
    assert first.idempotency_key.startswith(f"collect_metrics:{publication.id}:")


@pytest.mark.anyio
async def test_pending_metrics_rows_reports_only_safe_durable_stage_authority(db: AsyncSession):
    runner = load_runner()
    channel, _task, publication = await add_uploaded_publication(db)
    queue = runner.ChannelOpsQueueService()
    expected_stages = ["1h", "6h", "24h", "72h", "7d"]
    expected_schedule_ids = []

    for offset, stage in enumerate(expected_stages, start=1):
        schedule_id = uuid.uuid4()
        expected_schedule_ids.append(str(schedule_id))
        await queue.enqueue(
            db,
            kind="collect_metrics",
            idempotency_key=f"collect_metrics:{publication.id}:stage:{stage}:attempt:0",
            payload={
                "publication_id": str(publication.id),
                "metric_schedule_id": str(schedule_id),
                "snapshot_stage": stage,
                "metrics_poll_count": 0,
                "title": "must not enter evidence",
                "access_token": "must not enter evidence",
            },
            priority=90,
            run_after=runner.utc_now() + timedelta(hours=offset),
            channel_profile_id=channel.id,
            commit=False,
        )
    await queue.enqueue(
        db,
        kind="collect_metrics",
        idempotency_key=f"collect_metrics:{publication.id}:immediate",
        payload={
            "publication_id": str(publication.id),
            "snapshot_stage": "immediate",
        },
        priority=90,
        channel_profile_id=channel.id,
        commit=False,
    )
    await db.commit()

    rows = await runner.pending_metrics_rows(db, publication.id)

    assert [row["snapshot_stage"] for row in rows] == expected_stages
    assert [row["metric_schedule_id"] for row in rows] == expected_schedule_ids
    assert all(
        set(row)
        == {
            "id",
            "status",
            "run_after",
            "metrics_poll_count",
            "snapshot_stage",
            "metric_schedule_id",
        }
        for row in rows
    )


def test_exact_durable_metric_stage_policy_rejects_missing_or_reordered_rows():
    runner = load_runner()
    rows = [{"snapshot_stage": stage} for stage in runner.EXPECTED_DURABLE_METRIC_STAGES]

    runner.assert_exact_durable_metric_stages(rows)
    with pytest.raises(runner.CanaryError, match="exact five-stage"):
        runner.assert_exact_durable_metric_stages(rows[:-1])
    with pytest.raises(runner.CanaryError, match="exact five-stage"):
        runner.assert_exact_durable_metric_stages(list(reversed(rows)))


@pytest.mark.anyio
async def test_immediate_metrics_probe_requires_task_to_remain_scheduled(db: AsyncSession):
    runner = load_runner()
    _channel, task, _publication = await add_uploaded_publication(db)
    task.state = "scheduled"
    await db.commit()

    assert await runner.assert_immediate_metrics_task_state(db, task.id) == "scheduled"

    task = await db.get(ProductionTask, task.id)
    assert task is not None
    task.state = "measured"
    await db.commit()
    with pytest.raises(runner.CanaryError, match="prematurely changed task state"):
        await runner.assert_immediate_metrics_task_state(db, task.id)


def test_schedule_close_failure_marks_evidence_failed_without_overwriting_root_failure():
    runner = load_runner()
    evidence = {
        "status": "failed",
        "failure": {"type": "CanaryError", "message": "root failure"},
        "schedule": {"final_state": None},
    }

    runner.mark_schedule_close_failure(evidence, RuntimeError("sensitive detail"))

    assert evidence["status"] == "failed"
    assert evidence["failure"] == {"type": "CanaryError", "message": "root failure"}
    assert evidence["schedule"]["final_state"] == "UNKNOWN"
    assert evidence["schedule"]["close_error"] == "RuntimeError"


def test_schedule_close_failure_creates_sanitized_failure_after_success():
    runner = load_runner()
    evidence = {"status": "succeeded", "schedule": {"final_state": None}}

    runner.mark_schedule_close_failure(evidence, RuntimeError("postgresql://user:secret@example/db"))

    assert evidence["status"] == "failed"
    assert evidence["failure"] == {
        "type": "RuntimeError",
        "message": "final schedule close failed",
    }


def test_runner_task_wait_covers_deployed_daytime_throttle():
    runner = load_runner()

    wait_seconds = runner.runner_task_wait_seconds(
        "\n".join(
            (
                "CHANNELOPS_RUNNER_POLL_SECONDS=5",
                "CHANNELOPS_THROTTLE_ENABLED=true",
                "CHANNELOPS_THROTTLE_RUNNER_POLL_SECONDS=300",
            )
        )
    )

    assert wait_seconds == 360


def test_channelops_wait_budget_covers_deployed_runner_poll():
    runner = load_runner()

    wait_seconds = runner.channelops_wait_seconds(
        timeout_seconds=1_200,
        deployed_wait_seconds=360,
    )

    assert wait_seconds == 360


@pytest.mark.anyio
async def test_failure_cleanup_uses_naive_utc_for_job_and_node_columns(db: AsyncSession):
    runner = load_runner()
    channel = ChannelProfile(name="failed canary", dry_run=False)
    db.add(channel)
    await db.flush()
    job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=JobStatus.PENDING,
    )
    db.add(job)
    await db.flush()
    node = NodeExecution(
        job_id=job.id,
        node_id="source_1",
        node_type="source",
        status=NodeStatus.PENDING,
    )
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=uuid.uuid4(),
        prompt="owned canary",
        state="producing",
        job_id=job.id,
    )
    queue_item = ChannelOpsQueueItem(
        kind="observe_job",
        idempotency_key=f"observe_job:{job.id}:test",
        channel_profile_id=channel.id,
        payload_json={"job_id": str(job.id)},
    )
    db.add_all((node, task, queue_item))
    await db.commit()

    report = await runner.failure_cleanup(db, channel.id)

    assert report["cancelled_job_ids"] == [str(job.id)]
    assert report["cancelled_node_execution_ids"] == [str(node.id)]
    assert job.status == JobStatus.CANCELLED
    assert job.completed_at is not None and job.completed_at.tzinfo is None
    assert node.status == NodeStatus.CANCELLED
    assert node.completed_at is not None and node.completed_at.tzinfo is None
    assert task.state == "held"
    assert task.state_updated_at.tzinfo is timezone.utc
    assert queue_item.status == "dead_lettered"


@pytest.mark.anyio
async def test_python_schedule_releases_only_python_owned_jobs(db: AsyncSession):
    python_job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=JobStatus.WAITING_WINDOW,
        orchestrator_owner="python",
    )
    go_job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=JobStatus.WAITING_WINDOW,
        orchestrator_owner="go",
    )
    db.add_all((python_job, go_job))
    await db.commit()

    released = await release_waiting_video_jobs(db)

    assert released == [str(python_job.id)]
    assert python_job.status == JobStatus.PENDING
    assert go_job.status == JobStatus.WAITING_WINDOW


@pytest.mark.anyio
async def test_python_recovery_loads_only_python_owned_jobs(db: AsyncSession):
    python_job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=JobStatus.PENDING,
        orchestrator_owner="python",
    )
    go_job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=JobStatus.PENDING,
        orchestrator_owner="go",
    )
    db.add_all((python_job, go_job))
    await db.commit()

    jobs = await load_video_jobs_for_recovery(db)

    assert [job.id for job in jobs] == [python_job.id]


@pytest.mark.anyio
async def test_backlog_ignores_only_global_cleanup_maintenance(db: AsyncSession):
    runner = load_runner()
    cleanup = ChannelOpsQueueItem(
        kind="cleanup_expired",
        idempotency_key="cleanup_expired:2026-07-12",
        channel_profile_id=None,
        payload_json={},
    )
    db.add(cleanup)
    await db.commit()

    report = await runner.active_backlog(db)

    assert report["unsafe_queue_item_ids"] == []

    unsafe = ChannelOpsQueueItem(
        kind="agent_tick",
        idempotency_key="agent_tick:global:2026-07-12-18",
        channel_profile_id=None,
        payload_json={"channel_id": str(uuid.uuid4())},
    )
    db.add(unsafe)
    await db.commit()

    report = await runner.active_backlog(db)

    assert report["unsafe_queue_item_ids"] == [str(unsafe.id)]


@pytest.mark.anyio
async def test_backlog_ignores_queued_and_running_discovery_but_flags_publishing_queue_item(
    db: AsyncSession,
):
    runner = load_runner()
    queued_discovery = ChannelOpsQueueItem(
        kind="ingest_discovery",
        idempotency_key="ingest_discovery:queued:2026-07-21",
        channel_profile_id=None,
        payload_json={},
        status="queued",
    )
    running_discovery = ChannelOpsQueueItem(
        kind="ingest_discovery",
        idempotency_key="ingest_discovery:running:2026-07-21",
        channel_profile_id=None,
        payload_json={},
        status="running",
    )
    publishing = ChannelOpsQueueItem(
        kind="publish_task",
        idempotency_key="publish_task:2026-07-21",
        channel_profile_id=None,
        payload_json={},
        status="queued",
    )
    db.add_all((queued_discovery, running_discovery, publishing))
    await db.commit()

    report = await runner.active_backlog(db)

    assert report["unsafe_queue_item_ids"] == [str(publishing.id)]
