from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from app.services import worker_runtime_role_cli as runtime_cli
from app.services import worker_role_cli_common as role_common


def test_runtime_role_names_are_stable_and_generation_scoped() -> None:
    first = runtime_cli.role_names_for_generation(
        "vp-ffmpeg-worker-go-swarm",
        41,
    )
    repeated = runtime_cli.role_names_for_generation(
        "vp-ffmpeg-worker-go-swarm",
        41,
    )
    replacement = runtime_cli.role_names_for_generation(
        "vp-ffmpeg-worker-go-swarm",
        42,
    )

    assert first == repeated
    assert first.stable == "vp_worker_runtime"
    assert first.versioned.startswith("vp_worker_")
    assert first.versioned != replacement.versioned
    assert len(first.versioned.encode()) <= 63


@pytest.mark.parametrize(
    ("service_name", "generation"),
    [
        ("", 1),
        ("../worker", 1),
        ("vp-worker", 0),
        ("vp-worker", -1),
        ("vp-worker", True),
    ],
)
def test_runtime_role_names_reject_unsafe_identity(
    service_name: str,
    generation: int,
) -> None:
    with pytest.raises(runtime_cli.RuntimeRoleArgumentError):
        runtime_cli.role_names_for_generation(service_name, generation)


def test_secure_state_writer_rejects_nonprivate_file_modes(
    tmp_path: Path,
) -> None:
    with pytest.raises(role_common.WorkerRoleCommonError):
        role_common.write_secure_files(
            tmp_path / "state",
            ("generation",),
            {"secret": "value\n"},
            file_mode=0o440,
        )

    with pytest.raises(role_common.WorkerRoleCommonError):
        role_common.write_secure_files(
            tmp_path / "state",
            ("generation",),
            {"secret": "value\n", "state.json": "{}\n"},
            file_modes={
                "secret": 0o400,
                "state.json": 0o640,
            },
        )


def test_secure_state_reader_revalidates_mode_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = tmp_path / "secret"
    secret.write_text("sensitive\n", encoding="utf-8")
    secret.chmod(0o400)
    real_open = role_common.os.open

    def change_mode_then_open(
        path: os.PathLike[str] | str,
        flags: int,
        *args: object,
        **kwargs: object,
    ) -> int:
        if Path(path) == secret:
            if hasattr(os, "O_NONBLOCK"):
                assert flags & os.O_NONBLOCK
            secret.chmod(0o600)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(role_common.os, "open", change_mode_then_open)

    with pytest.raises(role_common.WorkerRoleCommonError):
        role_common.read_secure_file(secret, required_mode=0o400)


def test_owner_url_reader_rejects_premature_eof_after_valid_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner_url = tmp_path / "owner-url"
    valid_prefix = (
        b"postgresql://owner:secret@127.0.0.1:55439/"
        b"videoprocess_test\n"
    )
    owner_url.write_bytes(valid_prefix + b"must-not-be-ignored\n")
    owner_url.chmod(0o400)
    real_read = role_common.os.read
    reads = 0

    def premature_eof(descriptor: int, size: int) -> bytes:
        nonlocal reads
        reads += 1
        if reads == 1:
            return real_read(descriptor, len(valid_prefix))
        return b""

    monkeypatch.setattr(role_common.os, "read", premature_eof)
    monkeypatch.setenv("TASK4A_OWNER_URL_FILE", str(owner_url))

    with pytest.raises(
        role_common.WorkerRoleCommonError,
        match="database URL file changed",
    ):
        role_common.load_database_url_file("TASK4A_OWNER_URL_FILE")


def test_owner_url_reader_rejects_in_place_mutation_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner_url = tmp_path / "owner-url"
    owner_url.write_text(
        "postgresql://owner:secret@127.0.0.1:55439/"
        "videoprocess_test\n",
        encoding="utf-8",
    )
    owner_url.chmod(0o400)
    real_read = role_common.os.read
    mutated = False

    def mutate_during_read(descriptor: int, size: int) -> bytes:
        nonlocal mutated
        chunk = real_read(descriptor, size)
        if not mutated:
            mutated = True
            metadata = owner_url.stat()
            os.utime(
                owner_url,
                ns=(
                    metadata.st_atime_ns,
                    metadata.st_mtime_ns + 1_000_000_000,
                ),
            )
        return chunk

    monkeypatch.setattr(role_common.os, "read", mutate_during_read)
    monkeypatch.setenv("TASK4A_OWNER_URL_FILE", str(owner_url))

    with pytest.raises(
        role_common.WorkerRoleCommonError,
        match="database URL file changed",
    ):
        role_common.load_database_url_file("TASK4A_OWNER_URL_FILE")


def test_secure_state_reader_accepts_bounded_partial_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = tmp_path / "secret"
    secret.write_text("sensitive\n", encoding="utf-8")
    secret.chmod(0o400)
    real_read = role_common.os.read

    def bounded_read(descriptor: int, size: int) -> bytes:
        return real_read(descriptor, min(size, 2))

    monkeypatch.setattr(role_common.os, "read", bounded_read)

    assert (
        role_common.read_secure_file(secret, required_mode=0o400)
        == "sensitive\n"
    )


def test_owner_url_reader_rejects_growth_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner_url = tmp_path / "owner-url"
    owner_url.write_text(
        "postgresql://owner:secret@127.0.0.1:55439/"
        "videoprocess_test\n",
        encoding="utf-8",
    )
    owner_url.chmod(0o400)
    real_read = role_common.os.read
    grown = False

    def grow_during_read(descriptor: int, size: int) -> bytes:
        nonlocal grown
        chunk = real_read(descriptor, size)
        if not grown:
            grown = True
            owner_url.chmod(0o600)
            with owner_url.open("ab") as handle:
                handle.write(b"x")
            owner_url.chmod(0o400)
        return chunk

    monkeypatch.setattr(role_common.os, "read", grow_during_read)
    monkeypatch.setenv("TASK4A_OWNER_URL_FILE", str(owner_url))

    with pytest.raises(
        role_common.WorkerRoleCommonError,
        match="database URL file changed",
    ):
        role_common.load_database_url_file("TASK4A_OWNER_URL_FILE")


def test_worker_function_allowlist_is_exact() -> None:
    assert set(runtime_cli.WORKER_FUNCTIONS) == {
        "vp_worker_register(text,bigint,text,text,uuid,integer,text,jsonb,"
        "text,text,text,text,jsonb,text,text,text,text,text)",
        "vp_worker_heartbeat(uuid,text,uuid,bigint,text)",
        "vp_worker_release(uuid,text,uuid,bigint,text,text)",
        "vp_require_worker_lease(uuid,bigint)",
        "vp_claim_worker_node("
        "uuid,bigint,text,uuid,uuid,text,text,text,text,uuid)",
        "vp_require_worker_node_claim("
        "uuid,bigint,text,timestamp with time zone,uuid,uuid)",
        "vp_persist_worker_artifact("
        "uuid,bigint,text,timestamp with time zone,uuid,uuid,"
        "text,text,bigint,text,text,jsonb)",
        "vp_prepare_worker_event_emission("
        "uuid,bigint,text,timestamp with time zone,uuid,uuid,uuid,"
        "text,text,text,jsonb,text)",
        "vp_mark_worker_event_emitted(uuid,uuid,bigint,text)",
        "vp_list_worker_prepared_event_emissions(uuid,bigint,integer)",
        "vp_load_worker_prepared_event_emission(uuid,uuid,bigint)",
        "vp_require_worker_lease_margin(uuid,bigint,integer)",
        "vp_require_worker_task_ack_receipt("
        "uuid,bigint,text,timestamp with time zone,text,text,text,text,uuid)",
        "vp_authorize_worker_task_ack("
        "uuid,uuid,bigint,text,timestamp with time zone)",
        "vp_acknowledge_worker_task_delivery("
        "uuid,uuid,bigint,text,timestamp with time zone,"
        "text,text,text,text,uuid)",
        "vp_reserve_worker_youtube_upload("
        "uuid,bigint,text,timestamp with time zone,uuid,uuid,uuid,"
        "text,text,text)",
        "vp_transition_worker_youtube_upload("
        "uuid,bigint,text,timestamp with time zone,uuid,text,text,"
        "text,text,jsonb,text)",
        "vp_staging_janitor_readiness(integer,integer)",
        "vp_require_worker_redis_continuity(integer)",
    }
    assert not {
        "vp_attest_worker_task_delivery",
        "vp_observe_worker_lease",
        "vp_recover_registered_worker_node",
    } & {
        signature.split("(", 1)[0]
        for signature in runtime_cli.WORKER_FUNCTIONS
    }


async def test_provision_writes_read_only_secrets_and_transactional_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = tmp_path / "owner-url"
    owner.write_text(
        "postgresql://owner:secret@127.0.0.1:5432/videoprocess\n"
    )
    owner.chmod(0o400)
    state_dir = tmp_path / "state"
    observed: dict[str, object] = {}

    async def fake_provision(
        owner_url: str,
        service_name: str,
        generation: int,
        target: Path,
        names: runtime_cli.RuntimeRoleNames,
    ) -> None:
        observed.update(
            owner_url=owner_url,
            service_name=service_name,
            generation=generation,
            state_dir=target,
            names=names,
        )
        runtime_cli.write_generation_state(
            target,
            service_name,
            generation,
            names,
            database_url=(
                "postgresql://vp_worker:database-secret@"
                "127.0.0.1:5432/videoprocess"
            ),
            admission_token="admission-secret",
        )

    monkeypatch.setattr(runtime_cli, "_provision", fake_provision)
    monkeypatch.setenv(
        runtime_cli.OWNER_URL_FILE_ENV,
        str(owner),
    )

    result = await runtime_cli.run(
        [
            "provision",
            "--service-name",
            "vp-ffmpeg-worker-go-swarm",
            "--generation",
            "41",
            "--state-dir",
            str(state_dir),
        ]
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "database-secret" not in output
    assert "admission-secret" not in output
    payload = json.loads(output)
    assert payload["code"] == "worker_runtime_role_provisioned"
    paths = runtime_cli.credential_paths(
        state_dir,
        "vp-ffmpeg-worker-go-swarm",
        41,
    )
    assert set(paths) == {"database_url", "admission_token", "state"}
    assert stat.S_IMODE(paths["database_url"].stat().st_mode) == 0o400
    assert stat.S_IMODE(paths["admission_token"].stat().st_mode) == 0o400
    assert stat.S_IMODE(paths["state"].stat().st_mode) == 0o600
    stored = json.loads(paths["state"].read_text())
    assert stored == {
        "database_principal": observed["names"].versioned,
        "generation": 41,
        "service_name": "vp-ffmpeg-worker-go-swarm",
        "token_sha256": (
            "f3d66d8d07123105b2526662841766db2ed796e3cca87e9f"
            "f24d35cd7f7da529"
        ),
        "version": 1,
    }
    assert stat.S_IMODE(state_dir.stat().st_mode) == 0o700
    assert os.environ.get("DATABASE_URL") is None


async def test_runtime_cli_rejects_owner_url_from_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv(runtime_cli.OWNER_URL_FILE_ENV, raising=False)
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://owner:leaked@127.0.0.1/videoprocess",
    )

    result = await runtime_cli.run(
        [
            "provision",
            "--service-name",
            "vp-ffmpeg-worker-go-swarm",
            "--generation",
            "1",
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    assert result == 3
    output = capsys.readouterr().out
    assert "leaked" not in output
    assert json.loads(output)["code"] == "worker_runtime_owner_url_invalid"


async def test_runtime_cli_sanitizes_shared_role_helper_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = tmp_path / "owner-url"
    owner.write_text(
        "postgresql://owner:database-secret@"
        "127.0.0.1:5432/videoprocess\n"
    )
    owner.chmod(0o400)

    async def failing_provision(*arguments: object) -> None:
        raise role_common.WorkerRoleCommonError(
            "internal database-secret detail"
        )

    monkeypatch.setattr(runtime_cli, "_provision", failing_provision)
    monkeypatch.setenv(runtime_cli.OWNER_URL_FILE_ENV, str(owner))

    result = await runtime_cli.run(
        [
            "provision",
            "--service-name",
            "vp-ffmpeg-worker-go-swarm",
            "--generation",
            "41",
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    assert result == 4
    output = capsys.readouterr().out
    assert "database-secret" not in output
    assert json.loads(output) == {
        "code": "worker_runtime_role_operation_failed",
        "status": "error",
    }


async def test_runtime_cli_fails_safely_when_authority_lock_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = tmp_path / "owner-url"
    owner.write_text(
        "postgresql://owner:database-secret@"
        "127.0.0.1:5432/videoprocess\n"
    )
    owner.chmod(0o400)
    closed = False

    class FakeConnection:
        async def close(self) -> None:
            nonlocal closed
            closed = True

    async def fake_connect(url: str) -> FakeConnection:
        assert "database-secret" in url
        return FakeConnection()

    async def fail_lock(*arguments: object) -> None:
        raise role_common.WorkerRoleCommonError(
            "lock failed with database-secret"
        )

    monkeypatch.setattr(runtime_cli.asyncpg, "connect", fake_connect)
    monkeypatch.setattr(
        runtime_cli,
        "acquire_worker_service_authority_lock",
        fail_lock,
    )
    monkeypatch.setenv(runtime_cli.OWNER_URL_FILE_ENV, str(owner))

    result = await runtime_cli.run(
        [
            "provision",
            "--service-name",
            "vp-ffmpeg-worker-go-swarm",
            "--generation",
            "41",
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    assert result == 4
    assert closed
    output = capsys.readouterr().out
    assert "database-secret" not in output
    assert json.loads(output) == {
        "code": "worker_runtime_role_operation_failed",
        "status": "error",
    }


async def test_revoke_checks_durable_admission_when_login_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    owner = tmp_path / "owner-url"
    owner.write_text(
        "postgresql://owner:secret@127.0.0.1:5432/videoprocess\n"
    )
    owner.chmod(0o400)
    queries: list[str] = []

    class FakeTransaction:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(
            self,
            exc_type: object,
            exc: object,
            traceback: object,
        ) -> None:
            return None

    class FakeConnection:
        def transaction(self) -> FakeTransaction:
            return FakeTransaction()

        async def execute(
            self,
            query: str,
            *arguments: object,
        ) -> None:
            queries.append(query)

        async def fetchval(
            self,
            query: str,
            *arguments: object,
        ) -> bool:
            queries.append(query)
            if "worker_admission_grants" in query:
                return True
            return False

        async def close(self) -> None:
            return None

    async def fake_connect(url: str) -> FakeConnection:
        assert "secret" in url
        return FakeConnection()

    monkeypatch.setattr(runtime_cli.asyncpg, "connect", fake_connect)
    monkeypatch.setenv(runtime_cli.OWNER_URL_FILE_ENV, str(owner))

    result = await runtime_cli.run(
        [
            "revoke",
            "--service-name",
            "vp-ffmpeg-worker-go-swarm",
            "--generation",
            "41",
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    assert result == 4
    assert any("worker_admission_grants" in query for query in queries)
    assert not any("pg_terminate_backend" in query for query in queries)
    assert json.loads(capsys.readouterr().out) == {
        "code": "worker_runtime_role_operation_failed",
        "status": "error",
    }
