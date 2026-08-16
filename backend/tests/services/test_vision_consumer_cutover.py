from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
import redis.asyncio as redis

from app.services import vision_consumer_cutover as cutover
from app.services.vision_consumer_cutover import (
    VisionConsumerCutoverError,
    reconcile_vision_consumers,
    vision_consumers_converged,
)


REDIS_SECRET = (
    "redis://vision-watcher:redis-credential-sentinel@redis.example:6380/0"
)
DATABASE_SECRET = (
    "postgresql+asyncpg://vision-read:database-credential-sentinel@"
    "database.example:5432/videoprocess"
)


class FakeRedis:
    def __init__(
        self,
        snapshots: list[list[dict[str, object]]],
        *,
        deleted_pending: dict[str, int] | None = None,
    ):
        self.snapshots = snapshots
        self.deleted_pending = deleted_pending or {}
        self.deleted: list[str] = []
        self.reads = 0

    async def xinfo_consumers(self, stream: str, group: str):
        assert stream == "vp:tasks:vision"
        assert group == "vision-workers"
        index = min(self.reads, len(self.snapshots) - 1)
        self.reads += 1
        return self.snapshots[index]

    async def xgroup_delconsumer(self, stream: str, group: str, consumer: str):
        assert stream == "vp:tasks:vision"
        assert group == "vision-workers"
        self.deleted.append(consumer)
        return self.deleted_pending.get(consumer, 0)

    async def eval(
        self,
        _script: str,
        key_count: int,
        stream: str,
        group: str,
        _managed_pattern: str,
    ):
        assert key_count == 1
        assert stream == "vp:tasks:vision"
        assert group == "vision-workers"
        records = self.snapshots[max(0, self.reads - 1)]
        managed = [
            row["name"]
            for row in records
            if str(row["name"]).startswith("vision-worker@150-vision:")
        ]
        legacy = [row["name"] for row in records if row["name"] not in managed]
        if any(self.deleted_pending.get(str(name), 0) for name in legacy):
            raise redis.ResponseError("VISION_PENDING")
        self.deleted.extend(str(name) for name in legacy)
        return [*managed, *legacy]

    async def aclose(self):
        return None


class FakeCliRedis:
    def __init__(
        self,
        consumers: list[dict[str, object]] | None = None,
        *,
        pending: object = None,
        lag: object = 0,
        failure: Exception | None = None,
    ):
        self.consumers = consumers or []
        self.pending = {"pending": 0} if pending is None else pending
        self.lag = lag
        self.failure = failure
        self.closed = False

    async def xinfo_consumers(self, stream: str, group: str):
        assert stream == "vp:tasks:vision"
        assert group == "vision-workers"
        if self.failure is not None:
            raise self.failure
        return self.consumers

    async def xpending(self, stream: str, group: str):
        assert stream == "vp:tasks:vision"
        assert group == "vision-workers"
        if self.failure is not None:
            raise self.failure
        return self.pending

    async def xinfo_groups(self, stream: str):
        assert stream == "vp:tasks:vision"
        if self.failure is not None:
            raise self.failure
        return [{"name": "vision-workers", "lag": self.lag}]

    async def aclose(self):
        self.closed = True


class FakeDatabaseResult:
    def __init__(self, value: object):
        self.value = value

    def one_or_none(self):
        return self.value

    def scalar_one(self):
        return self.value


class FakeDatabaseConnection:
    def __init__(
        self,
        *,
        schedule: object,
        active_executions: int,
        failure: Exception | None = None,
    ):
        self.schedule = schedule
        self.active_executions = active_executions
        self.failure = failure

    async def execute(self, statement: object):
        if self.failure is not None:
            raise self.failure
        query = str(statement)
        if "FROM runtime_schedules" in query:
            return FakeDatabaseResult(self.schedule)
        if "FROM node_executions" in query:
            return FakeDatabaseResult(self.active_executions)
        raise AssertionError(f"unexpected safety query: {query}")


class FakeDatabaseConnectionContext:
    def __init__(self, connection: FakeDatabaseConnection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeDatabaseEngine:
    def __init__(
        self,
        *,
        schedule: object = None,
        active_executions: int = 0,
        failure: Exception | None = None,
    ):
        if schedule is None:
            schedule = SimpleNamespace(state="CLOSED", guarded_job_id=None)
        self.connection = FakeDatabaseConnection(
            schedule=schedule,
            active_executions=active_executions,
            failure=failure,
        )
        self.disposed = False

    def connect(self):
        return FakeDatabaseConnectionContext(self.connection)

    async def dispose(self):
        self.disposed = True


def write_secret(path: Path, value: str, *, mode: int = 0o400) -> Path:
    path.write_text(value, encoding="utf-8")
    path.chmod(mode)
    return path


def configure_secret_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    redis_secret = write_secret(tmp_path / "redis-url", REDIS_SECRET)
    database_secret = write_secret(tmp_path / "database-url", DATABASE_SECRET)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("VISION_CUTOVER_REDIS_URL_FILE", str(redis_secret))
    monkeypatch.setenv("VISION_CUTOVER_DATABASE_URL_FILE", str(database_secret))


def invoke_main(monkeypatch: pytest.MonkeyPatch, *arguments: str) -> int:
    monkeypatch.setattr(
        sys,
        "argv",
        ["vision-consumer-cutover", *arguments],
    )
    try:
        return cutover.main()
    except SystemExit as exc:
        if isinstance(exc.code, int):
            return exc.code
        raise AssertionError("CLI exited without a numeric status") from exc


def make_invalid_secret(path: Path, invalid_class: str, value: str) -> Path:
    if invalid_class == "symlink":
        target = write_secret(path.with_name(f"{path.name}-target"), value)
        path.symlink_to(target)
    elif invalid_class == "wrong-mode":
        write_secret(path, value, mode=0o600)
    elif invalid_class == "empty":
        write_secret(path, "")
    elif invalid_class == "oversized":
        write_secret(path, value + "x" * 4096)
    elif invalid_class == "missing":
        pass
    elif invalid_class == "malformed":
        write_secret(path, "credential-payload-sentinel")
    else:
        raise AssertionError(f"unknown invalid class: {invalid_class}")
    return path



def consumer(name: str, *, pending: int = 0) -> dict[str, object]:
    return {
        "name": name,
        "pending": pending,
        "idle": 500,
        "inactive": 500,
    }


@pytest.mark.anyio
async def test_converged_requires_only_one_zero_pending_managed_consumer():
    managed = consumer("vision-worker@150-vision:1")

    assert await vision_consumers_converged(FakeRedis([[managed]])) is True
    assert (
        await vision_consumers_converged(
            FakeRedis([[managed, consumer("vision-worker@legacy:1")]])
        )
        is False
    )
    assert (
        await vision_consumers_converged(
            FakeRedis([[consumer("vision-worker@150-vision:1", pending=1)]])
        )
        is False
    )


@pytest.mark.anyio
async def test_reconcile_removes_only_zero_pending_legacy_consumers():
    managed = consumer("vision-worker@150-vision:1")
    legacy = [
        consumer("vision-worker@17add19d51d0:1"),
        consumer("vision-worker@7a2bcf87f570:1"),
    ]
    redis = FakeRedis([legacy + [managed], [managed]])

    result = await reconcile_vision_consumers(redis, wait_attempts=1)

    assert result == {
        "managed_consumer": "vision-worker@150-vision:1",
        "removed_consumers": [
            "vision-worker@17add19d51d0:1",
            "vision-worker@7a2bcf87f570:1",
        ],
    }
    assert redis.deleted == [
        "vision-worker@17add19d51d0:1",
        "vision-worker@7a2bcf87f570:1",
    ]


@pytest.mark.anyio
async def test_reconcile_waits_for_managed_consumer_before_deleting(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(delay: float):
        sleeps.append(delay)

    monkeypatch.setattr("app.services.vision_consumer_cutover.asyncio.sleep", fake_sleep)
    legacy = consumer("vision-worker@7a2bcf87f570:1")
    managed = consumer("vision-worker@150-vision:1")
    redis = FakeRedis([[legacy], [legacy, managed], [managed]])

    await reconcile_vision_consumers(redis, wait_attempts=2, wait_delay_seconds=0.25)

    assert sleeps == [0.25]
    assert redis.deleted == ["vision-worker@7a2bcf87f570:1"]


@pytest.mark.anyio
async def test_reconcile_rejects_pending_without_deleting():
    redis = FakeRedis(
        [[consumer("vision-worker@7a2bcf87f570:1", pending=1), consumer("vision-worker@150-vision:1")]]
    )

    with pytest.raises(VisionConsumerCutoverError, match="pending"):
        await reconcile_vision_consumers(redis, wait_attempts=1)

    assert redis.deleted == []


@pytest.mark.anyio
async def test_reconcile_rejects_duplicate_managed_consumers_without_deleting():
    redis = FakeRedis(
        [
            [
                consumer("vision-worker@150-vision:1"),
                consumer("vision-worker@150-vision:2"),
            ]
        ]
    )

    with pytest.raises(VisionConsumerCutoverError, match="exactly one"):
        await reconcile_vision_consumers(redis, wait_attempts=1)

    assert redis.deleted == []


@pytest.mark.anyio
async def test_reconcile_rejects_pending_detected_by_atomic_cutover():
    legacy_name = "vision-worker@7a2bcf87f570:1"
    redis = FakeRedis(
        [[consumer(legacy_name), consumer("vision-worker@150-vision:1")]],
        deleted_pending={legacy_name: 1},
    )

    with pytest.raises(VisionConsumerCutoverError, match="pending"):
        await reconcile_vision_consumers(redis, wait_attempts=1)

    assert redis.deleted == []


@pytest.mark.anyio
async def test_reconcile_rejects_malformed_consumer_records():
    redis = FakeRedis([[{"name": "", "pending": 0}]])

    with pytest.raises(VisionConsumerCutoverError, match="malformed"):
        await reconcile_vision_consumers(redis, wait_attempts=1)


@pytest.mark.anyio
async def test_cli_rejects_redis_url_environment_only(monkeypatch, capsys):
    monkeypatch.setenv("REDIS_URL", REDIS_SECRET)
    monkeypatch.delenv("VISION_CUTOVER_REDIS_URL_FILE", raising=False)
    monkeypatch.setattr(
        cutover.redis,
        "from_url",
        lambda *_args, **_kwargs: FakeCliRedis(
            [consumer("vision-worker@150-vision:1")]
        ),
    )

    assert await cutover.run(check_only=True) == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "vision cutover configuration invalid\n"
    assert REDIS_SECRET not in captured.out + captured.err


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("client", "expected_status", "expected_stdout"),
    [
        pytest.param(
            FakeCliRedis([consumer("vision-worker@150-vision:1")]),
            0,
            '{"converged": true}\n',
            id="converged",
        ),
        pytest.param(
            FakeCliRedis([consumer("vision-worker@legacy:1")]),
            10,
            '{"converged": false}\n',
            id="cutover-required",
        ),
        pytest.param(
            FakeCliRedis(failure=redis.ConnectionError(REDIS_SECRET)),
            1,
            "",
            id="operational-error",
        ),
    ],
)
async def test_check_only_preserves_exit_semantics_with_secret_file(
    monkeypatch,
    tmp_path,
    capsys,
    client,
    expected_status,
    expected_stdout,
):
    configure_secret_files(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cutover.redis,
        "from_url",
        lambda *_args, **_kwargs: client,
    )

    assert await cutover.run(check_only=True) == expected_status

    captured = capsys.readouterr()
    assert captured.out == expected_stdout
    assert REDIS_SECRET not in captured.out + captured.err


@pytest.mark.parametrize("credential_kind", ["redis", "database"])
@pytest.mark.parametrize(
    "invalid_class",
    ["symlink", "wrong-mode", "empty", "oversized", "missing", "malformed"],
)
def test_cli_rejects_invalid_credential_files_without_disclosure(
    monkeypatch,
    tmp_path,
    capsys,
    credential_kind,
    invalid_class,
):
    configure_secret_files(monkeypatch, tmp_path)
    variable = (
        "VISION_CUTOVER_REDIS_URL_FILE"
        if credential_kind == "redis"
        else "VISION_CUTOVER_DATABASE_URL_FILE"
    )
    value = REDIS_SECRET if credential_kind == "redis" else DATABASE_SECRET
    invalid_path = tmp_path / f"invalid-{credential_kind}-{invalid_class}"
    monkeypatch.setenv(
        variable,
        str(make_invalid_secret(invalid_path, invalid_class, value)),
    )
    monkeypatch.setattr(
        cutover.redis,
        "from_url",
        lambda *_args, **_kwargs: FakeCliRedis(
            [consumer("vision-worker@150-vision:1")]
        ),
    )
    monkeypatch.setattr(
        cutover,
        "create_async_engine",
        lambda *_args, **_kwargs: FakeDatabaseEngine(),
        raising=False,
    )

    arguments = ("--check-only",) if credential_kind == "redis" else ("--safety",)
    status = invoke_main(monkeypatch, *arguments)

    captured = capsys.readouterr()
    assert status == 2
    assert captured.out == ""
    assert captured.err == "vision cutover configuration invalid\n"
    assert REDIS_SECRET not in captured.out + captured.err
    assert DATABASE_SECRET not in captured.out + captured.err


def test_safety_mode_passes_when_database_and_redis_are_idle(
    monkeypatch,
    tmp_path,
    capsys,
):
    configure_secret_files(monkeypatch, tmp_path)
    client = FakeCliRedis()
    engine = FakeDatabaseEngine()
    monkeypatch.setattr(
        cutover.redis,
        "from_url",
        lambda *_args, **_kwargs: client,
    )
    monkeypatch.setattr(
        cutover,
        "create_async_engine",
        lambda *_args, **_kwargs: engine,
        raising=False,
    )

    assert invoke_main(monkeypatch, "--safety") == 0

    captured = capsys.readouterr()
    assert captured.out == '{"safe": true}\n'
    assert captured.err == ""
    assert client.closed is True
    assert engine.disposed is True
    assert REDIS_SECRET not in captured.out + captured.err
    assert DATABASE_SECRET not in captured.out + captured.err


@pytest.mark.parametrize(
    ("schedule", "active_executions", "pending", "lag"),
    [
        pytest.param(None, 0, {"pending": 0}, 0, id="missing-schedule"),
        pytest.param(
            SimpleNamespace(state="OPEN", guarded_job_id=None),
            0,
            {"pending": 0},
            0,
            id="schedule-open",
        ),
        pytest.param(
            SimpleNamespace(state="CLOSED", guarded_job_id=uuid.uuid4()),
            0,
            {"pending": 0},
            0,
            id="guarded-job",
        ),
        pytest.param(
            SimpleNamespace(state="CLOSED", guarded_job_id=None),
            1,
            {"pending": 0},
            0,
            id="active-execution",
        ),
        pytest.param(
            SimpleNamespace(state="CLOSED", guarded_job_id=None),
            0,
            {"pending": 1},
            0,
            id="redis-pending",
        ),
        pytest.param(
            SimpleNamespace(state="CLOSED", guarded_job_id=None),
            0,
            {"pending": 0},
            1,
            id="redis-lag",
        ),
    ],
)
def test_safety_mode_fails_closed_for_each_busy_condition(
    monkeypatch,
    tmp_path,
    capsys,
    schedule,
    active_executions,
    pending,
    lag,
):
    configure_secret_files(monkeypatch, tmp_path)
    client = FakeCliRedis(pending=pending, lag=lag)
    engine = FakeDatabaseEngine(
        schedule=schedule,
        active_executions=active_executions,
    )
    if schedule is None:
        engine.connection.schedule = None
    monkeypatch.setattr(
        cutover.redis,
        "from_url",
        lambda *_args, **_kwargs: client,
    )
    monkeypatch.setattr(
        cutover,
        "create_async_engine",
        lambda *_args, **_kwargs: engine,
        raising=False,
    )

    assert invoke_main(monkeypatch, "--safety") == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "vision cutover safety check failed\n"
    assert REDIS_SECRET not in captured.out + captured.err
    assert DATABASE_SECRET not in captured.out + captured.err


def test_safety_mode_sanitizes_credential_bearing_exceptions(
    monkeypatch,
    tmp_path,
    capsys,
):
    configure_secret_files(monkeypatch, tmp_path)
    client = FakeCliRedis()
    engine = FakeDatabaseEngine(failure=RuntimeError(DATABASE_SECRET))
    monkeypatch.setattr(
        cutover.redis,
        "from_url",
        lambda *_args, **_kwargs: client,
    )
    monkeypatch.setattr(
        cutover,
        "create_async_engine",
        lambda *_args, **_kwargs: engine,
        raising=False,
    )

    assert invoke_main(monkeypatch, "--safety") == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "vision cutover operation failed\n"
    assert REDIS_SECRET not in captured.out + captured.err
    assert DATABASE_SECRET not in captured.out + captured.err


@pytest.mark.parametrize("arguments", [(), ("--reconcile",)])
def test_reconcile_is_default_and_can_be_selected_explicitly(
    monkeypatch,
    tmp_path,
    capsys,
    arguments,
):
    configure_secret_files(monkeypatch, tmp_path)
    managed = consumer("vision-worker@150-vision:1")
    client = FakeRedis([[consumer("vision-worker@legacy:1"), managed], [managed]])
    monkeypatch.setattr(
        cutover.redis,
        "from_url",
        lambda *_args, **_kwargs: client,
    )

    assert invoke_main(monkeypatch, *arguments) == 0

    captured = capsys.readouterr()
    assert captured.err == ""
    assert REDIS_SECRET not in captured.out


def test_cli_modes_are_mutually_exclusive(monkeypatch, capsys):
    assert invoke_main(monkeypatch, "--safety", "--check-only") == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "not allowed with argument" in captured.err


@pytest.mark.anyio
async def test_reconcile_atomically_preserves_consumer_when_pending_appears(
    monkeypatch: pytest.MonkeyPatch,
):
    redis_url = os.environ.get("VISION_CUTOVER_REDIS_TEST_URL")
    if not redis_url:
        pytest.skip("VISION_CUTOVER_REDIS_TEST_URL is not configured")

    stream = f"vp:test:vision-cutover:{uuid.uuid4()}"
    group = "vision-workers"
    legacy_name = "vision-worker@legacy-race:1"
    managed_name = "vision-worker@150-vision:1"
    primary = redis.from_url(redis_url, decode_responses=True)
    racing = redis.from_url(redis_url, decode_responses=True)

    class RacingRedis:
        def __init__(self):
            self.raced = False

        async def xinfo_consumers(self, target_stream: str, target_group: str):
            records = await primary.xinfo_consumers(target_stream, target_group)
            if not self.raced:
                self.raced = True
                claimed = await racing.xreadgroup(
                    target_group,
                    legacy_name,
                    {target_stream: ">"},
                    count=1,
                )
                assert claimed
            return records

        async def xgroup_delconsumer(
            self,
            target_stream: str,
            target_group: str,
            consumer_name: str,
        ):
            return await primary.xgroup_delconsumer(
                target_stream,
                target_group,
                consumer_name,
            )

        async def eval(self, *args):
            return await primary.eval(*args)

    monkeypatch.setattr(cutover, "VISION_STREAM", stream)
    monkeypatch.setattr(cutover, "VISION_GROUP", group)
    try:
        await primary.xadd(stream, {"task": "race"})
        await primary.xgroup_create(stream, group, id="0")
        await primary.xgroup_createconsumer(stream, group, legacy_name)
        await primary.xgroup_createconsumer(stream, group, managed_name)

        with pytest.raises(VisionConsumerCutoverError, match="pending"):
            await reconcile_vision_consumers(RacingRedis(), wait_attempts=1)

        consumers = await primary.xinfo_consumers(stream, group)
        assert any(
            row["name"] == legacy_name and row["pending"] == 1 for row in consumers
        )
    finally:
        await primary.delete(stream)
        await primary.aclose()
        await racing.aclose()
