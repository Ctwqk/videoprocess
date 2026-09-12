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
MANAGED_NAME = "vision-worker@150-vision:1:96431987-9cd9-4145-b6f8-106f30b74196"
PREVIOUS_NAME = "vision-worker@150-vision:1:12345678-1234-4234-8234-123456789abc"
SECOND_MANAGED_NAME = "vision-worker@150-vision:2:12345678-1234-4234-8234-123456789abc"
OBSOLETE_NAME = "vision-worker@150-vision:1"


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
        *_args: object,
    ):
        assert key_count == 1
        assert stream == "vp:tasks:vision"
        assert group == "vision-workers"
        records = self.snapshots[min(max(0, self.reads - 1), len(self.snapshots) - 1)]
        legacy = [row["name"] for row in records if row["name"] != MANAGED_NAME]
        if any(self.deleted_pending.get(str(name), 0) for name in legacy):
            raise redis.ResponseError("VISION_PENDING")
        self.deleted.extend(str(name) for name in legacy)
        return [MANAGED_NAME, *legacy]

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



def consumer(name: str, *, pending: int = 0, idle: object = 500) -> dict[str, object]:
    return {
        "name": name,
        "pending": pending,
        "idle": idle,
        "inactive": 500,
    }


@pytest.mark.anyio
async def test_converged_requires_only_one_zero_pending_managed_consumer():
    managed = consumer(MANAGED_NAME)

    assert await vision_consumers_converged(FakeRedis([[managed]])) is True
    assert (
        await vision_consumers_converged(
            FakeRedis([[managed, consumer("vision-worker@legacy:1")]])
        )
        is False
    )
    assert (
        await vision_consumers_converged(
            FakeRedis([[consumer(MANAGED_NAME, pending=1)]])
        )
        is False
    )


@pytest.mark.anyio
async def test_reconcile_removes_only_stale_zero_pending_legacy_consumers():
    managed = consumer(MANAGED_NAME)
    legacy = [
        consumer("vision-worker@17add19d51d0:1", idle=120001),
        consumer("vision-worker@7a2bcf87f570:1", idle=120001),
    ]
    redis = FakeRedis([legacy + [managed], [managed]])

    result = await reconcile_vision_consumers(redis, wait_attempts=1)

    assert result == {
        "managed_consumer": MANAGED_NAME,
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
    legacy = consumer("vision-worker@7a2bcf87f570:1", idle=120001)
    managed = consumer(MANAGED_NAME)
    redis = FakeRedis([[legacy], [legacy, managed], [managed]])

    await reconcile_vision_consumers(redis, wait_attempts=2, wait_delay_seconds=0.25)

    assert sleeps == [0.25]
    assert redis.deleted == ["vision-worker@7a2bcf87f570:1"]


@pytest.mark.anyio
async def test_reconcile_rejects_pending_without_deleting():
    redis = FakeRedis(
        [[consumer("vision-worker@7a2bcf87f570:1", pending=1), consumer(MANAGED_NAME)]]
    )

    with pytest.raises(VisionConsumerCutoverError, match="pending"):
        await reconcile_vision_consumers(redis, wait_attempts=1)

    assert redis.deleted == []


@pytest.mark.anyio
async def test_reconcile_rejects_duplicate_managed_consumers_without_deleting():
    redis = FakeRedis(
        [
            [
                consumer(MANAGED_NAME),
                consumer(SECOND_MANAGED_NAME),
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
        [[consumer(legacy_name, idle=120001), consumer(MANAGED_NAME)]],
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
@pytest.mark.parametrize(
    "name",
    [
        OBSOLETE_NAME,
        MANAGED_NAME.upper(),
        MANAGED_NAME.replace("96431987-", "96431987"),
        MANAGED_NAME.replace("96431987", "g6431987"),
        MANAGED_NAME.replace(":1:", ":0:"),
        MANAGED_NAME.replace(":1:", ":01:"),
        MANAGED_NAME + ":extra",
        MANAGED_NAME + "\n",
        MANAGED_NAME.replace("150-vision", "other-host"),
    ],
)
async def test_slot_only_and_noncanonical_ids_are_not_current_workers(name):
    client = FakeRedis([[consumer(name)]])

    assert await vision_consumers_converged(client) is False
    with pytest.raises(VisionConsumerCutoverError, match="exactly one"):
        await reconcile_vision_consumers(client, wait_attempts=1)
    assert client.deleted == []


@pytest.mark.anyio
@pytest.mark.parametrize(("idle", "expected"), [(0, True), (120000, True), (120001, False)])
async def test_converged_uses_attempted_interaction_idle_boundary(idle, expected):
    managed = consumer(MANAGED_NAME, idle=idle)
    managed["inactive"] = 9000000

    assert await vision_consumers_converged(FakeRedis([[managed]])) is expected


@pytest.mark.anyio
@pytest.mark.parametrize("name", [MANAGED_NAME, OBSOLETE_NAME])
@pytest.mark.parametrize("idle", [None, -1, True, False, 1.5, "500", float("nan")])
async def test_consumer_idle_must_be_a_nonnegative_integer(name, idle):
    records = [consumer(name, idle=idle)]
    if name != MANAGED_NAME:
        records.append(consumer(MANAGED_NAME))
    client = FakeRedis([records])

    with pytest.raises(VisionConsumerCutoverError, match="malformed"):
        await vision_consumers_converged(client)
    with pytest.raises(VisionConsumerCutoverError, match="malformed"):
        await reconcile_vision_consumers(client, wait_attempts=1)
    assert client.deleted == []


@pytest.mark.anyio
async def test_consumer_idle_is_required():
    record = consumer(MANAGED_NAME)
    del record["idle"]
    client = FakeRedis([[record]])

    with pytest.raises(VisionConsumerCutoverError, match="malformed"):
        await vision_consumers_converged(client)
    with pytest.raises(VisionConsumerCutoverError, match="malformed"):
        await reconcile_vision_consumers(client, wait_attempts=1)
    assert client.deleted == []


@pytest.mark.anyio
async def test_reconcile_removes_slot_only_and_stale_previous_uuid_instances():
    managed = consumer(MANAGED_NAME, idle=2970)
    client = FakeRedis([
        [consumer(OBSOLETE_NAME, idle=6533215), consumer(PREVIOUS_NAME, idle=120001), managed],
        [managed],
    ])

    assert await reconcile_vision_consumers(client, wait_attempts=1) == {
        "managed_consumer": MANAGED_NAME,
        "removed_consumers": [OBSOLETE_NAME, PREVIOUS_NAME],
    }
    assert client.deleted == [OBSOLETE_NAME, PREVIOUS_NAME]


@pytest.mark.anyio
@pytest.mark.parametrize("idle", [0, 120000])
async def test_reconcile_refuses_an_active_previous_uuid_instance(idle):
    client = FakeRedis([[consumer(MANAGED_NAME), consumer(PREVIOUS_NAME, idle=idle)]])

    with pytest.raises(VisionConsumerCutoverError, match="exactly one"):
        await reconcile_vision_consumers(client, wait_attempts=1)
    assert client.deleted == []


@pytest.mark.anyio
@pytest.mark.parametrize("records", [[], [consumer(OBSOLETE_NAME)], [consumer(MANAGED_NAME, idle=120001)]])
async def test_reconcile_refuses_missing_active_current_worker(records):
    client = FakeRedis([records])

    with pytest.raises(VisionConsumerCutoverError, match="exactly one"):
        await reconcile_vision_consumers(client, wait_attempts=1)
    assert client.deleted == []


@pytest.mark.anyio
@pytest.mark.parametrize("other_name", [OBSOLETE_NAME, PREVIOUS_NAME])
async def test_reconcile_waits_until_other_consumers_are_strictly_stale(monkeypatch, other_name):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(cutover.asyncio, "sleep", fake_sleep)
    managed = consumer(MANAGED_NAME)
    client = FakeRedis([
        [managed, consumer(other_name, idle=119999)],
        [managed, consumer(other_name, idle=120000)],
        [managed, consumer(other_name, idle=120001)],
        [managed],
    ])

    result = await reconcile_vision_consumers(client, wait_attempts=3)

    assert result["removed_consumers"] == [other_name]
    assert sleeps == [1.0, 1.0]
    assert client.reads == 4


@pytest.mark.anyio
async def test_reconcile_never_deletes_an_active_unmanaged_consumer(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(cutover.asyncio, "sleep", fake_sleep)
    client = FakeRedis([[consumer(MANAGED_NAME), consumer(OBSOLETE_NAME, idle=120000)]])

    with pytest.raises(VisionConsumerCutoverError, match="active"):
        await reconcile_vision_consumers(client, wait_attempts=3)
    assert client.deleted == []
    assert client.reads == 3
    assert sleeps == [1.0, 1.0]


@pytest.mark.anyio
@pytest.mark.parametrize("via_cli", [False, True])
async def test_default_wait_allows_180_attempts_at_one_second(monkeypatch, tmp_path, via_cli):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(cutover.asyncio, "sleep", fake_sleep)
    managed = consumer(MANAGED_NAME)
    waiting = [managed, consumer(OBSOLETE_NAME, idle=120000)]
    ready = [managed, consumer(OBSOLETE_NAME, idle=120001)]
    client = FakeRedis([waiting] * 179 + [ready, [managed]])

    if via_cli:
        configure_secret_files(monkeypatch, tmp_path)
        monkeypatch.delenv("VISION_CUTOVER_WAIT_ATTEMPTS", raising=False)
        monkeypatch.setattr(cutover.redis, "from_url", lambda *_args, **_kwargs: client)
        assert await cutover.run() == 0
    else:
        result = await reconcile_vision_consumers(client)
        assert result["managed_consumer"] == MANAGED_NAME
    assert client.deleted == [OBSOLETE_NAME]
    assert client.reads == 181
    assert sleeps == [1.0] * 179


@pytest.mark.anyio
@pytest.mark.parametrize("name", [MANAGED_NAME, PREVIOUS_NAME, OBSOLETE_NAME])
async def test_reconcile_refuses_pending_work_that_appears_while_waiting(monkeypatch, name):
    async def fake_sleep(_delay):
        pass

    monkeypatch.setattr(cutover.asyncio, "sleep", fake_sleep)
    managed = consumer(MANAGED_NAME)
    client = FakeRedis([
        [managed, consumer(OBSOLETE_NAME)],
        [consumer(name, pending=1, idle=120001), managed],
    ])

    with pytest.raises(VisionConsumerCutoverError, match="pending"):
        await reconcile_vision_consumers(client, wait_attempts=2)
    assert client.deleted == []


@pytest.mark.anyio
@pytest.mark.parametrize("final_records", [
    [],
    [consumer(PREVIOUS_NAME)],
    [consumer(MANAGED_NAME, pending=1)],
    [consumer(MANAGED_NAME, idle=120001)],
    [consumer(MANAGED_NAME), consumer(OBSOLETE_NAME, idle=120001)],
])
async def test_final_read_must_be_exactly_the_active_retained_consumer(final_records):
    client = FakeRedis([[consumer(MANAGED_NAME)], final_records])

    with pytest.raises(VisionConsumerCutoverError, match="converge"):
        await reconcile_vision_consumers(client, wait_attempts=1)


@pytest.mark.anyio
async def test_cli_rejects_redis_url_environment_only(monkeypatch, capsys):
    monkeypatch.setenv("REDIS_URL", REDIS_SECRET)
    monkeypatch.delenv("VISION_CUTOVER_REDIS_URL_FILE", raising=False)
    monkeypatch.setattr(
        cutover.redis,
        "from_url",
        lambda *_args, **_kwargs: FakeCliRedis(
            [consumer(MANAGED_NAME)]
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
            FakeCliRedis([consumer(MANAGED_NAME)]),
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
            [consumer(MANAGED_NAME)]
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
    managed = consumer(MANAGED_NAME)
    client = FakeRedis([[consumer("vision-worker@legacy:1", idle=120001), managed], [managed]])
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
@pytest.mark.parametrize("race", [
    "none",
    "pending-legacy",
    "pending-previous",
    "pending-current",
    "active-legacy",
    "active-previous",
    "active-slot-only",
    "missing-current",
    "replaced-current",
    "stale-current",
    "new-active-current",
    "noncanonical-current",
])
async def test_reconcile_real_redis_atomic_stale_cleanup_and_races(monkeypatch, race):
    redis_url = os.environ.get("VISION_CUTOVER_REDIS_TEST_URL")
    if not redis_url:
        pytest.skip("VISION_CUTOVER_REDIS_TEST_URL is not configured")

    stream = f"vp:test:vision-cutover:{uuid.uuid4()}"
    group = "vision-workers"
    legacy_name = "vision-worker@legacy-race:1"
    stale_names = [legacy_name, OBSOLETE_NAME, PREVIOUS_NAME]
    primary = redis.from_url(redis_url, decode_responses=True)
    racing = redis.from_url(redis_url, decode_responses=True)

    class RacingRedis:
        def __init__(self):
            self.raced = False

        async def xinfo_consumers(self, target_stream: str, target_group: str):
            records = await primary.xinfo_consumers(target_stream, target_group)
            if not self.raced:
                self.raced = True
                if race.startswith("pending-"):
                    name = {
                        "pending-legacy": legacy_name,
                        "pending-previous": PREVIOUS_NAME,
                        "pending-current": MANAGED_NAME,
                    }[race]
                    await racing.xadd(target_stream, {"task": "race"})
                    claimed = await racing.xreadgroup(
                        target_group, name, {target_stream: ">"}, count=1,
                    )
                    assert claimed
                elif race.startswith("active-"):
                    name = {
                        "active-legacy": legacy_name,
                        "active-previous": PREVIOUS_NAME,
                        "active-slot-only": OBSOLETE_NAME,
                    }[race]
                    # Redis >= 7.2 resets idle even when no work was returned.
                    assert await racing.xreadgroup(
                        target_group, name, {target_stream: ">"}, count=1,
                    ) == []
                elif race in {"missing-current", "replaced-current", "noncanonical-current"}:
                    await racing.xgroup_delconsumer(target_stream, target_group, MANAGED_NAME)
                    if race != "missing-current":
                        name = PREVIOUS_NAME if race == "replaced-current" else MANAGED_NAME.upper()
                        await racing.xreadgroup(target_group, name, {target_stream: ">"})
                elif race == "stale-current":
                    await cutover.asyncio.sleep(1.1)
                elif race == "new-active-current":
                    await racing.xgroup_createconsumer(target_stream, target_group, SECOND_MANAGED_NAME)
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
    # Scale only integration-test time; unit tests exercise the real 120000ms boundary.
    monkeypatch.setattr(cutover, "CONSUMER_ACTIVE_IDLE_MS", 1000, raising=False)
    try:
        await primary.xadd(stream, {"task": "seed"})
        await primary.xgroup_create(stream, group, id="$")
        for name in stale_names:
            await primary.xgroup_createconsumer(stream, group, name)
        await cutover.asyncio.sleep(1.1)
        await primary.xgroup_createconsumer(stream, group, MANAGED_NAME)

        if race == "none":
            result = await reconcile_vision_consumers(RacingRedis(), wait_attempts=1)
            assert result == {
                "managed_consumer": MANAGED_NAME,
                "removed_consumers": sorted(stale_names),
            }
        else:
            match = "pending" if race.startswith("pending-") else "active|exactly one|changed"
            with pytest.raises(VisionConsumerCutoverError, match=match):
                await reconcile_vision_consumers(RacingRedis(), wait_attempts=1)

        consumers = await primary.xinfo_consumers(stream, group)
        names = {row["name"] for row in consumers}
        if race == "none":
            assert names == {MANAGED_NAME}
            assert consumers[0]["pending"] == 0
        else:
            assert set(stale_names) <= names
            if race.startswith("pending-"):
                assert sum(row["pending"] for row in consumers) == 1
    finally:
        await primary.delete(stream)
        await primary.aclose()
        await racing.aclose()
