from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from uuid import UUID

import pytest

from app.services import registered_consumer_reconcile_runtime as runtime
from app.services.worker_control_role_cli import role_names_for_generation
from tests.services.test_registered_consumer_reconcile import (
    NOW,
    decode,
    document,
    facts,
    identity,
    inventories,
)


def invocation(*, replay_only=False):
    return runtime.Invocation(
        pins=decode(),
        attempt_id=UUID(int=900),
        replay_only=replay_only,
        control_generation="rcr-unit2",
        redis_generation="runtime-unit2",
        redis_username="vp-control-test",
        database_secret_id="a" * 25,
        redis_secret_id="b" * 25,
        database_secret_sha256="c" * 64,
        redis_secret_sha256="d" * 64,
    )


def native_record(values):
    from asyncpg.protocol.protocol import _create_record

    return _create_record(
        {name: i for i, name in enumerate(values)}, tuple(values.values())
    )


def test_guard_decodes_actual_asyncpg_records_offline():
    rows = [native_record(row) for row in guard_rows()]
    state = runtime.decode_guard(rows, invocation().pins)
    assert len(state.registrations) == 8


def test_parent_pg_fixture_has_complete_constraint_valid_seed_offline():
    from sqlalchemy import create_engine, insert

    from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
    from tests.migrations.test_registered_consumer_reconcile_postgres import (
        row_values,
        seed_rows,
    )

    payload, registrations, grants = seed_rows(NOW, "offline-schema-shape")
    assert len({row.database_principal for row in grants}) == 8
    assert len({row.token_sha256 for row in grants}) == 8
    assert len({row.lease_secret_sha256 for row in registrations}) == 8
    for row in [*registrations, *grants]:
        for column in row.__table__.columns:
            assert column.nullable or row_values(row)[column.name] is not None
    runtime.validate_database(decode(payload), registrations, grants, now=NOW)
    engine = create_engine("sqlite://")
    try:
        WorkerAdmissionGrant.__table__.create(engine)
        WorkerRegistration.__table__.create(engine)
        with engine.begin() as connection:
            connection.execute(
                insert(WorkerAdmissionGrant), [row_values(row) for row in grants]
            )
            for row in sorted(
                registrations, key=lambda row: row.superseded_by is not None
            ):
                connection.execute(insert(WorkerRegistration), row_values(row))
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "raw,confirmation",
    [
        (
            "postgresql://owner:synthetic@10.0.0.150:55465/vp_registered_reconcile_test_offline",
            "vp_registered_reconcile_test_offline",
        ),
        (
            "postgresql://owner:synthetic@127.0.0.1:5432/vp_registered_reconcile_test_offline",
            "vp_registered_reconcile_test_offline",
        ),
        ("postgresql://owner:synthetic@127.0.0.1:55465/videoprocess", "videoprocess"),
        (
            "postgresql://owner:synthetic@127.0.0.1:55465/vp_registered_reconcile_test_offline",
            "wrong",
        ),
        (
            "postgresql://owner:synthetic@127.0.0.1:55465/vp_registered_reconcile_test_offline?host=other",
            "vp_registered_reconcile_test_offline",
        ),
    ],
)
def test_parent_pg_fixture_refuses_unsafe_or_unconfirmed_urls(raw, confirmation):
    from tests.migrations.test_registered_consumer_reconcile_postgres import checked_url

    with pytest.raises(ValueError):
        checked_url(raw, confirmation)


@pytest.mark.parametrize(
    "raw",
    [
        "redis://10.0.0.150:55464/15",
        "redis://127.0.0.1:6379/15",
        "redis://127.0.0.1:55464/0",
        "redis://127.0.0.1:55464/15?db=0",
    ],
)
def test_parent_redis_fixture_refuses_non_disposable_url(raw):
    from tests.services.test_registered_consumer_reconcile_redis import (
        CONFIRMATION,
        checked_url,
    )

    with pytest.raises(ValueError):
        checked_url(raw, CONFIRMATION)


def test_parent_fixture_explicit_url_confirmation_and_command_shapes():
    from tests.migrations.test_registered_consumer_reconcile_postgres import (
        checked_url as pg_url,
    )
    from tests.services.test_registered_consumer_reconcile_redis import (
        CONFIRMATION,
        checked_url,
        group,
    )
    from tests.services.test_registered_consumer_reconcile import assess

    raw = "postgresql://owner:synthetic@127.0.0.1:55465/vp_registered_reconcile_test_offline"
    assert pg_url(raw, "vp_registered_reconcile_test_offline") == raw
    assert checked_url("redis://127.0.0.1:55464/15", CONFIRMATION).endswith("/15")
    with pytest.raises(ValueError):
        checked_url("redis://127.0.0.1:55464/15", "wrong")
    for command in assess().commands:
        assert group(command) == f"{command.worker.current.worker_type}-workers"
        assert isinstance(command.current, str) and isinstance(command.predecessor, str)


@pytest.mark.asyncio
async def test_actual_asyncpg_identity_record_is_accepted(boundaries, monkeypatch):
    db, _, events, _ = boundaries
    original = db.fetchrow

    async def fetchrow(query):
        return native_record(await original(query))

    monkeypatch.setattr(db, "fetchrow", fetchrow)
    result = await runtime.reconcile_registered_consumers(
        invocation(), Authority(events)
    )
    assert result.outcome == "reconciled"


class Authority:
    def __init__(self, events):
        self.events = events

    async def revalidate(self, request):
        self.events.append("authority")

    async def before_eval(self, request, command):
        self.events.append(("intent", command.stream))

    async def after_eval(self, request, command, outcome):
        self.events.append(("result", command.stream, outcome))


class Transaction:
    def __init__(self, db):
        self.db = db

    async def start(self):
        assert not self.db.locked
        self.db.events.append("begin")
        self.db.locked = True

    async def rollback(self):
        self.db.events.append("rollback")
        self.db.locked = False


class Database:
    def __init__(self, events):
        self.events = events
        self.locked = False
        self.closed = False

    def transaction(self):
        return Transaction(self)

    async def fetchrow(self, query):
        principal = role_names_for_generation("rcr-unit2").versioned["operator"]
        return {
            "session_user": principal,
            "current_user": principal,
            "database_name": "videoprocess",
        }

    async def close(self, *, timeout):
        assert not self.locked
        self.events.append("db-close")
        self.closed = True

    def terminate(self):
        self.events.append("db-terminate")
        self.locked = False
        self.closed = True


class Redis:
    def __init__(self, db, events):
        self.db, self.events = db, events
        self.inventory = inventories(document())
        self.calls = []
        self.fail_on = None
        self.closed = False

    async def acl_whoami(self):
        return "vp-control-test"

    async def info(self, section):
        assert section == "server"
        return {"redis_version": "7.4.7"}

    async def execute_command(self, *arguments):
        assert self.db.locked, "EVAL must retain the guard transaction"
        assert arguments[0] == "EVAL" and arguments[2] == 1
        stream, current, old = arguments[3:]
        assert ("intent", stream) in self.events
        self.calls.append(stream)
        self.events.append(("eval", stream))
        if stream == self.fail_on:
            raise TimeoutError("secret driver message")
        for item in self.inventory.values():
            if item["stream"] == stream:
                item["consumers"] = [
                    row for row in item["consumers"] if row["name"] != old
                ]
        return ["retired", current, old]

    async def aclose(self, *, close_connection_pool):
        assert close_connection_pool is True
        self.events.append("redis-close")
        self.closed = True


@pytest.fixture
def boundaries(monkeypatch):
    events = []
    db = Database(events)
    redis = Redis(db, events)
    rows = facts(document())

    monkeypatch.setattr(
        runtime,
        "load_credentials",
        lambda request: runtime.Credentials(
            database_url="postgresql://synthetic",
            redis_url="redis://synthetic",
            database_name="videoprocess",
            database_principal=role_names_for_generation("rcr-unit2").versioned[
                "operator"
            ],
        ),
    )

    async def connect(credentials):
        return db

    async def guard(connection, request):
        assert connection is db and db.locked
        events.append("guard")
        return runtime.GuardFacts(NOW, *rows)

    async def observe(client, pins):
        assert db.locked
        events.append("observe")
        return runtime.Observation(redis.inventory, (("vision",), ("orchestrator",)))

    monkeypatch.setattr(runtime, "connect_database", connect)
    monkeypatch.setattr(runtime, "create_redis", lambda credentials: redis)
    monkeypatch.setattr(runtime, "read_guard", guard)
    monkeypatch.setattr(runtime, "observe_redis", observe)
    return db, redis, events, rows


@pytest.mark.asyncio
async def test_runner_holds_guard_through_each_single_eval_and_final_observation(
    boundaries,
):
    db, redis, events, _ = boundaries
    result = await runtime.reconcile_registered_consumers(
        invocation(), Authority(events)
    )
    assert result.outcome == "reconciled"
    assert redis.calls == [
        "vp:tasks:ffmpeg_go",
        "vp:tasks:ffmpeg",
        "vp:tasks:youtube_publisher",
    ]
    assert events.index("rollback") > max(
        i
        for i, event in enumerate(events)
        if isinstance(event, tuple) and event[0] == "eval"
    )
    assert events[-2:] == ["redis-close", "db-close"]
    assert db.closed and redis.closed


@pytest.mark.asyncio
async def test_final_database_clock_is_fresh_after_last_redis_observation(boundaries):
    _, _, events, _ = boundaries
    await runtime.reconcile_registered_consumers(invocation(), Authority(events))
    last_guard = max(i for i, event in enumerate(events) if event == "guard")
    last_observe = max(i for i, event in enumerate(events) if event == "observe")
    last_authority = max(i for i, event in enumerate(events) if event == "authority")
    assert last_guard > last_observe > last_authority


@pytest.mark.asyncio
@pytest.mark.parametrize("replay", [False, True])
async def test_all_absent_is_read_only_and_never_consumes_an_attempt(
    boundaries, replay
):
    _, redis, events, _ = boundaries
    for item in redis.inventory.values():
        item["consumers"] = item["consumers"][:1]
    result = await runtime.reconcile_registered_consumers(
        invocation(replay_only=replay), Authority(events)
    )
    assert result.outcome == "already_absent"
    assert redis.calls == []
    assert not any(
        isinstance(event, tuple) and event[0] == "intent" for event in events
    )


@pytest.mark.asyncio
async def test_wait_releases_row_locks_and_rechecks_fresh_facts(
    boundaries, monkeypatch
):
    db, redis, events, _ = boundaries
    redis.inventory[next(iter(redis.inventory))]["consumers"][1]["idle"] = 120000

    async def wait(delay):
        assert not db.locked
        events.append("natural-wait")
        for item in redis.inventory.values():
            if len(item["consumers"]) == 2:
                item["consumers"][1]["idle"] = 120001

    monkeypatch.setattr(runtime, "wait_for_aging", wait)
    await runtime.reconcile_registered_consumers(invocation(), Authority(events))
    assert events.count("begin") == 2
    assert events[events.index("natural-wait") - 1] == "rollback"
    assert events.count("guard") >= 3


@pytest.mark.asyncio
async def test_uncertain_second_eval_stops_without_replay_and_closes(boundaries):
    db, redis, events, _ = boundaries
    redis.fail_on = "vp:tasks:ffmpeg"
    with pytest.raises(runtime.ReconcileRuntimeError) as failure:
        await runtime.reconcile_registered_consumers(invocation(), Authority(events))
    assert "secret" not in str(failure.value)
    assert redis.calls == ["vp:tasks:ffmpeg_go", "vp:tasks:ffmpeg"]
    assert ("result", "vp:tasks:ffmpeg", "unknown") in events
    assert db.closed and redis.closed and not db.locked


@pytest.mark.asyncio
async def test_unknown_journal_error_is_static_and_resources_still_close(boundaries):
    db, redis, events, _ = boundaries
    redis.fail_on = "vp:tasks:ffmpeg_go"

    class FailingJournal(Authority):
        async def after_eval(self, request, command, outcome):
            raise ValueError("sensitive transport details")

    with pytest.raises(runtime.ReconcileRuntimeError) as failure:
        await runtime.reconcile_registered_consumers(
            invocation(), FailingJournal(events)
        )
    assert "sensitive" not in str(failure.value)
    assert db.closed and redis.closed


@pytest.mark.asyncio
async def test_known_result_journal_failure_consumes_no_later_stream(boundaries):
    db, redis, events, _ = boundaries

    class FailingJournal(Authority):
        async def after_eval(self, request, command, outcome):
            self.events.append(("journal-outcome", outcome))
            if outcome != "unknown":
                raise TimeoutError("sensitive journal response")

    with pytest.raises(runtime.ReconcileRuntimeError):
        await runtime.reconcile_registered_consumers(
            invocation(), FailingJournal(events)
        )
    assert redis.calls == ["vp:tasks:ffmpeg_go"]
    assert ("journal-outcome", "unknown") in events
    assert db.closed and redis.closed


@pytest.mark.asyncio
async def test_close_timeout_forces_terminal_transport_and_no_orphan(
    boundaries, monkeypatch
):
    db, redis, events, _ = boundaries
    closed = asyncio.Event()

    async def close(**kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    monkeypatch.setattr(db, "close", close)
    monkeypatch.setattr(runtime, "CLOSE_SECONDS", 0.01)
    before = set(asyncio.all_tasks())
    with pytest.raises(runtime.ReconcileRuntimeError, match="resource_cleanup_failed"):
        await runtime.reconcile_registered_consumers(invocation(), Authority(events))
    assert closed.is_set() and db.closed and redis.closed
    assert set(asyncio.all_tasks()) == before


@pytest.mark.asyncio
async def test_cancellation_settles_current_io_then_rolls_back_without_later_eval(
    boundaries, monkeypatch
):
    db, redis, events, _ = boundaries
    started, settled = asyncio.Event(), asyncio.Event()

    async def blocked(*args):
        redis.calls.append(args[3])
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            settled.set()

    monkeypatch.setattr(redis, "execute_command", blocked)
    task = asyncio.create_task(
        runtime.reconcile_registered_consumers(invocation(), Authority(events))
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert settled.is_set()
    assert redis.calls == ["vp:tasks:ffmpeg_go"]
    assert db.closed and redis.closed and not db.locked


@pytest.mark.asyncio
async def test_changed_passive_membership_refuses_before_next_mutation(
    boundaries, monkeypatch
):
    _, redis, events, _ = boundaries
    calls = 0

    async def observe(client, pins):
        nonlocal calls
        calls += 1
        passive = (
            (("vision",), ("orchestrator",))
            if calls == 1
            else (("vision",), ("foreign",))
        )
        return runtime.Observation(redis.inventory, passive)

    monkeypatch.setattr(runtime, "observe_redis", observe)
    with pytest.raises(runtime.ReconcileRuntimeError):
        await runtime.reconcile_registered_consumers(invocation(), Authority(events))
    assert len(redis.calls) <= 1


@pytest.mark.asyncio
async def test_durable_authority_failure_does_not_dispatch(boundaries):
    _, redis, events, _ = boundaries

    class Refuse(Authority):
        async def before_eval(self, request, command):
            raise ValueError("ambiguous durable intent")

    with pytest.raises(runtime.ReconcileRuntimeError):
        await runtime.reconcile_registered_consumers(invocation(), Refuse(events))
    assert redis.calls == []


@pytest.mark.asyncio
async def test_last_observation_wait_does_not_dispatch_a_stale_ready_decision(
    boundaries, monkeypatch
):
    db, redis, events, _ = boundaries
    calls = 0

    async def observe(client, pins):
        nonlocal calls
        calls += 1
        if calls == 2:
            redis.inventory[next(iter(redis.inventory))]["consumers"][1]["idle"] = 0
        return runtime.Observation(redis.inventory, (("vision",), ("orchestrator",)))

    async def wait(delay):
        assert not db.locked and redis.calls == []
        raise RuntimeError("stop natural wait")

    monkeypatch.setattr(runtime, "observe_redis", observe)
    monkeypatch.setattr(runtime, "wait_for_aging", wait)
    with pytest.raises(runtime.ReconcileRuntimeError):
        await runtime.reconcile_registered_consumers(invocation(), Authority(events))
    assert redis.calls == []


@pytest.mark.asyncio
async def test_unknown_result_is_journaled_after_releasing_row_locks(boundaries):
    db, redis, events, _ = boundaries
    redis.fail_on = "vp:tasks:ffmpeg_go"

    class Checked(Authority):
        async def after_eval(self, request, command, outcome):
            if outcome == "unknown":
                assert not db.locked
            await super().after_eval(request, command, outcome)

    with pytest.raises(runtime.ReconcileRuntimeError):
        await runtime.reconcile_registered_consumers(invocation(), Checked(events))
    assert ("result", "vp:tasks:ffmpeg_go", "unknown") in events


@pytest.mark.asyncio
async def test_total_deadline_cancels_owned_wait_and_closes_without_eval(
    boundaries, monkeypatch
):
    db, redis, events, _ = boundaries
    redis.inventory[next(iter(redis.inventory))]["consumers"][1]["idle"] = 0
    monkeypatch.setattr(runtime, "TOTAL_SECONDS", 0.12)
    monkeypatch.setattr(runtime, "IO_SECONDS", 0.01)
    monkeypatch.setattr(runtime, "ROLLBACK_SECONDS", 0.01)
    monkeypatch.setattr(runtime, "CLOSE_SECONDS", 0.01)
    with pytest.raises(runtime.ReconcileRuntimeError):
        await runtime.reconcile_registered_consumers(invocation(), Authority(events))
    assert db.closed and redis.closed and not db.locked and redis.calls == []


@pytest.mark.asyncio
async def test_repeated_cancel_cannot_release_owner_before_cleanup_finishes(
    boundaries, monkeypatch
):
    db, redis, events, _ = boundaries
    started, release = asyncio.Event(), asyncio.Event()

    async def close(*, close_connection_pool):
        started.set()
        await release.wait()
        redis.closed = True

    monkeypatch.setattr(redis, "aclose", close)
    task = asyncio.create_task(
        runtime.reconcile_registered_consumers(invocation(), Authority(events))
    )
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done() and not db.closed
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert db.closed and redis.closed


@pytest.mark.asyncio
async def test_rollback_timeout_terminates_connection_without_background_child(
    boundaries, monkeypatch
):
    db, redis, events, _ = boundaries
    settled = asyncio.Event()

    async def hang(self):
        try:
            await asyncio.Event().wait()
        finally:
            settled.set()

    monkeypatch.setattr(Transaction, "rollback", hang)
    monkeypatch.setattr(runtime, "ROLLBACK_SECONDS", 0.01)
    with pytest.raises(runtime.ReconcileRuntimeError, match="rollback_failed"):
        await runtime.reconcile_registered_consumers(invocation(), Authority(events))
    assert settled.is_set() and "db-terminate" in events
    assert db.closed and redis.closed and not db.locked


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["6.2.0", "7.0.15", None, "unknown"])
async def test_pre72_or_malformed_redis_version_cannot_mutate(
    boundaries, monkeypatch, version
):
    _, redis, events, _ = boundaries

    async def info(section):
        return {"redis_version": version}

    monkeypatch.setattr(redis, "info", info)
    with pytest.raises(runtime.ReconcileRuntimeError):
        await runtime.reconcile_registered_consumers(invocation(), Authority(events))
    assert redis.calls == []


def guard_rows():
    rows, grants = facts(document())
    by_grant = {grant.id: grant for grant in grants}
    return [
        {
            "observed_at": NOW,
            "registration_id": row.id,
            "grant_id": row.grant_id,
            "worker_instance_id": row.worker_instance_id,
            "superseded_by": row.superseded_by,
            "registered_at": row.registered_at,
            "lease_expires_at": row.lease_expires_at,
            "registration_revoked_at": row.revoked_at,
            "grant_activated_at": by_grant[row.grant_id].activated_at,
            "grant_revoked_at": by_grant[row.grant_id].revoked_at,
            "registration_facts": json.dumps(
                {name: getattr(row, name) for name in runtime._REGISTRATION_FIELDS}
            ),
            "grant_facts": json.dumps(
                {
                    name: getattr(by_grant[row.grant_id], name)
                    for name in runtime._GRANT_FIELDS
                }
            ),
        }
        for row in rows
    ]


def test_guard_decoder_preserves_native_uuid_values_and_omits_secrets():
    from asyncpg.pgproto.pgproto import UUID as NativeUUID

    rows = guard_rows()
    for row in rows:
        for name in (
            "registration_id",
            "grant_id",
            "worker_instance_id",
            "superseded_by",
        ):
            if row[name] is not None:
                row[name] = NativeUUID(str(row[name]))
    decoded = runtime.decode_guard(rows, decode())
    assert type(decoded.registrations[0].id) is NativeUUID
    assert decoded.now == NOW
    assert decoded.grants[0].token_sha256 is None
    assert decoded.registrations[0].lease_secret_sha256 is None


@pytest.mark.parametrize(
    "variant",
    [
        "missing",
        "extra",
        "uuid_string",
        "naive_clock",
        "clock_changed",
        "too_many",
        "grant_generation",
        "secret_field",
    ],
)
def test_guard_facts_refuse_malformed_incomplete_or_changed_data(variant):
    from datetime import timedelta

    rows = guard_rows()
    if variant == "missing":
        del rows[0]["grant_facts"]
    elif variant == "extra":
        rows[0]["extra"] = True
    elif variant == "uuid_string":
        rows[0]["registration_id"] = str(rows[0]["registration_id"])
    elif variant == "naive_clock":
        rows[0]["observed_at"] = NOW.replace(tzinfo=None)
    elif variant == "clock_changed":
        rows[0]["observed_at"] += timedelta(seconds=1)
    elif variant == "too_many":
        rows.append(rows[0])
    else:
        value = json.loads(rows[0]["grant_facts"])
        value["generation" if variant == "grant_generation" else "token_sha256"] = (
            "unexpected"
        )
        rows[0]["grant_facts"] = json.dumps(value)
    with pytest.raises(runtime.ReconcileRuntimeError):
        runtime.decode_guard(rows, decode())


class InventoryClient:
    def __init__(self):
        self.group_rows = {
            stream: [{"name": group, "lag": 0}] for stream, group, _ in runtime.GROUPS
        }
        self.pending = {stream: {"pending": 0} for stream, _, _ in runtime.GROUPS}
        self.consumers = {
            item["stream"]: item["consumers"]
            for item in inventories(document()).values()
        }
        self.consumers["vp:tasks:vision"] = [
            {
                "name": identity(2)["redis_consumer_id"],
                "pending": 0,
                "idle": 0,
                "inactive": -1,
            }
        ]
        self.consumers["vp:events"] = [
            {"name": "orchestrator-1", "pending": 0, "idle": 0}
        ]

    async def xpending(self, stream, group):
        return self.pending[stream]

    async def xinfo_groups(self, stream):
        return self.group_rows[stream]

    async def xinfo_consumers(self, stream, group):
        return self.consumers[stream]


@pytest.mark.asyncio
async def test_five_group_observation_pins_passive_members_not_idle_counters():
    client = InventoryClient()
    first = await runtime.observe_redis(client, decode())
    client.consumers["vp:events"][0]["idle"] += 500
    second = await runtime.observe_redis(client, decode())
    assert first.passive_members == second.passive_members
    assert set(first.inventories) == set(inventories(document()))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stream",
    [
        row[0]
        for row in (
            ("vp:tasks:ffmpeg",),
            ("vp:tasks:ffmpeg_go",),
            ("vp:tasks:youtube_publisher",),
            ("vp:tasks:vision",),
            ("vp:events",),
        )
    ],
)
@pytest.mark.parametrize("bad", [1, None, False, "0"])
async def test_every_group_requires_integer_zero_pending(stream, bad):
    client = InventoryClient()
    client.pending[stream]["pending"] = bad
    with pytest.raises(runtime.ReconcileRuntimeError):
        await runtime.observe_redis(client, decode())


def test_redis_transport_disables_retries_and_caller_options():
    credentials = runtime.Credentials(
        "", "redis://control:synthetic@127.0.0.1:1/15", "", ""
    )
    client = runtime.create_redis(credentials)
    options = client.connection_pool.connection_kwargs
    assert options["retry"].get_retries() == 0
    assert options["retry_on_timeout"] is False
    assert options["retry_on_error"] == []
    assert options["socket_timeout"] == options["socket_connect_timeout"] == 2.0
    assert options["health_check_interval"] == 0
    assert client.single_connection_client is True


@pytest.fixture
def mounted_credentials(tmp_path, monkeypatch):
    from dataclasses import replace
    import os

    request = invocation()
    principal = role_names_for_generation(request.control_generation).versioned[
        "operator"
    ]
    values = {
        "database": f"postgresql://{principal}:synthetic@10.0.0.150:5435/videoprocess\n",
        "redis": "redis://vp-control-test:synthetic@10.0.0.150:6380/0\n",
        "pins": request.pins.canonical_json,
    }
    paths = {name: tmp_path / name for name in values}
    for name, path in paths.items():
        path.write_text(values[name])
        path.chmod(0o400)
    monkeypatch.setattr(runtime, "MOUNTS", paths)
    monkeypatch.setattr(runtime, "MOUNT_UID", os.getuid())
    # macOS temporary directories may supply an inherited group different from
    # the process primary group. Production still requires exactly 10001:10001.
    monkeypatch.setattr(runtime, "MOUNT_GID", paths["database"].stat().st_gid)
    for name in runtime.FORBIDDEN_ENV:
        monkeypatch.delenv(name, raising=False)
    return replace(
        request,
        database_secret_sha256=hashlib.sha256(values["database"].encode()).hexdigest(),
        redis_secret_sha256=hashlib.sha256(values["redis"].encode()).hexdigest(),
    ), paths


def test_exact_secret_bytes_generation_endpoints_and_pin_document(mounted_credentials):
    request, _ = mounted_credentials
    credentials = runtime.load_credentials(request)
    assert (
        credentials.database_principal
        == role_names_for_generation("rcr-unit2").versioned["operator"]
    )
    assert "synthetic" not in repr(credentials)


def test_wrong_secret_owner_refuses_before_read(mounted_credentials, monkeypatch):
    request, paths = mounted_credentials
    monkeypatch.setattr(runtime, "MOUNT_UID", paths["database"].stat().st_uid + 1)
    with pytest.raises(runtime.ReconcileRuntimeError, match="secret_owner_invalid"):
        runtime.load_credentials(request)


@pytest.mark.parametrize(
    "variant",
    ["raw_env", "mode", "digest", "query", "wrong_user", "endpoint", "pins", "symlink"],
)
def test_credential_alternatives_and_changed_mounts_refuse(
    mounted_credentials, monkeypatch, variant
):
    from dataclasses import replace

    request, paths = mounted_credentials
    if variant == "raw_env":
        monkeypatch.setenv("DATABASE_URL", "postgresql://fallback")
    elif variant == "mode":
        paths["database"].chmod(0o600)
    elif variant == "digest":
        request = replace(request, database_secret_sha256="0" * 64)
    elif variant == "pins":
        paths["pins"].chmod(0o600)
        paths["pins"].write_text(json.dumps({}))
        paths["pins"].chmod(0o400)
    elif variant == "symlink":
        other = paths["database"].with_name("other")
        paths["database"].rename(other)
        paths["database"].symlink_to(other)
    else:
        value = paths["database"].read_text().rstrip("\n")
        if variant == "query":
            value += "?options=-crole=postgres"
        elif variant == "wrong_user":
            value = value.replace(
                role_names_for_generation("rcr-unit2").versioned["operator"], "postgres"
            )
        else:
            value = value.replace("10.0.0.150", "10.0.0.151")
        paths["database"].chmod(0o600)
        paths["database"].write_text(value)
        paths["database"].chmod(0o400)
        request = replace(
            request, database_secret_sha256=hashlib.sha256(value.encode()).hexdigest()
        )
    with pytest.raises(runtime.ReconcileRuntimeError):
        runtime.load_credentials(request)


def test_guard_migration_is_the_executable_local_head_and_operator_only():
    import runpy
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    from app.services.worker_control_role_cli import ROLE_FUNCTIONS
    from app.services.worker_deployment_cli import EXPECTED_MIGRATION_HEAD

    root = Path(__file__).parents[2]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    assert (
        ScriptDirectory.from_config(config).get_current_head()
        == "037_registered_consumer_guard"
    )
    assert EXPECTED_MIGRATION_HEAD == "037_registered_consumer_guard"
    migration = runpy.run_path(
        str(root / "alembic/versions/037_registered_consumer_guard.py")
    )
    assert migration["down_revision"] == "036_worker_session_signal"
    signature = "vp_registered_consumer_reconcile_guard(text,uuid[],uuid[])"
    assert signature in ROLE_FUNCTIONS["operator"]
    assert all(
        signature not in functions
        for role, functions in ROLE_FUNCTIONS.items()
        if role != "operator"
    )
