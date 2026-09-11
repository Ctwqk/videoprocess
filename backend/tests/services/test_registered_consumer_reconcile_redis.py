"""Parent-only Redis >=7.2 qualification; two natural 120.1s aging phases.

REGISTERED_RECONCILE_DISPOSABLE_REDIS_URL must name loopback/nondefault port/DB15;
REGISTERED_RECONCILE_DISPOSABLE_REDIS_CONFIRM must be the literal
registered-consumer-reconcile-disposable-db15. Only empty fixed keys are seeded.
Admin fixture calls restore/delete only its own test keys and ephemeral ACL users.
The control client uses production no-retry settings and actual restricted ACLs.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.services import registered_consumer_reconcile as pure
from app.services import registered_consumer_reconcile_runtime as runtime
from tests.services.test_registered_consumer_reconcile import (
    NOW,
    assess,
    decode,
    document,
    facts,
)
from tests.services.test_registered_consumer_reconcile_runtime import (
    Authority,
    Database,
    invocation,
)


REAL_OBSERVE = runtime.observe_redis
REAL_CREATE_REDIS = runtime.create_redis
CONFIRMATION = "registered-consumer-reconcile-disposable-db15"


def group(command):
    return f"{command.worker.current.worker_type}-workers"


def unknown_lag_fixture_ids(last_delivered_id):
    milliseconds, sequence = map(int, last_delivered_id.split("-"))
    return f"{milliseconds}-{sequence + 1}", f"{milliseconds}-{sequence + 2}"


@pytest.mark.parametrize(
    "previous,expected",
    [
        ("1789099999999-0", ("1789099999999-1", "1789099999999-2")),
        ("1789099999999-7", ("1789099999999-8", "1789099999999-9")),
        ("9007199254740993-41", ("9007199254740993-42", "9007199254740993-43")),
    ],
)
def test_unknown_lag_fixture_ids_leave_an_interior_gap(previous, expected):
    interior, appended = unknown_lag_fixture_ids(previous)
    assert (interior, appended) == expected
    assert (
        tuple(map(int, previous.split("-")))
        < tuple(map(int, interior.split("-")))
        < tuple(map(int, appended.split("-")))
    )


def checked_url(raw, confirmation):
    try:
        url = urlsplit(raw)
        valid = (
            url.scheme == "redis"
            and url.hostname in {"127.0.0.1", "::1"}
            and url.port is not None
            and 1024 <= url.port <= 65535
            and url.port != 6379
            and url.path == "/15"
            and not url.query
            and not url.fragment
            and confirmation == CONFIRMATION
        )
    except Exception:
        valid = False
    if not valid:
        raise ValueError("explicit disposable Redis URL/confirmation required")
    return raw


def user_url(raw, user, password):
    url = urlsplit(raw)
    host = f"[{url.hostname}]" if ":" in url.hostname else url.hostname
    return urlunsplit(url._replace(netloc=f"{user}:{password}@{host}:{url.port}"))


class Case:
    def __init__(self, admin, control, watcher, credentials, username):
        self.admin, self.control, self.watcher = admin, control, watcher
        self.credentials, self.username = credentials, username
        self.commands = assess().commands

    async def names(self, command):
        return {
            row["name"]
            for row in await self.admin.xinfo_consumers(command.stream, group(command))
        }

    async def refresh(self):
        for worker in decode().workers:
            current = worker.current
            await self.admin.xreadgroup(
                f"{current.worker_type}-workers",
                current.redis_consumer_id,
                {f"vp:tasks:{current.worker_type}": ">"},
                count=1,
            )

    async def drain_fixture_message(self, command, message):
        entries = await self.admin.xreadgroup(
            group(command), command.current, {command.stream: ">"}, count=10
        )
        assert entries and message in {item[0] for item in entries[0][1]}
        assert await self.admin.xack(command.stream, group(command), message) == 1


@pytest.fixture
def redis_case():
    raw = os.environ.get("REGISTERED_RECONCILE_DISPOSABLE_REDIS_URL")
    if not raw:
        pytest.skip("parent-only disposable Redis not configured")
    url = checked_url(
        raw, os.environ.get("REGISTERED_RECONCILE_DISPOSABLE_REDIS_CONFIRM")
    )

    @asynccontextmanager
    async def opened():
        admin = Redis.from_url(
            url, decode_responses=True, socket_connect_timeout=2, socket_timeout=2
        )
        keys, users, clients = [], [], []
        try:
            version = (await admin.info("server"))["redis_version"]
            assert tuple(int(part) for part in version.split(".")[:2]) >= (7, 2)
            fixed = [stream for stream, _, _ in runtime.GROUPS]
            assert await admin.exists(*fixed) == 0, (
                "fixed disposable keys must initially be absent"
            )
            for stream, group, _ in runtime.GROUPS:
                await admin.xgroup_create(
                    stream, group, id="0-0", mkstream=True, entries_read=0
                )
                keys.append(stream)
            for worker in decode().workers:
                current = worker.current
                stream, group = (
                    f"vp:tasks:{current.worker_type}",
                    f"{current.worker_type}-workers",
                )
                await admin.xgroup_createconsumer(
                    stream, group, current.redis_consumer_id
                )
                if current.worker_type != "vision":
                    await admin.xgroup_createconsumer(
                        stream, group, worker.predecessor.redis_consumer_id
                    )
            await admin.xgroup_createconsumer(
                "vp:events", "orchestrator", "rcr-test-orchestrator"
            )
            passwords = [uuid4().hex, uuid4().hex]
            names = ["rcr_control_" + uuid4().hex, "rcr_watch_" + uuid4().hex]
            readonly = "(+xinfo +xpending " + " ".join("~" + key for key in fixed) + ")"
            mutation = (
                "(+eval +xinfo +xpending +xgroup|delconsumer "
                + " ".join("~" + key for key in fixed[:3])
                + ")"
            )
            for index, (name, password) in enumerate(
                zip(names, passwords, strict=True)
            ):
                assert await admin.acl_getuser(name) is None
                permissions = [
                    "reset",
                    "on",
                    ">" + password,
                    "-@all",
                    "resetkeys",
                    "resetchannels",
                    "+select",
                    "+acl|whoami",
                    "+info",
                    readonly,
                ]
                if index == 0:
                    permissions.append(mutation)
                await admin.execute_command("ACL", "SETUSER", name, *permissions)
                users.append(name)
                credentials = runtime.Credentials(
                    "unused", user_url(url, name, password), "unused", "unused"
                )
                clients.append(REAL_CREATE_REDIS(credentials))
            credentials = runtime.Credentials(
                "unused", user_url(url, names[0], passwords[0]), "unused", "unused"
            )
            case = Case(admin, *clients, credentials, names[0])
            yield case
        finally:
            for client in clients:
                await client.aclose(close_connection_pool=True)
            for name in users:
                await admin.acl_deluser(name)
            if keys:
                await admin.delete(*keys)
            await admin.aclose()

    return opened


@pytest.mark.asyncio
async def test_actual_lua_success_atomic_races_and_restricted_acls(redis_case):
    async with redis_case() as case:
        cpu, gpu, publisher = case.commands
        assert await case.control.acl_whoami() == case.username
        assert await case.watcher.xpending(cpu.stream, group(cpu)) == {
            "pending": 0,
            "min": None,
            "max": None,
            "consumers": [],
        }
        for arguments in (
            cpu.arguments,
            ("XGROUP", "DELCONSUMER", cpu.stream, group(cpu), cpu.predecessor),
            ("XACK", cpu.stream, group(cpu), "0-1"),
        ):
            with pytest.raises(ResponseError, match="permission|NOPERM|permissions"):
                await case.watcher.execute_command(*arguments)
        for arguments in (
            ("XACK", cpu.stream, group(cpu), "0-1"),
            ("XCLAIM", cpu.stream, group(cpu), cpu.current, 0, "0-1"),
            ("XAUTOCLAIM", cpu.stream, group(cpu), cpu.current, 0, "0-0"),
            ("XDEL", cpu.stream, "0-1"),
            ("DEL", cpu.stream),
            ("XGROUP", "SETID", cpu.stream, group(cpu), "0-0"),
            ("XGROUP", "DELCONSUMER", "vp:tasks:vision", "vision-workers", "foreign"),
            ("XINFO", "GROUPS", "outside:scope"),
        ):
            with pytest.raises(ResponseError, match="permission|NOPERM|permissions"):
                await case.control.execute_command(*arguments)

        # No threshold override: both pinned names really age on the server.
        await asyncio.sleep(120.1)
        with pytest.raises(ResponseError, match="inventory_changed"):
            await case.control.execute_command(*cpu.arguments)  # stale current
        await case.refresh()
        observation = await REAL_OBSERVE(case.control, decode())
        assert (
            pure.assess(
                decode(), *facts(document()), observation.inventories, now=NOW
            ).outcome
            == "ready"
        )

        message = await case.admin.xadd(cpu.stream, {"fixture": "lag-only"})
        assert (await case.admin.xpending(cpu.stream, group(cpu)))["pending"] == 0
        assert (await case.admin.xinfo_groups(cpu.stream))[0]["lag"] == 1
        with pytest.raises(ResponseError, match="backlog"):
            await case.control.execute_command(*cpu.arguments)
        assert cpu.predecessor in await case.names(cpu)
        await case.drain_fixture_message(cpu, message)

        message = await case.admin.xadd(cpu.stream, {"fixture": "pending-race"})
        await case.admin.xreadgroup(group(cpu), cpu.current, {cpu.stream: ">"}, count=1)
        assert (await case.admin.xpending(cpu.stream, group(cpu)))["pending"] == 1
        with pytest.raises(ResponseError, match="backlog"):
            await case.control.execute_command(*cpu.arguments)
        assert await case.admin.xack(cpu.stream, group(cpu), message) == 1

        # Before-first IDs permit inferred lag. Leave an absent interior ID
        # between the last ACKed entry and one new fixture-owned entry instead.
        groups = await case.admin.xinfo_groups(cpu.stream)
        interior, appended = unknown_lag_fixture_ids(groups[0]["last-delivered-id"])
        assert (
            await case.admin.xadd(
                cpu.stream, {"fixture": "unknown-lag-gap"}, id=appended
            )
            == appended
        )
        assert await case.admin.xrange(cpu.stream, min=interior, max=interior) == []
        await case.admin.xgroup_setid(cpu.stream, group(cpu), interior)
        assert (await case.admin.xpending(cpu.stream, group(cpu)))["pending"] == 0
        assert (await case.admin.xinfo_groups(cpu.stream))[0]["lag"] is None
        with pytest.raises(ResponseError, match="backlog"):
            await case.control.execute_command(*cpu.arguments)
        assert cpu.predecessor in await case.names(cpu)
        await case.admin.xgroup_setid(
            cpu.stream,
            group(cpu),
            groups[0]["last-delivered-id"],
            entries_read=groups[0]["entries-read"],
        )
        await case.drain_fixture_message(cpu, appended)
        assert (await case.admin.xpending(cpu.stream, group(cpu)))["pending"] == 0
        assert (await case.admin.xinfo_groups(cpu.stream))[0]["lag"] == 0

        await case.admin.xgroup_createconsumer(cpu.stream, group(cpu), "unknown-worker")
        with pytest.raises(ResponseError, match="inventory_changed"):
            await case.control.execute_command(*cpu.arguments)
        await case.admin.xgroup_delconsumer(cpu.stream, group(cpu), "unknown-worker")
        await case.admin.xgroup_delconsumer(cpu.stream, group(cpu), cpu.current)
        with pytest.raises(ResponseError, match="inventory_changed"):
            await case.control.execute_command(*cpu.arguments)
        await case.admin.xgroup_createconsumer(
            cpu.stream, group(cpu), "replacement-current"
        )
        with pytest.raises(ResponseError, match="inventory_changed"):
            await case.control.execute_command(*cpu.arguments)
        await case.admin.xgroup_delconsumer(
            cpu.stream, group(cpu), "replacement-current"
        )
        await case.admin.xgroup_createconsumer(cpu.stream, group(cpu), cpu.current)
        await case.refresh()

        for command in (cpu, gpu, publisher):
            assert (
                pure.validate_lua_result(
                    command, await case.control.execute_command(*command.arguments)
                )
                == "retired"
            )
            assert await case.names(command) == {command.current}
            assert (
                pure.validate_lua_result(
                    command, await case.control.execute_command(*command.arguments)
                )
                == "already_absent"
            )
        final = await REAL_OBSERVE(case.control, decode())
        assert final.passive_members == observation.passive_members
        assert (
            pure.assess(
                decode(),
                *facts(document()),
                final.inventories,
                now=NOW,
                replay_only=True,
            ).outcome
            == "already_absent"
        )

        # A fresh old-name metadata row is not silently retired or time-warped.
        await case.admin.xgroup_createconsumer(cpu.stream, group(cpu), cpu.predecessor)
        with pytest.raises(ResponseError, match="inventory_changed"):
            await case.control.execute_command(*cpu.arguments)


@pytest.mark.asyncio
async def test_real_redis_runtime_partial_failure_no_retry_or_replay(
    redis_case, monkeypatch
):
    """Real Redis transport/EVAL; DB/Unit3 authority are explicitly offline fakes."""
    events = []
    db = Database(events)

    async def connect(credentials):
        return db

    async def guard(connection, request):
        assert connection is db and db.locked
        return runtime.GuardFacts(NOW, *facts(document()))

    principal = runtime.role_names_for_generation("rcr-unit2").versioned["operator"]
    monkeypatch.setattr(
        runtime,
        "load_credentials",
        lambda request: runtime.Credentials(
            "unused", "unused", "videoprocess", principal
        ),
    )
    monkeypatch.setattr(runtime, "connect_database", connect)
    monkeypatch.setattr(runtime, "read_guard", guard)
    async with redis_case() as case:
        await asyncio.sleep(120.1)
        await case.refresh()
        calls = []

        class TrackedRedis:
            def __init__(self, client):
                self.client = client

            def __getattr__(self, name):
                return getattr(self.client, name)

            async def execute_command(self, *arguments):
                assert db.locked
                calls.append(arguments[3])
                return await self.client.execute_command(*arguments)

        class RaceAuthority(Authority):
            message = None

            async def before_eval(self, request, command):
                await super().before_eval(request, command)
                if command.stream == case.commands[1].stream:
                    self.message = await case.admin.xadd(
                        command.stream, {"fixture": "post-observation-lag"}
                    )

        authority = RaceAuthority(events)
        monkeypatch.setattr(
            runtime, "create_redis", lambda credentials: TrackedRedis(case.control)
        )
        monkeypatch.setattr(runtime, "observe_redis", REAL_OBSERVE)
        request = replace(invocation(), redis_username=case.username)
        with pytest.raises(runtime.ReconcileRuntimeError):
            await runtime.reconcile_registered_consumers(request, authority)
        assert calls == [command.stream for command in case.commands[:2]]
        assert db.closed and not db.locked
        assert await case.names(case.commands[0]) == {case.commands[0].current}
        for command in case.commands[1:]:
            assert command.predecessor in await case.names(command)
        assert ("result", case.commands[1].stream, "unknown") in events
        await case.drain_fixture_message(case.commands[1], authority.message)
        replay_client = REAL_CREATE_REDIS(case.credentials)
        monkeypatch.setattr(
            runtime, "create_redis", lambda credentials: TrackedRedis(replay_client)
        )
        with pytest.raises(runtime.ReconcileRuntimeError):
            await runtime.reconcile_registered_consumers(
                replace(request, replay_only=True), authority
            )
        assert calls == [command.stream for command in case.commands[:2]]
