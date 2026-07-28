from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import quote

import pytest
import redis.asyncio as aioredis
from redis.exceptions import NoPermissionError

from app.services import worker_redis_marker_control as control


REDIS_URL = os.environ.get("WORKER_REDIS_MARKER_TEST_URL", "")
CONTAINER = "videoprocess-marker-redis74"


def _payload_hash(fields: dict[str, str]) -> str:
    return hashlib.sha256(
        json.dumps(fields, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _expectation_row(
    expectation: control.MarkerExpectation,
) -> dict[str, object]:
    return {
        "marker_kind": expectation.marker_kind,
        "source_id": expectation.source_id,
        "marker_key": expectation.marker_key,
        "redis_stream": expectation.redis_stream,
        "expected_message_id": expectation.expected_message_id,
        "payload_sha256": expectation.payload_sha256,
        "source_state": expectation.source_state,
        "absence_allowed": expectation.absence_allowed,
    }


def _authorization_row(
    authorization: control.MarkerCleanupAuthorization,
) -> dict[str, object]:
    return {
        "id": authorization.id,
        "marker_kind": authorization.marker_kind,
        "source_id": authorization.source_id,
        "marker_key": authorization.marker_key,
        "redis_stream": authorization.redis_stream,
        "expected_message_id": authorization.expected_message_id,
        "payload_sha256": authorization.payload_sha256,
    }


class _Result:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _Database:
    def __init__(self, expectation: control.MarkerExpectation) -> None:
        self.expectation = expectation
        self.listed = False
        self.observations: list[dict[str, object]] = []
        self.finished: list[dict[str, object]] = []

    @asynccontextmanager
    async def begin(self):
        yield self

    async def scalar(self, statement: object, params: dict[str, object]):
        query = str(statement)
        if "vp_begin_worker_redis_continuity_check" in query:
            return "begun"
        if "vp_record_worker_redis_marker_observation" in query:
            self.observations.append(params)
            return True
        if "vp_finish_worker_redis_continuity_check" in query:
            self.finished.append(params)
            return True
        raise AssertionError(query)

    async def execute(self, statement: object, _params: dict[str, object]):
        query = str(statement)
        if "vp_list_worker_redis_marker_expectations" not in query:
            raise AssertionError(query)
        if self.listed:
            return _Result([])
        self.listed = True
        return _Result([_expectation_row(self.expectation)])


class _JanitorDatabase:
    def __init__(self, authorization: control.MarkerCleanupAuthorization) -> None:
        self.authorization = authorization
        self.finished: list[dict[str, object]] = []

    @asynccontextmanager
    async def begin(self):
        yield self

    async def execute(self, statement: object, _params: dict[str, object]):
        if "vp_claim_worker_redis_marker_cleanup" not in str(statement):
            raise AssertionError(str(statement))
        return _Result([_authorization_row(self.authorization)])

    async def scalar(self, statement: object, params: dict[str, object]):
        if "vp_finish_worker_redis_marker_cleanup" not in str(statement):
            raise AssertionError(str(statement))
        self.finished.append(params)
        return True


@pytest.fixture
async def redis_client() -> AsyncIterator[aioredis.Redis]:
    if not REDIS_URL:
        pytest.skip("set WORKER_REDIS_MARKER_TEST_URL for Redis integration tests")
    client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


def _expectation(*, expected_message_id: str | None, absence_allowed: bool = False) -> control.MarkerExpectation:
    return control.MarkerExpectation(
        marker_kind="event_emission",
        source_id=uuid.uuid4(),
        marker_key=f"vp:worker-event-emission:{uuid.uuid4()}",
        redis_stream="vp:events",
        expected_message_id=expected_message_id,
        payload_sha256=_payload_hash({"event": "node_completed"}),
        source_state="prepared" if expected_message_id is None else "emitted",
        absence_allowed=absence_allowed,
    )


@pytest.mark.asyncio
async def test_exact_active_marker_and_stream_payload_pass(redis_client: aioredis.Redis) -> None:
    expectation = _expectation(expected_message_id="1710000000000-0")
    message_id = await redis_client.xadd(expectation.redis_stream, {"event": "node_completed"}, id=expectation.expected_message_id)
    await redis_client.set(expectation.marker_key, message_id)
    database = _Database(expectation)

    result = await control.check_worker_redis_continuity(database, redis_client, "default")

    assert result.state == "ready"
    assert database.observations == [{"run_id": result.run_id, "marker_kind": "event_emission", "source_id": expectation.source_id, "message_id": message_id, "payload_sha256": expectation.payload_sha256}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("marker_value", "reason"),
    [(None, "active_marker_missing"), ("1710000000001-0", "active_marker_mismatch")],
)
async def test_missing_or_mismatched_active_marker_fails(
    redis_client: aioredis.Redis,
    marker_value: str | None,
    reason: str,
) -> None:
    expectation = _expectation(expected_message_id="1710000000000-0")
    await redis_client.xadd(expectation.redis_stream, {"event": "node_completed"}, id=expectation.expected_message_id)
    if marker_value is not None:
        await redis_client.set(expectation.marker_key, marker_value)

    result = await control.check_worker_redis_continuity(_Database(expectation), redis_client, "default")

    assert result.state == "error"
    assert result.reason_code == reason


@pytest.mark.asyncio
async def test_prepared_without_message_and_marker_is_consistent(redis_client: aioredis.Redis) -> None:
    expectation = _expectation(expected_message_id=None, absence_allowed=True)

    result = await control.check_worker_redis_continuity(_Database(expectation), redis_client, "default")

    assert result.state == "ready"


@pytest.mark.asyncio
async def test_janitor_exact_delete_absent_and_conflict_outcomes(redis_client: aioredis.Redis) -> None:
    authorization = control.MarkerCleanupAuthorization(
        id=uuid.uuid4(),
        marker_kind="event_emission",
        source_id=uuid.uuid4(),
        marker_key=f"vp:worker-event-emission:{uuid.uuid4()}",
        redis_stream="vp:events",
        expected_message_id="1710000000000-0",
        payload_sha256="a" * 64,
    )
    await redis_client.set(authorization.marker_key, authorization.expected_message_id)
    deleted_database = _JanitorDatabase(authorization)
    deleted = await control.run_worker_redis_marker_janitor(deleted_database, redis_client, uuid.uuid4())
    assert deleted == {"claimed": 1, "deleted": 1, "absent": 0, "conflict": 0}
    assert await redis_client.get(authorization.marker_key) is None

    absent_database = _JanitorDatabase(authorization)
    absent = await control.run_worker_redis_marker_janitor(absent_database, redis_client, uuid.uuid4())
    assert absent == {"claimed": 1, "deleted": 0, "absent": 1, "conflict": 0}

    await redis_client.set(authorization.marker_key, "1710000000001-0")
    conflict_database = _JanitorDatabase(authorization)
    conflict = await control.run_worker_redis_marker_janitor(conflict_database, redis_client, uuid.uuid4())
    assert conflict == {"claimed": 1, "deleted": 0, "absent": 0, "conflict": 1}
    assert await redis_client.get(authorization.marker_key) == "1710000000001-0"


@pytest.mark.asyncio
async def test_concurrent_janitors_delete_exact_marker_once(redis_client: aioredis.Redis) -> None:
    authorization = control.MarkerCleanupAuthorization(
        id=uuid.uuid4(),
        marker_kind="event_emission",
        source_id=uuid.uuid4(),
        marker_key=f"vp:worker-event-emission:{uuid.uuid4()}",
        redis_stream="vp:events",
        expected_message_id="1710000000000-0",
        payload_sha256="a" * 64,
    )
    await redis_client.set(authorization.marker_key, authorization.expected_message_id)
    outcomes = await asyncio.gather(
        control.run_worker_redis_marker_janitor(_JanitorDatabase(authorization), redis_client, uuid.uuid4()),
        control.run_worker_redis_marker_janitor(_JanitorDatabase(authorization), redis_client, uuid.uuid4()),
    )

    assert sum(result["deleted"] for result in outcomes) == 1
    assert sum(result["absent"] for result in outcomes) == 1
    assert await redis_client.get(authorization.marker_key) is None


def _restart_local_redis() -> None:
    subprocess.run(
        ["docker", "restart", CONTAINER],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.asyncio
async def test_aof_restart_preserves_marker_and_missing_marker_fails() -> None:
    if not REDIS_URL:
        pytest.skip("set WORKER_REDIS_MARKER_TEST_URL for Redis integration tests")
    first = aioredis.from_url(REDIS_URL, decode_responses=True)
    await first.flushdb()
    expectation = _expectation(expected_message_id="1710000000000-0")
    try:
        await first.xadd(expectation.redis_stream, {"event": "node_completed"}, id=expectation.expected_message_id)
        await first.set(expectation.marker_key, expectation.expected_message_id)
        await first.execute_command("WAITAOF", 1, 0, 1000)
        await first.aclose()
        _restart_local_redis()
        second = aioredis.from_url(REDIS_URL, decode_responses=True)
        try:
            assert (await control.check_worker_redis_continuity(_Database(expectation), second, "default")).state == "ready"
            await second.delete(expectation.marker_key)
            await second.execute_command("WAITAOF", 1, 0, 1000)
        finally:
            await second.aclose()
        _restart_local_redis()
        third = aioredis.from_url(REDIS_URL, decode_responses=True)
        try:
            result = await control.check_worker_redis_continuity(_Database(expectation), third, "default")
            assert result.reason_code == "active_marker_missing"
        finally:
            await third.flushdb()
            await third.aclose()
    finally:
        try:
            await first.aclose()
        except Exception:
            pass


async def _set_acl_user(client: aioredis.Redis, name: str, password: str, commands: list[str]) -> None:
    await client.execute_command(
        "ACL",
        "SETUSER",
        name,
        "reset",
        "on",
        f">{password}",
        "~vp:worker-event-emission:*",
        "~vp:worker-task-dispatch:*",
        "~vp:events",
        "-@all",
        *commands,
    )


@pytest.mark.asyncio
async def test_marker_acl_users_deny_each_others_mutation_surfaces(redis_client: aioredis.Redis) -> None:
    suffix = uuid.uuid4().hex
    users = {purpose: f"vp-marker-test-{purpose}-{suffix}" for purpose in ("readiness", "janitor", "repair")}
    passwords = {purpose: uuid.uuid4().hex for purpose in users}
    await _set_acl_user(redis_client, users["readiness"], passwords["readiness"], ["+select", "+ping", "+acl|whoami", "+info", "+config|get", "+get", "+xrange"])
    await _set_acl_user(redis_client, users["janitor"], passwords["janitor"], ["+select", "+eval", "+get", "+del"])
    await _set_acl_user(redis_client, users["repair"], passwords["repair"], ["+select", "+eval", "+get", "+set", "+xrange"])
    clients = {
        purpose: aioredis.from_url(
            REDIS_URL.replace("redis://", f"redis://{quote(users[purpose])}:{quote(passwords[purpose])}@"),
            decode_responses=True,
        )
        for purpose in users
    }
    marker = f"vp:worker-event-emission:{uuid.uuid4()}"
    try:
        assert await clients["readiness"].acl_whoami() == users["readiness"]
        with pytest.raises(NoPermissionError):
            await clients["readiness"].eval(control.COMPARE_DELETE, 1, marker, "1710000000000-0")
        with pytest.raises(NoPermissionError):
            await clients["janitor"].set(marker, "1710000000000-0")
        with pytest.raises(NoPermissionError):
            await clients["janitor"].xadd("vp:events", {"event": "forbidden"})
        with pytest.raises(NoPermissionError):
            await clients["repair"].delete(marker)
    finally:
        for client in clients.values():
            await client.aclose()
        await redis_client.execute_command("ACL", "DELUSER", *users.values())
