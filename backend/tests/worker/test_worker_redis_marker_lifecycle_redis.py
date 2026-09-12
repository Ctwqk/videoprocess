from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from urllib.parse import quote

import asyncpg
import pytest
import redis.asyncio as aioredis
from redis.exceptions import NoPermissionError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.services import worker_redis_marker_control as control
from tests.migrations.test_worker_redis_marker_lifecycle_postgres import (
    POSTGRES_URL as MARKER_POSTGRES_URL,
)
from tests.migrations.test_worker_redis_marker_lifecycle_postgres import (
    _seed_authority,
)


pytest_plugins = (
    "tests.migrations.test_worker_redis_marker_lifecycle_postgres",
)


REDIS_URL = os.environ.get("WORKER_REDIS_MARKER_TEST_URL", "")
CONTAINER = "videoprocess-marker-redis74"
REVIEWED_TASK_STREAMS = (
    "vp:tasks:ffmpeg",
    "vp:tasks:ffmpeg_go",
    "vp:tasks:vision",
    "vp:tasks:youtube_publisher",
)
MARKER_KEY_SELECTORS = (
    "~vp:worker-event-emission:*",
    "~vp:worker-task-dispatch:*",
)
READINESS_REPAIR_KEY_SELECTORS = (
    *MARKER_KEY_SELECTORS,
    "~vp:events",
    *(f"~{stream}" for stream in REVIEWED_TASK_STREAMS),
)


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


def _evidence_row(
    evidence: control.MarkerRepairEvidence,
) -> dict[str, object]:
    return {
        "marker_kind": evidence.marker_kind,
        "source_id": evidence.source_id,
        "marker_key": evidence.marker_key,
        "redis_stream": evidence.redis_stream,
        "expected_message_id": evidence.expected_message_id,
        "payload_sha256": evidence.payload_sha256,
        "source_state": evidence.source_state,
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


class _RepairDatabase:
    def __init__(
        self,
        evidence: control.MarkerRepairEvidence,
        *,
        authorize_hook: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.evidence = evidence
        self.authorize_hook = authorize_hook
        self.actions: list[str] = []
        self.promotions: list[dict[str, object]] = []

    @asynccontextmanager
    async def begin(self):
        yield self

    async def execute(
        self,
        statement: object,
        params: dict[str, object],
    ):
        if "vp_load_worker_redis_marker_repair" not in str(statement):
            raise AssertionError(str(statement))
        action = str(params["action"])
        self.actions.append(action)
        if action == "authorize_restore_marker" and self.authorize_hook:
            await self.authorize_hook()
        return _Result([_evidence_row(self.evidence)])

    async def scalar(
        self,
        statement: object,
        params: dict[str, object],
    ):
        if "vp_promote_observed_worker_event_emission" not in str(statement):
            raise AssertionError(str(statement))
        self.promotions.append(params)
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


async def _connect_restarted_redis() -> aioredis.Redis:
    client = aioredis.from_url(REDIS_URL, decode_responses=True)
    for _ in range(100):
        try:
            if await client.ping():
                return client
        except Exception:
            await asyncio.sleep(0.05)
    await client.aclose()
    pytest.fail("local Redis did not become ready after restart")


@pytest.mark.asyncio
@pytest.mark.skipif(
    not REDIS_URL or not MARKER_POSTGRES_URL,
    reason=(
        "set WORKER_REDIS_MARKER_TEST_URL and "
        "CHANNEL_OPS_POSTGRES_TEST_URL for combined integration tests"
    ),
)
async def test_real_postgres_and_redis_concurrent_janitor_is_restart_safe(
    marker_database,
) -> None:
    admin = marker_database["admin"]
    credential_urls = marker_database["credential_urls"]
    assert isinstance(credential_urls, dict)
    authorization_id = uuid.uuid4()
    marker_key = f"vp:worker-event-emission:{uuid.uuid4()}"
    expected_message_id = "1710000000000-0"
    await admin.execute(
        """
        INSERT INTO public.worker_redis_marker_cleanup_authorizations (
            id, marker_kind, source_id, marker_key, redis_stream,
            expected_message_id, payload_sha256
        ) VALUES (
            $1, 'event_emission', $2, $3, 'vp:events', $4, $5
        )
        """,
        authorization_id,
        uuid.uuid4(),
        marker_key,
        expected_message_id,
        "a" * 64,
    )

    redis_admin = aioredis.from_url(REDIS_URL, decode_responses=True)
    await redis_admin.flushdb()
    suffix = uuid.uuid4().hex
    redis_user = f"vp-marker-test-janitor-{suffix}"
    redis_password = uuid.uuid4().hex
    await _set_acl_user(
        redis_admin,
        redis_user,
        redis_password,
        ["+select", "+eval", "+get", "+del"],
        key_selectors=MARKER_KEY_SELECTORS,
    )
    janitor_redis = aioredis.from_url(
        _acl_url(redis_user, redis_password),
        decode_responses=True,
    )
    await redis_admin.set(marker_key, expected_message_id)
    acl_deleted = False

    engine = create_async_engine(
        str(credential_urls["janitor"]),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    first_session = session_factory()
    second_session = session_factory()
    assert isinstance(first_session, AsyncSession)
    assert isinstance(second_session, AsyncSession)
    try:
        outcomes = await asyncio.gather(
            control.run_worker_redis_marker_janitor(
                first_session,
                janitor_redis,
                uuid.uuid4(),
            ),
            control.run_worker_redis_marker_janitor(
                second_session,
                janitor_redis,
                uuid.uuid4(),
            ),
        )
        assert sum(outcome["claimed"] for outcome in outcomes) == 1
        assert sum(outcome["deleted"] for outcome in outcomes) == 1
        assert sum(outcome["absent"] for outcome in outcomes) == 0
        assert sum(outcome["conflict"] for outcome in outcomes) == 0
        assert await redis_admin.get(marker_key) is None

        terminal = await admin.fetchrow(
            """
            SELECT authorization_state, result_code,
                   claimed_by_run_id IS NOT NULL AS was_claimed,
                   finished_at IS NOT NULL AS was_finished
            FROM public.worker_redis_marker_cleanup_authorizations
            WHERE id = $1
            """,
            authorization_id,
        )
        assert dict(terminal) == {
            "authorization_state": "deleted",
            "result_code": "marker_deleted",
            "was_claimed": True,
            "was_finished": True,
        }

        await redis_admin.execute_command("WAITAOF", 1, 0, 1000)
        await janitor_redis.aclose()
        await redis_admin.aclose()
        _restart_local_redis()
        restarted_redis = await _connect_restarted_redis()
        try:
            assert await restarted_redis.get(marker_key) is None
            assert await control.run_worker_redis_marker_janitor(
                first_session,
                restarted_redis,
                uuid.uuid4(),
            ) == {
                "claimed": 0,
                "deleted": 0,
                "absent": 0,
                "conflict": 0,
            }
            assert await admin.fetchval(
                """
                SELECT authorization_state = 'deleted'
                   AND result_code = 'marker_deleted'
                   AND finished_at IS NOT NULL
                FROM public.worker_redis_marker_cleanup_authorizations
                WHERE id = $1
                """,
                authorization_id,
            )
        finally:
            await restarted_redis.flushdb()
            await restarted_redis.execute_command(
                "ACL",
                "DELUSER",
                redis_user,
            )
            acl_deleted = True
            await restarted_redis.aclose()
    finally:
        await first_session.close()
        await second_session.close()
        await engine.dispose()
        try:
            await janitor_redis.aclose()
        except Exception:
            pass
        try:
            await redis_admin.aclose()
        except Exception:
            pass
        if not acl_deleted:
            cleanup_redis = await _connect_restarted_redis()
            try:
                await cleanup_redis.flushdb()
                await cleanup_redis.execute_command(
                    "ACL",
                    "DELUSER",
                    redis_user,
                )
            finally:
                await cleanup_redis.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not REDIS_URL or not MARKER_POSTGRES_URL,
    reason=(
        "set WORKER_REDIS_MARKER_TEST_URL and "
        "CHANNEL_OPS_POSTGRES_TEST_URL for combined integration tests"
    ),
)
async def test_real_postgres_and_redis_active_markers_complete_readiness(
    marker_database,
    redis_client: aioredis.Redis,
) -> None:
    admin = marker_database["admin"]
    credential_urls = marker_database["credential_urls"]
    generation_roles = marker_database["generation_roles"]
    assert isinstance(credential_urls, dict)
    assert isinstance(generation_roles, dict)
    authority = await _seed_authority(admin, resolved=False)

    await redis_client.xadd(
        authority["emission_stream"],
        authority["emission_payload"],
        id=authority["emission_message_id"],
    )
    await redis_client.xadd(
        authority["dispatch_stream"],
        authority["dispatch_payload"],
        id=authority["dispatch_message_id"],
    )
    await redis_client.set(
        f"vp:worker-event-emission:{authority['emission_id']}",
        authority["emission_message_id"],
    )
    await redis_client.set(
        f"vp:worker-task-dispatch:{authority['dispatch_key']}",
        authority["dispatch_message_id"],
    )

    engine = create_async_engine(
        str(credential_urls["readiness"]),
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    readiness = session_factory()
    repair = await asyncpg.connect(
        str(credential_urls["repair"]).replace(
            "postgresql+asyncpg://",
            "postgresql://",
            1,
        )
    )
    try:
        ordinary = await control.check_worker_redis_continuity(
            readiness,
            redis_client,
            "default",
        )
        assert (ordinary.state, ordinary.reason_code) == ("ready", "ready")
        assert ordinary.expected_count == 2
        assert ordinary.checked_count == 2

        status = await admin.fetchrow(
            """
            SELECT run_id, state, reason_code, expected_count, checked_count
            FROM public.worker_redis_continuity_status
            WHERE singleton
            """
        )
        assert dict(status) == {
            "run_id": ordinary.run_id,
            "state": "ready",
            "reason_code": "ready",
            "expected_count": 2,
            "checked_count": 2,
        }
        observations = await admin.fetch(
            """
            SELECT marker_kind, source_id, observed_message_id,
                   observed_payload_sha256, observed_by,
                   observed_at IS NOT NULL AS was_observed
            FROM public.worker_redis_continuity_expectations
            WHERE run_id = $1
            ORDER BY marker_kind
            """,
            ordinary.run_id,
        )
        assert [dict(row) for row in observations] == [
            {
                "marker_kind": "event_emission",
                "source_id": authority["emission_id"],
                "observed_message_id": authority["emission_message_id"],
                "observed_payload_sha256": authority["emission_hash"],
                "observed_by": generation_roles["readiness"],
                "was_observed": True,
            },
            {
                "marker_kind": "task_dispatch",
                "source_id": authority["dispatch_id"],
                "observed_message_id": authority["dispatch_message_id"],
                "observed_payload_sha256": authority["dispatch_hash"],
                "observed_by": generation_roles["readiness"],
                "was_observed": True,
            },
        ]
        assert await admin.fetchval(
            """
            SELECT count(*)
            FROM public.worker_redis_marker_repair_audits
            WHERE source_id = ANY($1::uuid[])
              AND result_code = 'restored'
            """,
            [authority["emission_id"], authority["dispatch_id"]],
        ) == 0

        await repair.fetchrow(
            """
            SELECT *
            FROM public.vp_load_worker_redis_marker_repair(
                'authorize_restore_marker', $1
            )
            """,
            authority["emission_id"],
        )
        assert await admin.fetchval(
            """
            SELECT count(*)
            FROM public.worker_redis_marker_repair_audits
            WHERE source_id = $1
              AND action = 'restore_marker'
              AND result_code = 'restored'
            """,
            authority["emission_id"],
        ) == 0

        repaired = await control.check_worker_redis_continuity(
            readiness,
            redis_client,
            "default",
        )
        assert repaired.state == "ready"
        assert repaired.expected_count == 2
        assert repaired.checked_count == 2
        audits = await admin.fetch(
            """
            SELECT action, result_code, principal
            FROM public.worker_redis_marker_repair_audits
            WHERE source_id = $1
            ORDER BY created_at, id
            """,
            authority["emission_id"],
        )
        assert [dict(row) for row in audits] == [
            {
                "action": "restore_marker",
                "result_code": "authorized",
                "principal": generation_roles["repair"],
            },
            {
                "action": "restore_marker",
                "result_code": "restored",
                "principal": generation_roles["repair"],
            },
        ]
    finally:
        await repair.close()
        await readiness.close()
        await engine.dispose()


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


async def _set_acl_user(
    client: aioredis.Redis,
    name: str,
    password: str,
    commands: list[str],
    *,
    key_selectors: tuple[str, ...],
) -> None:
    await client.execute_command(
        "ACL",
        "SETUSER",
        name,
        "reset",
        "on",
        f">{password}",
        *key_selectors,
        "-@all",
        *commands,
    )


def _acl_url(name: str, password: str) -> str:
    return REDIS_URL.replace(
        "redis://",
        f"redis://{quote(name)}:{quote(password)}@",
        1,
    )


@pytest.mark.asyncio
async def test_restore_service_uses_real_redis_proof_and_set_nx(
    redis_client: aioredis.Redis,
) -> None:
    suffix = uuid.uuid4().hex
    user = f"vp-marker-test-repair-{suffix}"
    password = uuid.uuid4().hex
    await _set_acl_user(
        redis_client,
        user,
        password,
        ["+select", "+eval", "+get", "+set", "+xrange"],
        key_selectors=READINESS_REPAIR_KEY_SELECTORS,
    )
    repair_redis = aioredis.from_url(
        _acl_url(user, password),
        decode_responses=True,
    )
    evidence = control.MarkerRepairEvidence(
        marker_kind="event_emission",
        source_id=uuid.uuid4(),
        marker_key=f"vp:worker-event-emission:{uuid.uuid4()}",
        redis_stream="vp:events",
        expected_message_id="1710000000000-0",
        payload_sha256=_payload_hash({"event": "node_completed"}),
        source_state="emitted",
    )
    await redis_client.xadd(
        evidence.redis_stream,
        {"event": "node_completed"},
        id=evidence.expected_message_id,
    )
    try:
        dry_run_database = _RepairDatabase(evidence)
        assert await control.restore_worker_redis_marker(
            dry_run_database,
            repair_redis,
            evidence.source_id,
            apply=False,
        ) == "dry_run_ready"
        assert await redis_client.get(evidence.marker_key) is None
        assert dry_run_database.actions == ["restore_marker"]

        apply_database = _RepairDatabase(evidence)
        assert await control.restore_worker_redis_marker(
            apply_database,
            repair_redis,
            evidence.source_id,
            apply=True,
        ) == "restore_applied"
        assert await redis_client.get(
            evidence.marker_key
        ) == evidence.expected_message_id
        assert apply_database.actions == [
            "restore_marker",
            "authorize_restore_marker",
        ]

        await redis_client.delete(evidence.marker_key)
        conflicting_message_id = "1710000000001-0"

        async def insert_conflict_after_authorization() -> None:
            await redis_client.set(
                evidence.marker_key,
                conflicting_message_id,
            )

        conflict_database = _RepairDatabase(
            evidence,
            authorize_hook=insert_conflict_after_authorization,
        )
        assert await control.restore_worker_redis_marker(
            conflict_database,
            repair_redis,
            evidence.source_id,
            apply=True,
        ) == "marker_conflict"
        assert await redis_client.get(
            evidence.marker_key
        ) == conflicting_message_id
        with pytest.raises(NoPermissionError):
            await repair_redis.delete(evidence.marker_key)
    finally:
        await repair_redis.aclose()
        await redis_client.execute_command("ACL", "DELUSER", user)


@pytest.mark.asyncio
async def test_promote_service_uses_real_redis_exact_xrange_and_hash(
    redis_client: aioredis.Redis,
) -> None:
    suffix = uuid.uuid4().hex
    user = f"vp-marker-test-repair-{suffix}"
    password = uuid.uuid4().hex
    await _set_acl_user(
        redis_client,
        user,
        password,
        ["+select", "+eval", "+get", "+set", "+xrange"],
        key_selectors=READINESS_REPAIR_KEY_SELECTORS,
    )
    repair_redis = aioredis.from_url(
        _acl_url(user, password),
        decode_responses=True,
    )
    evidence = control.MarkerRepairEvidence(
        marker_kind="event_emission",
        source_id=uuid.uuid4(),
        marker_key=f"vp:worker-event-emission:{uuid.uuid4()}",
        redis_stream="vp:events",
        expected_message_id=None,
        payload_sha256=_payload_hash({"event": "node_completed"}),
        source_state="prepared",
    )
    message_id = await redis_client.xadd(
        evidence.redis_stream,
        {"event": "node_completed"},
        id="1710000000000-0",
    )
    await repair_redis.set(evidence.marker_key, message_id)
    try:
        database = _RepairDatabase(evidence)
        assert await control.promote_prepared_worker_event(
            database,
            repair_redis,
            evidence.source_id,
            apply=False,
        ) == "dry_run_ready"
        assert database.promotions == []
        assert await redis_client.get(evidence.marker_key) == message_id

        assert await control.promote_prepared_worker_event(
            database,
            repair_redis,
            evidence.source_id,
            apply=True,
        ) == "promotion_applied"
        assert database.promotions == [
            {
                "emission_id": evidence.source_id,
                "message_id": message_id,
                "payload_sha256": evidence.payload_sha256,
            }
        ]

        wrong_evidence = control.MarkerRepairEvidence(
            marker_kind="event_emission",
            source_id=uuid.uuid4(),
            marker_key=f"vp:worker-event-emission:{uuid.uuid4()}",
            redis_stream=evidence.redis_stream,
            expected_message_id=None,
            payload_sha256=evidence.payload_sha256,
            source_state="prepared",
        )
        wrong_message_id = await redis_client.xadd(
            wrong_evidence.redis_stream,
            {"event": "node_failed"},
            id="1710000000001-0",
        )
        await repair_redis.set(
            wrong_evidence.marker_key,
            wrong_message_id,
        )
        wrong_database = _RepairDatabase(wrong_evidence)
        with pytest.raises(
            control.MarkerControlError,
            match="event_payload_mismatch",
        ):
            await control.promote_prepared_worker_event(
                wrong_database,
                repair_redis,
                wrong_evidence.source_id,
                apply=True,
            )
        assert wrong_database.promotions == []
        with pytest.raises(NoPermissionError):
            await repair_redis.xadd(
                wrong_evidence.redis_stream,
                {"event": "forbidden"},
            )
    finally:
        await repair_redis.aclose()
        await redis_client.execute_command("ACL", "DELUSER", user)


@pytest.mark.asyncio
async def test_readiness_acl_allows_only_reviewed_task_streams(
    redis_client: aioredis.Redis,
) -> None:
    suffix = uuid.uuid4().hex
    user = f"vp-marker-test-readiness-{suffix}"
    password = uuid.uuid4().hex
    await _set_acl_user(
        redis_client,
        user,
        password,
        [
            "+select",
            "+ping",
            "+acl|whoami",
            "+info",
            "+config|get",
            "+get",
            "+xrange",
        ],
        key_selectors=READINESS_REPAIR_KEY_SELECTORS,
    )
    readiness_redis = aioredis.from_url(
        _acl_url(user, password),
        decode_responses=True,
    )
    allowed_entries = {}
    for index, stream in enumerate(REVIEWED_TASK_STREAMS):
        payload = {"dispatch_key": f"reviewed-{index}"}
        message_id = await redis_client.xadd(
            stream,
            payload,
            id=f"1710000001000-{index}",
        )
        allowed_entries[stream] = (message_id, payload)
    denied_entries = {}
    for index, stream in enumerate(
        ("vp:tasks:ffmpeg_shadow", "vp:tasks:planner")
    ):
        message_id = await redis_client.xadd(
            stream,
            {"dispatch_key": f"unreviewed-{index}"},
            id=f"1710000002000-{index}",
        )
        denied_entries[stream] = message_id
    try:
        for stream, (message_id, payload) in allowed_entries.items():
            assert await readiness_redis.xrange(
                stream,
                message_id,
                message_id,
            ) == [(message_id, payload)]
        for stream, message_id in denied_entries.items():
            with pytest.raises(NoPermissionError):
                await readiness_redis.xrange(
                    stream,
                    message_id,
                    message_id,
                )
    finally:
        await readiness_redis.aclose()
        await redis_client.execute_command("ACL", "DELUSER", user)


@pytest.mark.asyncio
async def test_marker_acl_users_deny_each_others_mutation_surfaces(redis_client: aioredis.Redis) -> None:
    suffix = uuid.uuid4().hex
    users = {purpose: f"vp-marker-test-{purpose}-{suffix}" for purpose in ("readiness", "janitor", "repair")}
    passwords = {purpose: uuid.uuid4().hex for purpose in users}
    await _set_acl_user(
        redis_client,
        users["readiness"],
        passwords["readiness"],
        [
            "+select",
            "+ping",
            "+acl|whoami",
            "+info",
            "+config|get",
            "+get",
            "+xrange",
        ],
        key_selectors=READINESS_REPAIR_KEY_SELECTORS,
    )
    await _set_acl_user(
        redis_client,
        users["janitor"],
        passwords["janitor"],
        ["+select", "+eval", "+get", "+del"],
        key_selectors=MARKER_KEY_SELECTORS,
    )
    await _set_acl_user(
        redis_client,
        users["repair"],
        passwords["repair"],
        ["+select", "+eval", "+get", "+set", "+xrange"],
        key_selectors=READINESS_REPAIR_KEY_SELECTORS,
    )
    clients = {
        purpose: aioredis.from_url(
            _acl_url(users[purpose], passwords[purpose]),
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
