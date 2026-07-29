from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import text

from worker.secret_config import WorkerSecretError, read_mode_0400_secret


DATABASE_URL_FILE_ENV = "WORKER_REDIS_MARKER_DATABASE_URL_FILE"
REDIS_URL_FILE_ENV = "WORKER_REDIS_MARKER_REDIS_URL_FILE"
CONTINUITY_PAGE_SIZE = 500
MAX_EXPECTATIONS = 100_000
CONTINUITY_STALE_RUN_SECONDS = 300
JANITOR_CLAIM_LIMIT = 100
JANITOR_CLAIM_LEASE_SECONDS = 300
MARKER_KINDS = {"event_emission", "task_dispatch"}
MESSAGE_ID_PATTERN = re.compile(r"^[0-9]+-[0-9]+$")
PAYLOAD_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

COMPARE_DELETE = """
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
if current ~= ARGV[1] then return -1 end
return redis.call('DEL', KEYS[1])
"""

RESTORE_IF_ABSENT = """
local current = redis.call('GET', KEYS[1])
if current then
  if current == ARGV[1] then return 0 end
  return -1
end
redis.call('SET', KEYS[1], ARGV[1], 'NX')
return 1
"""


class MarkerControlError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class MarkerControlConfigError(MarkerControlError):
    def __init__(self) -> None:
        super().__init__("marker_control_config_invalid")


@dataclass(frozen=True)
class MarkerControlConfig:
    database_url: str
    redis_url: str


@dataclass(frozen=True)
class MarkerExpectation:
    marker_kind: Literal["event_emission", "task_dispatch"]
    source_id: uuid.UUID
    marker_key: str
    redis_stream: str
    expected_message_id: str | None
    payload_sha256: str
    source_state: str
    absence_allowed: bool


@dataclass(frozen=True)
class ContinuityResult:
    run_id: uuid.UUID
    state: Literal["ready", "error", "overlap"]
    reason_code: str
    redis_run_id: str | None
    expected_count: int
    checked_count: int


@dataclass(frozen=True)
class MarkerCleanupAuthorization:
    id: uuid.UUID
    marker_kind: Literal["event_emission", "task_dispatch"]
    source_id: uuid.UUID
    marker_key: str
    redis_stream: str
    expected_message_id: str
    payload_sha256: str


@dataclass(frozen=True)
class MarkerRepairEvidence:
    marker_kind: Literal["event_emission", "task_dispatch"]
    source_id: uuid.UUID
    marker_key: str
    redis_stream: str
    expected_message_id: str | None
    payload_sha256: str
    source_state: str


def load_marker_control_config(
    *,
    production: bool,
    env: Mapping[str, str] | None = None,
) -> MarkerControlConfig:
    del production  # The control-plane contract is file-only in every mode.
    values = os.environ if env is None else env
    if values.get("DATABASE_URL", "").strip() or values.get(
        "REDIS_URL", ""
    ).strip():
        raise MarkerControlConfigError()
    database_path = values.get(DATABASE_URL_FILE_ENV, "").strip()
    redis_path = values.get(REDIS_URL_FILE_ENV, "").strip()
    if not database_path or not redis_path:
        raise MarkerControlConfigError()
    try:
        database_url = read_mode_0400_secret(
            database_path,
            label="worker Redis marker database URL",
        )
        redis_url = read_mode_0400_secret(
            redis_path,
            label="worker Redis marker Redis URL",
        )
    except (OSError, WorkerSecretError):
        raise MarkerControlConfigError() from None
    if not database_url or not redis_url:
        raise MarkerControlConfigError()
    return MarkerControlConfig(database_url, redis_url)


def canonical_payload_sha256(fields: Mapping[str, str]) -> str:
    canonical: dict[str, str] = {}
    for key, value in fields.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise MarkerControlError("payload_not_canonical")
        try:
            key.encode("utf-8")
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise MarkerControlError("payload_not_canonical") from exc
        canonical[key] = value
    encoded = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def check_worker_redis_continuity(
    db: Any,
    redis: Any,
    expected_user: str,
    *,
    page_size: int = CONTINUITY_PAGE_SIZE,
) -> ContinuityResult:
    if not isinstance(expected_user, str) or not expected_user:
        raise MarkerControlError("redis_user_mismatch")
    if page_size < 1 or page_size > CONTINUITY_PAGE_SIZE:
        raise MarkerControlError("expectation_page_incomplete")

    run_id = uuid.uuid4()
    try:
        begin = await _begin_continuity_run(db, run_id)
    except Exception:
        return _continuity_error(run_id, "continuity_begin_failed", 0, 0)
    if begin == "overlap":
        return ContinuityResult(
            run_id,
            "overlap",
            "continuity_check_overlap",
            None,
            0,
            0,
        )
    if begin != "begun":
        return _continuity_error(run_id, "continuity_begin_failed", 0, 0)

    try:
        expectations = await _load_expectations(db, page_size)
    except MarkerControlError as exc:
        await _finish_continuity_safely(
            db,
            run_id,
            "error",
            exc.code,
            None,
            0,
            0,
        )
        return _continuity_error(run_id, exc.code, 0, 0)
    except Exception:
        await _finish_continuity_safely(
            db,
            run_id,
            "error",
            "expectation_page_incomplete",
            None,
            0,
            0,
        )
        return _continuity_error(run_id, "expectation_page_incomplete", 0, 0)

    expected_count = len(expectations)
    checked_count = 0
    redis_run_id: str | None = None
    try:
        redis_run_id = await _verify_redis_prerequisites(redis, expected_user)
        for expectation in expectations:
            observed_message_id = await _verify_expectation(redis, expectation)
            checked_count += 1
            if observed_message_id is None:
                continue
            await _record_observation(
                db,
                run_id,
                expectation,
                observed_message_id,
            )
    except MarkerControlError as exc:
        await _finish_continuity_safely(
            db,
            run_id,
            "error",
            exc.code,
            redis_run_id,
            expected_count,
            checked_count,
        )
        return _continuity_error(
            run_id,
            exc.code,
            expected_count,
            checked_count,
            redis_run_id,
        )
    except Exception:
        await _finish_continuity_safely(
            db,
            run_id,
            "error",
            "redis_unavailable",
            redis_run_id,
            expected_count,
            checked_count,
        )
        return _continuity_error(
            run_id,
            "redis_unavailable",
            expected_count,
            checked_count,
            redis_run_id,
        )

    try:
        finished = await _finish_continuity(
            db,
            run_id,
            "ready",
            "ready",
            redis_run_id,
            expected_count,
            checked_count,
        )
    except Exception:
        return _continuity_error(
            run_id,
            "continuity_finish_failed",
            expected_count,
            checked_count,
            redis_run_id,
        )
    if finished is not True:
        return _continuity_error(
            run_id,
            "continuity_finish_failed",
            expected_count,
            checked_count,
            redis_run_id,
        )
    return ContinuityResult(
        run_id,
        "ready",
        "ready",
        redis_run_id,
        expected_count,
        checked_count,
    )


async def run_worker_redis_marker_janitor(
    db: Any,
    redis: Any,
    run_id: uuid.UUID,
) -> dict[str, int]:
    if not isinstance(run_id, uuid.UUID):
        raise MarkerControlError("marker_cleanup_claim_invalid")
    claims = await _claim_cleanup_authorizations(db, run_id)
    result = {"claimed": len(claims), "deleted": 0, "absent": 0, "conflict": 0}
    for authorization in claims:
        try:
            outcome = await redis.eval(
                COMPARE_DELETE,
                1,
                authorization.marker_key,
                authorization.expected_message_id,
            )
        except Exception as exc:
            raise MarkerControlError("redis_unavailable") from exc
        if outcome == 1:
            state: Literal["deleted", "absent", "conflict"] = "deleted"
            code = "marker_deleted"
        elif outcome == 0:
            state, code = "absent", "marker_absent"
        elif outcome == -1:
            state, code = "conflict", "marker_conflict"
        else:
            raise MarkerControlError("marker_cleanup_script_invalid")
        await _finish_cleanup_authorization(
            db,
            authorization.id,
            run_id,
            state,
            code,
        )
        result[state] += 1
    return result


async def audit_worker_redis_markers(
    db: Any,
    redis: Any,
) -> list[dict[str, str]]:
    evidence_rows = await _load_repair_evidence_rows(db, "audit", None)
    audits: list[dict[str, str]] = []
    for evidence in evidence_rows:
        code = await _audit_marker_evidence(redis, evidence)
        audits.append({"source_id": str(evidence.source_id), "code": code})
    return audits


async def restore_worker_redis_marker(
    db: Any,
    redis: Any,
    source_id: uuid.UUID,
    *,
    apply: bool,
) -> str:
    evidence = await _load_one_repair_evidence(db, "restore_marker", source_id)
    if evidence.expected_message_id is None:
        return "repair_evidence_missing"
    await _require_exact_stream_payload(redis, evidence, evidence.expected_message_id)
    try:
        marker_value = await redis.get(evidence.marker_key)
    except Exception as exc:
        raise MarkerControlError("redis_unavailable") from exc
    if marker_value is not None:
        return "marker_not_absent"
    if not apply:
        return "dry_run_ready"

    authorized = await _load_one_repair_evidence(
        db,
        "authorize_restore_marker",
        source_id,
    )
    if authorized != evidence:
        return "repair_evidence_changed"
    try:
        restored = await redis.eval(
            RESTORE_IF_ABSENT,
            1,
            evidence.marker_key,
            evidence.expected_message_id,
        )
    except Exception as exc:
        raise MarkerControlError("redis_unavailable") from exc
    if restored == 1:
        return "restore_applied"
    if restored == 0:
        return "marker_not_absent"
    if restored == -1:
        return "marker_conflict"
    raise MarkerControlError("marker_restore_script_invalid")


async def promote_prepared_worker_event(
    db: Any,
    redis: Any,
    emission_id: uuid.UUID,
    *,
    apply: bool,
) -> str:
    evidence = await _load_one_repair_evidence(
        db,
        "promote_prepared",
        emission_id,
    )
    if (
        evidence.marker_kind != "event_emission"
        or evidence.source_state != "prepared"
        or evidence.expected_message_id is not None
    ):
        return "repair_evidence_missing"
    try:
        marker_value = await redis.get(evidence.marker_key)
    except Exception as exc:
        raise MarkerControlError("redis_unavailable") from exc
    if not isinstance(marker_value, str) or not MESSAGE_ID_PATTERN.fullmatch(
        marker_value
    ):
        return "prepared_marker_missing"
    await _require_exact_stream_payload(redis, evidence, marker_value)
    if not apply:
        return "dry_run_ready"
    try:
        promoted = await _promote_prepared_event(
            db,
            evidence.source_id,
            marker_value,
            evidence.payload_sha256,
        )
    except Exception as exc:
        raise MarkerControlError("marker_promotion_failed") from exc
    if promoted is not True:
        return "marker_promotion_rejected"
    return "promotion_applied"


async def _load_expectations(
    db: Any,
    page_size: int,
) -> tuple[MarkerExpectation, ...]:
    after_key = ""
    expectations: list[MarkerExpectation] = []
    while True:
        rows = await _database_rows(
            db,
            """
            SELECT *
            FROM public.vp_list_worker_redis_marker_expectations(
                :after_key,
                :page_size
            )
            """,
            {"after_key": after_key, "page_size": page_size},
        )
        if len(rows) > page_size:
            raise MarkerControlError("expectation_page_incomplete")
        for row in rows:
            expectation = _marker_expectation_from_row(row)
            if expectation.marker_key <= after_key:
                raise MarkerControlError("expectation_page_incomplete")
            expectations.append(expectation)
            after_key = expectation.marker_key
            if len(expectations) > MAX_EXPECTATIONS:
                raise MarkerControlError("expectation_page_incomplete")
        if len(rows) < page_size:
            return tuple(expectations)


async def _verify_redis_prerequisites(redis: Any, expected_user: str) -> str:
    try:
        user = await redis.acl_whoami()
        server = await redis.info("server")
        persistence = await redis.info("persistence")
        eviction = await redis.config_get("maxmemory-policy")
    except Exception as exc:
        raise MarkerControlError("redis_unavailable") from exc
    if user != expected_user:
        raise MarkerControlError("redis_user_mismatch")
    if not isinstance(server, Mapping) or not isinstance(persistence, Mapping):
        raise MarkerControlError("redis_unavailable")
    if _is_truthy(server.get("loading")) or _is_truthy(
        persistence.get("loading")
    ):
        raise MarkerControlError("redis_loading")
    if str(persistence.get("aof_enabled", "0")) != "1":
        raise MarkerControlError("redis_aof_disabled")
    if (
        str(persistence.get("aof_last_write_status", "")) != "ok"
        or str(persistence.get("aof_last_bgrewrite_status", "")) != "ok"
    ):
        raise MarkerControlError("redis_aof_unhealthy")
    if (
        not isinstance(eviction, Mapping)
        or eviction.get("maxmemory-policy") != "noeviction"
    ):
        raise MarkerControlError("redis_eviction_policy_invalid")
    run_id = server.get("run_id")
    if not isinstance(run_id, str) or not run_id or len(run_id) > 255:
        raise MarkerControlError("redis_unavailable")
    return run_id


async def _verify_expectation(
    redis: Any,
    expectation: MarkerExpectation,
) -> str | None:
    try:
        marker_value = await redis.get(expectation.marker_key)
    except Exception as exc:
        raise MarkerControlError("redis_unavailable") from exc
    if marker_value is None:
        if expectation.absence_allowed:
            return None
        raise MarkerControlError("active_marker_missing")
    if not isinstance(marker_value, str):
        raise MarkerControlError("active_marker_mismatch")
    if expectation.expected_message_id is not None:
        if marker_value != expectation.expected_message_id:
            raise MarkerControlError("active_marker_mismatch")
    elif not MESSAGE_ID_PATTERN.fullmatch(marker_value):
        raise MarkerControlError("active_marker_mismatch")
    await _require_exact_stream_payload(redis, expectation, marker_value)
    return marker_value


async def _require_exact_stream_payload(
    redis: Any,
    evidence: MarkerExpectation | MarkerRepairEvidence,
    message_id: str,
) -> None:
    try:
        entries = await redis.xrange(evidence.redis_stream, message_id, message_id)
    except Exception as exc:
        raise MarkerControlError("redis_unavailable") from exc
    if (
        not isinstance(entries, Sequence)
        or len(entries) != 1
        or not isinstance(entries[0], Sequence)
        or len(entries[0]) != 2
        or entries[0][0] != message_id
        or not isinstance(entries[0][1], Mapping)
    ):
        raise MarkerControlError("event_stream_entry_missing")
    try:
        actual_hash = canonical_payload_sha256(entries[0][1])
    except MarkerControlError as exc:
        raise MarkerControlError("event_payload_mismatch") from exc
    if actual_hash != evidence.payload_sha256:
        raise MarkerControlError("event_payload_mismatch")


async def _record_observation(
    db: Any,
    run_id: uuid.UUID,
    expectation: MarkerExpectation,
    message_id: str,
) -> None:
    try:
        recorded = await _database_scalar(
            db,
            """
            SELECT public.vp_record_worker_redis_marker_observation(
                :run_id,
                :marker_kind,
                :source_id,
                :message_id,
                :payload_sha256
            )
            """,
            {
                "run_id": run_id,
                "marker_kind": expectation.marker_kind,
                "source_id": expectation.source_id,
                "message_id": message_id,
                "payload_sha256": expectation.payload_sha256,
            },
        )
    except Exception as exc:
        raise MarkerControlError(
            "marker_observation_database_error"
        ) from exc
    if recorded is not True:
        raise MarkerControlError("marker_observation_rejected")


async def _begin_continuity_run(db: Any, run_id: uuid.UUID) -> object:
    return await _database_scalar(
        db,
        """
        SELECT public.vp_begin_worker_redis_continuity_check(
            :run_id,
            :stale_run_seconds
        )
        """,
        {"run_id": run_id, "stale_run_seconds": CONTINUITY_STALE_RUN_SECONDS},
    )


async def _finish_continuity(
    db: Any,
    run_id: uuid.UUID,
    result: Literal["ready", "error"],
    reason_code: str,
    redis_run_id: str | None,
    expected_count: int,
    checked_count: int,
) -> object:
    return await _database_scalar(
        db,
        """
        SELECT public.vp_finish_worker_redis_continuity_check(
            :run_id,
            :result,
            :reason_code,
            :redis_run_id,
            :expected_count,
            :checked_count
        )
        """,
        {
            "run_id": run_id,
            "result": result,
            "reason_code": reason_code,
            "redis_run_id": redis_run_id,
            "expected_count": expected_count,
            "checked_count": checked_count,
        },
    )


async def _finish_continuity_safely(
    db: Any,
    run_id: uuid.UUID,
    result: Literal["ready", "error"],
    reason_code: str,
    redis_run_id: str | None,
    expected_count: int,
    checked_count: int,
) -> None:
    try:
        await _finish_continuity(
            db,
            run_id,
            result,
            reason_code,
            redis_run_id,
            expected_count,
            checked_count,
        )
    except Exception:
        pass


async def _claim_cleanup_authorizations(
    db: Any,
    run_id: uuid.UUID,
) -> tuple[MarkerCleanupAuthorization, ...]:
    rows = await _database_rows(
        db,
        """
        SELECT *
        FROM public.vp_claim_worker_redis_marker_cleanup(
            :run_id,
            :limit,
            :lease_seconds
        )
        """,
        {
            "run_id": run_id,
            "limit": JANITOR_CLAIM_LIMIT,
            "lease_seconds": JANITOR_CLAIM_LEASE_SECONDS,
        },
    )
    return tuple(_cleanup_authorization_from_row(row) for row in rows)


async def _finish_cleanup_authorization(
    db: Any,
    authorization_id: uuid.UUID,
    run_id: uuid.UUID,
    result: Literal["deleted", "absent", "conflict"],
    result_code: str,
) -> None:
    finished = await _database_scalar(
        db,
        """
        SELECT public.vp_finish_worker_redis_marker_cleanup(
            :authorization_id,
            :run_id,
            :result,
            :result_code
        )
        """,
        {
            "authorization_id": authorization_id,
            "run_id": run_id,
            "result": result,
            "result_code": result_code,
        },
    )
    if finished is not True:
        raise MarkerControlError("marker_cleanup_finish_rejected")


async def _load_repair_evidence_rows(
    db: Any,
    action: str,
    source_id: uuid.UUID | None,
) -> tuple[MarkerRepairEvidence, ...]:
    rows = await _database_rows(
        db,
        """
        SELECT *
        FROM public.vp_load_worker_redis_marker_repair(
            :action,
            :source_id
        )
        """,
        {"action": action, "source_id": source_id},
    )
    return tuple(_repair_evidence_from_row(row) for row in rows)


async def _load_one_repair_evidence(
    db: Any,
    action: str,
    source_id: uuid.UUID,
) -> MarkerRepairEvidence:
    if not isinstance(source_id, uuid.UUID):
        raise MarkerControlError("repair_evidence_missing")
    rows = await _load_repair_evidence_rows(db, action, source_id)
    if len(rows) != 1:
        raise MarkerControlError("repair_evidence_missing")
    return rows[0]


async def _promote_prepared_event(
    db: Any,
    emission_id: uuid.UUID,
    message_id: str,
    payload_sha256: str,
) -> object:
    return await _database_scalar(
        db,
        """
        SELECT public.vp_promote_observed_worker_event_emission(
            :emission_id,
            :message_id,
            :payload_sha256
        )
        """,
        {
            "emission_id": emission_id,
            "message_id": message_id,
            "payload_sha256": payload_sha256,
        },
    )


async def _audit_marker_evidence(redis: Any, evidence: MarkerRepairEvidence) -> str:
    try:
        marker_value = await redis.get(evidence.marker_key)
    except Exception:
        return "redis_unavailable"
    if marker_value is None:
        return "marker_absent"
    if not isinstance(marker_value, str):
        return "marker_mismatch"
    if evidence.expected_message_id is not None and marker_value != evidence.expected_message_id:
        return "marker_mismatch"
    if not MESSAGE_ID_PATTERN.fullmatch(marker_value):
        return "marker_mismatch"
    try:
        await _require_exact_stream_payload(redis, evidence, marker_value)
    except MarkerControlError as exc:
        return exc.code
    return "consistent"


@asynccontextmanager
async def _transaction(db: Any) -> AsyncIterator[None]:
    async with db.begin():
        yield


async def _database_scalar(
    db: Any,
    statement: str,
    params: dict[str, object],
) -> object:
    async with _transaction(db):
        return await db.scalar(text(statement), params)


async def _database_rows(
    db: Any,
    statement: str,
    params: dict[str, object],
) -> list[Mapping[str, object]]:
    async with _transaction(db):
        result = await db.execute(text(statement), params)
        mappings = result.mappings()
        return [dict(row) for row in mappings.all()]


def _marker_expectation_from_row(row: Mapping[str, object]) -> MarkerExpectation:
    return MarkerExpectation(
        marker_kind=_marker_kind(row.get("marker_kind")),
        source_id=_uuid_value(row.get("source_id")),
        marker_key=_marker_key(row.get("marker_key")),
        redis_stream=_nonempty_value(row.get("redis_stream")),
        expected_message_id=_message_id_or_none(row.get("expected_message_id")),
        payload_sha256=_payload_hash_value(row.get("payload_sha256")),
        source_state=_nonempty_value(row.get("source_state")),
        absence_allowed=_bool_value(row.get("absence_allowed")),
    )


def _cleanup_authorization_from_row(
    row: Mapping[str, object],
) -> MarkerCleanupAuthorization:
    message_id = _message_id_or_none(row.get("expected_message_id"))
    if message_id is None:
        raise MarkerControlError("marker_cleanup_claim_invalid")
    return MarkerCleanupAuthorization(
        id=_uuid_value(row.get("id")),
        marker_kind=_marker_kind(row.get("marker_kind")),
        source_id=_uuid_value(row.get("source_id")),
        marker_key=_marker_key(row.get("marker_key")),
        redis_stream=_nonempty_value(row.get("redis_stream")),
        expected_message_id=message_id,
        payload_sha256=_payload_hash_value(row.get("payload_sha256")),
    )


def _repair_evidence_from_row(row: Mapping[str, object]) -> MarkerRepairEvidence:
    return MarkerRepairEvidence(
        marker_kind=_marker_kind(row.get("marker_kind")),
        source_id=_uuid_value(row.get("source_id")),
        marker_key=_marker_key(row.get("marker_key")),
        redis_stream=_nonempty_value(row.get("redis_stream")),
        expected_message_id=_message_id_or_none(row.get("expected_message_id")),
        payload_sha256=_payload_hash_value(row.get("payload_sha256")),
        source_state=_nonempty_value(row.get("source_state")),
    )


def _marker_kind(value: object) -> Literal["event_emission", "task_dispatch"]:
    if value not in MARKER_KINDS:
        raise MarkerControlError("marker_evidence_invalid")
    return value  # type: ignore[return-value]


def _uuid_value(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    if isinstance(value, str):
        try:
            return uuid.UUID(value)
        except ValueError as exc:
            raise MarkerControlError("marker_evidence_invalid") from exc
    raise MarkerControlError("marker_evidence_invalid")


def _marker_key(value: object) -> str:
    key = _nonempty_value(value)
    if not key.startswith(("vp:worker-event-emission:", "vp:worker-task-dispatch:")):
        raise MarkerControlError("marker_evidence_invalid")
    return key


def _message_id_or_none(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not MESSAGE_ID_PATTERN.fullmatch(value):
        raise MarkerControlError("marker_evidence_invalid")
    return value


def _payload_hash_value(value: object) -> str:
    if not isinstance(value, str) or not PAYLOAD_SHA256_PATTERN.fullmatch(value):
        raise MarkerControlError("marker_evidence_invalid")
    return value


def _nonempty_value(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 255:
        raise MarkerControlError("marker_evidence_invalid")
    return value


def _bool_value(value: object) -> bool:
    if not isinstance(value, bool):
        raise MarkerControlError("marker_evidence_invalid")
    return value


def _is_truthy(value: object) -> bool:
    return value is True or str(value) == "1"


def _continuity_error(
    run_id: uuid.UUID,
    reason_code: str,
    expected_count: int,
    checked_count: int,
    redis_run_id: str | None = None,
) -> ContinuityResult:
    return ContinuityResult(
        run_id,
        "error",
        reason_code,
        redis_run_id,
        expected_count,
        checked_count,
    )
