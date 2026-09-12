from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import uuid
from copy import deepcopy
from dataclasses import fields, replace
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.job import NodeExecution
from app.models.worker_registration import (
    WorkerAdmissionGrant,
    WorkerRegistration,
)
from app.services.worker_registration import (
    HEARTBEAT_INTERVAL_SECONDS,
    LEASE_DURATION_SECONDS,
    WorkerRegistrationClaims,
    WorkerRegistrationError,
    WorkerRegistrationService,
    _database_identity,
    _normalized_endpoint_bindings,
    _redis_identity,
    dependency_fingerprints,
)


ADMISSION_TOKEN = "worker-admission-token-with-256-bits-of-test-entropy"
OTHER_ADMISSION_TOKEN = "other-admission-token-with-256-bits"
LEASE_SECRET = "registration-specific-lease-secret-aaaaaaaa"
OTHER_LEASE_SECRET = "registration-specific-lease-secret-bbbbbbbb"
RELEASE_COMMIT = "0123456789abcdef0123456789abcdef01234567"
IMAGE_IDENTITY = "vp-ffmpeg-worker-go:deploy-0123456789ab"
DATABASE_PRINCIPAL = "vp_worker_runtime_r7"
START = datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc)
REPO_ROOT = Path(__file__).resolve().parents[3]
BACKEND_ROOT = REPO_ROOT / "backend"
FINGERPRINT_FIXTURE = (
    REPO_ROOT / "tests/fixtures/worker_registration/fingerprints-v1.json"
)
POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")
FINGERPRINT_CASES = json.loads(FINGERPRINT_FIXTURE.read_text())["cases"]
ENDPOINT_VALIDATION_CASES = json.loads(FINGERPRINT_FIXTURE.read_text())[
    "endpoint_validation_cases"
]
MINIO_FINGERPRINT_CASE = next(
    case for case in FINGERPRINT_CASES if case["name"] == "minio"
)
ENDPOINT_BINDINGS = {
    name: json.loads(canonical)
    for name, canonical in MINIO_FINGERPRINT_CASE["canonical"].items()
}
ENDPOINT_FINGERPRINTS = MINIO_FINGERPRINT_CASE["sha256"]


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"invalid JSON numeric constant: {value}")


class MutableClock:
    def __init__(self, current: datetime = START) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current

    def advance(self, seconds: int) -> None:
        self.current += timedelta(seconds=seconds)


@pytest.fixture
async def registration_store(tmp_path: Path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'worker-registration.sqlite3'}",
        connect_args={"timeout": 10},
    )
    async with engine.begin() as connection:
        await connection.run_sync(WorkerAdmissionGrant.__table__.create)
        await connection.run_sync(WorkerRegistration.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    yield session_factory
    await engine.dispose()


def _sha256(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


async def _grant(
    session_factory,
    *,
    service_name: str = "vp-ffmpeg-worker-go-swarm",
    generation: int = 7,
    worker_type: str = "ffmpeg_go",
    worker_host: str = "colima-127",
    capabilities: list[str] | None = None,
    release_commit: str = RELEASE_COMMIT,
    image_identity: str = IMAGE_IDENTITY,
    redis_stream: str = "vp:tasks:ffmpeg_go",
    redis_group: str = "ffmpeg_go-workers",
    endpoint_bindings: dict[str, object] | None = None,
    token: str = ADMISSION_TOKEN,
    state: str = "active",
    database_principal: str = DATABASE_PRINCIPAL,
) -> WorkerAdmissionGrant:
    grant = WorkerAdmissionGrant(
        service_name=service_name,
        generation=generation,
        worker_type=worker_type,
        worker_host=worker_host,
        capabilities_json=capabilities or ["media_cpu"],
        release_commit=release_commit,
        image_identity=image_identity,
        database_principal=database_principal,
        redis_stream=redis_stream,
        redis_group=redis_group,
        endpoint_bindings_json=endpoint_bindings or ENDPOINT_BINDINGS,
        token_sha256=_sha256(token),
        state=state,
        issued_at=START - timedelta(minutes=5),
        issued_by="deployment-controller",
        activated_at=START - timedelta(minutes=4)
        if state == "active"
        else None,
        revoked_at=START - timedelta(minutes=1)
        if state == "revoked"
        else None,
        revoke_reason="operator" if state == "revoked" else None,
    )
    async with session_factory() as db:
        db.add(grant)
        await db.commit()
        await db.refresh(grant)
    return grant


def _claims(**changes: object) -> WorkerRegistrationClaims:
    values: dict[str, object] = {
        "service_name": "vp-ffmpeg-worker-go-swarm",
        "generation": 7,
        "worker_type": "ffmpeg_go",
        "worker_host": "colima-127",
        "worker_instance_id": uuid.UUID(
            "11111111-1111-4111-8111-111111111111"
        ),
        "worker_slot": 1,
        "redis_consumer_id": "ffmpeg-go-worker@colima-127:11111111",
        "capabilities": ("media_cpu",),
        "release_commit": RELEASE_COMMIT,
        "image_identity": IMAGE_IDENTITY,
        "redis_stream": "vp:tasks:ffmpeg_go",
        "redis_group": "ffmpeg_go-workers",
        "endpoint_bindings": ENDPOINT_BINDINGS,
        "database_fingerprint": ENDPOINT_FINGERPRINTS["database"],
        "redis_fingerprint": ENDPOINT_FINGERPRINTS["redis"],
        "storage_fingerprint": ENDPOINT_FINGERPRINTS["storage"],
    }
    values.update(changes)
    return WorkerRegistrationClaims(**values)


def _service(session_factory, clock: MutableClock) -> WorkerRegistrationService:
    async def principal_resolver(_db) -> str:
        return DATABASE_PRINCIPAL

    return WorkerRegistrationService(
        session_factory,
        clock=clock,
        lease_secret_factory=lambda: LEASE_SECRET,
        database_principal_resolver=principal_resolver,
        allow_non_postgres_test_backend=True,
    )


async def _rows(session_factory) -> list[WorkerRegistration]:
    async with session_factory() as db:
        return list(
            (
                await db.execute(
                    select(WorkerRegistration).order_by(
                        WorkerRegistration.lease_epoch
                    )
                )
            ).scalars()
        )


async def _assert_error(code: str, awaitable) -> WorkerRegistrationError:
    with pytest.raises(WorkerRegistrationError) as failure:
        await awaitable
    assert failure.value.code == code
    assert str(failure.value) == code
    return failure.value


def test_models_expose_generation_grant_registration_and_node_binding_fields() -> None:
    assert set(WorkerAdmissionGrant.__table__.columns.keys()) == {
        "id",
        "service_name",
        "generation",
        "worker_type",
        "worker_host",
        "capabilities_json",
        "release_commit",
        "image_identity",
        "database_principal",
        "redis_stream",
        "redis_group",
        "endpoint_bindings_json",
        "token_sha256",
        "state",
        "issued_at",
        "issued_by",
        "activated_at",
        "revoked_at",
        "revoke_reason",
        "created_at",
        "updated_at",
    }
    assert set(WorkerRegistration.__table__.columns.keys()) == {
        "id",
        "grant_id",
        "service_name",
        "worker_type",
        "worker_host",
        "capabilities_json",
        "worker_instance_id",
        "worker_slot",
        "redis_consumer_id",
        "image_identity",
        "database_principal",
        "database_fingerprint",
        "redis_fingerprint",
        "storage_fingerprint",
        "lease_epoch",
        "lease_secret_sha256",
        "status",
        "registered_at",
        "heartbeat_at",
        "lease_expires_at",
        "revoked_at",
        "revoke_reason",
        "superseded_by",
    }
    assert NodeExecution.__table__.columns[
        "worker_registration_id"
    ].nullable
    assert NodeExecution.__table__.columns["worker_lease_epoch"].nullable
    grant_constraints = {
        constraint.name
        for constraint in WorkerAdmissionGrant.__table__.constraints
        if constraint.name
    }
    registration_constraints = {
        constraint.name
        for constraint in WorkerRegistration.__table__.constraints
        if constraint.name
    }
    assert grant_constraints >= {
        "uq_worker_admission_grants_service_generation",
        "uq_worker_admission_grants_token_sha256",
        "ck_worker_admission_grant_generation_positive",
        "ck_worker_admission_grant_release_commit",
        "ck_worker_admission_grant_state",
        "ck_worker_admission_grant_lifecycle",
    }
    assert registration_constraints >= {
        "uq_worker_registrations_service_epoch",
        "ck_worker_registration_slot_positive",
        "ck_worker_registration_lease_secret_sha256",
        "ck_worker_registration_status",
        "ck_worker_registration_epoch_positive",
        "ck_worker_registration_revocation_state",
        "ck_worker_registration_supersession",
    }
    assert {
        index.name for index in WorkerAdmissionGrant.__table__.indexes
    } >= {"uq_worker_admission_grants_active_service"}
    assert {
        index.name for index in WorkerRegistration.__table__.indexes
    } >= {"uq_worker_registrations_active_service"}
    assert "database_principal" not in {
        field.name for field in fields(WorkerRegistrationClaims)
    }
    task_one_checks = [
        constraint
        for table in (
            WorkerAdmissionGrant.__table__,
            WorkerRegistration.__table__,
        )
        for constraint in table.constraints
        if constraint.name and constraint.name.startswith("ck_worker_")
    ]
    node_binding_check = next(
        constraint
        for constraint in NodeExecution.__table__.constraints
        if constraint.name == "ck_node_execution_worker_lease_binding"
    )
    assert all(
        "IS TRUE" in str(constraint.sqltext).upper()
        for constraint in (*task_one_checks, node_binding_check)
    )


@pytest.mark.parametrize(
    ("setup", "code"),
    [
        ("missing", "grant_missing"),
        ("pending", "grant_disabled"),
        ("revoked", "grant_disabled"),
        ("wrong_token", "token_invalid"),
    ],
)
async def test_register_rejects_missing_inactive_or_wrong_generation_token(
    registration_store,
    setup: str,
    code: str,
) -> None:
    if setup != "missing":
        await _grant(
            registration_store,
            state=setup if setup in {"pending", "revoked"} else "active",
        )
    service = _service(registration_store, MutableClock())

    token = (
        OTHER_ADMISSION_TOKEN if setup == "wrong_token" else ADMISSION_TOKEN
    )
    await _assert_error(code, service.register(_claims(), token))
    assert await _rows(registration_store) == []


@pytest.mark.parametrize(
    "change",
    [
        {"generation": 8},
        {"worker_type": "ffmpeg"},
        {"worker_host": "ccttww-lap"},
        {"capabilities": ("media_gpu",)},
        {"capabilities": ()},
        {"capabilities": ("media_cpu", "media_cpu")},
        {"capabilities": ("arbitrary_graph_execution",)},
        {"release_commit": "f" * 40},
        {"image_identity": "vp-ffmpeg-worker-go:deploy-ffffffffffff"},
        {"redis_stream": "vp:tasks:wrong"},
        {"redis_group": "wrong-workers"},
        {
            "endpoint_bindings": {
                **ENDPOINT_BINDINGS,
                "redis": {
                    "database": 4,
                    "host": "vp-redis-swarm",
                    "port": 6379,
                    "scheme": "redis",
                },
            }
        },
        {"worker_slot": 0},
    ],
)
async def test_register_requires_exact_generation_and_all_bound_claims(
    registration_store,
    change: dict[str, object],
) -> None:
    await _grant(registration_store)
    service = _service(registration_store, MutableClock())

    await _assert_error(
        "claim_mismatch",
        service.register(_claims(**change), ADMISSION_TOKEN),
    )
    assert await _rows(registration_store) == []


async def test_register_binds_database_session_principal_without_caller_claim(
    registration_store,
) -> None:
    await _grant(registration_store)

    async def wrong_principal(_db) -> str:
        return "vp_worker_runtime_wrong_generation"

    service = WorkerRegistrationService(
        registration_store,
        clock=MutableClock(),
        lease_secret_factory=lambda: LEASE_SECRET,
        database_principal_resolver=wrong_principal,
        allow_non_postgres_test_backend=True,
    )

    await _assert_error(
        "database_principal_mismatch",
        service.register(_claims(), ADMISSION_TOKEN),
    )
    assert await _rows(registration_store) == []


async def test_register_returns_independent_lease_secret_and_persists_only_hashes(
    registration_store,
) -> None:
    await _grant(
        registration_store,
        capabilities=["media_cpu", "media_gpu"],
    )
    clock = MutableClock()
    service = _service(registration_store, clock)

    lease = await service.register(
        _claims(capabilities=("media_gpu", "media_cpu")),
        ADMISSION_TOKEN,
    )

    assert lease.service_name == "vp-ffmpeg-worker-go-swarm"
    assert lease.worker_instance_id == _claims().worker_instance_id
    assert lease.worker_slot == 1
    assert lease.lease_epoch == 1
    assert lease.lease_expires_at == START + timedelta(seconds=180)
    assert lease.heartbeat_interval_seconds == 60
    assert lease.lease_secret == LEASE_SECRET
    assert lease.lease_secret != ADMISSION_TOKEN
    assert LEASE_SECRET not in repr(lease)
    rows = await _rows(registration_store)
    assert len(rows) == 1
    assert rows[0].capabilities_json == ["media_cpu", "media_gpu"]
    assert rows[0].lease_secret_sha256 == _sha256(LEASE_SECRET)
    assert rows[0].database_principal == DATABASE_PRINCIPAL
    durable_text = repr(
        {
            column.name: getattr(rows[0], column.name)
            for column in WorkerRegistration.__table__.columns
        }
    )
    assert ADMISSION_TOKEN not in durable_text
    assert LEASE_SECRET not in durable_text
    assert not hasattr(rows[0], "admission_token")
    assert not hasattr(rows[0], "lease_secret")
    assert LEASE_DURATION_SECONDS == 180
    assert HEARTBEAT_INTERVAL_SECONDS == 60


async def test_heartbeat_at_sixty_seconds_extends_expiry_to_180_seconds(
    registration_store,
) -> None:
    await _grant(registration_store)
    clock = MutableClock()
    service = _service(registration_store, clock)
    lease = await service.register(_claims(), ADMISSION_TOKEN)
    clock.advance(60)

    renewed = await service.heartbeat(lease)

    assert renewed.registration_id == lease.registration_id
    assert renewed.lease_epoch == lease.lease_epoch
    assert renewed.lease_secret == lease.lease_secret
    assert renewed.lease_expires_at == START + timedelta(seconds=240)
    row = (await _rows(registration_store))[0]
    assert row.heartbeat_at.replace(tzinfo=timezone.utc) == START + timedelta(
        seconds=60
    )
    assert row.lease_expires_at.replace(
        tzinfo=timezone.utc
    ) == START + timedelta(seconds=240)


async def test_heartbeat_rejects_wrong_registration_specific_lease_secret(
    registration_store,
) -> None:
    await _grant(registration_store)
    service = _service(registration_store, MutableClock())
    lease = await service.register(_claims(), ADMISSION_TOKEN)

    await _assert_error(
        "lease_fenced",
        service.heartbeat(
            replace(lease, lease_secret=OTHER_LEASE_SECRET)
        ),
    )


async def test_replacement_revokes_slot_and_records_superseding_registration(
    registration_store,
) -> None:
    await _grant(registration_store)
    clock = MutableClock()
    service = _service(registration_store, clock)
    first = await service.register(_claims(), ADMISSION_TOKEN)
    clock.advance(1)
    second_claims = replace(
        _claims(),
        worker_instance_id=uuid.UUID(
            "22222222-2222-4222-8222-222222222222"
        ),
        redis_consumer_id="ffmpeg-go-worker@colima-127:22222222",
    )

    second = await service.register(second_claims, ADMISSION_TOKEN)

    assert second.lease_epoch == 2
    rows = await _rows(registration_store)
    assert [row.status for row in rows] == ["revoked", "active"]
    assert rows[0].revoked_at is not None
    assert rows[0].revoke_reason == "superseded"
    assert rows[0].superseded_by == rows[1].id
    await _assert_error("lease_fenced", service.heartbeat(first))


async def test_different_fixed_slot_replaces_the_single_active_service_lease(
    registration_store,
) -> None:
    await _grant(registration_store)
    service = _service(registration_store, MutableClock())
    first = await service.register(_claims(), ADMISSION_TOKEN)
    second = await service.register(
        _claims(
            worker_instance_id=uuid.UUID(
                "33333333-3333-4333-8333-333333333333"
            ),
            worker_slot=2,
            redis_consumer_id="ffmpeg-go-worker@colima-127:33333333",
        ),
        ADMISSION_TOKEN,
    )

    assert (first.lease_epoch, second.lease_epoch) == (1, 2)
    rows = await _rows(registration_store)
    assert [row.status for row in rows] == ["revoked", "active"]
    assert rows[0].superseded_by == rows[1].id


async def test_expired_lease_cannot_be_renewed_and_remains_a_past_active_row(
    registration_store,
) -> None:
    await _grant(registration_store)
    clock = MutableClock()
    service = _service(registration_store, clock)
    lease = await service.register(_claims(), ADMISSION_TOKEN)
    clock.advance(181)

    await _assert_error("lease_expired", service.heartbeat(lease))

    row = (await _rows(registration_store))[0]
    assert row.status == "active"
    assert row.lease_expires_at.replace(tzinfo=timezone.utc) < clock.current


async def test_revoke_uses_lease_secret_is_idempotent_and_fences_heartbeat(
    registration_store,
) -> None:
    await _grant(registration_store)
    clock = MutableClock()
    service = _service(registration_store, clock)
    lease = await service.register(_claims(), ADMISSION_TOKEN)
    clock.advance(3)

    await service.revoke(lease, reason="shutdown")
    await service.revoke(lease, reason="different-retry-reason")

    row = (await _rows(registration_store))[0]
    assert row.status == "revoked"
    assert row.revoke_reason == "shutdown"
    assert row.revoked_at.replace(tzinfo=timezone.utc) == clock.current
    await _assert_error("lease_fenced", service.heartbeat(lease))


def test_dependency_fingerprints_match_cross_language_fixture() -> None:
    fixture = json.loads(FINGERPRINT_FIXTURE.read_text())

    assert fixture["version"] == 1
    assert {case["name"] for case in fixture["cases"]} == {
        "minio",
        "not_applicable",
    }
    for case in fixture["cases"]:
        assert dependency_fingerprints(case["env"]) == case["sha256"]
        for dependency, canonical in case["canonical"].items():
            assert (
                hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                == case["sha256"][dependency]
            )
        assert all(
            secret not in json.dumps(case["canonical"])
            for key, secret in case["env"].items()
            if "SECRET" in key or key.endswith("_URL")
        )


def test_dependency_identities_use_effective_client_query_targets() -> None:
    assert _database_identity(
        {
            "DATABASE_URL": (
                "postgresql+asyncpg://worker:redacted@"
                "db-claimed:5432/claimed"
                "?host=db-actual&port=5544&database=actual"
            )
        }
    ) == {
        "database": "actual",
        "driver": "postgresql",
        "host": "db-actual",
        "port": 5544,
    }
    assert _redis_identity(
        {
            "REDIS_URL": (
                "redis://worker:redacted@redis-claimed:6379/0?db=7"
            )
        }
    ) == {
        "database": 7,
        "host": "redis-claimed",
        "port": 6379,
        "scheme": "redis",
    }


@pytest.mark.parametrize(
    "case",
    ENDPOINT_VALIDATION_CASES,
    ids=lambda case: case["name"],
)
async def test_endpoint_canonicalization_matches_shared_fixture(
    registration_store,
    case: dict[str, object],
) -> None:
    if "bindings_json" in case:
        if case.get("invalid_json"):
            with pytest.raises(ValueError, match="invalid JSON numeric"):
                json.loads(
                    case["bindings_json"],
                    parse_float=Decimal,
                    parse_constant=_reject_json_constant,
                )
            return
        bindings = json.loads(
            case["bindings_json"],
            parse_float=Decimal,
            parse_constant=_reject_json_constant,
        )
    else:
        bindings = deepcopy(ENDPOINT_BINDINGS)
    if "bindings" in case:
        bindings = deepcopy(case["bindings"])
    elif "replace" in case:
        dependency, field_name, value = case["replace"]
        bindings[dependency][field_name] = value
    elif "extra" in case:
        dependency, field_name, value = case["extra"]
        bindings[dependency][field_name] = value

    if case["accepted"]:
        normalized, _, fingerprints = _normalized_endpoint_bindings(bindings)
        expected_bindings = {
            name: json.loads(canonical)
            for name, canonical in case["canonical"].items()
        }
        assert normalized == expected_bindings
        assert fingerprints == case["sha256"]
    else:
        with pytest.raises(
            WorkerRegistrationError,
            match="claim_mismatch",
        ):
            _normalized_endpoint_bindings(bindings)
        expected_bindings = ENDPOINT_BINDINGS
        fingerprints = ENDPOINT_FINGERPRINTS

    await _grant(
        registration_store,
        endpoint_bindings=expected_bindings,
    )
    service = _service(registration_store, MutableClock())
    registration = service.register(
        _claims(
            endpoint_bindings=bindings,
            database_fingerprint=fingerprints["database"],
            redis_fingerprint=fingerprints["redis"],
            storage_fingerprint=fingerprints["storage"],
        ),
        ADMISSION_TOKEN,
    )
    if case["accepted"]:
        lease = await registration
        assert lease.lease_epoch == 1
        for dependency, canonical in case["canonical"].items():
            assert (
                hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                == case["sha256"][dependency]
            )
    else:
        await _assert_error("claim_mismatch", registration)


@pytest.mark.parametrize(
    "binary_float",
    [
        1.0,
        float("5432.0000000000001"),
    ],
    ids=["integral", "precision-rounded"],
)
def test_endpoint_bindings_reject_every_binary_float(
    binary_float: float,
) -> None:
    bindings = deepcopy(ENDPOINT_BINDINGS)
    bindings["database"]["port"] = binary_float

    with pytest.raises(WorkerRegistrationError, match="claim_mismatch"):
        _normalized_endpoint_bindings(bindings)


@pytest.mark.parametrize(
    "nonfinite",
    [
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
    ],
)
def test_endpoint_bindings_reject_nonfinite_decimal(
    nonfinite: Decimal,
) -> None:
    bindings = deepcopy(ENDPOINT_BINDINGS)
    bindings["redis"]["database"] = nonfinite

    with pytest.raises(WorkerRegistrationError, match="claim_mismatch"):
        _normalized_endpoint_bindings(bindings)


@pytest.mark.parametrize(
    "endpoint_bindings",
    [
        {**ENDPOINT_BINDINGS, "password": "secret"},
        {
            **ENDPOINT_BINDINGS,
            "database": {
                **ENDPOINT_BINDINGS["database"],
                "user": DATABASE_PRINCIPAL,
            },
        },
        {
            **ENDPOINT_BINDINGS,
            "redis": {
                **ENDPOINT_BINDINGS["redis"],
                "password": "secret",
            },
        },
        {
            **ENDPOINT_BINDINGS,
            "storage": {
                **ENDPOINT_BINDINGS["storage"],
                "access_key": "secret",
            },
        },
    ],
)
async def test_register_rejects_secret_bearing_or_extra_endpoint_fields(
    registration_store,
    endpoint_bindings: dict[str, object],
) -> None:
    await _grant(registration_store)
    service = _service(registration_store, MutableClock())

    await _assert_error(
        "claim_mismatch",
        service.register(
            _claims(endpoint_bindings=endpoint_bindings),
            ADMISSION_TOKEN,
        ),
    )


@pytest.mark.parametrize(
    ("field_name", "fingerprint"),
    [
        ("database_fingerprint", "0" * 64),
        ("redis_fingerprint", "1" * 64),
        ("storage_fingerprint", "2" * 64),
    ],
)
async def test_register_cryptographically_binds_fingerprints_to_endpoints(
    registration_store,
    field_name: str,
    fingerprint: str,
) -> None:
    await _grant(registration_store)
    service = _service(registration_store, MutableClock())

    await _assert_error(
        "claim_mismatch",
        service.register(
            _claims(**{field_name: fingerprint}),
            ADMISSION_TOKEN,
        ),
    )


@pytest.mark.parametrize(
    "field_name",
    [field.name for field in fields(WorkerRegistrationClaims)],
)
async def test_register_returns_stable_error_for_every_null_claim(
    registration_store,
    field_name: str,
) -> None:
    await _grant(registration_store)
    service = _service(registration_store, MutableClock())

    await _assert_error(
        "claim_mismatch",
        service.register(
            replace(_claims(), **{field_name: None}),
            ADMISSION_TOKEN,
        ),
    )


async def test_non_postgres_service_is_fail_closed_without_explicit_test_opt_in(
    registration_store,
) -> None:
    await _grant(registration_store)
    service = WorkerRegistrationService(registration_store)

    await _assert_error(
        "database_principal_privileged",
        service.register(_claims(), ADMISSION_TOKEN),
    )


async def test_test_fallback_rechecks_principal_on_heartbeat_and_revoke(
    registration_store,
) -> None:
    await _grant(registration_store)
    principal = DATABASE_PRINCIPAL

    async def principal_resolver(_db) -> str:
        return principal

    service = WorkerRegistrationService(
        registration_store,
        clock=MutableClock(),
        lease_secret_factory=lambda: LEASE_SECRET,
        database_principal_resolver=principal_resolver,
        allow_non_postgres_test_backend=True,
    )
    lease = await service.register(_claims(), ADMISSION_TOKEN)
    principal = "vp_worker_runtime_wrong_generation"

    await _assert_error(
        "database_principal_mismatch",
        service.heartbeat(lease),
    )
    await _assert_error(
        "database_principal_mismatch",
        service.revoke(lease),
    )


async def test_revoke_null_reason_returns_stable_claim_error(
    registration_store,
) -> None:
    await _grant(registration_store)
    service = _service(registration_store, MutableClock())
    lease = await service.register(_claims(), ADMISSION_TOKEN)

    await _assert_error(
        "claim_mismatch",
        service.revoke(lease, reason=None),
    )


@pytest.mark.parametrize(
    "env",
    [
        {},
        {
            "DATABASE_URL": "not-a-url",
            "REDIS_URL": "redis://vp-redis-swarm/0",
            "STORAGE_BACKEND": "not_applicable",
        },
        {
            "DATABASE_URL": "postgresql://localhost/videoprocess",
            "REDIS_URL": "redis://vp-redis-swarm/0",
            "STORAGE_BACKEND": "not_applicable",
        },
        {
            "DATABASE_URL": "postgresql://vp-postgres-swarm/videoprocess",
            "REDIS_URL": "redis://127.0.0.1/0",
            "STORAGE_BACKEND": "not_applicable",
        },
    ],
)
def test_dependency_fingerprints_reject_malformed_or_local_dependencies(
    env: dict[str, str],
) -> None:
    with pytest.raises(ValueError):
        dependency_fingerprints(env)


def _database_url(database: str) -> str:
    return f"{POSTGRES_URL.rsplit('/', 1)[0]}/{database}"


def _asyncpg_url(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for concurrent registration test",
)
async def test_concurrent_registration_serializes_takeover_by_service_slot() -> None:
    database = f"vp_worker_registration_service_{uuid.uuid4().hex}"
    role_name = f"vp_worker_runtime_{uuid.uuid4().hex[:16]}"
    runtime_schema = f"vp_worker_runtime_schema_{uuid.uuid4().hex[:12]}"
    admin_url = _database_url("postgres")
    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    target_url = _database_url(database)
    completed = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=BACKEND_ROOT,
        env={**os.environ, "DATABASE_URL": target_url},
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    engine = create_async_engine(target_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        await _grant(session_factory)
        password = uuid.uuid4().hex
        admin = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            await admin.execute(
                f'CREATE ROLE "{role_name}" LOGIN PASSWORD \'{password}\''
            )
            await admin.execute(
                f'GRANT EXECUTE ON FUNCTION '
                f'public.vp_worker_register('
                f'text,bigint,text,text,uuid,integer,text,'
                f'jsonb,text,text,text,text,jsonb,text,text,text,text,text) '
                f'TO "{role_name}"'
            )
            await admin.execute(
                f'GRANT EXECUTE ON FUNCTION '
                f'public.vp_worker_heartbeat(uuid,text,uuid,bigint,text) '
                f'TO "{role_name}"'
            )
            await admin.execute(
                f'CREATE SCHEMA "{runtime_schema}" '
                f'AUTHORIZATION "{role_name}"'
            )
            await admin.execute(
                f'ALTER ROLE "{role_name}" IN DATABASE "{database}" '
                f'SET search_path = "{runtime_schema}", pg_catalog'
            )
            await admin.execute(
                "UPDATE worker_admission_grants "
                "SET database_principal = $1",
                role_name,
            )
        finally:
            await admin.close()
        runtime_url = (
            f"postgresql+asyncpg://{role_name}:{password}"
            f"@{target_url.split('@', 1)[1]}"
        )
        runtime_engine = create_async_engine(runtime_url)
        runtime_factory = async_sessionmaker(
            runtime_engine,
            expire_on_commit=False,
        )
        first_service = WorkerRegistrationService(
            runtime_factory,
            clock=MutableClock(),
            lease_secret_factory=lambda: LEASE_SECRET,
        )
        second_service = WorkerRegistrationService(
            runtime_factory,
            clock=MutableClock(),
            lease_secret_factory=lambda: OTHER_LEASE_SECRET,
        )
        first_claims = _claims()
        second_claims = replace(
            first_claims,
            worker_instance_id=uuid.UUID(
                "44444444-4444-4444-8444-444444444444"
            ),
            redis_consumer_id="ffmpeg-go-worker@colima-127:44444444",
        )

        leases = await asyncio.gather(
            first_service.register(first_claims, ADMISSION_TOKEN),
            second_service.register(second_claims, ADMISSION_TOKEN),
        )

        assert {lease.lease_epoch for lease in leases} == {1, 2}
        rows = await _rows(session_factory)
        assert [row.status for row in rows].count("active") == 1
        stale = min(leases, key=lambda lease: lease.lease_epoch)
        await _assert_error(
            "lease_fenced",
            first_service.heartbeat(stale),
        )
        await runtime_engine.dispose()
    finally:
        await engine.dispose()
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await admin.execute(f'DROP ROLE IF EXISTS "{role_name}"')
        finally:
            await admin.close()
