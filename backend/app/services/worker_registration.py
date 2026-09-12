from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from ipaddress import ip_address
from urllib.parse import urlsplit

from redis.asyncio.connection import parse_url as parse_redis_url
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.worker_registration import (
    WorkerAdmissionGrant,
    WorkerRegistration,
)


LEASE_DURATION_SECONDS = 180
HEARTBEAT_INTERVAL_SECONDS = 60
SUPPORTED_CAPABILITIES = frozenset(
    {
        "media_cpu",
        "media_gpu",
        "vision_gpu",
        "youtube_publisher",
    }
)
_STABLE_ERROR_CODES = (
    "database_principal_privileged",
    "database_principal_mismatch",
    "grant_missing",
    "grant_disabled",
    "token_invalid",
    "claim_mismatch",
    "lease_expired",
    "lease_fenced",
)
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IMAGE_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}$"
)
_DEPENDENCY_NAME_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$",
    re.ASCII,
)
_DNS_LABEL_PATTERN = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    re.ASCII,
)
_LOCAL_HOSTS = frozenset(
    {"", "localhost", "127.0.0.1", "0.0.0.0", "::1"}
)
DatabasePrincipalResolver = Callable[[AsyncSession], Awaitable[str]]


class WorkerRegistrationError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class WorkerRegistrationClaims:
    service_name: str
    generation: int
    worker_type: str
    worker_host: str
    worker_instance_id: uuid.UUID
    worker_slot: int
    redis_consumer_id: str
    capabilities: tuple[str, ...]
    release_commit: str
    image_identity: str
    redis_stream: str
    redis_group: str
    endpoint_bindings: Mapping[str, object]
    database_fingerprint: str
    redis_fingerprint: str
    storage_fingerprint: str


@dataclass(frozen=True)
class WorkerLease:
    registration_id: uuid.UUID
    grant_id: uuid.UUID
    service_name: str
    worker_instance_id: uuid.UUID
    worker_slot: int
    redis_consumer_id: str
    lease_epoch: int
    lease_secret: str = field(repr=False)
    lease_expires_at: datetime
    heartbeat_interval_seconds: int = HEARTBEAT_INTERVAL_SECONDS


@dataclass(frozen=True)
class _NormalizedClaims:
    service_name: str
    generation: int
    worker_type: str
    worker_host: str
    worker_instance_id: uuid.UUID
    worker_slot: int
    redis_consumer_id: str
    capabilities: tuple[str, ...]
    release_commit: str
    image_identity: str
    redis_stream: str
    redis_group: str
    endpoint_bindings: dict[str, object]
    endpoint_bindings_json: str
    database_fingerprint: str
    redis_fingerprint: str
    storage_fingerprint: str


class WorkerRegistrationService:
    """PostgreSQL lease service with an explicit unit-test-only ORM fallback."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] | None = None,
        lease_secret_factory: Callable[[], str] | None = None,
        database_principal_resolver: DatabasePrincipalResolver | None = None,
        allow_non_postgres_test_backend: bool = False,
    ) -> None:
        if (
            allow_non_postgres_test_backend
            and database_principal_resolver is None
        ):
            raise ValueError(
                "non-PostgreSQL test backend requires a principal resolver"
            )
        self._session_factory = session_factory
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lease_secret_factory = (
            lease_secret_factory or (lambda: secrets.token_urlsafe(32))
        )
        self._database_principal_resolver = (
            database_principal_resolver or _session_user
        )
        self._allow_non_postgres_test_backend = (
            allow_non_postgres_test_backend
        )

    async def register(
        self,
        claims: WorkerRegistrationClaims,
        admission_token: str | bytes,
    ) -> WorkerLease:
        normalized = _normalize_claims(claims)
        token_sha256 = _secret_sha256(admission_token, "token_invalid")
        lease_secret = self._lease_secret_factory()
        lease_secret_sha256 = _secret_sha256(
            lease_secret,
            "claim_mismatch",
        )

        async with self._session_factory() as db:
            if db.get_bind().dialect.name == "postgresql":
                return await self._postgres_register(
                    db,
                    normalized,
                    token_sha256,
                    lease_secret,
                    lease_secret_sha256,
                )
            self._require_test_backend()
            try:
                return await self._orm_register(
                    db,
                    normalized,
                    token_sha256,
                    lease_secret,
                    lease_secret_sha256,
                )
            except DBAPIError as error:
                raise _registration_error(error) from None

    async def heartbeat(self, lease: WorkerLease) -> WorkerLease:
        lease_secret_sha256 = _secret_sha256(
            lease.lease_secret,
            "lease_fenced",
        )
        async with self._session_factory() as db:
            if db.get_bind().dialect.name == "postgresql":
                expires_at = await self._postgres_heartbeat(
                    db,
                    lease,
                    lease_secret_sha256,
                )
                return _renewed_lease(lease, expires_at)
            self._require_test_backend()
            try:
                return await self._orm_heartbeat(
                    db,
                    lease,
                    lease_secret_sha256,
                )
            except DBAPIError as error:
                raise _registration_error(error) from None

    async def revoke(
        self,
        lease: WorkerLease,
        *,
        reason: str = "shutdown",
    ) -> None:
        normalized_reason = _exact_nonempty(reason, 255)
        if normalized_reason is None:
            raise WorkerRegistrationError("claim_mismatch")
        lease_secret_sha256 = _secret_sha256(
            lease.lease_secret,
            "lease_fenced",
        )
        async with self._session_factory() as db:
            if db.get_bind().dialect.name == "postgresql":
                await self._postgres_revoke(
                    db,
                    lease,
                    lease_secret_sha256,
                    normalized_reason,
                )
                return
            self._require_test_backend()
            try:
                await self._orm_revoke(
                    db,
                    lease,
                    lease_secret_sha256,
                    normalized_reason,
                )
            except DBAPIError as error:
                raise _registration_error(error) from None

    def _require_test_backend(self) -> None:
        if not self._allow_non_postgres_test_backend:
            raise WorkerRegistrationError(
                "database_principal_privileged"
            )

    async def _postgres_register(
        self,
        db: AsyncSession,
        claims: _NormalizedClaims,
        token_sha256: str,
        lease_secret: str,
        lease_secret_sha256: str,
    ) -> WorkerLease:
        statement = text(
            """
            SELECT *
            FROM public.vp_worker_register(
                :service_name,
                :generation,
                :worker_type,
                :worker_host,
                :worker_instance_id,
                :worker_slot,
                :redis_consumer_id,
                CAST(:capabilities_json AS jsonb),
                :release_commit,
                :image_identity,
                :redis_stream,
                :redis_group,
                CAST(:endpoint_bindings_json AS jsonb),
                :database_fingerprint,
                :redis_fingerprint,
                :storage_fingerprint,
                :token_sha256,
                :lease_secret_sha256
            )
            """
        )
        try:
            async with db.begin():
                row = (
                    await db.execute(
                        statement,
                        {
                            **claims.__dict__,
                            "capabilities_json": json.dumps(
                                list(claims.capabilities),
                                separators=(",", ":"),
                            ),
                            "token_sha256": token_sha256,
                            "lease_secret_sha256": lease_secret_sha256,
                        },
                    )
                ).mappings().one()
        except DBAPIError as error:
            raise _registration_error(error) from None
        return WorkerLease(
            registration_id=row["registration_id"],
            grant_id=row["grant_id"],
            service_name=claims.service_name,
            worker_instance_id=claims.worker_instance_id,
            worker_slot=claims.worker_slot,
            redis_consumer_id=claims.redis_consumer_id,
            lease_epoch=row["lease_epoch"],
            lease_secret=lease_secret,
            lease_expires_at=_utc(row["lease_expires_at"]),
        )

    async def _postgres_heartbeat(
        self,
        db: AsyncSession,
        lease: WorkerLease,
        lease_secret_sha256: str,
    ) -> datetime:
        try:
            async with db.begin():
                expires_at = await db.scalar(
                    text(
                        """
                        SELECT public.vp_worker_heartbeat(
                            :registration_id,
                            :service_name,
                            :worker_instance_id,
                            :lease_epoch,
                            :lease_secret_sha256
                        )
                        """
                    ),
                    {
                        **_lease_parameters(lease),
                        "lease_secret_sha256": lease_secret_sha256,
                    },
                )
        except DBAPIError as error:
            raise _registration_error(error) from None
        if not isinstance(expires_at, datetime):
            raise WorkerRegistrationError("lease_fenced")
        return _utc(expires_at)

    async def _postgres_revoke(
        self,
        db: AsyncSession,
        lease: WorkerLease,
        lease_secret_sha256: str,
        reason: str,
    ) -> None:
        try:
            async with db.begin():
                await db.scalar(
                    text(
                        """
                        SELECT public.vp_worker_release(
                            :registration_id,
                            :service_name,
                            :worker_instance_id,
                            :lease_epoch,
                            :lease_secret_sha256,
                            :reason
                        )
                        """
                    ),
                    {
                        **_lease_parameters(lease),
                        "lease_secret_sha256": lease_secret_sha256,
                        "reason": reason,
                    },
                )
        except DBAPIError as error:
            raise _registration_error(error) from None

    async def _orm_register(
        self,
        db: AsyncSession,
        claims: _NormalizedClaims,
        token_sha256: str,
        lease_secret: str,
        lease_secret_sha256: str,
    ) -> WorkerLease:
        now = _utc(self._clock())
        expires_at = now + timedelta(seconds=LEASE_DURATION_SECONDS)
        async with db.begin():
            grants = list(
                (
                    await db.execute(
                        select(WorkerAdmissionGrant)
                        .where(
                            WorkerAdmissionGrant.service_name
                            == claims.service_name
                        )
                        .order_by(WorkerAdmissionGrant.generation)
                        .with_for_update()
                    )
                ).scalars()
            )
            if not grants:
                raise WorkerRegistrationError("grant_missing")
            grant = next(
                (
                    candidate
                    for candidate in grants
                    if candidate.generation == claims.generation
                ),
                None,
            )
            if grant is None:
                raise WorkerRegistrationError("claim_mismatch")
            if grant.state != "active":
                raise WorkerRegistrationError("grant_disabled")
            if not hmac.compare_digest(grant.token_sha256, token_sha256):
                raise WorkerRegistrationError("token_invalid")
            principal = _valid_principal(
                await self._database_principal_resolver(db)
            )
            if not hmac.compare_digest(
                grant.database_principal,
                principal,
            ):
                raise WorkerRegistrationError(
                    "database_principal_mismatch"
                )
            _require_matching_claims(grant, claims)

            active = list(
                (
                    await db.execute(
                        select(WorkerRegistration)
                        .where(
                            WorkerRegistration.service_name
                            == claims.service_name,
                            WorkerRegistration.status == "active",
                        )
                        .with_for_update()
                    )
                ).scalars()
            )
            for registration in active:
                registration.status = "revoked"
                registration.revoked_at = now
                registration.revoke_reason = "superseded"
            await db.flush()

            current_epoch = await db.scalar(
                select(func.max(WorkerRegistration.lease_epoch)).where(
                    WorkerRegistration.service_name
                    == claims.service_name
                )
            )
            registration = WorkerRegistration(
                grant_id=grant.id,
                service_name=claims.service_name,
                worker_type=claims.worker_type,
                worker_host=claims.worker_host,
                capabilities_json=list(claims.capabilities),
                worker_instance_id=claims.worker_instance_id,
                worker_slot=claims.worker_slot,
                redis_consumer_id=claims.redis_consumer_id,
                image_identity=claims.image_identity,
                database_principal=principal,
                database_fingerprint=claims.database_fingerprint,
                redis_fingerprint=claims.redis_fingerprint,
                storage_fingerprint=claims.storage_fingerprint,
                lease_epoch=int(current_epoch or 0) + 1,
                lease_secret_sha256=lease_secret_sha256,
                status="active",
                registered_at=now,
                heartbeat_at=now,
                lease_expires_at=expires_at,
            )
            db.add(registration)
            await db.flush()
            for superseded in active:
                superseded.superseded_by = registration.id
            await db.flush()
            lease = _lease_from(registration, lease_secret)
        return lease

    async def _orm_heartbeat(
        self,
        db: AsyncSession,
        lease: WorkerLease,
        lease_secret_sha256: str,
    ) -> WorkerLease:
        now = _utc(self._clock())
        renewed: WorkerLease | None = None
        expired = False
        async with db.begin():
            registration = await _locked_registration(db, lease)
            principal = _valid_principal(
                await self._database_principal_resolver(db)
            )
            if not hmac.compare_digest(
                registration.database_principal,
                principal,
            ):
                raise WorkerRegistrationError(
                    "database_principal_mismatch"
                )
            if registration.status != "active":
                raise WorkerRegistrationError("lease_fenced")
            if not hmac.compare_digest(
                registration.lease_secret_sha256,
                lease_secret_sha256,
            ):
                raise WorkerRegistrationError("lease_fenced")
            if _utc(registration.lease_expires_at) <= now:
                expired = True
            else:
                registration.heartbeat_at = now
                registration.lease_expires_at = now + timedelta(
                    seconds=LEASE_DURATION_SECONDS
                )
                await db.flush()
                renewed = _lease_from(
                    registration,
                    lease.lease_secret,
                )
        if expired:
            raise WorkerRegistrationError("lease_expired")
        if renewed is None:
            raise WorkerRegistrationError("lease_fenced")
        return renewed

    async def _orm_revoke(
        self,
        db: AsyncSession,
        lease: WorkerLease,
        lease_secret_sha256: str,
        reason: str,
    ) -> None:
        now = _utc(self._clock())
        async with db.begin():
            registration = await _locked_registration(db, lease)
            principal = _valid_principal(
                await self._database_principal_resolver(db)
            )
            if not hmac.compare_digest(
                registration.database_principal,
                principal,
            ):
                raise WorkerRegistrationError(
                    "database_principal_mismatch"
                )
            if not hmac.compare_digest(
                registration.lease_secret_sha256,
                lease_secret_sha256,
            ):
                raise WorkerRegistrationError("lease_fenced")
            if registration.status != "active":
                return
            registration.status = "revoked"
            registration.revoked_at = now
            registration.revoke_reason = reason


def dependency_fingerprints(env: Mapping[str, str]) -> dict[str, str]:
    canonical = {
        "database": _database_identity(env),
        "redis": _redis_identity(env),
        "storage": _storage_identity(env),
    }
    return {
        name: hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
        for name, value in canonical.items()
    }


def _normalize_claims(
    claims: WorkerRegistrationClaims,
) -> _NormalizedClaims:
    try:
        instance_id = uuid.UUID(str(claims.worker_instance_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise WorkerRegistrationError("claim_mismatch") from error
    if (
        type(claims.generation) is not int
        or claims.generation <= 0
        or type(claims.worker_slot) is not int
        or claims.worker_slot <= 0
    ):
        raise WorkerRegistrationError("claim_mismatch")
    values = {
        "service_name": _exact_nonempty(claims.service_name, 255),
        "worker_type": _exact_nonempty(claims.worker_type, 64),
        "worker_host": _exact_nonempty(claims.worker_host, 255),
        "redis_consumer_id": _exact_nonempty(
            claims.redis_consumer_id,
            255,
        ),
        "redis_stream": _exact_nonempty(claims.redis_stream, 255),
        "redis_group": _exact_nonempty(claims.redis_group, 255),
    }
    if any(value is None for value in values.values()):
        raise WorkerRegistrationError("claim_mismatch")
    capabilities = _normalize_capabilities(claims.capabilities)
    if (
        not isinstance(claims.release_commit, str)
        or _COMMIT_PATTERN.fullmatch(claims.release_commit) is None
    ):
        raise WorkerRegistrationError("claim_mismatch")
    if (
        not isinstance(claims.image_identity, str)
        or _IMAGE_PATTERN.fullmatch(claims.image_identity) is None
    ):
        raise WorkerRegistrationError("claim_mismatch")
    fingerprints = (
        claims.database_fingerprint,
        claims.redis_fingerprint,
        claims.storage_fingerprint,
    )
    if any(
        not isinstance(value, str)
        or _SHA256_PATTERN.fullmatch(value) is None
        for value in fingerprints
    ):
        raise WorkerRegistrationError("claim_mismatch")
    (
        endpoint_bindings,
        endpoint_bindings_json,
        expected_fingerprints,
    ) = _normalized_endpoint_bindings(
        claims.endpoint_bindings
    )
    supplied_fingerprints = {
        "database": claims.database_fingerprint,
        "redis": claims.redis_fingerprint,
        "storage": claims.storage_fingerprint,
    }
    if any(
        not hmac.compare_digest(
            supplied_fingerprints[name],
            expected_fingerprints[name],
        )
        for name in expected_fingerprints
    ):
        raise WorkerRegistrationError("claim_mismatch")
    return _NormalizedClaims(
        service_name=values["service_name"] or "",
        generation=claims.generation,
        worker_type=values["worker_type"] or "",
        worker_host=values["worker_host"] or "",
        worker_instance_id=instance_id,
        worker_slot=claims.worker_slot,
        redis_consumer_id=values["redis_consumer_id"] or "",
        capabilities=capabilities,
        release_commit=claims.release_commit,
        image_identity=claims.image_identity,
        redis_stream=values["redis_stream"] or "",
        redis_group=values["redis_group"] or "",
        endpoint_bindings=endpoint_bindings,
        endpoint_bindings_json=endpoint_bindings_json,
        database_fingerprint=claims.database_fingerprint,
        redis_fingerprint=claims.redis_fingerprint,
        storage_fingerprint=claims.storage_fingerprint,
    )


def _normalize_capabilities(values: Sequence[str]) -> tuple[str, ...]:
    if not values or any(
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or value not in SUPPORTED_CAPABILITIES
        for value in values
    ):
        raise WorkerRegistrationError("claim_mismatch")
    if len(set(values)) != len(values):
        raise WorkerRegistrationError("claim_mismatch")
    return tuple(sorted(values))


def _require_matching_claims(
    grant: WorkerAdmissionGrant,
    claims: _NormalizedClaims,
) -> None:
    try:
        grant_capabilities = _normalize_capabilities(
            grant.capabilities_json
        )
        grant_endpoints, _, _ = _normalized_endpoint_bindings(
            grant.endpoint_bindings_json
        )
    except WorkerRegistrationError:
        raise WorkerRegistrationError("claim_mismatch") from None
    if (
        grant.service_name != claims.service_name
        or grant.generation != claims.generation
        or grant.worker_type != claims.worker_type
        or grant.worker_host != claims.worker_host
        or grant_capabilities != claims.capabilities
        or grant.release_commit != claims.release_commit
        or grant.image_identity != claims.image_identity
        or grant.redis_stream != claims.redis_stream
        or grant.redis_group != claims.redis_group
        or grant_endpoints != claims.endpoint_bindings
    ):
        raise WorkerRegistrationError("claim_mismatch")


async def _locked_registration(
    db: AsyncSession,
    lease: WorkerLease,
) -> WorkerRegistration:
    registration = (
        await db.execute(
            select(WorkerRegistration)
            .where(WorkerRegistration.id == lease.registration_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if registration is None or not _lease_matches(registration, lease):
        raise WorkerRegistrationError("lease_fenced")
    return registration


def _lease_matches(
    registration: WorkerRegistration,
    lease: WorkerLease,
) -> bool:
    return (
        registration.grant_id == lease.grant_id
        and registration.service_name == lease.service_name
        and registration.worker_instance_id == lease.worker_instance_id
        and registration.worker_slot == lease.worker_slot
        and registration.redis_consumer_id == lease.redis_consumer_id
        and registration.lease_epoch == lease.lease_epoch
    )


def _lease_from(
    registration: WorkerRegistration,
    lease_secret: str,
) -> WorkerLease:
    return WorkerLease(
        registration_id=registration.id,
        grant_id=registration.grant_id,
        service_name=registration.service_name,
        worker_instance_id=registration.worker_instance_id,
        worker_slot=registration.worker_slot,
        redis_consumer_id=registration.redis_consumer_id,
        lease_epoch=registration.lease_epoch,
        lease_secret=lease_secret,
        lease_expires_at=_utc(registration.lease_expires_at),
    )


def _renewed_lease(lease: WorkerLease, expires_at: datetime) -> WorkerLease:
    return WorkerLease(
        registration_id=lease.registration_id,
        grant_id=lease.grant_id,
        service_name=lease.service_name,
        worker_instance_id=lease.worker_instance_id,
        worker_slot=lease.worker_slot,
        redis_consumer_id=lease.redis_consumer_id,
        lease_epoch=lease.lease_epoch,
        lease_secret=lease.lease_secret,
        lease_expires_at=expires_at,
    )


def _lease_parameters(lease: WorkerLease) -> dict[str, object]:
    return {
        "registration_id": lease.registration_id,
        "service_name": lease.service_name,
        "worker_instance_id": lease.worker_instance_id,
        "lease_epoch": lease.lease_epoch,
    }


async def _session_user(db: AsyncSession) -> str:
    principal = await db.scalar(text("SELECT session_user"))
    return _valid_principal(principal)


def _valid_principal(value: object) -> str:
    principal = _exact_nonempty(value, 63)
    if principal is None:
        raise WorkerRegistrationError("database_principal_mismatch")
    return principal


def _secret_sha256(value: str | bytes, error_code: str) -> str:
    if isinstance(value, str):
        encoded = value.encode("utf-8")
    elif isinstance(value, bytes):
        encoded = value
    else:
        raise WorkerRegistrationError(error_code)
    if not encoded or len(encoded) > 4096:
        raise WorkerRegistrationError(error_code)
    return hashlib.sha256(encoded).hexdigest()


def _registration_error(error: DBAPIError) -> WorkerRegistrationError:
    message = str(error.orig)
    for code in _STABLE_ERROR_CODES:
        if code in message:
            return WorkerRegistrationError(code)
    return WorkerRegistrationError("lease_fenced")


def _normalized_endpoint_bindings(
    value: Mapping[str, object],
) -> tuple[dict[str, object], str, dict[str, str]]:
    if not isinstance(value, Mapping):
        raise WorkerRegistrationError("claim_mismatch")
    normalized = dict(value)
    if set(normalized) != {"database", "redis", "storage"}:
        raise WorkerRegistrationError("claim_mismatch")
    database = _validated_database_binding(normalized["database"])
    redis = _validated_redis_binding(normalized["redis"])
    storage = _validated_storage_binding(normalized["storage"])
    endpoint_bindings: dict[str, object] = {
        "database": database,
        "redis": redis,
        "storage": storage,
    }
    canonical_dependencies = {
        "database": _canonical_json(database),
        "redis": _canonical_json(redis),
        "storage": _canonical_json(storage),
    }
    fingerprints = {
        name: hashlib.sha256(identity.encode("utf-8")).hexdigest()
        for name, identity in canonical_dependencies.items()
    }
    return endpoint_bindings, _canonical_json(endpoint_bindings), fingerprints


def _validated_database_binding(value: object) -> dict[str, object]:
    binding = _exact_mapping(
        value,
        {"database", "driver", "host", "port"},
    )
    database = _dependency_name(binding["database"])
    host = _dependency_host(binding["host"])
    port = _dependency_port(binding["port"])
    if binding["driver"] != "postgresql":
        raise WorkerRegistrationError("claim_mismatch")
    return {
        "database": database,
        "driver": "postgresql",
        "host": host,
        "port": port,
    }


def _validated_redis_binding(value: object) -> dict[str, object]:
    binding = _exact_mapping(
        value,
        {"database", "host", "port", "scheme"},
    )
    database = _integral_json_number(
        binding["database"],
        minimum=0,
        maximum=2147483647,
    )
    scheme = binding["scheme"]
    if scheme not in {"redis", "rediss"}:
        raise WorkerRegistrationError("claim_mismatch")
    return {
        "database": database,
        "host": _dependency_host(binding["host"]),
        "port": _dependency_port(binding["port"]),
        "scheme": scheme,
    }


def _validated_storage_binding(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise WorkerRegistrationError("claim_mismatch")
    if dict(value) == {"backend": "not_applicable"}:
        return {"backend": "not_applicable"}
    binding = _exact_mapping(
        value,
        {"backend", "bucket", "host", "port"},
    )
    if binding["backend"] != "minio":
        raise WorkerRegistrationError("claim_mismatch")
    return {
        "backend": "minio",
        "bucket": _dependency_name(binding["bucket"]),
        "host": _dependency_host(binding["host"]),
        "port": _dependency_port(binding["port"]),
    }


def _exact_mapping(
    value: object,
    keys: set[str],
) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise WorkerRegistrationError("claim_mismatch")
    normalized = dict(value)
    if set(normalized) != keys:
        raise WorkerRegistrationError("claim_mismatch")
    return normalized


def _dependency_name(value: object) -> str:
    name = _exact_nonempty(value, 255)
    if (
        name is None
        or _DEPENDENCY_NAME_PATTERN.fullmatch(name) is None
    ):
        raise WorkerRegistrationError("claim_mismatch")
    return name


def _dependency_host(value: object) -> str:
    host = _exact_nonempty(value, 253)
    if (
        host is None
        or host != _normalized_host(host)
        or not _is_valid_dependency_host(host)
    ):
        raise WorkerRegistrationError("claim_mismatch")
    return host


def _dependency_port(value: object) -> int:
    return _integral_json_number(value, minimum=1, maximum=65535)


def _integral_json_number(
    value: object,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if type(value) is int:
        normalized = value
    elif (
        isinstance(value, Decimal)
        and value.is_finite()
        and value == value.to_integral_value()
        and minimum <= value <= maximum
    ):
        normalized = int(value)
    else:
        raise WorkerRegistrationError("claim_mismatch")
    if not minimum <= normalized <= maximum:
        raise WorkerRegistrationError("claim_mismatch")
    return normalized


def _database_identity(env: Mapping[str, str]) -> dict[str, object]:
    raw_url = str(env.get("DATABASE_URL", "")).strip()
    identity, _ = _database_connection_identity(raw_url)
    return identity


def _database_connection_identity(
    raw_url: str,
) -> tuple[dict[str, object], str]:
    try:
        parsed = make_url(raw_url)
        driver = parsed.drivername.split("+", 1)[0].lower()
        _, connection_options = PGDialect_asyncpg().create_connect_args(
            parsed
        )
        raw_host = connection_options.get("host")
        raw_port = connection_options.get("port", 5432)
        raw_database = connection_options.get("database")
        raw_principal = connection_options.get("user")
    except (ArgumentError, TypeError, ValueError) as error:
        raise ValueError("invalid database dependency") from error
    if any(
        isinstance(value, (list, tuple))
        for value in (raw_host, raw_port, raw_database, raw_principal)
    ):
        raise ValueError("invalid database dependency")
    host = _normalized_host(
        raw_host if isinstance(raw_host, str) else None
    )
    database = (
        raw_database if isinstance(raw_database, str) else ""
    )
    principal = (
        raw_principal if isinstance(raw_principal, str) else ""
    )
    try:
        port = int(raw_port)
    except (TypeError, ValueError) as error:
        raise ValueError("invalid database dependency") from error
    if (
        driver not in {"postgres", "postgresql"}
        or not host
        or not _is_valid_dependency_host(host)
        or _DEPENDENCY_NAME_PATTERN.fullmatch(database) is None
        or not 1 <= port <= 65535
    ):
        raise ValueError("invalid database dependency")
    return (
        {
            "driver": "postgresql",
            "host": host,
            "port": port,
            "database": database,
        },
        principal,
    )


def _redis_identity(env: Mapping[str, str]) -> dict[str, object]:
    raw_url = str(env.get("REDIS_URL", "")).strip()
    parsed = urlsplit(raw_url)
    scheme = parsed.scheme.lower()
    try:
        connection_options = parse_redis_url(raw_url)
        raw_host = connection_options.get("host")
        raw_port = connection_options.get("port", 6379)
        database = connection_options.get("db", 0)
        port = int(raw_port)
    except (TypeError, ValueError) as error:
        raise ValueError("invalid Redis dependency") from error
    host = _normalized_host(
        raw_host if isinstance(raw_host, str) else None
    )
    if (
        scheme not in {"redis", "rediss"}
        or not host
        or not _is_valid_dependency_host(host)
        or not 1 <= port <= 65535
        or type(database) is not int
        or not 0 <= database <= 2147483647
    ):
        raise ValueError("invalid Redis dependency")
    return {
        "scheme": scheme,
        "host": host,
        "port": port,
        "database": database,
    }


def _storage_identity(env: Mapping[str, str]) -> dict[str, object]:
    backend = str(env.get("STORAGE_BACKEND", "")).strip().lower()
    if backend == "not_applicable":
        return {"backend": "not_applicable"}
    if backend != "minio":
        raise ValueError("invalid storage dependency")
    raw_endpoint = str(env.get("MINIO_ENDPOINT", "")).strip()
    parsed = urlsplit(
        raw_endpoint if "://" in raw_endpoint else f"http://{raw_endpoint}"
    )
    host = _normalized_host(parsed.hostname)
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("invalid storage dependency") from error
    bucket = str(env.get("MINIO_BUCKET", "")).strip()
    if (
        parsed.scheme not in {"http", "https"}
        or not host
        or not _is_valid_dependency_host(host)
        or port is not None
        and not 1 <= port <= 65535
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or _DEPENDENCY_NAME_PATTERN.fullmatch(bucket) is None
    ):
        raise ValueError("invalid storage dependency")
    return {
        "backend": "minio",
        "host": host,
        "port": port or (443 if parsed.scheme == "https" else 9000),
        "bucket": bucket,
    }


def _exact_nonempty(value: object, maximum_length: int) -> str | None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum_length
    ):
        return None
    return value


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _normalized_host(host: str | None) -> str:
    return (host or "").strip("[]").lower().removesuffix(".")


def _is_valid_dependency_host(host: str) -> bool:
    if (
        not host
        or len(host) > 253
        or not host.isascii()
        or host != host.lower()
        or host.endswith(".")
        or ":" in host
    ):
        return False
    if host.replace(".", "").isdigit():
        try:
            address = ip_address(host)
        except ValueError:
            return False
        return (
            address.version == 4
            and str(address) == host
            and not address.is_loopback
            and not address.is_unspecified
        )
    if host in _LOCAL_HOSTS:
        return False
    return all(
        _DNS_LABEL_PATTERN.fullmatch(label) is not None
        for label in host.split(".")
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
