from __future__ import annotations

import asyncio
import re
import uuid
from collections.abc import Mapping
from typing import Protocol

from app.services import worker_registration as registration_contract
from app.services.worker_registration import (
    WorkerLease,
    WorkerRegistrationClaims,
    WorkerRegistrationError,
    WorkerRegistrationService,
)


class _RegistrationService(Protocol):
    async def register(
        self,
        claims: WorkerRegistrationClaims,
        admission_token: str | bytes,
    ) -> WorkerLease: ...

    async def heartbeat(self, lease: WorkerLease) -> WorkerLease: ...

    async def revoke(
        self,
        lease: WorkerLease,
        *,
        reason: str = "shutdown",
    ) -> None: ...


def build_worker_registration_claims(
    env: Mapping[str, str],
    *,
    database_url: str,
    redis_url: str,
    worker_instance_id: uuid.UUID,
) -> WorkerRegistrationClaims:
    service_name = _required(env, "WORKER_SERVICE_NAME")
    worker_type = _required(env, "WORKER_TYPE").lower()
    worker_host = _required(env, "WORKER_HOST")
    generation = _positive_integer(env, "WORKER_ADMISSION_GENERATION")
    worker_slot = _positive_integer(env, "WORKER_SLOT")
    redis_stream = _required(env, "WORKER_REDIS_STREAM")
    redis_group = _required(env, "WORKER_REDIS_GROUP")
    capabilities = tuple(
        sorted(
            value.strip()
            for value in _required(env, "WORKER_CAPABILITIES").split(",")
            if value.strip()
        )
    )
    consumer_id = (
        f"{worker_type}-worker@{worker_host}:{worker_slot}:{worker_instance_id}"
    )
    fingerprint_env = dict(env)
    fingerprint_env["DATABASE_URL"] = database_url
    fingerprint_env["REDIS_URL"] = redis_url
    endpoint_bindings: dict[str, object] = {
        "database": registration_contract._database_identity(fingerprint_env),
        "redis": registration_contract._redis_identity(fingerprint_env),
        "storage": registration_contract._storage_identity(fingerprint_env),
    }
    normalized, _, fingerprints = (
        registration_contract._normalized_endpoint_bindings(endpoint_bindings)
    )
    release_commit = _required(env, "WORKER_RELEASE_COMMIT")
    embedded_commit = str(env.get("VP_BUILD_COMMIT", "")).strip()
    deploy_mode = str(env.get("DEPLOY_MODE", "shared")).strip().lower()
    if (
        deploy_mode in {"", "shared", "production"}
        and not embedded_commit
    ):
        raise WorkerRegistrationError("claim_mismatch")
    if embedded_commit and (
        re.fullmatch(r"[0-9a-f]{40}", embedded_commit) is None
        or embedded_commit != release_commit
    ):
        raise WorkerRegistrationError("claim_mismatch")
    return WorkerRegistrationClaims(
        service_name=service_name,
        generation=generation,
        worker_type=worker_type,
        worker_host=worker_host,
        worker_instance_id=worker_instance_id,
        worker_slot=worker_slot,
        redis_consumer_id=consumer_id,
        capabilities=capabilities,
        release_commit=release_commit,
        image_identity=_required(env, "WORKER_IMAGE_IDENTITY"),
        redis_stream=redis_stream,
        redis_group=redis_group,
        endpoint_bindings=normalized,
        database_fingerprint=fingerprints["database"],
        redis_fingerprint=fingerprints["redis"],
        storage_fingerprint=fingerprints["storage"],
    )


class PythonWorkerRegistration:
    def __init__(
        self,
        service: _RegistrationService | WorkerRegistrationService,
        claims: WorkerRegistrationClaims,
        admission_token: str | bytes,
        *,
        close_timeout_seconds: float = 5.0,
    ) -> None:
        if close_timeout_seconds <= 0:
            raise ValueError("close_timeout_seconds must be positive")
        self._service = service
        self._claims = claims
        self._admission_token = admission_token
        self._close_timeout_seconds = close_timeout_seconds
        self._lease: WorkerLease | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._lost = asyncio.Event()
        self._loss: WorkerRegistrationError | None = None
        self._heartbeat_lock = asyncio.Lock()
        self._closed = False

    @property
    def lease(self) -> WorkerLease:
        if self._lease is None:
            raise RuntimeError("worker registration has not started")
        return self._lease

    @property
    def redis_consumer_id(self) -> str:
        return self.lease.redis_consumer_id

    @property
    def redis_stream(self) -> str:
        return self._claims.redis_stream

    @property
    def redis_group(self) -> str:
        return self._claims.redis_group

    @property
    def worker_host(self) -> str:
        return self._claims.worker_host

    async def start(self) -> WorkerLease:
        if self._lease is not None:
            raise RuntimeError("worker registration already started")
        registered = await self._service.register(
            self._claims,
            self._admission_token,
        )
        self._lease = registered
        try:
            renewed = await self.heartbeat_now()
        except BaseException:
            await self._revoke_bounded(
                registered,
                reason="startup_failed",
            )
            raise
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name="worker-registration-heartbeat",
        )
        return renewed

    async def heartbeat_now(
        self,
        *,
        minimum_margin_seconds: float = 0,
    ) -> WorkerLease:
        """Refresh the lease; PostgreSQL fences enforce any required margin."""
        _ = minimum_margin_seconds
        async with self._heartbeat_lock:
            if self._lease is None:
                raise RuntimeError("worker registration has not started")
            if self._loss is not None:
                raise self._loss
            try:
                renewed = await self._service.heartbeat(self._lease)
            except WorkerRegistrationError as exc:
                self._mark_lost(exc)
                raise
            except Exception as exc:
                loss = WorkerRegistrationError("lease_fenced")
                self._mark_lost(loss)
                raise loss from exc
            self._lease = renewed
            return renewed

    async def wait_lost(self) -> WorkerRegistrationError:
        await self._lost.wait()
        assert self._loss is not None
        return self._loss

    async def close(self, *, reason: str = "shutdown") -> None:
        if self._closed:
            return
        self._closed = True
        heartbeat_task = self._heartbeat_task
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
        lease = self._lease
        if lease is None:
            return
        await self._revoke_bounded(lease, reason=reason)

    async def _revoke_bounded(
        self,
        lease: WorkerLease,
        *,
        reason: str,
    ) -> None:
        try:
            await asyncio.wait_for(
                self._service.revoke(lease, reason=reason),
                timeout=self._close_timeout_seconds,
            )
        except Exception:
            return

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.lease.heartbeat_interval_seconds)
                await self.heartbeat_now()
            except asyncio.CancelledError:
                raise
            except WorkerRegistrationError:
                return

    def _mark_lost(self, error: WorkerRegistrationError) -> None:
        if self._loss is None:
            self._loss = error
            self._lost.set()


def _required(env: Mapping[str, str], key: str) -> str:
    value = str(env.get(key, "")).strip()
    if not value:
        raise WorkerRegistrationError("claim_mismatch")
    return value


def _positive_integer(env: Mapping[str, str], key: str) -> int:
    value = _required(env, key)
    if not value.isdigit() or int(value) <= 0:
        raise WorkerRegistrationError("claim_mismatch")
    return int(value)
