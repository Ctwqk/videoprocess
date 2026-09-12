"""Operator-only arming, with no Manager identity HTTP or expanded SQL reads.

The protected manifest attests source completeness and durable task/channel/graph
relationships. Account proof is an operator's fresh channels.list(mine=True) in
the pinned Manager process, with exclusive ingress and credential continuity
checked at audit close. auth/status is NOT account proof. This loader checks
the attestation and pins, not that external observation; the live evidence
validator must independently establish it before calling a drill conclusive.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import stat
import uuid
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any

from app.services.job_execution_authority import NodeExecutionClaim
from app.services.worker_registration import WorkerLease
from app.services.youtube_upload_operations import UploadOperationContext
from worker.task_delivery import WorkerTaskDelivery
from worker.youtube_ack_drill import (
    AckDrillArmingIdentity, AckDrillTarget, OwnedUnlistedAckDrill, _digest,
)

def _object(value: Any, keys: set[str]) -> dict[str, Any]:
    if type(value) is not dict or set(value) != keys:
        raise ValueError("ack drill manifest has missing or unknown fields")
    return value


def _uuid(value: Any) -> uuid.UUID:
    if not isinstance(value, str):
        raise ValueError("ack drill manifest requires canonical UUIDs")
    parsed = uuid.UUID(value)
    if not parsed.int or str(parsed) != value:
        raise ValueError("ack drill manifest requires canonical UUIDs")
    return parsed


def _sha256(value: Any) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("ack drill manifest requires SHA-256")


def _date(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("ack drill manifest requires offset timestamps")
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError("ack drill manifest requires offset timestamps")
    return parsed


def _version(value: Any) -> None:
    if type(value) is not int or value != 1:
        raise ValueError("ack drill manifest version is unsupported")


def _path(value: Any) -> Path:
    if not isinstance(value, str) or len(value) > 4096:
        raise ValueError("ack drill requires an absolute protected path")
    path = Path(value)
    if not path.is_absolute() or len(path.parts) < 2 or ".." in path.parts or str(path) != value:
        raise ValueError("ack drill requires an absolute protected path")
    return path


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("ack drill manifest contains duplicate JSON keys")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError("ack drill manifest contains nonfinite JSON")


def _file_identity(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _check_ancestors(
    ancestors: list[tuple[int, str, int, tuple[int, ...]]],
) -> tuple[tuple[int, ...], ...]:
    for parent, name, child, expected in ancestors:
        for info in (os.fstat(child), os.stat(name, dir_fd=parent, follow_symlinks=False)):
            if (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid) != expected:
                raise ValueError("ack drill manifest ancestor was replaced")
    return tuple(expected for _, _, _, expected in ancestors)


@dataclass(frozen=True)
class _ManifestRead:
    raw: bytes | None
    ancestors: tuple[tuple[int, ...], ...]


def _read_manifest(path: Path) -> _ManifestRead:
    # Retain every ancestor descriptor and recheck the full name chain after
    # reading. O_NOFOLLOW alone does not catch rename/replacement races.
    descriptors = [os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
    ancestors: list[tuple[int, str, int, tuple[int, ...]]] = []
    try:
        for part in path.parts[1:-1]:
            parent = descriptors[-1]
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            descriptors.append(child)
            info = os.fstat(child)
            if info.st_uid not in {0, os.geteuid()} or stat.S_IMODE(info.st_mode) & 0o022:
                raise ValueError("ack drill manifest ancestor is not protected")
            identity = (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid)
            ancestors.append((parent, part, child, identity))
        parent = descriptors[-1]
        try:
            fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        except FileNotFoundError:
            return _ManifestRead(None, _check_ancestors(ancestors))
        descriptors.append(fd)
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o400 or before.st_nlink != 1
            or not 0 < before.st_size <= 65536
        ):
            raise ValueError("ack drill manifest must be a bounded single-link owner-read-only file")
        chunks: list[bytes] = []
        remaining = before.st_size + 1
        while remaining:
            chunk = os.read(fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(fd)
        named = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        if len(raw) != before.st_size or _file_identity(before) != _file_identity(after) or (
            _file_identity(before) != _file_identity(named)
        ):
            raise ValueError("ack drill manifest changed during read")
        return _ManifestRead(raw, _check_ancestors(ancestors))
    finally:
        for fd in reversed(descriptors):
            os.close(fd)


@dataclass(frozen=True)
class AckDrillArming:
    manifest_path: Path
    execution_claim: NodeExecutionClaim
    release_commit: str
    service_name: str
    redis_stream: str
    consumer_group: str
    message_id: str
    payload_sha256: str
    dispatch_key: str
    attestation_id: str

    @classmethod
    def from_environment(
        cls, *, worker_type: str | None, worker_lease: WorkerLease | None,
        execution_claim: NodeExecutionClaim | None, delivery: WorkerTaskDelivery | None,
    ) -> AckDrillArming | None:
        enabled = os.environ.get("VP_YOUTUBE_ACK_DRILL_ENABLED", "false")
        if enabled == "false":
            return None
        if enabled != "true":
            raise ValueError("ack drill enabled flag must be exactly true or false")
        manifest_path = _path(os.environ.get("VP_YOUTUBE_ACK_DRILL_MANIFEST"))
        from worker import registration

        env = os.environ
        if (
            worker_type != "youtube_publisher" or env.get("WORKER_TYPE") != "youtube_publisher"
            or env.get("WORKER_CONCURRENCY") != "1"
            or env.get("WORKER_CAPABILITIES") != "youtube_publisher"
            or env.get("YOUTUBE_PUBLISH_ENABLED") != "true"
            or env.get("PUBLIC_PUBLISH_ENABLED") != "false"
            or type(worker_lease) is not WorkerLease or type(execution_claim) is not NodeExecutionClaim
            or type(delivery) is not WorkerTaskDelivery
        ):
            raise ValueError("ack drill requires a dedicated registered unlisted publisher")
        if (
            execution_claim.worker_registration_id != worker_lease.registration_id
            or execution_claim.worker_lease_epoch != worker_lease.lease_epoch
            or execution_claim.worker_id != worker_lease.redis_consumer_id
            or worker_lease.lease_expires_at.utcoffset() is None
            or worker_lease.lease_expires_at <= datetime.now(timezone.utc)
            or env.get("WORKER_SERVICE_NAME") != worker_lease.service_name
            or env.get("WORKER_REDIS_STREAM") != delivery.redis_stream
            or env.get("WORKER_REDIS_GROUP") != delivery.consumer_group
            or not isinstance(delivery.dispatch_key, uuid.UUID) or not delivery.dispatch_key.int
            or not isinstance(delivery.attestation_id, uuid.UUID) or not delivery.attestation_id.int
        ):
            raise ValueError("ack drill runtime claim, service or delivery does not match admission")
        embedded = registration.EMBEDDED_BUILD_COMMIT
        if re.fullmatch(r"[0-9a-f]{40}", embedded) is None or env.get("WORKER_RELEASE_COMMIT") != embedded:
            raise ValueError("ack drill requires the full matching embedded release commit")
        return cls(
            manifest_path, execution_claim, embedded, worker_lease.service_name,
            delivery.redis_stream, delivery.consumer_group, delivery.message_id, delivery.payload_sha256,
            str(delivery.dispatch_key), str(delivery.attestation_id),
        )

    async def arm(
        self, context: UploadOperationContext, *, manager_origin: str, cancelled: asyncio.Event,
    ) -> OwnedUnlistedAckDrill:
        from worker.handlers.base import CancelledError

        deadline = monotonic() + 45.0
        ancestors: tuple[tuple[int, ...], ...] | None = None
        while True:
            if cancelled.is_set():
                raise CancelledError("youtube ack drill arming cancelled")
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ValueError("ack drill manifest wait timed out")
            reading = asyncio.create_task(asyncio.to_thread(
                self._read_and_validate, context, manager_origin, ancestors,
            ))
            cancellation = asyncio.create_task(cancelled.wait())
            try:
                done, _ = await asyncio.wait(
                    (reading, cancellation), timeout=remaining, return_when=asyncio.FIRST_COMPLETED,
                )
                if cancelled.is_set():
                    raise CancelledError("youtube ack drill arming cancelled")
                if reading not in done:
                    raise ValueError("ack drill manifest wait timed out")
                observed, helper = reading.result()
            finally:
                for task in (reading, cancellation):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(reading, cancellation, return_exceptions=True)
            if cancelled.is_set():
                raise CancelledError("youtube ack drill arming cancelled")
            ancestors = observed.ancestors
            if helper is not None:
                if monotonic() >= deadline:
                    raise ValueError("ack drill manifest wait timed out")
                helper.check_arming_before_claim(cancelled)
                return helper
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ValueError("ack drill manifest wait timed out")
            try:
                await asyncio.wait_for(cancelled.wait(), timeout=min(0.1, remaining))
            except TimeoutError:
                pass

    def _read_and_validate(
        self, context: UploadOperationContext, manager_origin: str,
        ancestors: tuple[tuple[int, ...], ...] | None,
    ) -> tuple[_ManifestRead, OwnedUnlistedAckDrill | None]:
        # This entire read-only operation, including persistent-state checks,
        # stays off the event loop and inside arm's cancellation/deadline race.
        observed = _read_manifest(self.manifest_path)
        if ancestors is not None and ancestors != observed.ancestors:
            raise ValueError("ack drill manifest ancestor was replaced while waiting")
        helper = None if observed.raw is None else self._validate(observed.raw, context, manager_origin)
        return observed, helper

    def _validate(
        self, raw: bytes, context: UploadOperationContext, manager_origin: str,
    ) -> OwnedUnlistedAckDrill:
        try:
            manifest = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                                  parse_constant=_reject_constant)
        except (UnicodeError, ValueError, RecursionError) as exc:
            raise ValueError("ack drill manifest JSON is invalid") from exc
        manifest = _object(manifest, {
            "version", "drill_id", "issued_at", "expires_at", "state_dir", "production_task_id",
            "manager_origin", "context", "arming_identity", "source_evidence",
            "owned_attestation_sha256", "account_attestation",
        })
        _version(manifest["version"])
        issued, expires = _date(manifest["issued_at"]), _date(manifest["expires_at"])
        now = datetime.now(timezone.utc)
        if not issued <= now < expires or not 0 < (expires - issued).total_seconds() <= 300:
            raise ValueError("ack drill manifest is expired or exceeds the five-minute window")
        expected_context = {
            "job_id": str(context.job_id), "node_execution_id": str(context.node_execution_id),
            "input_artifact_id": str(context.input_artifact_id), "content_sha256": context.content_sha256,
            "title": context.title, "privacy": context.privacy,
            "execution_claim": {
                "worker_id": context.execution_claim.worker_id,
                "started_at": context.execution_claim.started_at.isoformat(),
                "worker_registration_id": str(context.execution_claim.worker_registration_id),
                "worker_lease_epoch": context.execution_claim.worker_lease_epoch,
            },
        }
        if (
            context.execution_claim != self.execution_claim or context.privacy != "unlisted"
            or manifest["context"] != expected_context or manifest["manager_origin"] != manager_origin
            or type(manifest["context"]["execution_claim"]["worker_lease_epoch"]) is not int
        ):
            raise ValueError("ack drill manifest does not match the actual upload context or origin")
        metadata = _object(manifest["arming_identity"], {field.name for field in fields(AckDrillArmingIdentity)})
        identity = AckDrillArmingIdentity(**metadata)
        for key in ("release_commit", "service_name", "redis_stream", "consumer_group", "message_id",
                    "payload_sha256", "dispatch_key", "attestation_id"):
            if getattr(identity, key) != getattr(self, key):
                raise ValueError("ack drill manifest does not match runtime delivery or publisher")
        task_id = _uuid(manifest["production_task_id"])
        self._validate_sources(manifest, context, identity)
        self._validate_account(manifest["account_attestation"], identity, manager_origin, issued, expires, now)
        state_dir = _path(manifest["state_dir"])
        if state_dir == self.manifest_path.parent or state_dir in self.manifest_path.parents or (
            self.manifest_path.parent in state_dir.parents
        ):
            raise ValueError("ack drill mutable state must be separate from operator control directory")
        helper = OwnedUnlistedAckDrill(AckDrillTarget(
            context=context, production_task_id=task_id, drill_id=_uuid(manifest["drill_id"]),
            expires_at=expires, owned_attestation_sha256=manifest["owned_attestation_sha256"],
            manager_origin=manager_origin, arming_identity=identity,
        ), state_dir=state_dir)
        # Fail on prior/partial state before reserving; the helper still performs
        # its durable exclusive-start checks and never removes or rearms state.
        with helper._directory() as fd:
            helper._check_records(fd)
        return helper

    @staticmethod
    def _validate_sources(manifest: dict, context: UploadOperationContext, identity: AckDrillArmingIdentity) -> None:
        evidence = _object(manifest["source_evidence"], {
            "version", "production_task_id", "job_id", "channel_id", "account_id", "input_artifact_id",
            "content_sha256", "graph_sha256", "sources_complete", "sources",
        })
        _version(evidence["version"])
        _sha256(manifest["owned_attestation_sha256"])
        _sha256(evidence["graph_sha256"])
        expected = {
            "production_task_id": manifest["production_task_id"], "job_id": str(context.job_id),
            "channel_id": identity.channel_id, "account_id": identity.account_id,
            "input_artifact_id": str(context.input_artifact_id), "content_sha256": context.content_sha256,
        }
        if any(evidence[key] != value for key, value in expected.items()) or (
            evidence["sources_complete"] is not True or _digest(evidence) != manifest["owned_attestation_sha256"]
        ):
            raise ValueError("ack drill source attestation or output binding is invalid")
        sources = evidence["sources"]
        if type(sources) is not list or not 1 <= len(sources) <= 128:
            raise ValueError("ack drill requires a bounded complete owned source inventory")
        seen: set[uuid.UUID] = set()
        for source in sources:
            source = _object(source, {"asset_id", "content_sha256", "license", "provenance"})
            asset_id = _uuid(source["asset_id"])
            _sha256(source["content_sha256"])
            if asset_id in seen or source["license"] != "owned" or source["provenance"] != "generated":
                raise ValueError("ack drill rejects duplicate, external, mixed or unknown sources")
            seen.add(asset_id)

    @staticmethod
    def _validate_account(
        value: Any, identity: AckDrillArmingIdentity, origin: str,
        issued: datetime, expires: datetime, now: datetime,
    ) -> None:
        pins = {key: value for key, value in asdict(identity).items() if key in {
            "account_id", "platform_channel_id", "receiver_container_id", "receiver_image_id",
            "audit_window_id", "audit_cursor_sha256",
        }}
        attestation = _object(value, set(pins) | {
            "version", "method", "verified_at", "expires_at", "manager_origin",
            "exclusive_ingress", "credential_fingerprint_sha256",
        })
        _version(attestation["version"])
        _sha256(attestation["credential_fingerprint_sha256"])
        verified, until = _date(attestation["verified_at"]), _date(attestation["expires_at"])
        if (
            any(attestation[key] != expected for key, expected in pins.items())
            or attestation["method"] != "channels.list(mine=True)"
            or attestation["exclusive_ingress"] is not True or attestation["manager_origin"] != origin
            or not verified <= issued <= now < expires <= until
            or not 0 < (until - verified).total_seconds() <= 300
        ):
            raise ValueError("ack drill account attestation is stale or does not match exact pins")
