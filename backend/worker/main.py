from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import os
import socket
import tempfile
import time
import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import httpx
import redis.asyncio as aioredis
from redis.typing import EncodableT
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings
from app.models.artifact import Artifact, ArtifactKind
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.services.job_execution_authority import (
    JobExecutionAuthorityBlocked,
    NodeExecutionClaim,
    acknowledge_worker_task_delivery,
    authorize_worker_task_ack,
    claim_registered_worker_node,
    list_prepared_worker_event_emission_ids,
    load_prepared_worker_event_emission,
    lock_job_execution_authority,
    mark_worker_event_emitted,
    persist_registered_worker_artifact,
    prepare_worker_event_emission,
    require_active_execution_authority,
    require_matching_node_execution_claim,
    require_registered_worker_node_claim,
    require_worker_registration_lease,
    require_worker_task_ack_receipt,
)
from app.services.worker_admission import (
    WorkerAdmissionError,
    enforce_worker_admission_from_env,
)
from app.services.worker_registration import (
    WorkerLease,
    WorkerRegistrationError,
    WorkerRegistrationService,
)
from app.storage.base import StorageBackend
from app.storage.manager import get_storage
from worker.handlers import HANDLER_MAP
from worker.handlers.base import BaseHandler, CancelledError
from worker.handlers.youtube_upload import YouTubeUploadHandler
from worker.registration import (
    PythonWorkerRegistration,
    build_worker_registration_claims,
)
from worker.secret_config import (
    WorkerSecretError,
    load_worker_admission_token,
    load_worker_database_url,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

WORKER_TYPE = os.environ.get("WORKER_TYPE", "ffmpeg").strip() or "ffmpeg"
TASK_STREAM = f"vp:tasks:{WORKER_TYPE}"
EVENT_STREAM = "vp:events"
CONSUMER_GROUP = f"{WORKER_TYPE}-workers"
WORKER_HOST = os.environ.get("WORKER_HOST", socket.gethostname().split(".")[0]).strip() or "unknown"
WORKER_ID = f"{WORKER_TYPE}-worker@{WORKER_HOST}:{os.getpid()}"

PEL_RECLAIM_INTERVAL = 60  # seconds between periodic PEL reclaims
PEL_MIN_IDLE = int(os.environ.get("WORKER_PEL_MIN_IDLE_MS", "900000"))
HEARTBEAT_INTERVAL = int(os.environ.get("WORKER_HEARTBEAT_INTERVAL_SECONDS", "15"))
AFFINITY_WAIT_SECONDS = int(os.environ.get("WORKER_AFFINITY_WAIT_SECONDS", "20"))
AFFINITY_MAX_BOUNCES = int(os.environ.get("WORKER_AFFINITY_MAX_BOUNCES", "6"))
AFFINITY_RECLAIM_INTERVAL_SECONDS = 1.0
AFFINITY_RECLAIM_MIN_IDLE_MS = 500
REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS = float(os.environ.get("WORKER_REDIS_CONNECT_TIMEOUT_SECONDS", "5"))
REDIS_SOCKET_TIMEOUT_SECONDS = float(os.environ.get("WORKER_REDIS_SOCKET_TIMEOUT_SECONDS", "30"))
REDIS_HEALTH_CHECK_INTERVAL_SECONDS = int(os.environ.get("WORKER_REDIS_HEALTH_CHECK_INTERVAL_SECONDS", "30"))
ARTIFACT_DOWNLOAD_BASE_URL = os.environ.get(
    "VP_ARTIFACT_DOWNLOAD_BASE_URL",
    "http://vp-api-swarm:8080/api/v1",
).strip().rstrip("/")
ARTIFACT_DOWNLOAD_MAX_BYTES = int(
    os.environ.get("VP_ARTIFACT_DOWNLOAD_MAX_BYTES", str(10 * 1024 * 1024 * 1024))
)
ARTIFACT_DOWNLOAD_TOTAL_TIMEOUT_SECONDS = float(
    os.environ.get("VP_ARTIFACT_DOWNLOAD_TOTAL_TIMEOUT_SECONDS", "900")
)
REMOTE_ARTIFACT_CLEANUP_TIMEOUT_SECONDS = 15.0
EVENT_EMISSION_SEND_ATTEMPTS = 3
EVENT_EMISSION_RECONCILE_INTERVAL_SECONDS = 5.0
EVENT_EMISSION_RECONCILE_LIMIT = 50
WORKER_REDIS_CONTINUITY_MAX_AGE_SECONDS = 90

engine_db: AsyncEngine | None = None
worker_session: async_sessionmaker[AsyncSession] | None = None


@dataclass
class WorkerTaskDelivery:
    redis_stream: str
    consumer_group: str
    message_id: str
    payload_sha256: str
    dispatch_key: uuid.UUID | None
    attestation_id: uuid.UUID | None = None
    event_emission_id: uuid.UUID | None = None


_current_task_delivery: ContextVar[WorkerTaskDelivery | None] = ContextVar(
    "worker_task_delivery",
    default=None,
)

_IDEMPOTENT_EVENT_XADD_SCRIPT = """
local existing = redis.call('GET', KEYS[2])
if existing then
    return existing
end
local message_id = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 1))
redis.call('SET', KEYS[2], message_id)
return message_id
"""


def _canonical_task_payload_sha256(payload: dict) -> str:
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in payload.items()
    ):
        raise JobExecutionAuthorityBlocked(
            "registered worker task payload must contain strings"
        )
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def configure_worker_database(database_url: str | None = None) -> None:
    """Initialize worker DB state only after startup admission succeeds."""
    global engine_db, worker_session
    if engine_db is not None and worker_session is not None:
        return

    # Remote workers can hold idle DB connections long enough for the
    # server/network to close them, so proactively ping and recycle.
    engine_db = create_async_engine(
        database_url or settings.database_url,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    worker_session = async_sessionmaker(engine_db, expire_on_commit=False)


def get_worker_session() -> async_sessionmaker[AsyncSession]:
    if worker_session is None:
        configure_worker_database()
    assert worker_session is not None
    return worker_session


def _redis() -> aioredis.Redis:
    return aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
        socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
        health_check_interval=REDIS_HEALTH_CHECK_INTERVAL_SECONDS,
    )


async def _start_worker_registration(
    env: dict[str, str],
    database_url: str,
    admission_token: str,
) -> PythonWorkerRegistration:
    instance_id = uuid.uuid4()
    claims = build_worker_registration_claims(
        env,
        database_url=database_url,
        worker_instance_id=instance_id,
    )
    lifecycle = PythonWorkerRegistration(
        WorkerRegistrationService(get_worker_session()),
        claims,
        admission_token,
    )
    await lifecycle.start()
    return lifecycle


async def _require_worker_redis_continuity() -> None:
    try:
        async with get_worker_session()() as db:
            await db.execute(
                text(
                    "SELECT public.vp_require_worker_redis_continuity("
                    ":max_age_seconds)"
                ),
                {
                    "max_age_seconds": (
                        WORKER_REDIS_CONTINUITY_MAX_AGE_SECONDS
                    )
                },
            )
    except Exception:
        raise WorkerRegistrationError(
            "worker_redis_continuity_unready"
        ) from None


async def _require_worker_redis_identity(redis: aioredis.Redis) -> None:
    connection_pool = getattr(redis, "connection_pool", None)
    connection_kwargs = getattr(
        connection_pool,
        "connection_kwargs",
        {},
    )
    expected_user = connection_kwargs.get("username")
    if (
        not isinstance(expected_user, str)
        or not expected_user
        or expected_user == "default"
    ):
        raise WorkerRegistrationError("worker_redis_identity_unready")
    try:
        observed_user = await redis.acl_whoami()
    except Exception:
        raise WorkerRegistrationError(
            "worker_redis_identity_unready"
        ) from None
    if observed_user != expected_user:
        raise WorkerRegistrationError("worker_redis_identity_unready")


@dataclass(frozen=True)
class CancelState:
    job_id: uuid.UUID | None
    node_status: NodeStatus | None
    job_status: JobStatus | None
    is_cancelled: bool
    cancel_reason: str | None


@dataclass(frozen=True)
class InputArtifactSnapshot:
    id: uuid.UUID
    media_info: dict
    storage_backend: str
    storage_path: str
    filename: str
    file_size: int | None


async def _load_cancel_state(node_execution_id: str) -> CancelState:
    """Load node/job cancellation state for a worker task in a single DB session."""
    async with get_worker_session()() as db:
        ne = await db.get(NodeExecution, uuid.UUID(node_execution_id))
        if not ne:
            return CancelState(
                job_id=None,
                node_status=None,
                job_status=None,
                is_cancelled=False,
                cancel_reason=None,
            )

        job = await db.get(Job, ne.job_id)
        if ne.status == NodeStatus.CANCELLED:
            return CancelState(
                job_id=ne.job_id,
                node_status=ne.status,
                job_status=job.status if job else None,
                is_cancelled=True,
                cancel_reason="node_execution cancelled",
            )
        if job and job.status == JobStatus.CANCELLED:
            return CancelState(
                job_id=ne.job_id,
                node_status=ne.status,
                job_status=job.status,
                is_cancelled=True,
                cancel_reason="job cancelled",
            )

        return CancelState(
            job_id=ne.job_id,
            node_status=ne.status,
            job_status=job.status if job else None,
            is_cancelled=False,
            cancel_reason=None,
        )


async def _claim_node_execution(
    job_id: str,
    node_execution_id: str,
    *,
    worker_lease: WorkerLease | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> NodeExecutionClaim | None:
    """Atomically claim a queued node under durable execution authority."""

    try:
        resolved_job_id = uuid.UUID(job_id)
        resolved_node_id = uuid.UUID(node_execution_id)
    except ValueError:
        logger.error("Invalid worker execution ids job=%s node=%s", job_id, node_execution_id)
        return None

    factory = session_factory or get_worker_session()
    async with factory() as db:
        try:
            async with db.begin():
                worker_id = (
                    worker_lease.redis_consumer_id
                    if worker_lease is not None
                    else WORKER_ID
                )
                if worker_lease is not None:
                    delivery = _current_task_delivery.get()
                    if (
                        delivery is None
                        or not isinstance(delivery.dispatch_key, uuid.UUID)
                    ):
                        raise JobExecutionAuthorityBlocked(
                            "registered worker claim has no durable task dispatch"
                        )
                    claim, delivery.attestation_id = (
                        await claim_registered_worker_node(
                            db,
                            job_id=resolved_job_id,
                            node_execution_id=resolved_node_id,
                            registration_id=worker_lease.registration_id,
                            lease_epoch=worker_lease.lease_epoch,
                            worker_id=worker_id,
                            redis_stream=delivery.redis_stream,
                            consumer_group=delivery.consumer_group,
                            message_id=delivery.message_id,
                            payload_sha256=delivery.payload_sha256,
                            dispatch_key=delivery.dispatch_key,
                        )
                    )
                    return claim
                authority = await lock_job_execution_authority(
                    db,
                    resolved_job_id,
                    node_execution_id=resolved_node_id,
                )
                node = authority.node
                assert node is not None
                require_active_execution_authority(
                    authority,
                    job_statuses={JobStatus.RUNNING},
                    node_statuses={NodeStatus.QUEUED},
                )
                claimed_at = datetime.now(timezone.utc)
                claim = NodeExecutionClaim(
                    job_id=resolved_job_id,
                    node_execution_id=resolved_node_id,
                    worker_id=worker_id,
                    started_at=claimed_at,
                )
                node.status = NodeStatus.RUNNING
                node.started_at = claimed_at
                node.worker_id = worker_id
                node.worker_registration_id = None
                node.worker_lease_epoch = None
                await db.flush()
            return claim
        except JobExecutionAuthorityBlocked as exc:
            await db.rollback()
            logger.info(
                "Skipping stale worker delivery job=%s node=%s: %s",
                job_id,
                node_execution_id,
                exc,
            )
            return None


async def _require_current_node_execution_claim(
    claim: NodeExecutionClaim,
    *,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> None:
    """Require the same durable node claim under execution-authority locks."""

    factory = session_factory or get_worker_session()
    async with factory() as db:
        async with db.begin():
            if (
                getattr(claim, "worker_registration_id", None) is not None
                and _session_is_postgresql(db)
            ):
                await require_registered_worker_node_claim(db, claim)
                return
            authority = await lock_job_execution_authority(
                db,
                claim.job_id,
                node_execution_id=claim.node_execution_id,
            )
            require_active_execution_authority(
                authority,
                job_statuses={JobStatus.RUNNING},
                node_statuses={NodeStatus.RUNNING},
            )
            require_matching_node_execution_claim(authority, claim)
            if getattr(claim, "worker_registration_id", None) is not None:
                await require_worker_registration_lease(db, claim)


async def _persist_artifact_for_current_claim(
    claim: NodeExecutionClaim,
    *,
    filename: str,
    mime_type: str,
    file_size: int,
    storage_backend: str,
    storage_path: str,
    media_info: dict | None,
    before_persist: Callable[[], Awaitable[None]] | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> str:
    """Persist an artifact only while its exact worker claim remains authoritative."""

    factory = session_factory or get_worker_session()
    async with factory() as db:
        async with db.begin():
            if (
                getattr(claim, "worker_registration_id", None) is not None
                and _session_is_postgresql(db)
            ):
                await require_registered_worker_node_claim(db, claim)
                if before_persist is not None:
                    await before_persist()
                    await require_registered_worker_node_claim(db, claim)
                artifact_id = await persist_registered_worker_artifact(
                    db,
                    claim,
                    filename=filename,
                    mime_type=mime_type,
                    file_size=file_size,
                    storage_backend=storage_backend,
                    storage_path=storage_path,
                    media_info=media_info,
                )
                return str(artifact_id)
            authority = await lock_job_execution_authority(
                db,
                claim.job_id,
                node_execution_id=claim.node_execution_id,
            )
            require_active_execution_authority(
                authority,
                job_statuses={JobStatus.RUNNING},
                node_statuses={NodeStatus.RUNNING},
            )
            require_matching_node_execution_claim(authority, claim)
            if getattr(claim, "worker_registration_id", None) is not None:
                await require_worker_registration_lease(db, claim)
            if before_persist is not None:
                await before_persist()
                if claim.worker_registration_id is not None:
                    await require_worker_registration_lease(db, claim)
            artifact = Artifact(
                job_id=claim.job_id,
                node_execution_id=claim.node_execution_id,
                kind=ArtifactKind.INTERMEDIATE,
                filename=filename,
                mime_type=mime_type,
                file_size=file_size,
                storage_backend=storage_backend,
                storage_path=storage_path,
                media_info=media_info,
            )
            db.add(artifact)
            await db.flush()
            return str(artifact.id)


async def _report_failure_for_current_claim(
    claim: NodeExecutionClaim,
    job_id: str,
    node_execution_id: str,
    error: str,
) -> bool:
    try:
        await _require_current_node_execution_claim(claim)
        await _report_failure(job_id, node_execution_id, error, claim)
    except JobExecutionAuthorityBlocked as exc:
        logger.info(
            "Skipping stale worker failure event job=%s node=%s: %s",
            job_id,
            node_execution_id,
            exc,
        )
        return False
    except Exception:
        logger.exception(
            "Could not verify worker claim before failure event job=%s node=%s; "
            "leaving the task pending",
            job_id,
            node_execution_id,
        )
        raise

    return True


async def process_task(
    data: dict,
    *,
    worker_lease: WorkerLease | None = None,
    lease_refresher: Callable[..., Awaitable[object]] | None = None,
) -> NodeExecutionClaim | None:
    """Process a single node execution task."""
    job_id = data["job_id"]
    node_execution_id = data["node_execution_id"]
    node_type = data["node_type"]
    config = json.loads(data.get("config", "{}"))
    input_artifacts_map = json.loads(data.get("input_artifacts", "{}"))

    logger.info(f"Processing node {data['node_id']} (type={node_type}) for job {job_id}")

    claim = await _claim_node_execution(
        job_id,
        node_execution_id,
        worker_lease=worker_lease,
    )
    if claim is None:
        return None

    # Get handler
    handler_cls = HANDLER_MAP.get(node_type)
    if not handler_cls:
        reported = await _report_failure_for_current_claim(
            claim,
            job_id,
            node_execution_id,
            f"No handler for node type: {node_type}",
        )
        return claim if reported else None

    if node_type == "youtube_upload":
        try:
            config, input_artifacts_map = await _authoritative_youtube_upload_inputs(
                job_id=job_id,
                node_execution_id=node_execution_id,
                node_id=data["node_id"],
                input_artifacts_map=input_artifacts_map,
            )
        except Exception as exc:
            reported = await _report_failure_for_current_claim(
                claim,
                job_id,
                node_execution_id,
                str(exc),
            )
            return claim if reported else None
        config["_job_id"] = job_id
        config["_node_execution_id"] = node_execution_id
        config["_input_artifact_ids"] = dict(input_artifacts_map)
        config["_execution_claim"] = {
            "worker_id": claim.worker_id,
            "started_at": _claim_started_at_utc(claim),
        }
        if claim.worker_registration_id is not None:
            config["_execution_claim"].update(
                {
                    "worker_registration_id": str(
                        claim.worker_registration_id
                    ),
                    "worker_lease_epoch": claim.worker_lease_epoch,
                }
            )

    try:
        handler: BaseHandler
        if node_type == "youtube_upload":
            handler = YouTubeUploadHandler(
                session_factory=get_worker_session(),
                lease_refresher=lease_refresher,
            )
        else:
            handler = handler_cls()
    except Exception as exc:
        reported = await _report_failure_for_current_claim(
            claim,
            job_id,
            node_execution_id,
            str(exc),
        )
        if reported:
            logger.exception(
                "Failed to initialize handler for node %s",
                data["node_id"],
            )
        return claim if reported else None

    # Background task: periodically check cancel status and kill handler if needed
    cancel_check_task = None
    cancel_event = asyncio.Event()

    async def _cancel_watcher():
        while True:
            cancel_state = await _load_cancel_state(node_execution_id)
            if cancel_state.is_cancelled:
                logger.info(
                    "Cancel detected for node %s for job %s during execution: %s",
                    data["node_id"],
                    job_id,
                    cancel_state.cancel_reason,
                )
                handler.cancel()
                cancel_event.set()
                return
            await asyncio.sleep(2)

    temp_files: list[str] = []  # track temp files for cleanup (for MinIO)
    output_local_path: str | None = None
    output_storage: StorageBackend | None = None
    artifact_storage_backend: str | None = None
    artifact_storage_path: str | None = None
    remote_output_may_exist = False
    artifact_persisted = False
    try:
        cancel_check_task = asyncio.create_task(_cancel_watcher())

        # Resolve input artifact paths to local file paths
        input_paths: dict[str, str] = {}
        input_artifacts: list[tuple[str, InputArtifactSnapshot]] = []
        async with get_worker_session()() as db:
            for port_name, artifact_id_str in input_artifacts_map.items():
                input_artifact_id = uuid.UUID(artifact_id_str)
                artifact_row = await db.get(Artifact, input_artifact_id)
                if not artifact_row:
                    raise FileNotFoundError(f"Input artifact {artifact_id_str} not found")
                media_info = (
                    artifact_row.media_info
                    if isinstance(artifact_row.media_info, dict)
                    else {}
                )
                input_artifacts.append(
                    (
                        port_name,
                        InputArtifactSnapshot(
                            id=input_artifact_id,
                            media_info=dict(media_info),
                            storage_backend=artifact_row.storage_backend,
                            storage_path=artifact_row.storage_path,
                            filename=artifact_row.filename,
                            file_size=getattr(artifact_row, "file_size", None),
                        ),
                    )
                )

        input_artifact_meta: dict[str, dict] = {}
        for port_name, input_artifact in input_artifacts:
            input_artifact_meta[port_name] = input_artifact.media_info
            storage = get_storage(input_artifact.storage_backend)
            local_path = storage.get_local_path(input_artifact.storage_path)
            if local_path and not Path(local_path).is_file():
                local_path = await _download_artifact_with_cancel(
                    input_artifact,
                    cancel_event,
                )
                temp_files.append(local_path)
            elif not local_path:
                # MinIO or remote storage: download to temp file
                content = await storage.read(input_artifact.storage_path)
                ext = Path(input_artifact.filename).suffix or ".mp4"
                fd, tmp_path = tempfile.mkstemp(suffix=ext, prefix="vp_input_")
                os.close(fd)
                with open(tmp_path, "wb") as input_file:
                    input_file.write(content)
                local_path = tmp_path
                temp_files.append(tmp_path)
            input_paths[port_name] = local_path

        config["_input_artifact_meta"] = input_artifact_meta
        if node_type != "youtube_upload":
            config["_input_artifact_ids"] = dict(input_artifacts_map)

        # Prepare output path
        output_ext = _get_output_extension(node_type, config)
        output_filename = (
            f"{node_execution_id}-{_claim_generation_token(claim)}{output_ext}"
        )
        storage_prefix = (
            "artifacts"
            if settings.storage_backend == "local"
            else "staging/artifacts"
        )
        output_storage_path = (
            f"{storage_prefix}/{job_id}/{output_filename}"
        )
        output_local_dir = Path(settings.storage_local_root) / "artifacts" / job_id
        output_local_dir.mkdir(parents=True, exist_ok=True)
        output_local_path = str(output_local_dir / output_filename)

        cancel_state = await _load_cancel_state(node_execution_id)
        if cancel_event.is_set() or cancel_state.is_cancelled:
            handler.cancel()
            raise CancelledError(cancel_state.cancel_reason or "node cancelled before handler execution")
        try:
            await _require_current_node_execution_claim(claim)
        except JobExecutionAuthorityBlocked as exc:
            logger.info(
                "Execution claim lost for node %s for job %s before handler execution: %s",
                data["node_id"],
                job_id,
                exc,
            )
            handler.cancel()
            raise CancelledError("node execution claim changed before handler execution") from exc

        # Execute handler. Some handlers return artifact metadata and storage hints.
        handler_result = await handler.execute(config, input_paths, output_local_path)
        try:
            await _require_current_node_execution_claim(claim)
        except JobExecutionAuthorityBlocked as exc:
            logger.info(
                "Execution claim lost for node %s for job %s after handler execution: %s",
                data["node_id"],
                job_id,
                exc,
            )
            handler.cancel()
            Path(output_local_path).unlink(missing_ok=True)
            raise CancelledError("node execution claim changed after handler execution") from exc

        # Verify output exists
        if not os.path.exists(output_local_path):
            raise RuntimeError(f"Handler did not produce output file: {output_local_path}")

        file_size = os.path.getsize(output_local_path)

        artifact_storage_backend, artifact_storage_path = _resolve_artifact_storage(
            output_local_path=output_local_path,
            output_storage_path=output_storage_path,
        )
        artifact_media_info = None
        skip_upload = False

        if isinstance(handler_result, dict):
            artifact_media_info = {k: v for k, v in handler_result.items() if not k.startswith("_")}
            storage_path_override = handler_result.get("_storage_path")
            if storage_path_override:
                artifact_storage_path = str(storage_path_override)
            skip_upload = bool(handler_result.get("_skip_upload", False))

        # If using remote storage (MinIO), upload the output file unless the handler
        # already persisted the exact object and returned a storage-path override.
        output_storage = get_storage(settings.storage_backend)
        save_remote_output: Callable[[], Awaitable[None]] | None = None
        if settings.storage_backend != "local" and not skip_upload:
            async def save_remote_output() -> None:
                nonlocal remote_output_may_exist
                remote_output_may_exist = True
                with open(output_local_path, "rb") as output_file:
                    await output_storage.save(
                        artifact_storage_path,
                        output_file,
                    )

        if save_remote_output is None:
            output_artifact_id = await _persist_artifact_for_current_claim(
                claim,
                filename=output_filename,
                mime_type=_guess_mime(output_ext),
                file_size=file_size,
                storage_backend=artifact_storage_backend,
                storage_path=artifact_storage_path,
                media_info=artifact_media_info,
            )
        else:
            output_artifact_id = await _persist_artifact_for_current_claim(
                claim,
                filename=output_filename,
                mime_type=_guess_mime(output_ext),
                file_size=file_size,
                storage_backend=artifact_storage_backend,
                storage_path=artifact_storage_path,
                media_info=artifact_media_info,
                before_persist=save_remote_output,
            )
        artifact_persisted = True

        # Report success
        await _report_success(
            job_id,
            node_execution_id,
            output_artifact_id,
            claim,
        )
        logger.info(f"Node {data['node_id']} completed successfully")
        return claim

    except CancelledError:
        logger.info(f"Node {data['node_id']} cancelled, cleaning up")
        # Don't report failure — orchestrator already knows about the cancel
        return claim if worker_lease is None else None
    except Exception as e:
        reported = await _report_failure_for_current_claim(
            claim,
            job_id,
            node_execution_id,
            str(e),
        )
        if reported:
            logger.exception(f"Node {data['node_id']} failed")
        else:
            logger.info(
                "Node %s stopped after losing its execution claim",
                data["node_id"],
            )
            handler.cancel()
        return claim if reported else None
    finally:
        if cancel_check_task and not cancel_check_task.done():
            cancel_check_task.cancel()
            try:
                await cancel_check_task
            except asyncio.CancelledError:
                pass
        if output_local_path and (
            not artifact_persisted or artifact_storage_backend != "local"
        ):
            Path(output_local_path).unlink(missing_ok=True)
        if (
            not artifact_persisted
            and remote_output_may_exist
            and output_storage is not None
            and artifact_storage_path is not None
        ):
            await _cleanup_uncommitted_remote_output(
                output_storage,
                artifact_storage_path,
            )
        # Clean up any temp files downloaded from remote storage
        for tmp in temp_files:
            try:
                os.unlink(tmp)
            except OSError:
                pass


async def _cleanup_uncommitted_remote_output(
    storage: StorageBackend,
    storage_path: str,
) -> None:
    try:
        async with asyncio.timeout(REMOTE_ARTIFACT_CLEANUP_TIMEOUT_SECONDS):
            await storage.delete(storage_path)
    except TimeoutError:
        logger.error(
            "Timed out cleaning uncommitted remote output %s",
            storage_path,
        )
    except Exception:
        logger.exception(
            "Failed to clean uncommitted remote output %s",
            storage_path,
        )


async def _download_artifact_with_cancel(
    artifact: InputArtifactSnapshot,
    cancel_event: asyncio.Event,
) -> str:
    download_task = asyncio.create_task(_download_artifact_via_api(artifact))
    cancel_task = asyncio.create_task(cancel_event.wait())
    try:
        done, _pending = await asyncio.wait(
            {download_task, cancel_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if cancel_task in done and cancel_event.is_set():
            download_task.cancel()
            completed_path = None
            try:
                completed_path = await download_task
            except BaseException:
                pass
            if completed_path:
                try:
                    os.unlink(completed_path)
                except OSError:
                    pass
            raise CancelledError("node cancelled during input artifact download")
        return await download_task
    finally:
        for task in (download_task, cancel_task):
            if not task.done():
                task.cancel()
        for task in (download_task, cancel_task):
            try:
                await task
            except BaseException:
                pass


async def _download_artifact_via_api(artifact: Artifact | InputArtifactSnapshot) -> str:
    if not ARTIFACT_DOWNLOAD_BASE_URL:
        raise RuntimeError(
            f"Input artifact {artifact.id} is not present on this worker and "
            "VP_ARTIFACT_DOWNLOAD_BASE_URL is not configured"
        )
    if ARTIFACT_DOWNLOAD_MAX_BYTES <= 0:
        raise RuntimeError("VP_ARTIFACT_DOWNLOAD_MAX_BYTES must be positive")
    if ARTIFACT_DOWNLOAD_TOTAL_TIMEOUT_SECONDS <= 0:
        raise RuntimeError("VP_ARTIFACT_DOWNLOAD_TOTAL_TIMEOUT_SECONDS must be positive")

    expected_size = artifact.file_size
    if expected_size is not None and expected_size < 0:
        raise RuntimeError(f"Input artifact {artifact.id} has an invalid negative file size")
    if expected_size is not None and expected_size > ARTIFACT_DOWNLOAD_MAX_BYTES:
        raise RuntimeError(
            f"Input artifact {artifact.id} expected size {expected_size} exceeds "
            f"the configured download limit {ARTIFACT_DOWNLOAD_MAX_BYTES}"
        )
    download_limit = expected_size if expected_size is not None else ARTIFACT_DOWNLOAD_MAX_BYTES

    suffix = Path(artifact.filename).suffix or ".mp4"
    fd, temp_path = tempfile.mkstemp(suffix=suffix, prefix="vp_input_")
    os.close(fd)
    downloaded_size = 0
    digest = hashlib.sha256()
    url = f"{ARTIFACT_DOWNLOAD_BASE_URL}/artifacts/{artifact.id}/download"

    try:
        try:
            async with asyncio.timeout(ARTIFACT_DOWNLOAD_TOTAL_TIMEOUT_SECONDS):
                timeout = httpx.Timeout(60.0, connect=10.0)
                async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
                    async with client.stream("GET", url) as response:
                        if response.status_code != 200:
                            raise RuntimeError(
                                f"Artifact download API returned HTTP {response.status_code} "
                                f"for input artifact {artifact.id}"
                            )
                        with open(temp_path, "wb") as handle:
                            async for chunk in response.aiter_bytes():
                                if not chunk:
                                    continue
                                next_size = downloaded_size + len(chunk)
                                if next_size > download_limit:
                                    raise RuntimeError(
                                        f"Downloaded input artifact {artifact.id} exceeds "
                                        f"the allowed size {download_limit}"
                                    )
                                handle.write(chunk)
                                downloaded_size = next_size
                                digest.update(chunk)
        except TimeoutError as exc:
            raise RuntimeError(f"Input artifact {artifact.id} download timed out") from exc

        if expected_size is not None and downloaded_size != expected_size:
            raise RuntimeError(
                f"Downloaded input artifact {artifact.id} size mismatch: "
                f"expected {expected_size}, got {downloaded_size}"
            )

        media_info = artifact.media_info if isinstance(artifact.media_info, dict) else {}
        expected_digest = str(media_info.get("content_sha256") or "").strip().lower()
        if (
            len(expected_digest) == 64
            and all(char in "0123456789abcdef" for char in expected_digest)
            and digest.hexdigest() != expected_digest
        ):
            raise RuntimeError(f"Downloaded input artifact {artifact.id} content hash mismatch")

        return temp_path
    except BaseException:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


async def _authoritative_youtube_upload_inputs(
    *,
    job_id: str,
    node_execution_id: str,
    node_id: object,
    input_artifacts_map: object,
) -> tuple[dict, dict[str, str]]:
    if not isinstance(node_id, str) or not node_id:
        raise RuntimeError("youtube upload queue message has an invalid node id")
    if not isinstance(input_artifacts_map, dict) or set(input_artifacts_map) != {"input"}:
        raise RuntimeError("youtube upload queue message must contain exactly the input artifact port")

    try:
        authoritative_job_id = uuid.UUID(job_id)
        authoritative_node_execution_id = uuid.UUID(node_execution_id)
        queued_input_artifact_id = uuid.UUID(str(input_artifacts_map["input"]))
    except (AttributeError, TypeError, ValueError) as exc:
        raise RuntimeError("youtube upload queue message has invalid UUID identifiers") from exc

    async with get_worker_session()() as db:
        node_execution = await db.get(NodeExecution, authoritative_node_execution_id)
        if node_execution is None:
            raise RuntimeError("youtube upload node execution was not found")
        if node_execution.job_id != authoritative_job_id:
            raise RuntimeError("youtube upload node execution does not belong to the queued job")
        if node_execution.node_id != node_id:
            raise RuntimeError("youtube upload node id does not match the queued node")
        if node_execution.node_type != "youtube_upload":
            raise RuntimeError("youtube upload node type does not match the queued node")
        expected_input_ids = list(node_execution.input_artifact_ids or [])
        if expected_input_ids != [queued_input_artifact_id]:
            raise RuntimeError("youtube upload input artifacts do not match the node execution")
        artifact = await db.get(Artifact, queued_input_artifact_id)
        if artifact is None:
            raise RuntimeError("youtube upload input artifact was not found")
        if artifact.job_id != authoritative_job_id:
            raise RuntimeError("youtube upload input artifact does not belong to the queued job")
        if not isinstance(node_execution.node_config, dict):
            raise RuntimeError("youtube upload node configuration is invalid")
        return dict(node_execution.node_config), {"input": str(queued_input_artifact_id)}


async def _report_success(
    job_id: str,
    node_execution_id: str,
    artifact_id: str,
    claim: NodeExecutionClaim,
) -> None:
    r = _redis()
    try:
        payload: dict[EncodableT, EncodableT] = {
            "event": "node_completed",
            "job_id": job_id,
            "node_execution_id": node_execution_id,
            "output_artifact_id": artifact_id,
            "worker_id": claim.worker_id,
            "started_at": _claim_started_at_utc(claim),
        }
        if claim.worker_registration_id is not None:
            payload["worker_registration_id"] = str(
                claim.worker_registration_id
            )
            assert claim.worker_lease_epoch is not None
            payload["worker_lease_epoch"] = str(claim.worker_lease_epoch)
            _bind_registered_event_to_task_delivery(payload)
        await _xadd_event_for_claim(r, payload, claim)
    finally:
        await r.aclose()


async def _report_failure(
    job_id: str,
    node_execution_id: str,
    error: str,
    claim: NodeExecutionClaim,
) -> None:
    r = _redis()
    try:
        payload: dict[EncodableT, EncodableT] = {
            "event": "node_failed",
            "job_id": job_id,
            "node_execution_id": node_execution_id,
            "error": error[:2000],
            "worker_id": claim.worker_id,
            "started_at": _claim_started_at_utc(claim),
        }
        if claim.worker_registration_id is not None:
            payload["worker_registration_id"] = str(
                claim.worker_registration_id
            )
            assert claim.worker_lease_epoch is not None
            payload["worker_lease_epoch"] = str(claim.worker_lease_epoch)
            _bind_registered_event_to_task_delivery(payload)
        await _xadd_event_for_claim(r, payload, claim)
    finally:
        await r.aclose()


def _bind_registered_event_to_task_delivery(
    payload: dict[EncodableT, EncodableT],
) -> None:
    delivery = _current_task_delivery.get()
    if delivery is None:
        raise JobExecutionAuthorityBlocked(
            "registered worker event has no task delivery"
        )
    if not all(
        isinstance(value, str) and value.strip()
        for value in (
            delivery.redis_stream,
            delivery.consumer_group,
            delivery.message_id,
        )
    ):
        raise JobExecutionAuthorityBlocked(
            "registered worker event task delivery is invalid"
        )
    payload["task_stream"] = delivery.redis_stream
    payload["task_group"] = delivery.consumer_group
    payload["task_message_id"] = delivery.message_id
    if (
        len(delivery.payload_sha256) != 64
        or any(
            character not in "0123456789abcdef"
            for character in delivery.payload_sha256
        )
        or not isinstance(delivery.dispatch_key, uuid.UUID)
    ):
        raise JobExecutionAuthorityBlocked(
            "registered worker event task dispatch is invalid"
        )
    payload["task_payload_sha256"] = delivery.payload_sha256
    payload["task_dispatch_key"] = str(delivery.dispatch_key)


async def _xadd_event_for_claim(
    redis: aioredis.Redis,
    payload: dict[EncodableT, EncodableT],
    claim: NodeExecutionClaim,
) -> uuid.UUID | None:
    if getattr(claim, "worker_registration_id", None) is None:
        await redis.xadd(EVENT_STREAM, payload)
        return None
    delivery = _current_task_delivery.get()
    if delivery is None:
        raise JobExecutionAuthorityBlocked(
            "registered worker event has no exact task delivery"
        )
    canonical_payload: dict[str, str] = {}
    for key, value in payload.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise JobExecutionAuthorityBlocked(
                "registered worker event payload is not canonical"
            )
        canonical_payload[key] = value
    payload_sha256 = _canonical_task_payload_sha256(canonical_payload)
    async with get_worker_session()() as db:
        async with db.begin():
            if not _session_is_postgresql(db):
                authority = await lock_job_execution_authority(
                    db,
                    claim.job_id,
                    node_execution_id=claim.node_execution_id,
                )
                require_active_execution_authority(
                    authority,
                    job_statuses={JobStatus.RUNNING},
                    node_statuses={NodeStatus.RUNNING},
                )
                require_matching_node_execution_claim(authority, claim)
                await require_worker_registration_lease(db, claim)
                await redis.xadd(EVENT_STREAM, payload)
                delivery.event_emission_id = uuid.uuid4()
                return delivery.event_emission_id
            if not isinstance(delivery.attestation_id, uuid.UUID):
                raise JobExecutionAuthorityBlocked(
                    "registered worker event has no exact task attestation"
                )
            emission_id = await prepare_worker_event_emission(
                db,
                claim,
                attestation_id=delivery.attestation_id,
                redis_stream=EVENT_STREAM,
                consumer_group="orchestrator",
                payload_sha256=payload_sha256,
                payload=canonical_payload,
                event_type=canonical_payload["event"],
            )
    registration_id = claim.worker_registration_id
    lease_epoch = claim.worker_lease_epoch
    assert isinstance(registration_id, uuid.UUID)
    assert isinstance(lease_epoch, int)
    await _send_prepared_event_emission(
        redis,
        emission_id,
        registration_id=registration_id,
        lease_epoch=lease_epoch,
        max_attempts=EVENT_EMISSION_SEND_ATTEMPTS,
    )
    delivery.event_emission_id = emission_id
    return emission_id


async def _send_prepared_event_emission(
    redis: aioredis.Redis,
    emission_id: uuid.UUID,
    *,
    registration_id: uuid.UUID,
    lease_epoch: int,
    max_attempts: int = EVENT_EMISSION_SEND_ATTEMPTS,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> NodeExecutionClaim:
    if (
        not isinstance(emission_id, uuid.UUID)
        or not isinstance(registration_id, uuid.UUID)
        or type(lease_epoch) is not int
        or lease_epoch <= 0
        or type(max_attempts) is not int
        or not 1 <= max_attempts <= 5
    ):
        raise ValueError("invalid prepared event emission retry request")
    sessions = session_factory or get_worker_session()
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            async with sessions() as db:
                async with db.begin():
                    emission = await load_prepared_worker_event_emission(
                        db,
                        emission_id,
                        registration_id=registration_id,
                        lease_epoch=lease_epoch,
                    )
                    if (
                        emission.redis_stream != EVENT_STREAM
                        or emission.consumer_group != "orchestrator"
                        or emission.payload.get("event")
                        != emission.event_type
                        or _canonical_task_payload_sha256(
                            emission.payload
                        )
                        != emission.payload_sha256
                    ):
                        raise JobExecutionAuthorityBlocked(
                            "prepared worker event payload is invalid"
                        )
                    fields: list[str] = []
                    for key, value in sorted(
                        emission.payload.items()
                    ):
                        fields.extend((key, value))
                    message_id = await cast(
                        Awaitable[object],
                        redis.eval(
                            _IDEMPOTENT_EVENT_XADD_SCRIPT,
                            2,
                            emission.redis_stream,
                            (
                                "vp:worker-event-emission:"
                                f"{emission.id}"
                            ),
                            *fields,
                        ),
                    )
                    if isinstance(message_id, bytes):
                        message_id = message_id.decode()
                    if (
                        not isinstance(message_id, str)
                        or not message_id.strip()
                    ):
                        raise JobExecutionAuthorityBlocked(
                            "registered worker event message identity "
                            "is invalid"
                        )
                    await mark_worker_event_emitted(
                        db,
                        emission.claim,
                        emission_id=emission.id,
                        message_id=message_id.strip(),
                    )
            return emission.claim
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt + 1 < max_attempts:
                await asyncio.sleep(min(0.25 * (2**attempt), 1.0))
    assert last_error is not None
    raise last_error


async def _reconcile_prepared_worker_event_emissions(
    redis: aioredis.Redis,
    worker_lease: WorkerLease,
    *,
    limit: int = EVENT_EMISSION_RECONCILE_LIMIT,
) -> int:
    async with get_worker_session()() as db:
        async with db.begin():
            emission_ids = (
                await list_prepared_worker_event_emission_ids(
                    db,
                    registration_id=worker_lease.registration_id,
                    lease_epoch=worker_lease.lease_epoch,
                    limit=limit,
                )
            )
    emitted = 0
    for emission_id in emission_ids:
        try:
            await _send_prepared_event_emission(
                redis,
                emission_id,
                registration_id=worker_lease.registration_id,
                lease_epoch=worker_lease.lease_epoch,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "Prepared worker event replay deferred emission=%s",
                emission_id,
            )
        else:
            emitted += 1
    return emitted


async def _prepared_event_reconciler_loop(
    redis: aioredis.Redis,
    worker_lease: WorkerLease,
) -> None:
    while True:
        try:
            await _reconcile_prepared_worker_event_emissions(
                redis,
                worker_lease,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Prepared worker event reconciliation deferred")
        await asyncio.sleep(EVENT_EMISSION_RECONCILE_INTERVAL_SECONDS)


def _claim_generation_token(claim: NodeExecutionClaim) -> str:
    material = "\0".join(
        (
            str(claim.job_id),
            str(claim.node_execution_id),
            claim.worker_id,
            _claim_started_at_utc(claim),
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _claim_started_at_utc(claim: NodeExecutionClaim) -> str:
    started_at = claim.started_at
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    return started_at.astimezone(timezone.utc).isoformat()


def _get_output_extension(node_type: str, config: dict) -> str:
    """Determine output file extension based on node type and config."""
    if node_type in {"speech_to_subtitle", "subtitle_translate"}:
        return ".srt"
    if node_type == "subtitle_to_speech":
        return ".wav"
    if node_type == "material_library_ingest":
        return ".json"
    if node_type == "transcode":
        fmt = config.get("format", "mp4")
        return f".{fmt}"
    fmt = config.get("output_format", "mp4")
    if fmt:
        return f".{fmt}"
    return ".mp4"


def _guess_mime(ext: str) -> str:
    return {
        ".mp4": "video/mp4",
        ".mkv": "video/x-matroska",
        ".json": "application/json",
        ".webm": "video/webm",
        ".avi": "video/x-msvideo",
        ".mov": "video/quicktime",
        ".srt": "application/x-subrip",
        ".wav": "audio/wav",
        ".mp3": "audio/mpeg",
    }.get(ext, "video/mp4")


def _resolve_artifact_storage(*, output_local_path: str, output_storage_path: str) -> tuple[str, str]:
    storage_backend = settings.storage_backend
    if storage_backend == "local":
        return storage_backend, output_local_path
    return storage_backend, output_storage_path


async def _reclaim_pending(
    r: aioredis.Redis,
    *,
    worker_lease: WorkerLease | None = None,
) -> None:
    """Reclaim stale pending messages from any consumer in the group."""
    try:
        claimed = await r.xautoclaim(
            TASK_STREAM, CONSUMER_GROUP, WORKER_ID,
            min_idle_time=PEL_MIN_IDLE,
            start_id="0-0",
            count=50,
        )
        if claimed and len(claimed) > 1 and claimed[1]:
            for msg_id, data in claimed[1]:
                if data:
                    logger.info(f"Reclaimed pending task {msg_id}")
                    await _process_message(
                        r,
                        msg_id,
                        data,
                        worker_lease=worker_lease,
                    )
    except Exception:
        logger.exception("PEL reclaim failed")


async def _reclaim_preferred_pending(
    r: aioredis.Redis,
    *,
    worker_lease: WorkerLease,
    message_scheduler: (
        Callable[[str, dict], Awaitable[None]] | None
    ) = None,
) -> None:
    """Claim only exact pending messages that still prefer this worker host."""

    try:
        pending = await r.xpending_range(
            TASK_STREAM,
            CONSUMER_GROUP,
            "-",
            "+",
            50,
        )
        for item in pending:
            message_id = item.get("message_id")
            owner = item.get("consumer")
            idle_ms = item.get("time_since_delivered", 0)
            if isinstance(message_id, bytes):
                message_id = message_id.decode()
            if isinstance(owner, bytes):
                owner = owner.decode()
            if (
                not isinstance(message_id, str)
                or not message_id
                or owner == WORKER_ID
                or type(idle_ms) is not int
                or idle_ms < AFFINITY_RECLAIM_MIN_IDLE_MS
            ):
                continue
            exact = await r.xrange(
                TASK_STREAM,
                message_id,
                message_id,
                1,
            )
            if len(exact) != 1:
                continue
            exact_id, payload = exact[0]
            if isinstance(exact_id, bytes):
                exact_id = exact_id.decode()
            if exact_id != message_id or not isinstance(payload, dict):
                continue
            preferred_hosts = _parse_preferred_hosts(payload)
            if WORKER_HOST not in preferred_hosts:
                continue
            try:
                enqueued_at = int(
                    payload.get("affinity_enqueued_at", "0") or "0"
                )
            except (TypeError, ValueError):
                continue
            age_seconds = (
                max(0, int(time.time()) - enqueued_at)
                if enqueued_at
                else 0
            )
            if age_seconds >= AFFINITY_WAIT_SECONDS:
                continue
            claimed = await r.xclaim(
                TASK_STREAM,
                CONSUMER_GROUP,
                WORKER_ID,
                min_idle_time=AFFINITY_RECLAIM_MIN_IDLE_MS,
                message_ids=[message_id],
            )
            for claimed_id, claimed_payload in claimed or []:
                if isinstance(claimed_id, bytes):
                    claimed_id = claimed_id.decode()
                if claimed_id != message_id or not claimed_payload:
                    continue
                if message_scheduler is not None:
                    await message_scheduler(
                        claimed_id,
                        claimed_payload,
                    )
                else:
                    await _process_message(
                        r,
                        claimed_id,
                        claimed_payload,
                        worker_lease=worker_lease,
                    )
    except Exception:
        logger.exception("Preferred affinity PEL reclaim failed")


async def _heartbeat_message(r: aioredis.Redis, msg_id: str) -> None:
    """Keep a long-running task fresh in the PEL so other workers do not reclaim it."""
    while True:
        try:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            await r.xclaim(
                TASK_STREAM,
                CONSUMER_GROUP,
                WORKER_ID,
                min_idle_time=0,
                message_ids=[msg_id],
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Heartbeat failed for task %s", msg_id)


async def _process_message(
    r: aioredis.Redis,
    msg_id: str,
    data: dict,
    *,
    worker_lease: WorkerLease | None = None,
    lease_refresher: Callable[..., Awaitable[object]] | None = None,
) -> None:
    payload_sha256 = _canonical_task_payload_sha256(data)
    dispatch_key_raw = data.get("dispatch_key")
    try:
        dispatch_key = (
            uuid.UUID(dispatch_key_raw)
            if isinstance(dispatch_key_raw, str)
            else None
        )
    except ValueError as exc:
        raise JobExecutionAuthorityBlocked(
            "registered worker task dispatch key is invalid"
        ) from exc
    if worker_lease is not None and dispatch_key is None:
        raise JobExecutionAuthorityBlocked(
            "registered worker task has no durable dispatch key"
        )
    if await _maybe_defer_for_affinity(
        r,
        msg_id,
        data,
        worker_lease=worker_lease,
    ):
        return

    delivery_token = _current_task_delivery.set(
        WorkerTaskDelivery(
            redis_stream=TASK_STREAM,
            consumer_group=CONSUMER_GROUP,
            message_id=msg_id,
            payload_sha256=payload_sha256,
            dispatch_key=dispatch_key,
        )
    )
    try:
        heartbeat_task = asyncio.create_task(_heartbeat_message(r, msg_id))
        claim: NodeExecutionClaim | None = None
        try:
            if worker_lease is None:
                claim = await process_task(data)
            elif lease_refresher is not None:
                claim = await process_task(
                    data,
                    worker_lease=worker_lease,
                    lease_refresher=lease_refresher,
                )
            else:
                claim = await process_task(data, worker_lease=worker_lease)
        except Exception:
            logger.exception(
                "Unhandled error processing %s; leaving message pending because no "
                "durable execution claim is available",
                msg_id,
            )
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            if claim is not None:
                await _ack_message_for_claim(r, msg_id, claim)
    finally:
        _current_task_delivery.reset(delivery_token)


async def _ack_message_for_claim(
    redis: aioredis.Redis,
    message_id: str,
    claim: NodeExecutionClaim,
) -> None:
    if getattr(claim, "worker_registration_id", None) is None:
        await redis.xack(TASK_STREAM, CONSUMER_GROUP, message_id)
        return
    delivery = _current_task_delivery.get()
    if (
        delivery is None
        or delivery.message_id != message_id
        or not isinstance(delivery.dispatch_key, uuid.UUID)
        or not isinstance(delivery.attestation_id, uuid.UUID)
    ):
        raise JobExecutionAuthorityBlocked(
            "worker task acknowledgement has no exact dispatch delivery"
        )
    async with get_worker_session()() as db:
        async with db.begin():
            is_postgresql = _session_is_postgresql(db)
            if (
                is_postgresql
                and not isinstance(delivery.event_emission_id, uuid.UUID)
            ):
                raise JobExecutionAuthorityBlocked(
                    "worker task acknowledgement has no exact event emission"
                )
            if is_postgresql:
                await require_registered_worker_node_claim(db, claim)
            else:
                authority = await lock_job_execution_authority(
                    db,
                    claim.job_id,
                    node_execution_id=claim.node_execution_id,
                )
                require_matching_node_execution_claim(authority, claim)
                try:
                    await require_worker_registration_lease(db, claim)
                except JobExecutionAuthorityBlocked:
                    pass
                else:
                    await authorize_worker_task_ack(
                        db,
                        claim,
                        attestation_id=delivery.attestation_id,
                    )
            if is_postgresql:
                await authorize_worker_task_ack(
                    db,
                    claim,
                    attestation_id=delivery.attestation_id,
                )
            await require_worker_task_ack_receipt(
                db,
                claim,
                redis_stream=delivery.redis_stream,
                consumer_group=delivery.consumer_group,
                message_id=message_id,
                payload_sha256=delivery.payload_sha256,
                dispatch_key=delivery.dispatch_key,
            )
            result = await redis.xack(
                delivery.redis_stream,
                delivery.consumer_group,
                message_id,
            )
            _require_task_xack_result(result)
            await acknowledge_worker_task_delivery(
                db,
                claim,
                attestation_id=delivery.attestation_id,
                redis_stream=delivery.redis_stream,
                consumer_group=delivery.consumer_group,
                message_id=delivery.message_id,
                payload_sha256=delivery.payload_sha256,
                dispatch_key=delivery.dispatch_key,
            )


def _session_is_postgresql(db: object) -> bool:
    get_bind = getattr(db, "get_bind", None)
    if not callable(get_bind):
        return False
    bind = get_bind()
    dialect = getattr(bind, "dialect", None)
    return getattr(dialect, "name", None) == "postgresql"


def _require_task_xack_result(result: object) -> None:
    if type(result) is not int or result not in {0, 1}:
        raise RuntimeError("Redis worker task acknowledgement result is invalid")


def _parse_preferred_hosts(data: dict) -> list[str]:
    raw = data.get("preferred_hosts")
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


async def _maybe_defer_for_affinity(
    r: aioredis.Redis,
    msg_id: str,
    data: dict,
    *,
    worker_lease: WorkerLease | None = None,
) -> bool:
    preferred_hosts = _parse_preferred_hosts(data)
    if not preferred_hosts or WORKER_HOST in preferred_hosts:
        return False

    try:
        enqueued_at = int(data.get("affinity_enqueued_at", "0") or "0")
    except ValueError:
        enqueued_at = 0
    try:
        bounces = int(data.get("affinity_bounces", "0") or "0")
    except ValueError:
        bounces = 0

    now = int(time.time())
    age_seconds = max(0, now - enqueued_at) if enqueued_at else 0

    if bounces >= AFFINITY_MAX_BOUNCES or age_seconds >= AFFINITY_WAIT_SECONDS:
        logger.info(
            "Affinity relaxed for task %s on host %s (preferred=%s, age=%ss, bounces=%s)",
            msg_id, WORKER_HOST, preferred_hosts, age_seconds, bounces,
        )
        return False

    if worker_lease is not None:
        logger.info(
            "Leaving registered task %s pending for preferred host "
            "(current=%s preferred=%s age=%ss)",
            msg_id,
            WORKER_HOST,
            preferred_hosts,
            age_seconds,
        )
        return True

    bounced = dict(data)
    bounced["affinity_bounces"] = str(bounces + 1)
    if not bounced.get("affinity_enqueued_at"):
        bounced["affinity_enqueued_at"] = str(now)
    await r.xadd(TASK_STREAM, bounced)
    await r.xack(TASK_STREAM, CONSUMER_GROUP, msg_id)
    logger.info(
        "Deferred task %s on host %s for affinity (preferred=%s, age=%ss, bounce=%s)",
        msg_id, WORKER_HOST, preferred_hosts, age_seconds, bounces + 1,
    )
    return True


async def _run_until_registration_loss(
    registration: PythonWorkerRegistration,
    consumer: Awaitable[None],
) -> None:
    consumer_task = asyncio.ensure_future(consumer)
    loss_task = asyncio.create_task(registration.wait_lost())
    try:
        done, _ = await asyncio.wait(
            {consumer_task, loss_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if loss_task in done:
            raise loss_task.result()
        await consumer_task
    finally:
        for task in (consumer_task, loss_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(
            consumer_task,
            loss_task,
            return_exceptions=True,
        )


async def _consume_registered_worker(
    redis: aioredis.Redis,
    registration: PythonWorkerRegistration,
) -> None:
    global TASK_STREAM, CONSUMER_GROUP, WORKER_HOST, WORKER_ID
    previous_binding = (
        TASK_STREAM,
        CONSUMER_GROUP,
        WORKER_HOST,
        WORKER_ID,
    )
    TASK_STREAM = registration.redis_stream
    CONSUMER_GROUP = registration.redis_group
    WORKER_HOST = registration.worker_host
    WORKER_ID = registration.redis_consumer_id
    message_tasks: set[asyncio.Task[None]] = set()
    emission_reconciler_task: asyncio.Task[None] | None = None
    try:
        try:
            await redis.xgroup_create(
                TASK_STREAM,
                CONSUMER_GROUP,
                id="0",
                mkstream=True,
            )
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        concurrency = int(os.environ.get("WORKER_CONCURRENCY", "2"))
        semaphore = asyncio.Semaphore(concurrency)

        async def schedule_message(
            message_id: str,
            data: dict,
        ) -> None:
            await semaphore.acquire()

            async def run_message() -> None:
                try:
                    await _process_message(
                        redis,
                        message_id,
                        data,
                        worker_lease=registration.lease,
                        lease_refresher=registration.heartbeat_now,
                    )
                finally:
                    semaphore.release()

            task = asyncio.create_task(run_message())
            message_tasks.add(task)
            task.add_done_callback(message_tasks.discard)

        logger.info(
            "Worker %s started (concurrency=%s)",
            registration.redis_consumer_id,
            concurrency,
        )
        emission_reconciler_task = asyncio.create_task(
            _prepared_event_reconciler_loop(
                redis,
                registration.lease,
            ),
            name="worker-event-emission-reconciler",
        )
        await _reclaim_pending(
            redis,
            worker_lease=registration.lease,
        )
        last_reclaim = asyncio.get_event_loop().time()
        last_affinity_reclaim = 0.0

        while True:
            try:
                now = asyncio.get_event_loop().time()
                if (
                    now - last_affinity_reclaim
                    >= AFFINITY_RECLAIM_INTERVAL_SECONDS
                ):
                    await _reclaim_preferred_pending(
                        redis,
                        worker_lease=registration.lease,
                        message_scheduler=schedule_message,
                    )
                    last_affinity_reclaim = now
                if now - last_reclaim > PEL_RECLAIM_INTERVAL:
                    await _reclaim_pending(
                        redis,
                        worker_lease=registration.lease,
                    )
                    last_reclaim = now

                messages = await redis.xreadgroup(
                    CONSUMER_GROUP,
                    registration.redis_consumer_id,
                    {TASK_STREAM: ">"},
                    count=1,
                    block=5000,
                )
                for _stream_name, entries in messages or []:
                    for message_id, data in entries:
                        await schedule_message(message_id, data)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Worker loop error, reconnecting in 2s"
                )
                await asyncio.sleep(2)
    finally:
        if emission_reconciler_task is not None:
            emission_reconciler_task.cancel()
            await asyncio.gather(
                emission_reconciler_task,
                return_exceptions=True,
            )
        for task in message_tasks:
            task.cancel()
        if message_tasks:
            await asyncio.gather(*message_tasks, return_exceptions=True)
        (
            TASK_STREAM,
            CONSUMER_GROUP,
            WORKER_HOST,
            WORKER_ID,
        ) = previous_binding


async def main() -> None:
    """Main worker loop: consume tasks from Redis Stream."""
    global WORKER_ID, engine_db, worker_session
    previous_worker_id = WORKER_ID
    previous_engine_db = engine_db
    previous_worker_session = worker_session
    env = dict(os.environ)
    registration: PythonWorkerRegistration | None = None
    redis: aioredis.Redis | None = None
    try:
        try:
            enforce_worker_admission_from_env()
            database_url = load_worker_database_url(env)
            configure_worker_database(database_url)
            admission_token = load_worker_admission_token(env)
            registration = await _start_worker_registration(
                env,
                database_url,
                admission_token,
            )
            try:
                await _require_worker_redis_continuity()
            except WorkerRegistrationError:
                await registration.close(
                    reason="worker_redis_continuity_unready"
                )
                raise
        except (
            WorkerAdmissionError,
            WorkerSecretError,
            WorkerRegistrationError,
            ValueError,
        ) as exc:
            logger.critical("Worker admission denied: %s", exc)
            raise SystemExit(2) from exc

        WORKER_ID = registration.redis_consumer_id
        redis = _redis()
        try:
            await _require_worker_redis_identity(redis)
        except WorkerRegistrationError as exc:
            logger.critical("Worker admission denied: %s", exc)
            raise SystemExit(2) from exc
        await _run_until_registration_loss(
            registration,
            _consume_registered_worker(redis, registration),
        )
    finally:
        try:
            if redis is not None:
                await redis.aclose()
        finally:
            try:
                if registration is not None:
                    await registration.close()
            finally:
                current_engine_db = engine_db
                try:
                    if (
                        current_engine_db is not None
                        and current_engine_db is not previous_engine_db
                    ):
                        await current_engine_db.dispose()
                finally:
                    WORKER_ID = previous_worker_id
                    engine_db = previous_engine_db
                    worker_session = previous_worker_session


if __name__ == "__main__":
    asyncio.run(main())
