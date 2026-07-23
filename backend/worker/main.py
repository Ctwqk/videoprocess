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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import httpx
import redis.asyncio as aioredis
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
    lock_job_execution_authority,
    require_active_execution_authority,
)
from app.services.worker_admission import (
    WorkerAdmissionError,
    enforce_worker_admission_from_env,
)
from app.storage.manager import get_storage
from worker.handlers import HANDLER_MAP
from worker.handlers.base import BaseHandler, CancelledError
from worker.handlers.youtube_upload import YouTubeUploadHandler

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

engine_db: AsyncEngine | None = None
worker_session: async_sessionmaker[AsyncSession] | None = None


def configure_worker_database() -> None:
    """Initialize worker DB state only after startup admission succeeds."""
    global engine_db, worker_session
    if engine_db is not None and worker_session is not None:
        return

    # Remote workers can hold idle DB connections long enough for the
    # server/network to close them, so proactively ping and recycle.
    engine_db = create_async_engine(
        settings.database_url,
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
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> bool:
    """Atomically claim a queued node under durable execution authority."""

    try:
        resolved_job_id = uuid.UUID(job_id)
        resolved_node_id = uuid.UUID(node_execution_id)
    except ValueError:
        logger.error("Invalid worker execution ids job=%s node=%s", job_id, node_execution_id)
        return False

    factory = session_factory or get_worker_session()
    async with factory() as db:
        try:
            async with db.begin():
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
                node.status = NodeStatus.RUNNING
                node.started_at = datetime.utcnow()
                node.worker_id = WORKER_ID
            return True
        except JobExecutionAuthorityBlocked as exc:
            await db.rollback()
            logger.info(
                "Skipping stale worker delivery job=%s node=%s: %s",
                job_id,
                node_execution_id,
                exc,
            )
            return False


async def process_task(data: dict) -> None:
    """Process a single node execution task."""
    job_id = data["job_id"]
    node_execution_id = data["node_execution_id"]
    node_type = data["node_type"]
    config = json.loads(data.get("config", "{}"))
    input_artifacts_map = json.loads(data.get("input_artifacts", "{}"))

    logger.info(f"Processing node {data['node_id']} (type={node_type}) for job {job_id}")

    if not await _claim_node_execution(job_id, node_execution_id):
        return

    # Get handler
    handler_cls = HANDLER_MAP.get(node_type)
    if not handler_cls:
        await _report_failure(job_id, node_execution_id, f"No handler for node type: {node_type}")
        return

    if node_type == "youtube_upload":
        try:
            config, input_artifacts_map = await _authoritative_youtube_upload_inputs(
                job_id=job_id,
                node_execution_id=node_execution_id,
                node_id=data["node_id"],
                input_artifacts_map=input_artifacts_map,
            )
        except Exception as exc:
            await _report_failure(job_id, node_execution_id, str(exc))
            return
        config["_job_id"] = job_id
        config["_node_execution_id"] = node_execution_id
        config["_input_artifact_ids"] = dict(input_artifacts_map)

    handler: BaseHandler
    if node_type == "youtube_upload":
        handler = YouTubeUploadHandler(session_factory=get_worker_session())
    else:
        handler = handler_cls()

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
    try:
        cancel_check_task = asyncio.create_task(_cancel_watcher())

        # Resolve input artifact paths to local file paths
        input_paths: dict[str, str] = {}
        input_artifacts: list[tuple[str, InputArtifactSnapshot]] = []
        async with get_worker_session()() as db:
            for port_name, artifact_id_str in input_artifacts_map.items():
                artifact_id = uuid.UUID(artifact_id_str)
                artifact = await db.get(Artifact, artifact_id)
                if not artifact:
                    raise FileNotFoundError(f"Input artifact {artifact_id_str} not found")
                media_info = artifact.media_info if isinstance(artifact.media_info, dict) else {}
                input_artifacts.append(
                    (
                        port_name,
                        InputArtifactSnapshot(
                            id=artifact_id,
                            media_info=dict(media_info),
                            storage_backend=artifact.storage_backend,
                            storage_path=artifact.storage_path,
                            filename=artifact.filename,
                            file_size=getattr(artifact, "file_size", None),
                        ),
                    )
                )

        input_artifact_meta: dict[str, dict] = {}
        for port_name, artifact in input_artifacts:
            input_artifact_meta[port_name] = artifact.media_info
            storage = get_storage(artifact.storage_backend)
            local_path = storage.get_local_path(artifact.storage_path)
            if local_path and not Path(local_path).is_file():
                local_path = await _download_artifact_with_cancel(artifact, cancel_event)
                temp_files.append(local_path)
            elif not local_path:
                # MinIO or remote storage: download to temp file
                content = await storage.read(artifact.storage_path)
                ext = Path(artifact.filename).suffix or ".mp4"
                fd, tmp_path = tempfile.mkstemp(suffix=ext, prefix="vp_input_")
                os.close(fd)
                with open(tmp_path, "wb") as f:
                    f.write(content)
                local_path = tmp_path
                temp_files.append(tmp_path)
            input_paths[port_name] = local_path

        config["_input_artifact_meta"] = input_artifact_meta
        if node_type != "youtube_upload":
            config["_input_artifact_ids"] = dict(input_artifacts_map)

        # Prepare output path
        output_ext = _get_output_extension(node_type, config)
        output_filename = f"{node_execution_id}{output_ext}"
        output_storage_path = f"artifacts/{job_id}/{output_filename}"
        output_local_dir = Path(settings.storage_local_root) / "artifacts" / job_id
        output_local_dir.mkdir(parents=True, exist_ok=True)
        output_local_path = str(output_local_dir / output_filename)

        cancel_state = await _load_cancel_state(node_execution_id)
        if cancel_event.is_set() or cancel_state.is_cancelled:
            handler.cancel()
            raise CancelledError(cancel_state.cancel_reason or "node cancelled before handler execution")

        # Execute handler. Some handlers return artifact metadata and storage hints.
        handler_result = await handler.execute(config, input_paths, output_local_path)

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
        if settings.storage_backend != "local" and not skip_upload:
            with open(output_local_path, "rb") as f:
                await output_storage.save(artifact_storage_path, f)

        # Create artifact record
        async with get_worker_session()() as db:
            artifact = Artifact(
                job_id=uuid.UUID(job_id),
                node_execution_id=uuid.UUID(node_execution_id),
                kind=ArtifactKind.INTERMEDIATE,
                filename=output_filename,
                mime_type=_guess_mime(output_ext),
                file_size=file_size,
                storage_backend=artifact_storage_backend,
                storage_path=artifact_storage_path,
                media_info=artifact_media_info,
            )
            db.add(artifact)
            await db.flush()
            artifact_id = str(artifact.id)
            await db.commit()

        # Report success
        await _report_success(job_id, node_execution_id, artifact_id)
        logger.info(f"Node {data['node_id']} completed successfully")

    except CancelledError:
        logger.info(f"Node {data['node_id']} cancelled, cleaning up")
        # Don't report failure — orchestrator already knows about the cancel
    except Exception as e:
        logger.exception(f"Node {data['node_id']} failed")
        await _report_failure(job_id, node_execution_id, str(e))
    finally:
        if cancel_check_task and not cancel_check_task.done():
            cancel_check_task.cancel()
            try:
                await cancel_check_task
            except asyncio.CancelledError:
                pass
        # Clean up any temp files downloaded from remote storage
        for tmp in temp_files:
            try:
                os.unlink(tmp)
            except OSError:
                pass


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


async def _report_success(job_id: str, node_execution_id: str, artifact_id: str) -> None:
    r = _redis()
    try:
        await r.xadd(EVENT_STREAM, {
            "event": "node_completed",
            "job_id": job_id,
            "node_execution_id": node_execution_id,
            "output_artifact_id": artifact_id,
        })
    finally:
        await r.aclose()


async def _report_failure(job_id: str, node_execution_id: str, error: str) -> None:
    r = _redis()
    try:
        await r.xadd(EVENT_STREAM, {
            "event": "node_failed",
            "job_id": job_id,
            "node_execution_id": node_execution_id,
            "error": error[:2000],
        })
    finally:
        await r.aclose()


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


async def _reclaim_pending(r: aioredis.Redis) -> None:
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
                    await _process_message(r, msg_id, data)
    except Exception:
        logger.exception("PEL reclaim failed")


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


async def _process_message(r: aioredis.Redis, msg_id: str, data: dict) -> None:
    if await _maybe_defer_for_affinity(r, msg_id, data):
        return

    heartbeat_task = asyncio.create_task(_heartbeat_message(r, msg_id))
    should_ack = False
    try:
        await process_task(data)
        should_ack = True
    except Exception:
        logger.exception(f"Unhandled error processing {msg_id}")
        try:
            await _report_failure(
                data["job_id"],
                data["node_execution_id"],
                "Worker failed before task state could be updated. See worker logs for details.",
            )
            should_ack = True
        except Exception:
            logger.exception("Failed to report failure for %s; leaving message pending", msg_id)
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        if should_ack:
            await r.xack(TASK_STREAM, CONSUMER_GROUP, msg_id)


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


async def _maybe_defer_for_affinity(r: aioredis.Redis, msg_id: str, data: dict) -> bool:
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


async def main() -> None:
    """Main worker loop: consume tasks from Redis Stream."""
    try:
        enforce_worker_admission_from_env()
    except WorkerAdmissionError as exc:
        logger.critical("Worker admission denied: %s", exc)
        raise SystemExit(2) from exc

    configure_worker_database()
    r = _redis()

    # Create consumer group
    try:
        await r.xgroup_create(TASK_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    concurrency = int(os.environ.get("WORKER_CONCURRENCY", "2"))
    semaphore = asyncio.Semaphore(concurrency)

    logger.info(f"Worker {WORKER_ID} started (concurrency={concurrency})")

    # Initial PEL recovery on startup
    await _reclaim_pending(r)

    last_reclaim = asyncio.get_event_loop().time()

    try:
        while True:
            try:
                # Periodic PEL reclaim
                now = asyncio.get_event_loop().time()
                if now - last_reclaim > PEL_RECLAIM_INTERVAL:
                    await _reclaim_pending(r)
                    last_reclaim = now

                messages = await r.xreadgroup(
                    CONSUMER_GROUP,
                    WORKER_ID,
                    {TASK_STREAM: ">"},
                    count=1,
                    block=5000,
                )

                if not messages:
                    continue

                for stream_name, entries in messages:
                    for msg_id, data in entries:
                        await semaphore.acquire()

                        async def _run(mid=msg_id, d=data):
                            try:
                                await _process_message(r, mid, d)
                            finally:
                                semaphore.release()

                        asyncio.create_task(_run())

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Worker loop error, reconnecting in 2s")
                await asyncio.sleep(2)
    finally:
        await r.aclose()
        if engine_db is not None:
            await engine_db.dispose()


if __name__ == "__main__":
    asyncio.run(main())
