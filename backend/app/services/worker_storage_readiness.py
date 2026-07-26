from __future__ import annotations

import io
import os
import uuid
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlsplit

import httpx

from app.storage.base import StorageBackend


_PROBE_PAYLOAD = b"vp-worker-storage-readiness-v1\n"


class ReadinessFailure(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def artifact_api_health_url(base_url: str) -> str:
    parsed = urlsplit(base_url)
    try:
        port = parsed.port
    except ValueError:
        raise ValueError("invalid artifact API base URL")

    if (
        parsed.scheme != "http"
        or parsed.hostname != "vp-api-swarm"
        or port != 8080
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"/api/v1", "/api/v1/"}
    ):
        raise ValueError("invalid artifact API base URL")

    return "http://vp-api-swarm:8080/health"


def _read_scratch_probe(path: Path) -> bytes:
    return path.read_bytes()


async def probe_worker_storage(
    env: Mapping[str, str],
    *,
    require_artifact_api: bool,
    storage: StorageBackend | None = None,
    http_client_factory: Callable[..., httpx.AsyncClient] | None = None,
) -> dict[str, object]:
    if env.get("STORAGE_BACKEND") != "minio":
        raise ReadinessFailure("configuration_invalid")
    local_root = env.get("STORAGE_LOCAL_ROOT", "")
    if not local_root:
        raise ReadinessFailure("configuration_invalid")

    scratch_dir = Path(local_root) / "deploy-readiness"
    scratch_setup_failure: ReadinessFailure | None = None
    try:
        scratch_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        scratch_setup_failure = ReadinessFailure("scratch_unavailable")
    if scratch_setup_failure is not None:
        raise scratch_setup_failure

    scratch_path = scratch_dir / f"{uuid.uuid4()}.probe"
    scratch_failure: ReadinessFailure | None = None
    scratch_cleanup_failure: ReadinessFailure | None = None
    try:
        try:
            fd = os.open(
                scratch_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            with os.fdopen(fd, "wb") as probe:
                probe.write(_PROBE_PAYLOAD)
            if _read_scratch_probe(scratch_path) != _PROBE_PAYLOAD:
                raise ReadinessFailure("scratch_mismatch")
        except ReadinessFailure as exc:
            scratch_failure = exc
        except Exception:
            scratch_failure = ReadinessFailure("scratch_unavailable")
    finally:
        try:
            if scratch_path.exists():
                scratch_path.unlink()
        except Exception:
            scratch_cleanup_failure = ReadinessFailure("cleanup_failed")

    if scratch_cleanup_failure is not None:
        raise scratch_cleanup_failure
    if scratch_failure is not None:
        raise scratch_failure

    minio_setup_failure: ReadinessFailure | None = None
    if storage is None:
        from app.storage.manager import get_storage

        try:
            minio = get_storage("minio", create_bucket=False)
        except Exception:
            minio_setup_failure = ReadinessFailure("minio_unavailable")
    else:
        minio = storage
    if minio_setup_failure is not None:
        raise minio_setup_failure

    object_path = f"health/deploy-readiness/{uuid.uuid4()}.probe"
    minio_failure: ReadinessFailure | None = None
    minio_cleanup_failure: ReadinessFailure | None = None
    try:
        try:
            count = await minio.save(object_path, io.BytesIO(_PROBE_PAYLOAD))
            if count != len(_PROBE_PAYLOAD):
                raise ReadinessFailure("minio_mismatch")
            if await minio.read(object_path) != _PROBE_PAYLOAD:
                raise ReadinessFailure("minio_mismatch")
        except ReadinessFailure as exc:
            minio_failure = exc
        except Exception:
            minio_failure = ReadinessFailure("minio_unavailable")
    finally:
        try:
            await minio.delete(object_path)
            if await minio.exists(object_path):
                minio_cleanup_failure = ReadinessFailure("cleanup_failed")
        except Exception:
            minio_cleanup_failure = ReadinessFailure("cleanup_failed")

    if minio_cleanup_failure is not None:
        raise minio_cleanup_failure
    if minio_failure is not None:
        raise minio_failure

    artifact_api_status = "not_required"
    api_failure: ReadinessFailure | None = None
    if require_artifact_api:
        try:
            health_url = artifact_api_health_url(
                env["VP_ARTIFACT_DOWNLOAD_BASE_URL"]
            )
            client_factory = http_client_factory or httpx.AsyncClient
            async with client_factory(
                timeout=httpx.Timeout(5.0),
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await client.get(health_url)
                if response.status_code != 200:
                    raise RuntimeError("artifact API health check failed")
        except Exception:
            api_failure = ReadinessFailure("api_unavailable")
    if api_failure is not None:
        raise api_failure
    if require_artifact_api:
        artifact_api_status = "ready"

    return {
        "status": "ready",
        "components": {
            "scratch": "ready",
            "minio": "ready",
            "artifact_api": artifact_api_status,
        },
    }
