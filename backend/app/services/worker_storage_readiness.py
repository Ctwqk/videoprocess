from __future__ import annotations

import io
import os
import uuid
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlsplit, urlunsplit

import httpx

from app.storage.base import StorageBackend
from app.storage.manager import get_storage


_PROBE_PAYLOAD = b"vp-worker-storage-readiness-v1\n"


class ReadinessFailure(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def artifact_api_health_url(base_url: str) -> str:
    parsed = urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("invalid artifact API base URL")

    path = parsed.path.rstrip("/")
    if path.endswith("/api/v1"):
        path = path[: -len("/api/v1")]
    health_path = f"{path}/health" if path else "/health"
    return urlunsplit((parsed.scheme, parsed.netloc, health_path, "", ""))


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
    try:
        scratch_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise ReadinessFailure("scratch_unavailable") from exc
    scratch_path = scratch_dir / f"{uuid.uuid4()}.probe"
    scratch_failure: ReadinessFailure | None = None
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
        except Exception as exc:
            scratch_failure = ReadinessFailure("scratch_unavailable")
            scratch_failure.__cause__ = exc
    finally:
        try:
            if scratch_path.exists():
                scratch_path.unlink()
        except Exception as exc:
            raise ReadinessFailure("cleanup_failed") from exc

    if scratch_failure is not None:
        raise scratch_failure

    try:
        minio = storage or get_storage("minio")
    except Exception as exc:
        raise ReadinessFailure("minio_unavailable") from exc

    object_path = f"health/deploy-readiness/{uuid.uuid4()}.probe"
    minio_failure: ReadinessFailure | None = None
    try:
        try:
            count = await minio.save(object_path, io.BytesIO(_PROBE_PAYLOAD))
            if count != len(_PROBE_PAYLOAD):
                raise ReadinessFailure("minio_mismatch")
            if await minio.read(object_path) != _PROBE_PAYLOAD:
                raise ReadinessFailure("minio_mismatch")
        except ReadinessFailure as exc:
            minio_failure = exc
        except Exception as exc:
            minio_failure = ReadinessFailure("minio_unavailable")
            minio_failure.__cause__ = exc
    finally:
        try:
            await minio.delete(object_path)
            if await minio.exists(object_path):
                raise ReadinessFailure("cleanup_failed")
        except ReadinessFailure:
            raise
        except Exception as exc:
            raise ReadinessFailure("cleanup_failed") from exc

    if minio_failure is not None:
        raise minio_failure

    artifact_api_status = "not_required"
    if require_artifact_api:
        try:
            health_url = artifact_api_health_url(
                env["VP_ARTIFACT_DOWNLOAD_BASE_URL"]
            )
            client_factory = http_client_factory or httpx.AsyncClient
            async with client_factory(
                timeout=httpx.Timeout(5.0), follow_redirects=False
            ) as client:
                response = await client.get(health_url)
                if response.status_code != 200:
                    raise RuntimeError("artifact API health check failed")
        except Exception as exc:
            raise ReadinessFailure("api_unavailable") from exc
        artifact_api_status = "ready"

    return {
        "status": "ready",
        "components": {
            "scratch": "ready",
            "minio": "ready",
            "artifact_api": artifact_api_status,
        },
    }
