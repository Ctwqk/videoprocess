from __future__ import annotations

import os
from dataclasses import dataclass
from ipaddress import ip_address
from typing import Mapping
from urllib.parse import urlparse


LOCAL_HOSTS = {"", "localhost", "127.0.0.1", "0.0.0.0", "::1"}
PRODUCTION_DEPLOY_MODES = {"shared", "production"}
MINIO_SETTINGS = ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET")
MINIO_WORKER_TYPES = {"ffmpeg", "youtube_publisher"}


class WorkerAdmissionError(RuntimeError):
    """Raised when a worker is not allowed to join production queues."""


@dataclass(frozen=True)
class WorkerAdmissionDecision:
    allowed: bool
    reasons: tuple[str, ...] = ()


def validate_worker_admission(env: Mapping[str, str]) -> WorkerAdmissionDecision:
    deploy_mode = _env_value(env, "DEPLOY_MODE", "shared").lower()
    redis_url = _env_value(env, "REDIS_URL", "redis://localhost:6379/0")
    worker_type = _env_value(env, "WORKER_TYPE", "ffmpeg").lower()
    storage_backend = _env_value(env, "STORAGE_BACKEND", "local").lower()

    if not _is_production_queue_consumer(deploy_mode=deploy_mode, redis_url=redis_url):
        return WorkerAdmissionDecision(allowed=True)

    reasons: list[str] = []
    if not _env_value(env, "WORKER_HOST", ""):
        reasons.append("production workers require explicit WORKER_HOST")

    if worker_type in MINIO_WORKER_TYPES:
        _append_minio_reasons(env, worker_type, storage_backend, reasons)

    if worker_type in MINIO_WORKER_TYPES and _env_value(env, "YOUTUBE_CREDENTIALS_DIR", ""):
        reasons.append("production workers must not set YOUTUBE_CREDENTIALS_DIR")

    if worker_type == "youtube_publisher":
        manager_url = _env_value(env, "YOUTUBE_MANAGER_URL", "")
        if not manager_url or _is_local_host(_host_from_url(manager_url)):
            reasons.append(
                "production youtube_publisher workers require a non-local YOUTUBE_MANAGER_URL"
            )
        if _env_value(env, "YOUTUBE_PUBLISH_ENABLED", "false").lower() != "true":
            reasons.append(
                "production youtube_publisher workers require YOUTUBE_PUBLISH_ENABLED=true"
            )
        if _env_value(env, "PUBLIC_PUBLISH_ENABLED", "false").lower() != "false":
            reasons.append(
                "production youtube_publisher workers require PUBLIC_PUBLISH_ENABLED=false"
            )

    return WorkerAdmissionDecision(allowed=not reasons, reasons=tuple(reasons))


def enforce_worker_admission_from_env(env: Mapping[str, str] | None = None) -> None:
    decision = validate_worker_admission(os.environ if env is None else env)
    if not decision.allowed:
        raise WorkerAdmissionError("; ".join(decision.reasons))


def _env_value(env: Mapping[str, str], key: str, default: str) -> str:
    return str(env.get(key, default)).strip()


def _append_minio_reasons(
    env: Mapping[str, str],
    worker_type: str,
    storage_backend: str,
    reasons: list[str],
) -> None:
    worker_label = f"production {worker_type} workers"
    if storage_backend != "minio":
        reasons.append(f"{worker_label} require STORAGE_BACKEND=minio")

    for key in MINIO_SETTINGS:
        if not _env_value(env, key, ""):
            reasons.append(f"{worker_label} require {key}")

    minio_endpoint = _env_value(env, "MINIO_ENDPOINT", "")
    if minio_endpoint and _is_local_host(_host_from_endpoint(minio_endpoint)):
        reasons.append(f"production MinIO endpoint must not point at {minio_endpoint}")


def _is_production_queue_consumer(*, deploy_mode: str, redis_url: str) -> bool:
    if deploy_mode in PRODUCTION_DEPLOY_MODES:
        return True
    return not _is_local_host(_host_from_url(redis_url))


def _host_from_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url if "://" in url else f"//{url}")
    return _normalize_host(parsed.hostname)


def _host_from_endpoint(endpoint: str) -> str:
    parsed = urlparse(endpoint if "://" in endpoint else f"//{endpoint}")
    return _normalize_host(parsed.hostname)


def _normalize_host(hostname: str | None) -> str:
    return (hostname or "").lower().strip("[]").removesuffix(".")


def _is_local_host(hostname: str) -> bool:
    normalized_hostname = _normalize_host(hostname)
    if normalized_hostname in LOCAL_HOSTS:
        return True
    try:
        address = ip_address(normalized_hostname)
    except ValueError:
        return False
    return address.is_loopback or address.is_unspecified
