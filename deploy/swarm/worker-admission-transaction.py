#!/usr/bin/env python3
"""Durable worker-admission transaction and single-writer lock helper."""

from __future__ import annotations

import copy
import datetime
from contextlib import contextmanager
import fcntl
import hashlib
import http.client
import io
import json
import os
import re
import runpy
import secrets
import selectors
import stat
import sys
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, overload


LOCK_NAME = "transaction.lock"
TRANSACTIONS_NAME = "transactions"
ACTIVE_NAME = "active.json"
SNAPSHOTS_NAME = "snapshots.json"
APP_PROGRESS_NAME = "app-progress.json"
LOCK_MODE = 0o600
FILE_MODE = 0o600
CREDENTIAL_MODE = 0o400
DIRECTORY_MODE = 0o700
LOCK_CONTENTION_STATUS = 75
MAX_DOCUMENT_BYTES = 1024 * 1024
CURRENT_DOCUMENT_SCHEMA = 3

APP_SERVICES = {
    "vp-api-swarm",
    "vp-frontend-swarm",
    "vp-autoflow-api-swarm",
    "vp-event-outbox-relay-swarm",
    "vp-channel-agent-runner-swarm",
    "vp-ffmpeg-worker-go-swarm",
    "vp-ffmpeg-worker-gpu-swarm",
    "vp-vision-worker-swarm",
    "vp-youtube-publisher-swarm",
}
WORKER_STAGE_SUCCESSORS = {
    "pending": "prepared",
    "prepared": "applied",
    "applied": "verified",
}
WORKER_STAGE_ORDER = {
    "pending": 0,
    "prepared": 1,
    "applied": 2,
    "verified": 3,
}

DATABASE_PURPOSES = (
    "deploy_migrator",
    "deploy_read",
    "control_role_owner",
    "runtime_role_owner",
)
RUNTIME_AUTHORITY_SERVICES = {
    "vp-ffmpeg-worker-go-swarm",
    "vp-ffmpeg-worker-gpu-swarm",
    "vp-vision-worker-swarm",
    "vp-youtube-publisher-swarm",
}
AUTHORITY_STATES = {"planned", "provisioning", "provisioned", "revoked"}
PHASES = {
    "PREPARING",
    "ABORTING",
    "FORWARD_APPLYING",
    "FORWARD_VERIFIED",
    "WORKERS_PROMOTED",
    "MARKER_PROMOTED",
    "CONTROL_PROMOTED",
    "RETIRING",
    "ROLLBACK_PREPARING",
    "ROLLBACK_APPLYING",
    "ROLLBACK_VERIFIED",
    "ROLLBACK_WORKERS_PROMOTED",
    "ROLLBACK_MARKER_PROMOTED",
    "ROLLBACK_CONTROL_PROMOTED",
    "CANDIDATE_RESTORE_REQUIRED",
    "CANDIDATE_RESTORING",
    "CANDIDATE_RESTORED",
    "DONE",
}
PROMOTION_BY_PHASE = {
    "PREPARING": (False, False, False),
    "ABORTING": (False, False, False),
    "FORWARD_APPLYING": (False, False, False),
    "FORWARD_VERIFIED": (False, False, False),
    "WORKERS_PROMOTED": (True, False, False),
    "MARKER_PROMOTED": (True, True, False),
    "CONTROL_PROMOTED": (True, True, True),
    "RETIRING": (True, True, True),
    "ROLLBACK_PREPARING": (False, False, False),
    "ROLLBACK_APPLYING": (False, False, False),
    "ROLLBACK_VERIFIED": (False, False, False),
    "ROLLBACK_WORKERS_PROMOTED": (True, False, False),
    "ROLLBACK_MARKER_PROMOTED": (True, True, False),
    "ROLLBACK_CONTROL_PROMOTED": (True, True, True),
    "CANDIDATE_RESTORE_REQUIRED": (False, False, False),
    "CANDIDATE_RESTORING": (False, False, False),
    "CANDIDATE_RESTORED": (False, False, False),
    "DONE": (True, True, True),
}
LEGAL_TRANSITIONS = {
    "PREPARING": {"ABORTING", "FORWARD_APPLYING", "ROLLBACK_PREPARING"},
    "ABORTING": {"DONE"},
    "FORWARD_APPLYING": {
        "ABORTING",
        "FORWARD_VERIFIED",
        "ROLLBACK_PREPARING",
    },
    "FORWARD_VERIFIED": {"WORKERS_PROMOTED", "ROLLBACK_PREPARING"},
    "WORKERS_PROMOTED": {"MARKER_PROMOTED"},
    "MARKER_PROMOTED": {"CONTROL_PROMOTED"},
    "CONTROL_PROMOTED": {"RETIRING"},
    "RETIRING": {"DONE"},
    "ROLLBACK_PREPARING": {
        "ABORTING",
        "ROLLBACK_APPLYING",
        "CANDIDATE_RESTORE_REQUIRED",
    },
    "ROLLBACK_APPLYING": {
        "ROLLBACK_VERIFIED",
        "CANDIDATE_RESTORE_REQUIRED",
    },
    "ROLLBACK_VERIFIED": {"ROLLBACK_WORKERS_PROMOTED"},
    "ROLLBACK_WORKERS_PROMOTED": {"ROLLBACK_MARKER_PROMOTED"},
    "ROLLBACK_MARKER_PROMOTED": {"ROLLBACK_CONTROL_PROMOTED"},
    "ROLLBACK_CONTROL_PROMOTED": {"RETIRING"},
    "CANDIDATE_RESTORE_REQUIRED": {"CANDIDATE_RESTORING"},
    "CANDIDATE_RESTORING": {"CANDIDATE_RESTORED"},
    "CANDIDATE_RESTORED": {
        "FORWARD_VERIFIED",
        "ROLLBACK_PREPARING",
    },
    "DONE": set(),
}
INTENT_PHASES = {
    "REMOVE_PREPARED_SECRET": ("ABORTING", "ABORTING"),
    "PROMOTE_WORKERS": ("FORWARD_VERIFIED", "WORKERS_PROMOTED"),
    "PROMOTE_MARKER": ("WORKERS_PROMOTED", "MARKER_PROMOTED"),
    "PROMOTE_CONTROL": ("MARKER_PROMOTED", "CONTROL_PROMOTED"),
    "PROMOTE_ROLLBACK_WORKERS": (
        "ROLLBACK_VERIFIED",
        "ROLLBACK_WORKERS_PROMOTED",
    ),
    "PROMOTE_ROLLBACK_MARKER": (
        "ROLLBACK_WORKERS_PROMOTED",
        "ROLLBACK_MARKER_PROMOTED",
    ),
    "PROMOTE_ROLLBACK_CONTROL": (
        "ROLLBACK_MARKER_PROMOTED",
        "ROLLBACK_CONTROL_PROMOTED",
    ),
}
REPLAY_ACTIONS = {
    "PREPARING": "RESUME_PREPARING",
    "ABORTING": "ABORT_PREPARED_SECRETS",
    "FORWARD_APPLYING": "RECONCILE_FORWARD",
    "FORWARD_VERIFIED": "VERIFY_FORWARD",
    "WORKERS_PROMOTED": "PROMOTE_MARKER",
    "MARKER_PROMOTED": "PROMOTE_CONTROL",
    "CONTROL_PROMOTED": "ENTER_RETIRING",
    "RETIRING": "RETIRE_EXACT_IDENTITIES",
    "ROLLBACK_PREPARING": "RESUME_ROLLBACK_PREPARING",
    "ROLLBACK_APPLYING": "RECONCILE_ROLLBACK",
    "ROLLBACK_VERIFIED": "VERIFY_ROLLBACK",
    "ROLLBACK_WORKERS_PROMOTED": "PROMOTE_ROLLBACK_MARKER",
    "ROLLBACK_MARKER_PROMOTED": "PROMOTE_ROLLBACK_CONTROL",
    "ROLLBACK_CONTROL_PROMOTED": "ENTER_RETIRING",
    "CANDIDATE_RESTORE_REQUIRED": "RESTORE_CANDIDATE",
    "CANDIDATE_RESTORING": "RECONCILE_CANDIDATE_RESTORE",
    "CANDIDATE_RESTORED": "VERIFY_CANDIDATE_RESTORE",
    "DONE": "ARCHIVE",
}

LEGACY_SCHEMA_1_TOP_LEVEL_FIELDS = {
    "schema",
    "transaction_id",
    "revision",
    "phase",
    "outcome",
    "target_commit",
    "target_backend_image",
    "target_go_image",
    "created_at",
    "database_credentials",
    "runtime_redis",
    "prepared_secrets",
    "baseline",
    "failed_forward",
    "forward",
    "rollback",
    "promotion",
    "pending_retirements",
    "janitor",
    "last_error",
    "operation",
    "abort",
}
TOP_LEVEL_FIELDS = LEGACY_SCHEMA_1_TOP_LEVEL_FIELDS | {
    "authorities",
    "retiring_outcome",
    "vision_jobs",
}
IDENTITY_FIELDS = {
    "kind",
    "docker_id",
    "name",
    "service",
    "generation",
    "purpose",
    "spec_digest",
}
SNAPSHOT_FIELDS = {
    "schema",
    "transaction_id",
    "revision",
    "baseline",
    "failed_forward",
    "forward",
    "rollback",
    "janitor",
}


class TransactionError(Exception):
    """A stable, non-secret transaction validation failure."""


@dataclass(frozen=True)
class LegacySchema1Quarantine:
    transaction_id: str
    revision: int
    phase: str
    journal_sha256: str


def _identity(metadata: os.stat_result) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


def _require_exact_fields(value: object, fields: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise TransactionError
    return value


def _require_string(
    value: object,
    pattern: str,
    *,
    maximum: int = 255,
) -> str:
    if (
        not isinstance(value, str)
        or len(value) > maximum
        or re.fullmatch(pattern, value) is None
    ):
        raise TransactionError
    return value


def _require_optional_string(
    value: object,
    pattern: str,
    *,
    maximum: int = 255,
) -> str | None:
    if value is None:
        return None
    return _require_string(value, pattern, maximum=maximum)


def _require_integer(value: object, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise TransactionError
    return value


def _require_exact_schema(value: object, expected: int) -> int:
    if type(value) is not int or value != expected:
        raise TransactionError
    return value


def _require_absolute(path: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise TransactionError
    return candidate


def _require_directory(metadata: os.stat_result) -> None:
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_gid != os.getgid()
        or stat.S_IMODE(metadata.st_mode) != DIRECTORY_MODE
    ):
        raise TransactionError


def _require_regular(
    metadata: os.stat_result,
    mode: int,
    *,
    single_link: bool,
) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_gid != os.getgid()
        or stat.S_IMODE(metadata.st_mode) != mode
        or (single_link and metadata.st_nlink != 1)
    ):
        raise TransactionError


def _require_lock(metadata: os.stat_result) -> None:
    _require_regular(metadata, LOCK_MODE, single_link=True)


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | os.O_CLOEXEC
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _read_file_flags() -> int:
    return os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)


def _lock_file_flags() -> int:
    return os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)


def _open_admission_root(raw_root: str) -> tuple[Path, int]:
    root = _require_absolute(raw_root)
    before = os.lstat(root)
    _require_directory(before)
    descriptor = os.open(root, _directory_flags())
    try:
        opened = os.fstat(descriptor)
        _require_directory(opened)
        if _identity(before) != _identity(opened):
            raise TransactionError
    except Exception:
        os.close(descriptor)
        raise
    return root, descriptor


def _open_child_directory(
    parent_descriptor: int,
    name: str,
    *,
    create: bool,
) -> int:
    try:
        before = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        if not create:
            raise
        os.mkdir(name, DIRECTORY_MODE, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
        before = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    _require_directory(before)
    descriptor = os.open(name, _directory_flags(), dir_fd=parent_descriptor)
    try:
        opened = os.fstat(descriptor)
        _require_directory(opened)
        if _identity(before) != _identity(opened):
            raise TransactionError
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def _open_transactions(
    raw_root: str,
    *,
    create: bool,
) -> tuple[Path, int, int]:
    root, root_descriptor = _open_admission_root(raw_root)
    try:
        transactions_descriptor = _open_child_directory(
            root_descriptor,
            TRANSACTIONS_NAME,
            create=create,
        )
    except Exception:
        os.close(root_descriptor)
        raise
    return root, root_descriptor, transactions_descriptor


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written < 1:
            raise TransactionError
        view = view[written:]


def _canonical(value: object) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise TransactionError from error
    return encoded + b"\n"


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise TransactionError
        result[key] = value
    return result


def _decode_canonical(payload: bytes) -> object:
    if not payload or len(payload) > MAX_DOCUMENT_BYTES:
        raise TransactionError
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise TransactionError from error
    if _canonical(value) != payload:
        raise TransactionError
    return value


def _read_limited(descriptor: int) -> bytes:
    chunks: list[bytes] = []
    remaining = MAX_DOCUMENT_BYTES + 1
    while remaining:
        chunk = os.read(descriptor, min(65536, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    payload = b"".join(chunks)
    if len(payload) > MAX_DOCUMENT_BYTES:
        raise TransactionError
    return payload


def _validate_database_credentials(value: object) -> dict[str, Any]:
    credentials = _require_exact_fields(value, set(DATABASE_PURPOSES))
    paths: set[str] = set()
    identities: set[tuple[int, int]] = set()
    principals: set[str] = set()
    for purpose in DATABASE_PURPOSES:
        entry = _require_exact_fields(
            credentials[purpose],
            {
                "canonical_path",
                "device",
                "inode",
                "mode",
                "expected_principal",
            },
        )
        path = _require_string(entry["canonical_path"], r"/[^\r\n]{0,4094}", maximum=4095)
        device = _require_integer(entry["device"])
        inode = _require_integer(entry["inode"], 1)
        if entry["mode"] != CREDENTIAL_MODE:
            raise TransactionError
        principal = _require_string(
            entry["expected_principal"],
            r"[A-Za-z_][A-Za-z0-9_.$@-]{0,127}",
            maximum=128,
        )
        if path in paths or (device, inode) in identities or principal in principals:
            raise TransactionError
        paths.add(path)
        identities.add((device, inode))
        principals.add(principal)
    return credentials


def _validate_app_progress(value: object) -> dict[str, Any]:
    progress = _require_exact_fields(
        value,
        {
            "schema",
            "transaction_id",
            "target_commit",
            "attempted_services",
            "migration_state",
        },
    )
    _require_exact_schema(progress["schema"], 1)
    _require_string(progress["transaction_id"], r"tx-[0-9a-f]{32}", maximum=35)
    _require_string(progress["target_commit"], r"[0-9a-f]{40}", maximum=40)
    attempted_services = progress["attempted_services"]
    if not isinstance(attempted_services, list):
        raise TransactionError
    seen: set[str] = set()
    for service in attempted_services:
        if service not in APP_SERVICES or service in seen:
            raise TransactionError
        seen.add(service)
    if progress["migration_state"] not in {"pending", "applying", "applied"}:
        raise TransactionError
    return progress


def _validate_secret_ref(value: object) -> None:
    reference = _require_exact_fields(
        value,
        {
            "name",
            "docker_secret_id",
            "service",
            "generation",
            "purpose",
        },
    )
    _require_string(reference["name"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    _require_string(
        reference["docker_secret_id"],
        r"[a-z0-9]{20,64}",
        maximum=64,
    )
    _require_string(reference["service"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    _require_string(reference["generation"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
    _require_string(reference["purpose"], r"[a-z][a-z0-9_-]{0,63}", maximum=64)


def _validate_service_identity(value: object) -> None:
    service = _require_exact_fields(
        value,
        {
            "name",
            "existed",
            "docker_service_id",
            "image",
            "spec_digest",
        },
    )
    _require_string(service["name"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    if not isinstance(service["existed"], bool):
        raise TransactionError
    _require_optional_string(
        service["docker_service_id"],
        r"[0-9a-z]{12,64}",
        maximum=64,
    )
    _require_optional_string(service["image"], r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}")
    _require_optional_string(service["spec_digest"], r"[0-9a-f]{64}", maximum=64)
    present_fields = (
        service["docker_service_id"],
        service["image"],
        service["spec_digest"],
    )
    if service["existed"] != all(value is not None for value in present_fields):
        raise TransactionError


def _validate_service_identities(value: object) -> None:
    if not isinstance(value, list):
        raise TransactionError
    names: set[str] = set()
    docker_ids: set[str] = set()
    for service in value:
        _validate_service_identity(service)
        name = service["name"]
        docker_id = service["docker_service_id"]
        if name in names or (docker_id is not None and docker_id in docker_ids):
            raise TransactionError
        names.add(name)
        if docker_id is not None:
            docker_ids.add(docker_id)


def _validate_secret_refs(value: object, *, exact_count: int | None = None) -> None:
    if not isinstance(value, list) or (
        exact_count is not None and len(value) != exact_count
    ):
        raise TransactionError
    names: set[str] = set()
    docker_ids: set[str] = set()
    identities: set[tuple[str, str, str]] = set()
    for item in value:
        _validate_secret_ref(item)
        name = item["name"]
        docker_id = item["docker_secret_id"]
        identity = (item["service"], item["generation"], item["purpose"])
        if name in names or docker_id in docker_ids or identity in identities:
            raise TransactionError
        names.add(name)
        docker_ids.add(docker_id)
        identities.add(identity)


def _validate_authority(
    value: object,
    target_commit: str,
    *,
    rollback_control: dict[str, Any] | None = None,
) -> dict[str, str]:
    authority = _require_exact_fields(
        value,
        {
            "kind",
            "service",
            "generation",
            "state",
            "control_image",
            "control_generation",
            "operator_reference",
        },
    )
    if authority["kind"] not in {"control", "marker", "runtime"}:
        raise TransactionError
    _require_string(
        authority["service"],
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}",
    )
    _require_string(
        authority["generation"],
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
    )
    if authority["state"] not in AUTHORITY_STATES:
        raise TransactionError
    _require_string(
        authority["control_image"],
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    _require_string(
        authority["control_generation"],
        r"c-[0-9a-f]{20}",
        maximum=22,
    )
    operator_reference = authority["operator_reference"]
    _require_string(
        operator_reference,
        r"[A-Za-z0-9][A-Za-z0-9_./-]{0,254}",
    )
    expected_control_generation = f"c-{target_commit[:20]}"
    expected_control_image = (
        f"vp-ffmpeg-worker-python:deploy-{target_commit[:12]}"
    )
    if rollback_control is not None:
        if authority["kind"] != "runtime":
            raise TransactionError
        expected_control_generation = rollback_control["generation"]
        expected_control_image = rollback_control["image"]
    expected_control_operator_reference = (
        f"control/{expected_control_generation}/"
        "worker-registration-operator-database-url"
    )
    if (
        authority["control_generation"] != expected_control_generation
        or authority["control_image"] != expected_control_image
    ):
        raise TransactionError
    if (
        authority["kind"] == "control"
        and (
            authority["service"] != "vp-worker-control"
            or authority["generation"] != expected_control_generation
            or operator_reference != expected_control_operator_reference
        )
    ) or (
        authority["kind"] == "marker"
        and (
            authority["service"] != "worker-redis-marker-control"
            or re.fullmatch(
                r"m-[0-9a-f]{12}-[1-9][0-9]*-[0-9]{4}",
                authority["generation"],
            )
            is None
            or operator_reference
            != (
                f"marker/{authority['generation']}/"
                "worker-marker-owner-database-url"
            )
        )
    ) or (
        authority["kind"] == "runtime"
        and (
            authority["service"] not in RUNTIME_AUTHORITY_SERVICES
            or re.fullmatch(r"[1-9][0-9]*", authority["generation"]) is None
            or operator_reference != expected_control_operator_reference
        )
    ):
        raise TransactionError
    return authority


def _validate_authorities(
    value: object,
    target_commit: str,
    *,
    document: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise TransactionError
    identities: set[tuple[str, str, str]] = set()
    control_images: set[str] = set()
    rollback_services: set[str] = set()
    authorities: list[dict[str, str]] = []
    for item in value:
        rollback_authority = (
            document is not None and _is_rollback_runtime_authority(document, item)
        )
        authority = _validate_authority(
            item, target_commit,
            rollback_control=document["rollback"]["control"] if rollback_authority else None,
        )
        identity = (
            authority["kind"],
            authority["service"],
            authority["generation"],
        )
        if identity in identities:
            raise TransactionError
        identities.add(identity)
        if rollback_authority:
            if authority["service"] in rollback_services:
                raise TransactionError
            rollback_services.add(authority["service"])
        else:
            control_images.add(authority["control_image"])
        authorities.append(authority)
    if len(control_images) > 1:
        raise TransactionError
    return authorities


def _rollback_control(document: dict[str, Any]) -> dict[str, Any] | None:
    rollback = document["rollback"]
    control = rollback["control"]
    if (
        document["phase"] in {"PREPARING", "FORWARD_APPLYING", "ABORTING"}
        or document["outcome"] == "aborted"
        or not document["baseline"]["captured"]
        or not document["failed_forward"]["captured"]
        or rollback["attempt"] < 1
        or rollback["namespace"] is None
        or rollback["marker_generation"]
        != f"m-rb-{document['transaction_id'][3:15]}-{rollback['attempt']}"
        or control is None
        or control != document["baseline"]["control"]
    ):
        return None
    return control


def _is_rollback_runtime_authority(document: dict[str, Any], value: object) -> bool:
    control = _rollback_control(document)
    if control is None or not isinstance(value, dict):
        return False
    service = value.get("service")
    generation = value.get("generation")
    return (
        value.get("kind") == "runtime"
        and service in RUNTIME_AUTHORITY_SERVICES
        and value.get("control_image") == control["image"]
        and value.get("control_generation") == control["generation"]
        and all(
            any(item["name"] == service and item["existed"] for item in document[field]["services"])
            for field in ("baseline", "failed_forward")
        )
        and not any(
            worker["service"] == service and str(worker["generation"]) == generation
            for worker in document["forward"]["workers"]
        )
    )


def _is_rollback_prepared_secret(document: dict[str, Any], reference: dict[str, str]) -> bool:
    control = _rollback_control(document)
    if control is None:
        return False
    rollback = document["rollback"]
    service, generation, purpose = (
        reference["service"], reference["generation"], reference["purpose"]
    )
    if service == "worker-redis-marker-control":
        if (
            generation != rollback["marker_generation"]
            or purpose not in {"readiness-database", "janitor-database", "repair-database"}
            or reference["name"] != f"vp-wrm-{purpose.removesuffix('-database')}-db-{generation}"
        ):
            return False
        marker = rollback["marker"]
        return marker is None or (
            marker["generation"] == generation and marker["image"] == control["image"]
            and any(all(item.get(key) == value for key, value in reference.items())
                    for item in marker["secrets"])
        )
    if service not in RUNTIME_AUTHORITY_SERVICES or purpose not in {"database", "admission"}:
        return False
    workers = [worker for worker in rollback["workers"] if worker["service"] == service]
    if workers:
        return len(workers) == 1 and all(
            workers[0][purpose + "_secret"].get(key) == value
            for key, value in reference.items()
        )
    # A fresh worker journals authority before Docker IDs and the worker plan exist.
    kind = {
        "vp-ffmpeg-worker-go-swarm": "ffmpeg-go",
        "vp-ffmpeg-worker-gpu-swarm": "ffmpeg",
        "vp-vision-worker-swarm": "vision",
        "vp-youtube-publisher-swarm": "youtube-publisher",
    }[service]
    suffix = "db" if purpose == "database" else "admission"
    return reference["name"] == f"vp-wr-{kind}-{suffix}-{generation}" and any(
        _is_rollback_runtime_authority(document, authority)
        and authority["service"] == service and authority["generation"] == generation
        and authority["state"] == "provisioned"
        for authority in document["authorities"]
    )


def _validate_legacy_schema_1_abort_authority(
    value: object,
) -> dict[str, str]:
    authority = _require_exact_fields(
        value,
        {"kind", "service", "generation"},
    )
    if authority["kind"] not in {"control", "runtime"}:
        raise TransactionError
    _require_string(
        authority["service"],
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}",
    )
    _require_string(
        authority["generation"],
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
    )
    if (
        authority["kind"] == "control"
        and authority["service"] != "vp-worker-control"
    ) or (
        authority["kind"] == "runtime"
        and authority["service"] == "vp-worker-control"
    ):
        raise TransactionError
    return authority


def _validate_legacy_schema_1_abort(value: object) -> dict[str, Any]:
    abort = _require_exact_fields(value, {"reason", "authorities"})
    _require_string(
        abort["reason"],
        r"[a-z][a-z0-9_]{0,63}",
        maximum=64,
    )
    if not isinstance(abort["authorities"], list):
        raise TransactionError
    identities: set[tuple[str, str, str]] = set()
    for value in abort["authorities"]:
        authority = _validate_legacy_schema_1_abort_authority(value)
        identity = (
            authority["kind"],
            authority["service"],
            authority["generation"],
        )
        if identity in identities:
            raise TransactionError
        identities.add(identity)
    return abort


def _validate_abort(
    value: object,
    target_commit: str,
) -> dict[str, Any]:
    abort = _require_exact_fields(value, {"reason", "authorities"})
    _require_string(
        abort["reason"],
        r"[a-z][a-z0-9_]{0,63}",
        maximum=64,
    )
    _validate_authorities(abort["authorities"], target_commit)
    return abort


def _validate_control_identity(value: object) -> None:
    control = _require_exact_fields(
        value,
        {"generation", "image", "manifest_sha256", "secrets"},
    )
    _require_string(
        control["generation"],
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
    )
    _require_string(
        control["image"],
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    _require_string(control["manifest_sha256"], r"[0-9a-f]{64}", maximum=64)
    _validate_secret_refs(control["secrets"], exact_count=7)


def _validate_marker_identity(value: object) -> None:
    marker = _require_exact_fields(
        value,
        {
            "generation",
            "image",
            "config_sha256",
            "cron_sha256",
            "secrets",
        },
    )
    _require_string(
        marker["generation"],
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
    )
    _require_string(
        marker["image"],
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    _require_string(marker["config_sha256"], r"[0-9a-f]{64}", maximum=64)
    _require_string(marker["cron_sha256"], r"[0-9a-f]{64}", maximum=64)
    _validate_secret_refs(marker["secrets"])


def _validate_worker_identity(
    value: object,
    *,
    require_target_spec_digest: bool = True,
) -> None:
    fields = {
        "service",
        "generation",
        "commit",
        "image",
        "database_secret",
        "admission_secret",
        "docker_service_id",
        "applied_stage",
    }
    if require_target_spec_digest:
        fields.add("target_spec_digest")
    worker = _require_exact_fields(
        value,
        fields,
    )
    _require_string(worker["service"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    _require_integer(worker["generation"], 1)
    _require_string(worker["commit"], r"[0-9a-f]{40}", maximum=40)
    _require_string(
        worker["image"],
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    if require_target_spec_digest:
        _require_optional_string(
            worker["target_spec_digest"],
            r"[0-9a-f]{64}",
            maximum=64,
        )
    _validate_secret_ref(worker["database_secret"])
    _validate_secret_ref(worker["admission_secret"])
    if (
        worker["database_secret"]["docker_secret_id"]
        == worker["admission_secret"]["docker_secret_id"]
    ):
        raise TransactionError
    if require_target_spec_digest and (
        worker["database_secret"]["service"] != worker["service"]
        or worker["admission_secret"]["service"] != worker["service"]
        or worker["database_secret"]["generation"]
        != str(worker["generation"])
        or worker["admission_secret"]["generation"]
        != str(worker["generation"])
        or worker["database_secret"]["purpose"] != "database"
        or worker["admission_secret"]["purpose"] != "admission"
    ):
        raise TransactionError
    _require_optional_string(
        worker["docker_service_id"],
        r"[0-9a-z]{12,64}",
        maximum=64,
    )
    if worker["applied_stage"] not in {
        "pending",
        "prepared",
        "applied",
        "verified",
    }:
        raise TransactionError
    has_service_id = worker["docker_service_id"] is not None
    if require_target_spec_digest:
        has_spec_digest = worker["target_spec_digest"] is not None
        is_applied = worker["applied_stage"] in {"applied", "verified"}
        if has_service_id != is_applied or has_spec_digest != is_applied:
            raise TransactionError


def _validate_worker_identities(
    value: object,
    *,
    require_target_spec_digest: bool = True,
) -> None:
    if not isinstance(value, list):
        raise TransactionError
    worker_keys: set[tuple[str, int]] = set()
    service_ids: set[str] = set()
    secret_names: set[str] = set()
    secret_ids: set[str] = set()
    for worker in value:
        _validate_worker_identity(
            worker,
            require_target_spec_digest=require_target_spec_digest,
        )
        worker_key = (worker["service"], worker["generation"])
        service_id = worker["docker_service_id"]
        if worker_key in worker_keys or (
            service_id is not None and service_id in service_ids
        ):
            raise TransactionError
        worker_keys.add(worker_key)
        if service_id is not None:
            service_ids.add(service_id)
        for field in ("database_secret", "admission_secret"):
            secret = worker[field]
            if (
                secret["name"] in secret_names
                or secret["docker_secret_id"] in secret_ids
            ):
                raise TransactionError
            secret_names.add(secret["name"])
            secret_ids.add(secret["docker_secret_id"])


def _validate_failed_forward_control(value: object) -> None:
    control = _require_exact_fields(
        value,
        {"generation", "image", "config_sha256", "cron_sha256"},
    )
    _require_string(
        control["generation"],
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
    )
    _require_string(
        control["image"],
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    _require_string(control["config_sha256"], r"[0-9a-f]{64}", maximum=64)
    _require_string(control["cron_sha256"], r"[0-9a-f]{64}", maximum=64)


def _staging_control_config(control: dict, network_id: str) -> bytes:
    _validate_control_identity(control)
    _require_string(network_id, r"[a-z0-9]{12,64}")
    purposes = {
        "operator", "orchestrator", "staging-janitor", "staging-minio-access",
        "staging-minio-secret", "worker-minio-access", "worker-minio-secret",
    }
    references = {item["purpose"]: item for item in control["secrets"]}
    if set(references) != purposes or any(
        item["service"] != "vp-worker-control"
        or item["generation"] != control["generation"]
        for item in references.values()
    ):
        raise TransactionError
    return (
        "VERSION=2\n"
        f"GENERATION={control['generation']}\nIMAGE={control['image']}\n"
        f"NETWORK=vp-pipeline-net\nNETWORK_ID={network_id}\n"
        f"DATABASE_SECRET={references['staging-janitor']['name']}\n"
        f"MINIO_ACCESS_SECRET={references['staging-minio-access']['name']}\n"
        f"MINIO_SECRET_SECRET={references['staging-minio-secret']['name']}\n"
        "EVIDENCE_VOLUME=vp-staging-janitor-evidence\nMANAGER_NODE=ccttww-lap\n"
    ).encode("ascii")


def _control_cron_blocks(sync_root: str) -> tuple[bytes, bytes]:
    _require_string(sync_root, r"/[A-Za-z0-9_./-]+", maximum=4095)
    if str(Path(sync_root)) != sync_root or ".." in Path(sync_root).parts:
        raise TransactionError
    marker_root = sync_root + "/state/worker-redis-marker-control"
    prefix = (
        f"VP_WORKER_REDIS_MARKER_CONFIG_FILE={marker_root}/control.conf "
        f"VP_WORKER_REDIS_MARKER_STATE_DIR={marker_root}/status "
        f"VP_WORKER_REDIS_MARKER_LOCK_DIR={marker_root}/locks "
        f"{sync_root}/bin/worker-redis-marker-control.sh"
    )
    marker = (
        "# BEGIN VIDEOPROCESS WORKER REDIS MARKER CONTROL\n"
        f"* * * * * {prefix} readiness >> {sync_root}/logs/worker-redis-marker-readiness.log 2>&1\n"
        f"*/5 * * * * {prefix} janitor >> {sync_root}/logs/worker-redis-marker-janitor.log 2>&1\n"
        "# END VIDEOPROCESS WORKER REDIS MARKER CONTROL\n"
    ).encode("ascii")
    staging = (
        "# BEGIN VIDEOPROCESS STAGING JANITOR\n"
        f"*/5 * * * * VP_STAGING_JANITOR_CONFIG_FILE={sync_root}/state/vp-worker-admission/staging-object-janitor.conf "
        f"{sync_root}/bin/vp-staging-object-janitor-run.sh >> {sync_root}/logs/vp-staging-object-janitor.log 2>&1\n"
        "# END VIDEOPROCESS STAGING JANITOR\n"
    ).encode("ascii")
    return marker, staging


def _control_cron_foreign(cron: bytes, sync_root: str) -> bytes:
    if not cron or len(cron) > MAX_DOCUMENT_BYTES or not cron.endswith(b"\n"):
        raise TransactionError
    foreign = cron
    for block in _control_cron_blocks(sync_root):
        position = foreign.find(block)
        if foreign.count(block) != 1 or (position > 0 and foreign[position - 1] != 10):
            raise TransactionError
        foreign = foreign.replace(block, b"", 1)
    # Duplicate/malformed blocks and unframed invocations are never foreign data.
    if any(token in foreign for token in (
        b"VIDEOPROCESS STAGING JANITOR", b"VIDEOPROCESS WORKER REDIS MARKER CONTROL",
        b"vp-staging-object-janitor-run.sh", b"worker-redis-marker-control.sh",
    )):
        raise TransactionError
    return foreign


def _control_config_selections(document: dict, network_id: str) -> dict[bytes, dict]:
    selections: dict[bytes, dict] = {}
    for scope in ("baseline", "forward", "rollback"):
        control = document.get(scope, {}).get("control")
        if control is None:
            continue
        payload = _staging_control_config(control, network_id)
        if payload in selections and selections[payload] != control:
            raise TransactionError
        selections[payload] = control
    return selections


def _observe_failed_control(
    document: dict, sync_root: str, network_id: str, config: bytes, cron: bytes,
) -> dict:
    selections = _control_config_selections(document, network_id)
    if config not in selections:
        raise TransactionError
    _control_cron_foreign(cron, sync_root)
    selected = selections[config]
    return dict(
        generation=selected["generation"], image=selected["image"],
        config_sha256=hashlib.sha256(config).hexdigest(),
        cron_sha256=hashlib.sha256(cron).hexdigest(),
    )


def _preinstall_failed_control(document: dict, progress: dict) -> bool:
    attempted = progress.get("attempted_services")
    workers = document["forward"]["workers"]
    return (
        document["baseline"]["kind"] == "managed"
        and document["baseline"]["captured"] is True
        and attempted == [
            "vp-api-swarm", "vp-frontend-swarm", "vp-autoflow-api-swarm",
        ]
        and progress.get("migration_state") == "applied"
        and {item["name"] for item in document["failed_forward"]["services"]}
        == set(attempted)
        and len(workers) == len(RUNTIME_AUTHORITY_SERVICES)
        and {item["service"] for item in workers} == RUNTIME_AUTHORITY_SERVICES
        and all(
            item["applied_stage"] == "prepared"
            and item["docker_service_id"] is None
            and item["target_spec_digest"] is None
            for item in workers
        )
    )


def _recover_failed_control(
    *, document: dict, progress: dict, sync_root: str, network_id: str,
    baseline_cron: bytes | None, current_cron: bytes, current_config: bytes,
    observation: dict | None,
) -> dict:
    if document["phase"] not in {
        "CANDIDATE_RESTORE_REQUIRED", "CANDIDATE_RESTORING", "CANDIDATE_RESTORED",
    } or document["operation"] is not None or not document["failed_forward"]["captured"]:
        raise TransactionError
    expected = document["failed_forward"]["control"]
    _validate_failed_forward_control(expected)
    selections = _control_config_selections(document, network_id)
    matching = [
        control for payload, control in selections.items()
        if hashlib.sha256(payload).hexdigest() == expected["config_sha256"]
    ]
    if len(matching) != 1 or current_config not in selections:
        raise TransactionError
    selected = matching[0]
    actual_labels = (selected["generation"], selected["image"])
    recorded_labels = (expected["generation"], expected["image"])
    if actual_labels != recorded_labels:
        forward = document["forward"]["control"]
        if (
            selected != document["baseline"]["control"]
            or recorded_labels != (forward["generation"], forward["image"])
            or observation is not None
            or not _preinstall_failed_control(document, progress)
        ):
            raise TransactionError
    if observation is not None:
        observation = _require_exact_fields(observation, {"config", "cron"})
        original_config = observation["config"].encode("utf-8")
        original_cron = observation["cron"].encode("utf-8")
        if _observe_failed_control(
            document, sync_root, network_id, original_config, original_cron,
        ) != expected:
            raise TransactionError
    elif (
        actual_labels == recorded_labels
        and hashlib.sha256(current_cron).hexdigest() == expected["cron_sha256"]
    ):
        original_cron = current_cron
    else:
        # Old captures have no raw observation. Prove this exact pre-install
        # preimage using the retained baseline and the normal marker transform.
        if (
            selected != document["baseline"]["control"]
            or not _preinstall_failed_control(document, progress)
            or baseline_cron is None
        ):
            raise TransactionError
        _control_cron_foreign(baseline_cron, sync_root)
        marker, _staging = _control_cron_blocks(sync_root)
        original_cron = baseline_cron.replace(marker, b"", 1) + marker
    if hashlib.sha256(original_cron).hexdigest() != expected["cron_sha256"]:
        raise TransactionError
    if _control_cron_foreign(current_cron, sync_root) != _control_cron_foreign(
        original_cron, sync_root,
    ):
        raise TransactionError
    return selected


def _failed_control_read_file(
    parent_descriptor: int, name: str, *, modes: tuple[int, ...] = (FILE_MODE,),
) -> bytes:
    before = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    mode = stat.S_IMODE(before.st_mode)
    if mode not in modes:
        raise TransactionError
    _require_regular(before, mode, single_link=True)
    descriptor = os.open(name, _read_file_flags(), dir_fd=parent_descriptor)
    try:
        opened = os.fstat(descriptor)
        if opened != before:
            raise TransactionError
        payload = _read_limited(descriptor)
        after = os.fstat(descriptor)
        if (
            _identity(after) != _identity(opened)
            or after.st_size != opened.st_size
            or after.st_mtime_ns != opened.st_mtime_ns
            or after.st_ctime_ns != opened.st_ctime_ns
        ):
            raise TransactionError
        return payload
    finally:
        os.close(descriptor)


def _failed_control_observation(value: object) -> dict:
    result = _require_exact_fields(value, {"config", "cron"})
    if (
        any(not isinstance(item, str) for item in result.values())
        or len(_canonical(result)) > MAX_DOCUMENT_BYTES
    ):
        raise TransactionError
    return result


def failed_control(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, mode, network_id = arguments
    if mode not in {"observe", "select", "verify"}:
        raise TransactionError
    _require_writer_lock(raw_root, raw_lock_descriptor)
    root, root_fd, transactions_fd = _open_transactions(raw_root, create=False)
    transaction_fd = None
    try:
        if root.name != "vp-worker-admission" or root.parent.name != "state":
            raise TransactionError
        sync_root = str(root.parent.parent)
        document, _identity = _read_active_from_descriptor(transactions_fd, allow_missing=False)
        if document is None:
            raise TransactionError
        transaction_fd = _open_child_directory(
            transactions_fd, document["transaction_id"], create=False,
        )
        config = _failed_control_read_file(root_fd, "staging-object-janitor.conf")
        cron = sys.stdin.buffer.read(MAX_DOCUMENT_BYTES + 1)
        name = "failed-control-observation.json"
        try:
            observation = _failed_control_observation(_decode_canonical(
                _failed_control_read_file(transaction_fd, name),
            ))
        except FileNotFoundError:
            observation = None
        if mode == "observe":
            if document["phase"] not in {"PREPARING", "FORWARD_APPLYING"} or document["operation"] is not None:
                raise TransactionError
            identity = _observe_failed_control(document, sync_root, network_id, config, cron)
            if document["failed_forward"]["captured"] and document["failed_forward"]["control"] != identity:
                raise TransactionError
            desired = dict(config=config.decode("utf-8"), cron=cron.decode("utf-8"))
            if observation is not None and observation != desired:
                raise TransactionError
            if observation is None:
                _write_document(
                    transaction_fd, name, desired, expected_identity=None,
                    validator=_failed_control_observation,
                )
            _print_json(identity)
            return
        progress, _progress_identity = _read_app_progress_from_descriptor(
            transaction_fd, allow_missing=False,
        )
        if progress is None or (
            progress["transaction_id"] != document["transaction_id"]
            or progress["target_commit"] != document["target_commit"]
        ):
            raise TransactionError
        baseline_cron = None
        if observation is None:
            descriptors = []
            try:
                _path, marker_fd = _open_admission_root(str(root.parent / "worker-redis-marker-control"))
                descriptors.append(marker_fd)
                for child in ("transactions", document["transaction_id"], "baseline-managed-state"):
                    descriptors.append(_open_child_directory(descriptors[-1], child, create=False))
                if _failed_control_read_file(descriptors[-1], "captured").rstrip(b"\n") != b"VERSION=1":
                    raise TransactionError
                baseline_cron = _failed_control_read_file(
                    descriptors[-1], "crontab", modes=(0o600, 0o644, 0o664),
                )
            except FileNotFoundError:
                # Strict original-byte equality remains available to old,
                # non-hybrid records even when no reconstruction is possible.
                baseline_cron = None
            finally:
                for descriptor in reversed(descriptors):
                    os.close(descriptor)
        selected = _recover_failed_control(
            document=document, progress=progress, sync_root=sync_root,
            network_id=network_id, baseline_cron=baseline_cron,
            current_cron=cron, current_config=config, observation=observation,
        )
        if mode == "verify" and config != _staging_control_config(selected, network_id):
            raise TransactionError
        _print_json(selected)
    finally:
        if transaction_fd is not None:
            os.close(transaction_fd)
        os.close(transactions_fd)
        os.close(root_fd)


def _validate_janitor_service(value: object) -> None:
    service = _require_exact_fields(
        value,
        {"name", "docker_service_id", "generation", "spec_digest"},
    )
    _require_string(service["name"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    _require_string(
        service["docker_service_id"],
        r"[0-9a-z]{12,64}",
        maximum=64,
    )
    _require_string(
        service["generation"],
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
    )
    _require_string(service["spec_digest"], r"[0-9a-f]{64}", maximum=64)


def _validate_vision_jobs(value: object) -> None:
    if not isinstance(value, list):
        raise TransactionError
    modes: set[str] = set()
    service_ids: set[str] = set()
    for value in value:
        job = _require_exact_fields(
            value,
            {
                "mode",
                "name",
                "image",
                "redis_secret",
                "database_secret",
                "docker_service_id",
                "state",
                "exit_code",
            },
        )
        if job["mode"] not in {
            "safety",
            "final-safety",
            "check",
            "reconcile",
        }:
            raise TransactionError
        _require_string(
            job["name"],
            r"vp-vision-cutover-(safety|final-safety|check|reconcile)-[0-9a-f]{12}",
        )
        _require_string(
            job["image"],
            r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
        )
        _validate_secret_ref(job["redis_secret"])
        if job["database_secret"] is not None:
            _validate_secret_ref(job["database_secret"])
        if (job["mode"] in {"safety", "final-safety"}) != (
            job["database_secret"] is not None
        ):
            raise TransactionError
        service_id = _require_optional_string(
            job["docker_service_id"],
            r"[0-9a-z]{12,64}",
            maximum=64,
        )
        if job["state"] not in {"planned", "created", "terminal", "removed"}:
            raise TransactionError
        if (
            (job["state"] == "planned" and service_id is not None)
            or (job["state"] in {"created", "terminal"} and service_id is None)
        ):
            raise TransactionError
        exit_code = job["exit_code"]
        if job["state"] in {"terminal", "removed"}:
            _require_integer(exit_code)
            if exit_code > 255:
                raise TransactionError
        elif exit_code is not None:
            raise TransactionError
        if job["mode"] in modes or (
            service_id is not None and service_id in service_ids
        ):
            raise TransactionError
        modes.add(job["mode"])
        if service_id is not None:
            service_ids.add(service_id)


def _validate_identity(value: object) -> dict[str, Any]:
    identity = _require_exact_fields(value, IDENTITY_FIELDS)
    if identity["kind"] not in {"secret", "service", "manifest"}:
        raise TransactionError
    _require_optional_string(identity["docker_id"], r"[0-9a-z]{12,64}", maximum=64)
    _require_string(identity["name"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    _require_string(identity["service"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    _require_string(identity["generation"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
    _require_string(identity["purpose"], r"[a-z][a-z0-9_-]{0,63}", maximum=64)
    _require_optional_string(identity["spec_digest"], r"[0-9a-f]{64}", maximum=64)
    if identity["kind"] in {"secret", "service"} and identity["docker_id"] is None:
        raise TransactionError
    return identity


def _validate_snapshots(value: object) -> dict[str, Any]:
    snapshots = _require_exact_fields(value, SNAPSHOT_FIELDS)
    _require_exact_schema(snapshots["schema"], 1)
    _require_string(snapshots["transaction_id"], r"tx-[0-9a-f]{32}", maximum=35)
    _require_integer(snapshots["revision"])

    baseline = _require_exact_fields(
        snapshots["baseline"],
        {"control", "services"},
    )
    if baseline["control"] is not None:
        _validate_control_identity(baseline["control"])
    _validate_service_identities(baseline["services"])

    failed_forward = _require_exact_fields(
        snapshots["failed_forward"],
        {"control", "services"},
    )
    if failed_forward["control"] is not None:
        _validate_failed_forward_control(failed_forward["control"])
    _validate_service_identities(failed_forward["services"])

    for field in ("forward", "rollback"):
        selection = _require_exact_fields(
            snapshots[field],
            {"control", "marker", "workers"},
        )
        if selection["control"] is not None:
            _validate_control_identity(selection["control"])
        if selection["marker"] is not None:
            _validate_marker_identity(selection["marker"])
        _validate_worker_identities(
            selection["workers"],
            require_target_spec_digest=False,
        )

    janitor = _require_exact_fields(snapshots["janitor"], {"service"})
    if janitor["service"] is not None:
        _validate_janitor_service(janitor["service"])
    return snapshots


def _validate_legacy_schema_1_document(value: object) -> dict[str, Any]:
    document = _require_exact_fields(
        value,
        LEGACY_SCHEMA_1_TOP_LEVEL_FIELDS,
    )
    _require_exact_schema(document["schema"], 1)
    _require_string(document["transaction_id"], r"tx-[0-9a-f]{32}", maximum=35)
    _require_integer(document["revision"])
    if document["phase"] not in PHASES:
        raise TransactionError
    if document["outcome"] not in {
        None,
        "succeeded",
        "rolled_back",
        "manual",
        "aborted",
    }:
        raise TransactionError
    if (document["phase"] == "DONE") != (document["outcome"] is not None):
        raise TransactionError
    _require_string(document["target_commit"], r"[0-9a-f]{40}", maximum=40)
    _require_string(
        document["target_backend_image"],
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    _require_string(
        document["target_go_image"],
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    _require_string(
        document["created_at"],
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
        maximum=20,
    )
    _validate_database_credentials(document["database_credentials"])

    runtime_redis = document["runtime_redis"]
    if not isinstance(runtime_redis, dict):
        raise TransactionError
    runtime_secret_names: set[str] = set()
    runtime_secret_ids: set[str] = set()
    for role, reference in runtime_redis.items():
        _require_string(role, r"[a-z][a-z0-9_-]{0,63}", maximum=64)
        entry = _require_exact_fields(
            reference,
            {"runtime_generation", "secret_name", "docker_secret_id"},
        )
        _require_string(
            entry["runtime_generation"],
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
        )
        _require_string(
            entry["secret_name"],
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}",
        )
        _require_string(
            entry["docker_secret_id"],
            r"[a-z0-9]{20,64}",
            maximum=64,
        )
        if (
            entry["secret_name"] in runtime_secret_names
            or entry["docker_secret_id"] in runtime_secret_ids
        ):
            raise TransactionError
        runtime_secret_names.add(entry["secret_name"])
        runtime_secret_ids.add(entry["docker_secret_id"])

    _validate_secret_refs(document["prepared_secrets"])

    baseline = _require_exact_fields(
        document["baseline"],
        {"kind", "control", "services"},
    )
    if baseline["kind"] not in {"managed", "legacy_no_control"}:
        raise TransactionError
    if baseline["control"] is not None:
        _validate_control_identity(baseline["control"])
    _validate_service_identities(baseline["services"])

    failed_forward = _require_exact_fields(
        document["failed_forward"],
        {"services", "control"},
    )
    _validate_service_identities(failed_forward["services"])
    if failed_forward["control"] is not None:
        _validate_failed_forward_control(failed_forward["control"])

    forward = _require_exact_fields(
        document["forward"],
        {"namespace", "control", "marker", "workers"},
    )
    _require_string(
        forward["namespace"],
        r"[a-z0-9][a-z0-9-]{0,127}",
        maximum=128,
    )
    if forward["control"] is not None:
        _validate_control_identity(forward["control"])
    if forward["marker"] is not None:
        _validate_marker_identity(forward["marker"])
    _validate_worker_identities(
        forward["workers"],
        require_target_spec_digest=False,
    )

    rollback = _require_exact_fields(
        document["rollback"],
        {"attempt", "control", "marker", "workers"},
    )
    _require_integer(rollback["attempt"])
    if rollback["control"] is not None:
        _validate_control_identity(rollback["control"])
    if rollback["marker"] is not None:
        _validate_marker_identity(rollback["marker"])
    _validate_worker_identities(
        rollback["workers"],
        require_target_spec_digest=False,
    )

    promotion = _require_exact_fields(
        document["promotion"],
        {"workers", "marker", "control"},
    )
    if any(not isinstance(promotion[key], bool) for key in promotion):
        raise TransactionError
    expected_promotion = PROMOTION_BY_PHASE[document["phase"]]
    if document["phase"] == "DONE" and document["outcome"] == "aborted":
        expected_promotion = (False, False, False)
    if (
        promotion["workers"],
        promotion["marker"],
        promotion["control"],
    ) != expected_promotion:
        raise TransactionError

    retirements = document["pending_retirements"]
    if not isinstance(retirements, list):
        raise TransactionError
    retirement_ids: set[str] = set()
    docker_ids: set[str] = set()
    logical_keys: set[tuple[str, str, str, str, str]] = set()
    for item in retirements:
        retirement = _require_exact_fields(
            item,
            {"retirement_id", "identity"},
        )
        retirement_id = _require_string(
            retirement["retirement_id"],
            r"retirement-[0-9a-f]{32}",
            maximum=43,
        )
        identity = _validate_identity(retirement["identity"])
        docker_id = identity["docker_id"]
        logical_key = (
            identity["service"],
            identity["generation"],
            identity["kind"],
            identity["purpose"],
            identity["name"],
        )
        if (
            retirement_id in retirement_ids
            or (docker_id is not None and docker_id in docker_ids)
            or logical_key in logical_keys
        ):
            raise TransactionError
        retirement_ids.add(retirement_id)
        logical_keys.add(logical_key)
        if docker_id is not None:
            docker_ids.add(docker_id)

    janitor = _require_exact_fields(document["janitor"], {"service"})
    if janitor["service"] is not None:
        _validate_janitor_service(janitor["service"])

    if document["last_error"] is not None:
        last_error = _require_exact_fields(
            document["last_error"],
            {"code", "phase"},
        )
        _require_string(
            last_error["code"],
            r"[a-z][a-z0-9_]{0,63}",
            maximum=64,
        )
        if last_error["phase"] not in PHASES:
            raise TransactionError

    abort = document["abort"]
    if abort is None:
        if document["phase"] == "ABORTING" or document["outcome"] == "aborted":
            raise TransactionError
    else:
        _validate_legacy_schema_1_abort(abort)
        if document["phase"] != "ABORTING" and not (
            document["phase"] == "DONE" and document["outcome"] == "aborted"
        ):
            raise TransactionError

    if document["operation"] is not None:
        operation = _require_exact_fields(
            document["operation"],
            {"operation_id", "kind", "target_phase", "identity"},
        )
        _require_string(
            operation["operation_id"],
            r"operation-[0-9a-f]{32}",
            maximum=42,
        )
        if operation["kind"] not in INTENT_PHASES:
            raise TransactionError
        current_phase, target_phase = INTENT_PHASES[operation["kind"]]
        if (
            document["phase"] != current_phase
            or operation["target_phase"] != target_phase
        ):
            raise TransactionError
        identity = _validate_identity(operation["identity"])
        if operation["kind"] == "REMOVE_PREPARED_SECRET":
            matches = [
                reference
                for reference in document["prepared_secrets"]
                if (
                    reference["docker_secret_id"] == identity["docker_id"]
                    and reference["name"] == identity["name"]
                    and reference["service"] == identity["service"]
                    and reference["generation"] == identity["generation"]
                    and reference["purpose"] == identity["purpose"]
                    and identity["kind"] == "secret"
                    and identity["spec_digest"] is None
                )
            ]
            if len(matches) != 1:
                raise TransactionError
    return document


def _validate_document(value: object) -> dict[str, Any]:
    fields = TOP_LEVEL_FIELDS | (
        {"registered_reconcile"}
        if isinstance(value, dict) and "registered_reconcile" in value
        else set()
    )
    document = _require_exact_fields(value, fields)
    if "registered_reconcile" in document:
        _validate_registered_jobs(document["registered_reconcile"])
    _require_exact_schema(document["schema"], CURRENT_DOCUMENT_SCHEMA)
    _require_string(document["transaction_id"], r"tx-[0-9a-f]{32}", maximum=35)
    _require_integer(document["revision"])
    if document["phase"] not in PHASES:
        raise TransactionError
    if document["outcome"] not in {
        None,
        "succeeded",
        "rolled_back",
        "manual",
        "aborted",
    }:
        raise TransactionError
    if (document["phase"] == "DONE") != (document["outcome"] is not None):
        raise TransactionError
    _require_string(document["target_commit"], r"[0-9a-f]{40}", maximum=40)
    _require_string(
        document["target_backend_image"],
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    _require_string(
        document["target_go_image"],
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    _require_string(
        document["created_at"],
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
        maximum=20,
    )
    _validate_database_credentials(document["database_credentials"])

    runtime_redis = document["runtime_redis"]
    if not isinstance(runtime_redis, dict):
        raise TransactionError
    runtime_secret_names: set[str] = set()
    runtime_secret_ids: set[str] = set()
    for role, reference in runtime_redis.items():
        _require_string(role, r"[a-z][a-z0-9_-]{0,63}", maximum=64)
        entry = _require_exact_fields(
            reference,
            {"runtime_generation", "secret_name", "docker_secret_id"},
        )
        _require_string(
            entry["runtime_generation"],
            r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
        )
        _require_string(entry["secret_name"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
        _require_string(
            entry["docker_secret_id"],
            r"[a-z0-9]{20,64}",
            maximum=64,
        )
        if (
            entry["secret_name"] in runtime_secret_names
            or entry["docker_secret_id"] in runtime_secret_ids
        ):
            raise TransactionError
        runtime_secret_names.add(entry["secret_name"])
        runtime_secret_ids.add(entry["docker_secret_id"])

    baseline = _require_exact_fields(
        document["baseline"],
        {"captured", "kind", "control", "services"},
    )
    if not isinstance(baseline["captured"], bool):
        raise TransactionError
    if baseline["kind"] not in {"managed", "legacy_no_control"}:
        raise TransactionError
    if baseline["control"] is not None:
        _validate_control_identity(baseline["control"])
    _validate_service_identities(baseline["services"])
    baseline_names = {service["name"] for service in baseline["services"]}
    if baseline["captured"]:
        if baseline_names != APP_SERVICES:
            raise TransactionError
    elif baseline["control"] is not None or baseline["services"]:
        raise TransactionError

    failed_forward = _require_exact_fields(
        document["failed_forward"],
        {"captured", "services", "control"},
    )
    if not isinstance(failed_forward["captured"], bool):
        raise TransactionError
    _validate_service_identities(failed_forward["services"])
    if failed_forward["control"] is not None:
        _validate_failed_forward_control(failed_forward["control"])
    if not failed_forward["captured"] and (
        failed_forward["control"] is not None or failed_forward["services"]
    ):
        raise TransactionError
    if any(
        service["name"] not in APP_SERVICES
        for service in failed_forward["services"]
    ):
        raise TransactionError

    forward = _require_exact_fields(
        document["forward"],
        {"namespace", "control", "marker", "workers"},
    )
    _require_string(
        forward["namespace"],
        r"[a-z0-9][a-z0-9-]{0,127}",
        maximum=128,
    )
    if forward["control"] is not None:
        _validate_control_identity(forward["control"])
    if forward["marker"] is not None:
        _validate_marker_identity(forward["marker"])
    _validate_worker_identities(forward["workers"])

    rollback = _require_exact_fields(
        document["rollback"],
        {
            "attempt",
            "namespace",
            "marker_generation",
            "control",
            "marker",
            "workers",
        },
    )
    _require_integer(rollback["attempt"])
    _require_optional_string(
        rollback["namespace"],
        r"rollback-[1-9][0-9]{1,19}",
        maximum=29,
    )
    _require_optional_string(
        rollback["marker_generation"],
        r"m-rb-[0-9a-f]{12}-[1-9][0-9]*",
        maximum=64,
    )
    if rollback["attempt"] == 0:
        rollback_identity_valid = (
            rollback["namespace"] is None
            and rollback["marker_generation"] is None
        )
    else:
        rollback_identity_valid = (
            rollback["namespace"] is not None
            and rollback["marker_generation"] is not None
        )
    if not rollback_identity_valid:
        raise TransactionError
    if rollback["control"] is not None:
        _validate_control_identity(rollback["control"])
    if rollback["marker"] is not None:
        _validate_marker_identity(rollback["marker"])
    _validate_worker_identities(rollback["workers"])

    authorities = _validate_authorities(
        document["authorities"], document["target_commit"], document=document,
    )
    for authority in authorities:
        if document["phase"] in {"PREPARING", "FORWARD_APPLYING"} or (
            document["phase"] == "ROLLBACK_PREPARING"
            and _is_rollback_runtime_authority(document, authority)
        ):
            if authority["state"] == "revoked":
                raise TransactionError
        elif document["phase"] == "ABORTING" or (
            document["phase"] == "DONE" and document["outcome"] == "aborted"
        ):
            pass
        elif authority["state"] != "provisioned":
            raise TransactionError

    _validate_secret_refs(document["prepared_secrets"])
    for reference in document["prepared_secrets"]:
        if _is_rollback_prepared_secret(document, reference) or (
            reference["service"] == "vision-cutover"
            and reference["generation"] == document["transaction_id"]
            and reference["purpose"] in {"safety-database", "final-safety-database"}
        ):
            continue
        if reference["service"] == "vp-worker-control":
            expected_kind = "control"
        elif reference["service"] == "worker-redis-marker-control":
            expected_kind = "marker"
        else:
            expected_kind = "runtime"
        matches = [
            authority for authority in authorities
            if authority["kind"] == expected_kind
            and authority["service"] == reference["service"]
            and authority["generation"] == reference["generation"]
            and authority["state"] == "provisioned"
            and not _is_rollback_runtime_authority(document, authority)
        ]
        if len(matches) != 1:
            raise TransactionError

    retiring_outcome = document["retiring_outcome"]
    if retiring_outcome not in {None, "succeeded", "rolled_back", "manual"}:
        raise TransactionError
    if document["phase"] == "RETIRING":
        if retiring_outcome is None:
            raise TransactionError
    elif document["phase"] == "DONE" and document["outcome"] != "aborted":
        if retiring_outcome != document["outcome"]:
            raise TransactionError
    elif retiring_outcome is not None:
        raise TransactionError

    phase = document["phase"]
    transaction_advanced = phase not in {"PREPARING", "ABORTING"} and not (
        phase == "DONE" and document["outcome"] == "aborted"
    )
    if transaction_advanced and not baseline["captured"]:
        raise TransactionError
    rollback_started = phase in {
        "ROLLBACK_PREPARING",
        "ROLLBACK_APPLYING",
        "ROLLBACK_VERIFIED",
        "ROLLBACK_WORKERS_PROMOTED",
        "ROLLBACK_MARKER_PROMOTED",
        "ROLLBACK_CONTROL_PROMOTED",
        "CANDIDATE_RESTORE_REQUIRED",
        "CANDIDATE_RESTORING",
        "CANDIDATE_RESTORED",
    } or (
        phase in {"RETIRING", "DONE"}
        and retiring_outcome == "rolled_back"
    )
    if rollback_started and not failed_forward["captured"]:
        raise TransactionError
    rollback_allocated = phase in {
        "ROLLBACK_APPLYING",
        "ROLLBACK_VERIFIED",
        "ROLLBACK_WORKERS_PROMOTED",
        "ROLLBACK_MARKER_PROMOTED",
        "ROLLBACK_CONTROL_PROMOTED",
    } or (
        phase in {"RETIRING", "DONE"}
        and retiring_outcome == "rolled_back"
    )
    if rollback_allocated and rollback["attempt"] < 1:
        raise TransactionError
    forward_verified = phase in {
        "FORWARD_VERIFIED",
        "WORKERS_PROMOTED",
        "MARKER_PROMOTED",
        "CONTROL_PROMOTED",
    } or (
        phase in {"RETIRING", "DONE"}
        and retiring_outcome == "succeeded"
    )
    if forward_verified and any(
        worker["applied_stage"] != "verified"
        for worker in forward["workers"]
    ):
        raise TransactionError
    rollback_verified = phase in {
        "ROLLBACK_VERIFIED",
        "ROLLBACK_WORKERS_PROMOTED",
        "ROLLBACK_MARKER_PROMOTED",
        "ROLLBACK_CONTROL_PROMOTED",
    } or (
        phase in {"RETIRING", "DONE"}
        and retiring_outcome == "rolled_back"
    )
    if rollback_verified and any(
        worker["applied_stage"] != "verified"
        for worker in rollback["workers"]
    ):
        raise TransactionError

    promotion = _require_exact_fields(
        document["promotion"],
        {"workers", "marker", "control"},
    )
    if any(not isinstance(promotion[key], bool) for key in promotion):
        raise TransactionError
    expected_promotion = PROMOTION_BY_PHASE[document["phase"]]
    if document["phase"] == "DONE" and document["outcome"] == "aborted":
        expected_promotion = (False, False, False)
    if (
        promotion["workers"],
        promotion["marker"],
        promotion["control"],
    ) != expected_promotion:
        raise TransactionError

    retirements = document["pending_retirements"]
    if not isinstance(retirements, list):
        raise TransactionError
    retirement_ids: set[str] = set()
    docker_ids: set[str] = set()
    logical_keys: set[tuple[str, str, str, str, str]] = set()
    for item in retirements:
        retirement = _require_exact_fields(
            item,
            {"retirement_id", "identity"},
        )
        retirement_id = _require_string(
            retirement["retirement_id"],
            r"retirement-[0-9a-f]{32}",
            maximum=43,
        )
        identity = _validate_identity(retirement["identity"])
        docker_id = identity["docker_id"]
        logical_key = (
            identity["service"],
            identity["generation"],
            identity["kind"],
            identity["purpose"],
            identity["name"],
        )
        if retirement_id in retirement_ids or (
            docker_id is not None and docker_id in docker_ids
        ) or logical_key in logical_keys:
            raise TransactionError
        retirement_ids.add(retirement_id)
        logical_keys.add(logical_key)
        if docker_id is not None:
            docker_ids.add(docker_id)

    janitor = _require_exact_fields(document["janitor"], {"service"})
    if janitor["service"] is not None:
        _validate_janitor_service(janitor["service"])
    _validate_vision_jobs(document["vision_jobs"])

    if document["last_error"] is not None:
        last_error = _require_exact_fields(
            document["last_error"],
            {"code", "phase"},
        )
        _require_string(last_error["code"], r"[a-z][a-z0-9_]{0,63}", maximum=64)
        if last_error["phase"] not in PHASES:
            raise TransactionError

    abort = document["abort"]
    if abort is None:
        if document["phase"] == "ABORTING" or document["outcome"] == "aborted":
            raise TransactionError
    else:
        _validate_abort(abort, document["target_commit"])
        if document["phase"] != "ABORTING" and not (
            document["phase"] == "DONE" and document["outcome"] == "aborted"
        ):
            raise TransactionError
        remaining_authorities = [
            authority
            for authority in authorities
            if authority["state"] != "revoked"
        ]
        if abort["authorities"] != remaining_authorities:
            raise TransactionError

    if document["operation"] is not None:
        operation = _require_exact_fields(
            document["operation"],
            {"operation_id", "kind", "target_phase", "identity"},
        )
        _require_string(
            operation["operation_id"],
            r"operation-[0-9a-f]{32}",
            maximum=42,
        )
        if operation["kind"] not in INTENT_PHASES:
            raise TransactionError
        current_phase, target_phase = INTENT_PHASES[operation["kind"]]
        if document["phase"] != current_phase or operation["target_phase"] != target_phase:
            raise TransactionError
        identity = _validate_identity(operation["identity"])
        if operation["kind"] == "REMOVE_PREPARED_SECRET":
            matches = [
                reference
                for reference in document["prepared_secrets"]
                if (
                    reference["docker_secret_id"] == identity["docker_id"]
                    and reference["name"] == identity["name"]
                    and reference["service"] == identity["service"]
                    and reference["generation"] == identity["generation"]
                    and reference["purpose"] == identity["purpose"]
                    and identity["kind"] == "secret"
                    and identity["spec_digest"] is None
                )
            ]
            if len(matches) != 1:
                raise TransactionError
    return document


@overload
def _read_active_from_descriptor(
    transactions_descriptor: int,
    *,
    allow_missing: bool,
    allow_legacy_quarantine: Literal[False] = False,
) -> tuple[dict[str, Any] | None, tuple[int, int] | None]: ...


@overload
def _read_active_from_descriptor(
    transactions_descriptor: int,
    *,
    allow_missing: bool,
    allow_legacy_quarantine: Literal[True],
) -> tuple[
    dict[str, Any] | LegacySchema1Quarantine | None,
    tuple[int, int] | None,
]: ...


def _read_active_from_descriptor(
    transactions_descriptor: int,
    *,
    allow_missing: bool,
    allow_legacy_quarantine: bool = False,
) -> tuple[
    dict[str, Any] | LegacySchema1Quarantine | None,
    tuple[int, int] | None,
]:
    try:
        before = os.stat(
            ACTIVE_NAME,
            dir_fd=transactions_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        if allow_missing:
            return None, None
        raise TransactionError
    _require_regular(before, FILE_MODE, single_link=True)
    descriptor = os.open(
        ACTIVE_NAME,
        _read_file_flags(),
        dir_fd=transactions_descriptor,
    )
    try:
        opened = os.fstat(descriptor)
        _require_regular(opened, FILE_MODE, single_link=True)
        if _identity(before) != _identity(opened):
            raise TransactionError
        payload = _read_limited(descriptor)
    finally:
        os.close(descriptor)
    decoded = _decode_canonical(payload)
    if isinstance(decoded, dict) and set(decoded) == LEGACY_SCHEMA_1_TOP_LEVEL_FIELDS:
        _require_exact_schema(decoded.get("schema"), 1)
        legacy = _validate_legacy_schema_1_document(decoded)
        if not allow_legacy_quarantine:
            raise TransactionError
        document: dict[str, Any] | LegacySchema1Quarantine = (
            LegacySchema1Quarantine(
                transaction_id=legacy["transaction_id"],
                revision=legacy["revision"],
                phase=legacy["phase"],
                journal_sha256=hashlib.sha256(payload).hexdigest(),
            )
        )
    else:
        document = _validate_document(decoded)
    return document, _identity(opened)


def _read_app_progress_from_descriptor(
    transaction_descriptor: int,
    *,
    allow_missing: bool,
) -> tuple[dict[str, Any] | None, tuple[int, int] | None]:
    try:
        before = os.stat(
            APP_PROGRESS_NAME,
            dir_fd=transaction_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        if allow_missing:
            return None, None
        raise TransactionError
    _require_regular(before, FILE_MODE, single_link=True)
    descriptor = os.open(
        APP_PROGRESS_NAME,
        _read_file_flags(),
        dir_fd=transaction_descriptor,
    )
    try:
        opened = os.fstat(descriptor)
        _require_regular(opened, FILE_MODE, single_link=True)
        if _identity(before) != _identity(opened):
            raise TransactionError
        payload = _read_limited(descriptor)
    finally:
        os.close(descriptor)
    return _validate_app_progress(_decode_canonical(payload)), _identity(opened)


def _write_document(
    parent_descriptor: int,
    destination_name: str,
    document: dict[str, Any],
    *,
    expected_identity: tuple[int, int] | None,
    validator: Any,
) -> None:
    validator(document)
    payload = _canonical(document)
    temporary_name = f".{destination_name}.tmp.{secrets.token_hex(16)}"
    descriptor = os.open(
        temporary_name,
        (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0)
        ),
        FILE_MODE,
        dir_fd=parent_descriptor,
    )
    try:
        _write_all(descriptor, payload)
        os.fchmod(descriptor, FILE_MODE)
        os.fsync(descriptor)
        _require_regular(os.fstat(descriptor), FILE_MODE, single_link=True)
    except Exception:
        os.close(descriptor)
        try:
            os.unlink(temporary_name, dir_fd=parent_descriptor)
        except OSError:
            pass
        raise
    else:
        os.close(descriptor)
    try:
        try:
            current = os.stat(
                destination_name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            current_identity = None
        else:
            _require_regular(current, FILE_MODE, single_link=True)
            current_identity = _identity(current)
        if current_identity != expected_identity:
            raise TransactionError
        os.replace(
            temporary_name,
            destination_name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
        )
        os.fsync(parent_descriptor)
    except Exception:
        try:
            os.unlink(temporary_name, dir_fd=parent_descriptor)
        except OSError:
            pass
        raise


def _write_active(
    transactions_descriptor: int,
    document: dict[str, Any],
    *,
    expected_identity: tuple[int, int] | None,
) -> None:
    _write_document(
        transactions_descriptor,
        ACTIVE_NAME,
        document,
        expected_identity=expected_identity,
        validator=_validate_document,
    )


def _new_snapshots(document: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": 1,
        "transaction_id": document["transaction_id"],
        "revision": 0,
        "baseline": {"control": None, "services": []},
        "failed_forward": {"control": None, "services": []},
        "forward": {"control": None, "marker": None, "workers": []},
        "rollback": {"control": None, "marker": None, "workers": []},
        "janitor": {"service": None},
    }


def _load_identity_file(raw_path: str) -> dict[str, Any]:
    path = _require_absolute(raw_path)
    before = os.lstat(path)
    _require_regular(before, FILE_MODE, single_link=True)
    descriptor = os.open(path, _read_file_flags())
    try:
        opened = os.fstat(descriptor)
        _require_regular(opened, FILE_MODE, single_link=True)
        if _identity(before) != _identity(opened):
            raise TransactionError
        payload = _read_limited(descriptor)
    finally:
        os.close(descriptor)
    return _validate_identity(_decode_canonical(payload))


def _capture_credential(raw_path: str, expected_principal: str) -> dict[str, Any]:
    path = _require_absolute(raw_path)
    lexical = os.lstat(path)
    _require_regular(lexical, CREDENTIAL_MODE, single_link=False)
    try:
        canonical = path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise TransactionError from error
    metadata = os.stat(canonical)
    _require_regular(metadata, CREDENTIAL_MODE, single_link=False)
    descriptor = os.open(canonical, _read_file_flags())
    try:
        opened = os.fstat(descriptor)
        _require_regular(opened, CREDENTIAL_MODE, single_link=False)
        if _identity(metadata) != _identity(opened):
            raise TransactionError
    finally:
        os.close(descriptor)
    principal = _require_string(
        expected_principal,
        r"[A-Za-z_][A-Za-z0-9_.$@-]{0,127}",
        maximum=128,
    )
    return {
        "canonical_path": str(canonical),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "mode": stat.S_IMODE(metadata.st_mode),
        "expected_principal": principal,
    }


def _capture_credentials(arguments: list[str]) -> dict[str, dict[str, Any]]:
    if len(arguments) != len(DATABASE_PURPOSES) * 2:
        raise TransactionError
    credentials: dict[str, dict[str, Any]] = {}
    for index, purpose in enumerate(DATABASE_PURPOSES):
        credentials[purpose] = _capture_credential(
            arguments[index * 2],
            arguments[index * 2 + 1],
        )
    _validate_database_credentials(credentials)
    return credentials


def _read_captured_credentials() -> dict[str, Any]:
    payload = sys.stdin.buffer.read(MAX_DOCUMENT_BYTES + 1)
    if len(payload) > MAX_DOCUMENT_BYTES:
        raise TransactionError
    return _validate_database_credentials(_decode_canonical(payload))


def _verify_captured_credentials(
    credentials: dict[str, Any],
) -> dict[str, Any]:
    _validate_database_credentials(credentials)
    for purpose in DATABASE_PURPOSES:
        expected = credentials[purpose]
        captured = _capture_credential(
            expected["canonical_path"],
            expected["expected_principal"],
        )
        if captured != expected:
            raise TransactionError
    return credentials


def prepare_lock(raw_root: str) -> None:
    root, root_descriptor = _open_admission_root(raw_root)
    created = False
    try:
        try:
            descriptor = os.open(
                LOCK_NAME,
                (
                    os.O_RDWR
                    | os.O_CREAT
                    | os.O_EXCL
                    | os.O_CLOEXEC
                    | getattr(os, "O_NOFOLLOW", 0)
                ),
                LOCK_MODE,
                dir_fd=root_descriptor,
            )
            created = True
        except FileExistsError:
            before = os.stat(
                LOCK_NAME,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
            _require_lock(before)
            descriptor = os.open(
                LOCK_NAME,
                _lock_file_flags(),
                dir_fd=root_descriptor,
            )
        try:
            opened = os.fstat(descriptor)
            _require_lock(opened)
            if not created and _identity(before) != _identity(opened):
                raise TransactionError
            if created:
                os.fchmod(descriptor, LOCK_MODE)
                os.fsync(descriptor)
                os.fsync(root_descriptor)
        finally:
            os.close(descriptor)
    finally:
        os.close(root_descriptor)
    print(root / LOCK_NAME)


def acquire_lock(raw_root: str, raw_descriptor: str) -> str:
    _root, root_descriptor = _open_admission_root(raw_root)
    try:
        try:
            descriptor = int(raw_descriptor, 10)
        except ValueError as error:
            raise TransactionError from error
        if descriptor < 3:
            raise TransactionError
        path_metadata = os.stat(
            LOCK_NAME,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        _require_lock(path_metadata)
        opened = os.fstat(descriptor)
        _require_lock(opened)
        if _identity(path_metadata) != _identity(opened):
            raise TransactionError
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(LOCK_CONTENTION_STATUS)
        verified = os.stat(
            LOCK_NAME,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        _require_lock(verified)
        if _identity(opened) != _identity(verified):
            raise TransactionError
        return f"{opened.st_dev}:{opened.st_ino}"
    finally:
        os.close(root_descriptor)


def _require_writer_lock(raw_root: str, raw_descriptor: str) -> None:
    acquire_lock(raw_root, raw_descriptor)


def _parse_revision(raw_revision: str) -> int:
    if re.fullmatch(r"0|[1-9][0-9]{0,18}", raw_revision) is None:
        raise TransactionError
    return int(raw_revision, 10)


def _print_json(value: object) -> None:
    sys.stdout.buffer.write(_canonical(value))


def _read_stdin_json() -> object:
    payload = sys.stdin.buffer.read(MAX_DOCUMENT_BYTES + 1)
    if not payload or len(payload) > MAX_DOCUMENT_BYTES:
        raise TransactionError
    return _decode_canonical(payload)


def _new_document(
    *,
    target_commit: str,
    target_backend_image: str,
    target_go_image: str,
    namespace: str,
    baseline_kind: str,
    credentials: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    _require_string(target_commit, r"[0-9a-f]{40}", maximum=40)
    _require_string(
        target_backend_image,
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    _require_string(
        target_go_image,
        r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}",
    )
    _require_string(namespace, r"[a-z0-9][a-z0-9-]{0,127}", maximum=128)
    if baseline_kind not in {"managed", "legacy_no_control"}:
        raise TransactionError
    created_at = (
        datetime.datetime.now(datetime.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    return {
        "schema": CURRENT_DOCUMENT_SCHEMA,
        "transaction_id": f"tx-{secrets.token_hex(16)}",
        "revision": 0,
        "phase": "PREPARING",
        "outcome": None,
        "target_commit": target_commit,
        "target_backend_image": target_backend_image,
        "target_go_image": target_go_image,
        "created_at": created_at,
        "database_credentials": credentials,
        "runtime_redis": {},
        "authorities": [],
        "prepared_secrets": [],
        "baseline": {
            "captured": False,
            "kind": baseline_kind,
            "control": None,
            "services": [],
        },
        "failed_forward": {
            "captured": False,
            "services": [],
            "control": None,
        },
        "forward": {
            "namespace": namespace,
            "control": None,
            "marker": None,
            "workers": [],
        },
        "rollback": {
            "attempt": 0,
            "namespace": None,
            "marker_generation": None,
            "control": None,
            "marker": None,
            "workers": [],
        },
        "promotion": {
            "workers": False,
            "marker": False,
            "control": False,
        },
        "pending_retirements": [],
        "retiring_outcome": None,
        "janitor": {"service": None},
        "vision_jobs": [],
        "last_error": None,
        "operation": None,
        "abort": None,
    }


def begin(arguments: list[str]) -> None:
    if len(arguments) != 7:
        raise TransactionError
    (
        raw_root,
        raw_lock_descriptor,
        target_commit,
        target_backend_image,
        target_go_image,
        namespace,
        baseline_kind,
    ) = arguments
    _require_writer_lock(raw_root, raw_lock_descriptor)
    credentials = _verify_captured_credentials(
        _read_captured_credentials()
    )
    document = _new_document(
        target_commit=target_commit,
        target_backend_image=target_backend_image,
        target_go_image=target_go_image,
        namespace=namespace,
        baseline_kind=baseline_kind,
        credentials=credentials,
    )
    document["registered_reconcile"] = dict(
        version=1, baseline=None, current=None, run=None
    )
    root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=True,
    )
    try:
        active, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=True,
        )
        if active is not None:
            raise TransactionError
        transaction_directory = document["transaction_id"]
        transaction_descriptor = _open_child_directory(
            transactions_descriptor,
            transaction_directory,
            create=True,
        )
        try:
            _write_document(
                transaction_descriptor,
                SNAPSHOTS_NAME,
                _new_snapshots(document),
                expected_identity=None,
                validator=_validate_snapshots,
            )
        finally:
            os.close(transaction_descriptor)
        _write_active(
            transactions_descriptor,
            document,
            expected_identity=None,
        )
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    _print_json(document)


def verify_preparing(arguments: list[str]) -> None:
    if len(arguments) != 5:
        raise TransactionError
    (
        raw_root,
        raw_lock_descriptor,
        target_commit,
        target_backend_image,
        target_go_image,
    ) = arguments
    _require_writer_lock(raw_root, raw_lock_descriptor)
    credentials = _verify_captured_credentials(
        _read_captured_credentials()
    )
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if (
            document is None
            or document["phase"] != "PREPARING"
            or document["target_commit"] != target_commit
            or document["target_backend_image"] != target_backend_image
            or document["target_go_image"] != target_go_image
            or document["database_credentials"] != credentials
        ):
            raise TransactionError
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    _print_json(document)


def validate_credentials(arguments: list[str]) -> None:
    credentials = _capture_credentials(arguments)
    _print_json(credentials)


def verify_credential_record(arguments: list[str]) -> None:
    if len(arguments) != 3:
        raise TransactionError
    purpose, raw_path, expected_principal = arguments
    if purpose not in DATABASE_PURPOSES:
        raise TransactionError
    credentials = _read_captured_credentials()
    expected = credentials[purpose]
    captured = _capture_credential(raw_path, expected_principal)
    if captured != expected:
        raise TransactionError
    print(expected["canonical_path"])


def verify_credential(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, purpose, raw_path = arguments
    if purpose not in DATABASE_PURPOSES:
        raise TransactionError
    _require_writer_lock(raw_root, raw_lock_descriptor)
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if document is None:
            raise TransactionError
        expected = document["database_credentials"][purpose]
        captured = _capture_credential(
            raw_path,
            expected["expected_principal"],
        )
        if captured != expected:
            raise TransactionError
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    print(captured["canonical_path"])


def _prepared_secret_reference(arguments: list[str]) -> dict[str, str]:
    if len(arguments) != 5:
        raise TransactionError
    name, docker_secret_id, service, generation, purpose = arguments
    reference = {
        "name": name,
        "docker_secret_id": docker_secret_id,
        "service": service,
        "generation": generation,
        "purpose": purpose,
    }
    _validate_secret_ref(reference)
    return reference


def _update_current_document(
    raw_root: str,
    raw_lock_descriptor: str,
    updater: Any,
) -> dict[str, Any]:
    _require_writer_lock(raw_root, raw_lock_descriptor)
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if document is None:
            raise TransactionError
        revision = document["revision"]
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    return _update_document(
        raw_root,
        raw_lock_descriptor,
        str(revision),
        updater,
    )


def record_authority_intent(arguments: list[str]) -> None:
    if len(arguments) != 8:
        raise TransactionError
    (
        raw_root,
        raw_lock_descriptor,
        kind,
        service,
        generation,
        control_image,
        control_generation,
        operator_reference,
    ) = arguments

    def updater(document: dict[str, Any]) -> bool | None:
        if document["phase"] not in {
            "PREPARING", "FORWARD_APPLYING", "ROLLBACK_PREPARING", "ROLLBACK_APPLYING",
        }:
            raise TransactionError
        rollback_control = (
            _rollback_control(document) if document["phase"].startswith("ROLLBACK_") else None
        )
        authority = _validate_authority(
            {
                "kind": kind,
                "service": service,
                "generation": generation,
                "state": "planned",
                "control_image": control_image,
                "control_generation": control_generation,
                "operator_reference": operator_reference,
            },
            document["target_commit"],
            rollback_control=rollback_control,
        )
        if document["phase"].startswith("ROLLBACK_") and (
            rollback_control is None or not _is_rollback_runtime_authority(document, authority)
        ):
            raise TransactionError
        identity = (
            authority["kind"],
            authority["service"],
            authority["generation"],
        )
        for existing in document["authorities"]:
            existing_identity = (
                existing["kind"],
                existing["service"],
                existing["generation"],
            )
            if existing_identity != identity:
                continue
            if (
                existing["control_image"] != authority["control_image"]
                or existing["control_generation"]
                != authority["control_generation"]
                or existing["operator_reference"]
                != authority["operator_reference"]
                or existing["state"] == "revoked"
            ):
                raise TransactionError
            if document["phase"] == "ROLLBACK_APPLYING":
                if existing["state"] != "provisioned":
                    raise TransactionError
                return False
            return None
        if document["phase"] == "ROLLBACK_APPLYING":
            raise TransactionError
        document["authorities"].append(authority)
        return None

    document = _update_current_document(
        raw_root,
        raw_lock_descriptor,
        updater,
    )
    _print_json(document)


def _mark_authority(
    arguments: list[str],
    *,
    target_state: str,
) -> None:
    if len(arguments) != 5:
        raise TransactionError
    raw_root, raw_lock_descriptor, kind, service, generation = arguments

    def updater(document: dict[str, Any]) -> bool | None:
        if document["phase"] not in {
            "PREPARING", "FORWARD_APPLYING", "ROLLBACK_PREPARING", "ROLLBACK_APPLYING",
        }:
            raise TransactionError
        matches = [
            authority
            for authority in document["authorities"]
            if (
                authority["kind"] == kind
                and authority["service"] == service
                and authority["generation"] == generation
            )
        ]
        if len(matches) != 1:
            raise TransactionError
        authority = matches[0]
        if document["phase"].startswith("ROLLBACK_") and not _is_rollback_runtime_authority(document, authority):
            raise TransactionError
        if document["phase"] == "ROLLBACK_APPLYING":
            if authority["state"] != "provisioned":
                raise TransactionError
            return False
        if target_state == "provisioning":
            if authority["state"] == "planned":
                authority["state"] = "provisioning"
            elif authority["state"] not in {"provisioning", "provisioned"}:
                raise TransactionError
        elif target_state == "provisioned":
            if authority["state"] == "provisioning":
                authority["state"] = "provisioned"
            elif authority["state"] != "provisioned":
                raise TransactionError
        else:
            raise TransactionError
        return None

    document = _update_current_document(
        raw_root,
        raw_lock_descriptor,
        updater,
    )
    _print_json(document)


def mark_authority_provisioning(arguments: list[str]) -> None:
    _mark_authority(arguments, target_state="provisioning")


def mark_authority_provisioned(arguments: list[str]) -> None:
    _mark_authority(arguments, target_state="provisioned")


def record_prepared_secret(arguments: list[str]) -> None:
    if len(arguments) != 7:
        raise TransactionError
    raw_root, raw_lock_descriptor, *reference_arguments = arguments
    reference = _prepared_secret_reference(reference_arguments)

    def updater(document: dict[str, Any]) -> bool | None:
        if document["phase"] not in {"PREPARING", "FORWARD_APPLYING"} and not (
            document["phase"] in {"ROLLBACK_PREPARING", "ROLLBACK_APPLYING"}
            and _is_rollback_prepared_secret(document, reference)
        ):
            raise TransactionError
        logical_key = (
            reference["name"],
            reference["service"],
            reference["generation"],
            reference["purpose"],
        )
        for existing in document["prepared_secrets"]:
            existing_key = (
                existing["name"],
                existing["service"],
                existing["generation"],
                existing["purpose"],
            )
            if existing == reference:
                return False if document["phase"] == "ROLLBACK_APPLYING" else None
            if (
                existing["name"] == reference["name"]
                or existing["docker_secret_id"]
                == reference["docker_secret_id"]
                or existing_key == logical_key
            ):
                raise TransactionError
        if document["phase"] == "ROLLBACK_APPLYING":
            raise TransactionError
        document["prepared_secrets"].append(reference)
        return None

    document = _update_current_document(
        raw_root,
        raw_lock_descriptor,
        updater,
    )
    _print_json(document)


def lookup_prepared_secret(arguments: list[str]) -> None:
    if len(arguments) != 6:
        raise TransactionError
    (
        raw_root,
        raw_lock_descriptor,
        name,
        service,
        generation,
        purpose,
    ) = arguments
    _require_string(name, r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    _require_string(service, r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    _require_string(generation, r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
    _require_string(purpose, r"[a-z][a-z0-9_-]{0,63}", maximum=64)
    _require_writer_lock(raw_root, raw_lock_descriptor)
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if document is None or (
            document["phase"] not in {"PREPARING", "FORWARD_APPLYING"} and not (
                document["phase"] in {"ROLLBACK_PREPARING", "ROLLBACK_APPLYING"}
                and _is_rollback_prepared_secret(document, {
                    "name": name, "service": service, "generation": generation, "purpose": purpose,
                })
            )
        ):
            raise TransactionError
        matches = [
            reference
            for reference in document["prepared_secrets"]
            if (
                reference["name"] == name
                and reference["service"] == service
                and reference["generation"] == generation
                and reference["purpose"] == purpose
            )
        ]
        if len(matches) > 1 or (document["phase"] == "ROLLBACK_APPLYING" and not matches):
            raise TransactionError
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    print(matches[0]["docker_secret_id"] if matches else "-")


def _set_phase(
    document: dict[str, Any], target_phase: str, outcome: str | None
) -> None:
    current_phase = document["phase"]
    if (
        target_phase not in PHASES
        or target_phase not in LEGAL_TRANSITIONS[current_phase]
        or document["operation"] is not None
    ):
        raise TransactionError
    if target_phase == "FORWARD_APPLYING" and not document["baseline"]["captured"]:
        raise TransactionError
    if target_phase in {
        "FORWARD_VERIFIED",
        "WORKERS_PROMOTED",
        "MARKER_PROMOTED",
        "CONTROL_PROMOTED",
    }:
        _registered_gate(document, success=True)
    if target_phase in {"ROLLBACK_PREPARING", "ABORTING", "RETIRING", "DONE"}:
        _registered_gate(document, success=False)
    if (
        target_phase == "FORWARD_VERIFIED"
        and any(
            worker["applied_stage"] != "verified"
            for worker in document["forward"]["workers"]
        )
    ):
        raise TransactionError
    if (
        target_phase == "ROLLBACK_PREPARING"
        and not document["failed_forward"]["captured"]
    ):
        raise TransactionError
    if target_phase == "ROLLBACK_APPLYING":
        rollback = document["rollback"]
        if (
            rollback["attempt"] < 1
            or rollback["namespace"] is None
            or rollback["marker_generation"] is None
        ):
            raise TransactionError
    if (
        target_phase == "ROLLBACK_VERIFIED"
        and any(
            worker["applied_stage"] != "verified"
            for worker in document["rollback"]["workers"]
        )
    ):
        raise TransactionError
    if target_phase == "WORKERS_PROMOTED":
        document["promotion"]["workers"] = True
    elif target_phase == "MARKER_PROMOTED":
        if not document["promotion"]["workers"]:
            raise TransactionError
        document["promotion"]["marker"] = True
    elif target_phase == "CONTROL_PROMOTED":
        if not document["promotion"]["marker"]:
            raise TransactionError
        document["promotion"]["control"] = True
    elif target_phase == "ROLLBACK_WORKERS_PROMOTED":
        document["promotion"] = {
            "workers": True,
            "marker": False,
            "control": False,
        }
    elif target_phase == "ROLLBACK_MARKER_PROMOTED":
        if not document["promotion"]["workers"]:
            raise TransactionError
        document["promotion"]["marker"] = True
    elif target_phase == "ROLLBACK_CONTROL_PROMOTED":
        if not document["promotion"]["marker"]:
            raise TransactionError
        document["promotion"]["control"] = True
    elif target_phase == "RETIRING":
        if outcome is None:
            outcome = (
                "rolled_back"
                if current_phase == "ROLLBACK_CONTROL_PROMOTED"
                else "succeeded"
            )
        if outcome not in {"succeeded", "rolled_back", "manual"}:
            raise TransactionError
        document["retiring_outcome"] = outcome
    elif target_phase == "DONE":
        if any(job["state"] != "removed" for job in document["vision_jobs"]):
            raise TransactionError
        if outcome == "aborted":
            if (
                current_phase != "ABORTING"
                or document["prepared_secrets"]
                or document["abort"] is None
                or document["abort"]["authorities"]
                or any(
                    authority["state"] != "revoked"
                    for authority in document["authorities"]
                )
                or any(document["promotion"].values())
                or document["pending_retirements"]
            ):
                raise TransactionError
        elif (
            document["pending_retirements"]
            or not all(document["promotion"].values())
            or outcome not in {"succeeded", "rolled_back", "manual"}
            or outcome != document["retiring_outcome"]
        ):
            raise TransactionError
        document["outcome"] = outcome
    if target_phase not in {"RETIRING", "DONE"} and outcome is not None:
        raise TransactionError
    document["phase"] = target_phase


def _update_document(
    raw_root: str,
    raw_lock_descriptor: str,
    raw_revision: str,
    updater: Any,
) -> dict[str, Any]:
    _require_writer_lock(raw_root, raw_lock_descriptor)
    expected_revision = _parse_revision(raw_revision)
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if (
            document is None
            or active_identity is None
            or document["revision"] != expected_revision
        ):
            raise TransactionError
        changed = updater(document)
        if changed is False:
            return document
        document["revision"] = expected_revision + 1
        _write_active(
            transactions_descriptor,
            document,
            expected_identity=active_identity,
        )
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    return document


def _secret_reference_identity(reference: dict[str, str]) -> dict[str, Any]:
    identity = {
        "kind": "secret",
        "docker_id": reference["docker_secret_id"],
        "name": reference["name"],
        "service": reference["service"],
        "generation": reference["generation"],
        "purpose": reference["purpose"],
        "spec_digest": None,
    }
    return _validate_identity(identity)


def _legacy_candidate_eligible(
    document: dict[str, Any], progress: object, *, resume_forward: bool = False,
) -> bool:
    """Validate complete bootstrap authority before either recovery direction."""
    try:
        _validate_document(document)
        progress = _validate_app_progress(progress)
        forward = document["forward"]
        rollback = document["rollback"]
        control = forward["control"]
        marker = forward["marker"]
        baseline = document["baseline"]
        if (
            document["phase"] not in {"FORWARD_APPLYING", "ROLLBACK_PREPARING"}
            or baseline["kind"] != "legacy_no_control"
            or not baseline["captured"] or baseline["control"] is not None
            or document["operation"] is not None or document["abort"] is not None
            or any(document["promotion"].values())
            or document["pending_retirements"]
            or (not resume_forward and document["janitor"]["service"] is not None)
            or rollback["attempt"] > 1
            or rollback["control"] is not None or rollback["marker"] is not None
            or rollback["workers"]
            or control is None or marker is None
            or progress["transaction_id"] != document["transaction_id"]
            or progress["target_commit"] != document["target_commit"]
            or (not resume_forward
                and set(progress["attempted_services"]) & RUNTIME_AUTHORITY_SERVICES)
        ):
            return False
        workers = forward["workers"]
        if (
            len(workers) != 4
            or {worker["service"] for worker in workers} != RUNTIME_AUTHORITY_SERVICES
            or any(worker["applied_stage"] not in (
                {"prepared", "applied", "verified"} if resume_forward else {"prepared"}
            ) for worker in workers)
            or control["generation"] != "c-" + document["target_commit"][:20]
            or marker["image"] != control["image"]
        ):
            return False
        expected_authorities = {
            ("control", "vp-worker-control", control["generation"]),
            ("marker", "worker-redis-marker-control", marker["generation"]),
            *(("runtime", worker["service"], str(worker["generation"])) for worker in workers),
        }
        authorities = document["authorities"]
        if len(authorities) != 6 or {
            (item["kind"], item["service"], item["generation"]) for item in authorities
        } != expected_authorities or any(
            item["state"] != "provisioned"
            or item["control_generation"] != control["generation"]
            or item["control_image"] != control["image"]
            for item in authorities
        ):
            return False
        expected_secrets = control["secrets"] + [
            item for item in marker["secrets"] if item["purpose"].endswith("-database")
        ] + [worker[key] for worker in workers for key in ("database_secret", "admission_secret")]
        if sorted(expected_secrets, key=lambda item: item["docker_secret_id"]) != sorted(
            document["prepared_secrets"], key=lambda item: item["docker_secret_id"]
        ):
            return False
        failed = document["failed_forward"]
        if resume_forward:
            if (
                document["phase"] != "ROLLBACK_PREPARING"
                or progress["migration_state"] != "applied"
                or not failed["captured"]
                or not any(worker["applied_stage"] in {"applied", "verified"} for worker in workers)
            ):
                return False
            failed_services = {item["name"]: item for item in failed["services"]}
            for worker in workers:
                applied = worker["applied_stage"] in {"applied", "verified"}
                if applied != (worker["service"] in progress["attempted_services"]):
                    return False
                if applied:
                    identity = failed_services.get(worker["service"], {})
                    if (
                        identity.get("docker_service_id") != worker["docker_service_id"]
                        or identity.get("spec_digest") != worker["target_spec_digest"]
                        or identity.get("image") != worker["image"]
                    ):
                        return False
        if failed["captured"]:
            baseline_services = {item["name"]: item for item in baseline["services"]}
            if (
                {item["name"] for item in failed["services"]} != set(progress["attempted_services"])
                or failed["control"] is None
                or failed["control"]["generation"] != control["generation"]
                or failed["control"]["image"] != control["image"]
                or any(item["docker_service_id"] != baseline_services[item["name"]]["docker_service_id"]
                       for item in failed["services"])
            ):
                return False
        return True
    except (TransactionError, KeyError, TypeError, ValueError):
        return False


def legacy_preapply_abort_eligible(document: dict[str, Any], progress: object) -> bool:
    """Only a complete, unused bootstrap candidate can bypass managed rollback."""
    return _legacy_candidate_eligible(document, progress)


def legacy_forward_resume_eligible(document: dict[str, Any], progress: object) -> bool:
    """An applied bootstrap candidate may resume only before rollback has effects."""
    return _legacy_candidate_eligible(document, progress, resume_forward=True)


def resume_legacy_forward(arguments: list[str]) -> None:
    if len(arguments) != 5:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, transaction_id, commit = arguments

    def updater(document: dict[str, Any]) -> None:
        if document["transaction_id"] != transaction_id or document["target_commit"] != commit:
            raise TransactionError
        _verify_captured_credentials(document["database_credentials"])
        _root, root_descriptor, transactions_descriptor = _open_transactions(raw_root, create=False)
        try:
            descriptor = _open_child_directory(
                transactions_descriptor, document["transaction_id"], create=False,
            )
            try:
                progress, _identity = _read_app_progress_from_descriptor(descriptor, allow_missing=False)
            finally:
                os.close(descriptor)
        finally:
            os.close(transactions_descriptor)
            os.close(root_descriptor)
        if not legacy_forward_resume_eligible(document, progress):
            raise TransactionError
        # This explicit recovery command is not a general reverse phase transition.
        # Preserve the original failure snapshot and require normal worker verification.
        document["last_error"] = {"code": "legacy_forward_resumed", "phase": document["phase"]}
        document["phase"] = "FORWARD_APPLYING"

    _print_json(_update_document(raw_root, raw_lock_descriptor, raw_revision, updater))


def begin_abort(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, reason = arguments
    _require_string(reason, r"[a-z][a-z0-9_]{0,63}", maximum=64)

    def updater(document: dict[str, Any]) -> None:
        legacy_preapply = document["phase"] == "ROLLBACK_PREPARING" or (
            document["baseline"]["kind"] == "legacy_no_control"
            and len(document["forward"]["workers"]) == 4
        )
        if legacy_preapply:
            _root, root_descriptor, transactions_descriptor = _open_transactions(raw_root, create=False)
            try:
                descriptor = _open_child_directory(
                    transactions_descriptor, document["transaction_id"], create=False,
                )
                try:
                    progress, _identity = _read_app_progress_from_descriptor(descriptor, allow_missing=False)
                finally:
                    os.close(descriptor)
            finally:
                os.close(transactions_descriptor)
                os.close(root_descriptor)
            if not legacy_preapply_abort_eligible(document, progress):
                raise TransactionError
        aborting_forward = document["phase"] == "FORWARD_APPLYING"
        if (
            document["phase"] not in {"PREPARING", "FORWARD_APPLYING"}
            and not legacy_preapply
        ) or (
            document["operation"] is not None
            or document["abort"] is not None
            or (
                aborting_forward
                and (
                    document["janitor"]["service"] is not None
                    or any(
                        worker["applied_stage"] in {"applied", "verified"}
                        for worker in document["forward"]["workers"]
                    )
                )
            )
        ):
            raise TransactionError
        document["abort"] = {
            "reason": reason,
            "authorities": [
                dict(authority)
                for authority in document["authorities"]
                if authority["state"] != "revoked"
            ],
        }
        document["last_error"] = {
            "code": reason,
            "phase": document["phase"],
        }
        _set_phase(document, "ABORTING", None)

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def list_abort(arguments: list[str]) -> None:
    if len(arguments) != 2:
        raise TransactionError
    raw_root, raw_lock_descriptor = arguments
    _require_writer_lock(raw_root, raw_lock_descriptor)
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if (
            document is None
            or document["phase"] != "ABORTING"
            or document["abort"] is None
        ):
            raise TransactionError
        state = {
            "phase": document["phase"],
            "revision": document["revision"],
            "reason": document["abort"]["reason"],
            "operation": document["operation"],
            "prepared_secrets": list(reversed(document["prepared_secrets"])),
            "authorities": list(reversed(document["abort"]["authorities"])),
        }
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    _print_json(state)


def intent_prepared_secret_removal(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, docker_secret_id = arguments
    _require_string(
        docker_secret_id,
        r"[a-z0-9]{20,64}",
        maximum=64,
    )

    def updater(document: dict[str, Any]) -> None:
        if document["phase"] != "ABORTING" or document["operation"] is not None:
            raise TransactionError
        matches = [
            reference
            for reference in document["prepared_secrets"]
            if reference["docker_secret_id"] == docker_secret_id
        ]
        if len(matches) != 1:
            raise TransactionError
        document["operation"] = {
            "operation_id": f"operation-{secrets.token_hex(16)}",
            "kind": "REMOVE_PREPARED_SECRET",
            "target_phase": "ABORTING",
            "identity": _secret_reference_identity(matches[0]),
        }

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def complete_prepared_secret_removal(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, operation_id = arguments

    def updater(document: dict[str, Any]) -> None:
        operation = document["operation"]
        if (
            document["phase"] != "ABORTING"
            or operation is None
            or operation["operation_id"] != operation_id
            or operation["kind"] != "REMOVE_PREPARED_SECRET"
        ):
            raise TransactionError
        identity = operation["identity"]
        matches = [
            reference
            for reference in document["prepared_secrets"]
            if (
                reference["docker_secret_id"] == identity["docker_id"]
                and reference["name"] == identity["name"]
                and reference["service"] == identity["service"]
                and reference["generation"] == identity["generation"]
                and reference["purpose"] == identity["purpose"]
            )
        ]
        if len(matches) != 1:
            raise TransactionError
        document["prepared_secrets"] = [
            reference
            for reference in document["prepared_secrets"]
            if reference != matches[0]
        ]
        document["operation"] = None

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def complete_abort_authority(arguments: list[str]) -> None:
    if len(arguments) != 6:
        raise TransactionError
    (
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        kind,
        service,
        generation,
    ) = arguments
    if kind not in {"control", "marker", "runtime"}:
        raise TransactionError
    _require_string(service, r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    _require_string(generation, r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")

    def updater(document: dict[str, Any]) -> None:
        if (
            document["phase"] != "ABORTING"
            or document["operation"] is not None
            or document["prepared_secrets"]
            or document["abort"] is None
        ):
            raise TransactionError
        queue_matches = [
            existing
            for existing in document["abort"]["authorities"]
            if (
                existing["kind"] == kind
                and existing["service"] == service
                and existing["generation"] == generation
            )
        ]
        authority_matches = [
            existing
            for existing in document["authorities"]
            if (
                existing["kind"] == kind
                and existing["service"] == service
                and existing["generation"] == generation
                and existing["state"] != "revoked"
            )
        ]
        if len(queue_matches) != 1 or len(authority_matches) != 1:
            raise TransactionError
        authority_matches[0]["state"] = "revoked"
        document["abort"]["authorities"] = [
            existing
            for existing in document["abort"]["authorities"]
            if existing != queue_matches[0]
        ]

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def finish_abort(arguments: list[str]) -> None:
    if len(arguments) != 3:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision = arguments
    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        lambda value: _set_phase(value, "DONE", "aborted"),
    )
    _print_json(document)


def _update_app_progress(
    raw_root: str,
    raw_lock_descriptor: str,
    allowed_phases: set[str],
    updater: Any,
) -> dict[str, Any]:
    _require_writer_lock(raw_root, raw_lock_descriptor)
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if document is None or document["phase"] not in allowed_phases:
            raise TransactionError
        transaction_descriptor = _open_child_directory(
            transactions_descriptor,
            document["transaction_id"],
            create=False,
        )
        try:
            progress, progress_identity = _read_app_progress_from_descriptor(
                transaction_descriptor,
                allow_missing=False,
            )
            if (
                progress is None
                or progress_identity is None
                or progress["transaction_id"] != document["transaction_id"]
                or progress["target_commit"] != document["target_commit"]
            ):
                raise TransactionError
            changed = updater(progress)
            if changed is not False:
                _write_document(
                    transaction_descriptor,
                    APP_PROGRESS_NAME,
                    progress,
                    expected_identity=progress_identity,
                    validator=_validate_app_progress,
                )
        finally:
            os.close(transaction_descriptor)
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    return progress


def init_app_progress(arguments: list[str]) -> None:
    if len(arguments) != 2:
        raise TransactionError
    raw_root, raw_lock_descriptor = arguments
    _require_writer_lock(raw_root, raw_lock_descriptor)
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if document is None or document["phase"] != "PREPARING":
            raise TransactionError
        transaction_descriptor = _open_child_directory(
            transactions_descriptor,
            document["transaction_id"],
            create=False,
        )
        try:
            expected = {
                "schema": 1,
                "transaction_id": document["transaction_id"],
                "target_commit": document["target_commit"],
                "attempted_services": [],
                "migration_state": "pending",
            }
            progress, progress_identity = _read_app_progress_from_descriptor(
                transaction_descriptor,
                allow_missing=True,
            )
            if progress is None:
                _write_document(
                    transaction_descriptor,
                    APP_PROGRESS_NAME,
                    expected,
                    expected_identity=None,
                    validator=_validate_app_progress,
                )
                progress = expected
            elif progress_identity is None or progress != expected:
                raise TransactionError
        finally:
            os.close(transaction_descriptor)
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    _print_json(progress)


def record_app_attempt(arguments: list[str]) -> None:
    if len(arguments) != 3:
        raise TransactionError
    raw_root, raw_lock_descriptor, service = arguments
    if service not in APP_SERVICES:
        raise TransactionError

    def updater(progress: dict[str, Any]) -> bool | None:
        if service in progress["attempted_services"]:
            return False
        progress["attempted_services"].append(service)
        return None

    progress = _update_app_progress(
        raw_root,
        raw_lock_descriptor,
        {"FORWARD_APPLYING"},
        updater,
    )
    _print_json(progress)


def remove_app_attempt(arguments: list[str]) -> None:
    if len(arguments) != 3:
        raise TransactionError
    raw_root, raw_lock_descriptor, service = arguments
    if service not in APP_SERVICES:
        raise TransactionError

    def updater(progress: dict[str, Any]) -> bool | None:
        if service not in progress["attempted_services"]:
            return False
        progress["attempted_services"] = [
            attempted
            for attempted in progress["attempted_services"]
            if attempted != service
        ]
        return None

    progress = _update_app_progress(
        raw_root,
        raw_lock_descriptor,
        {"FORWARD_APPLYING"},
        updater,
    )
    _print_json(progress)


def advance_migration_state(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, expected_state, target_state = arguments
    if (expected_state, target_state) not in {
        ("pending", "applying"),
        ("applying", "applied"),
    }:
        raise TransactionError

    def updater(progress: dict[str, Any]) -> bool | None:
        current_state = progress["migration_state"]
        if current_state == target_state:
            return False
        if current_state != expected_state:
            raise TransactionError
        progress["migration_state"] = target_state
        return None

    progress = _update_app_progress(
        raw_root,
        raw_lock_descriptor,
        {"FORWARD_APPLYING"},
        updater,
    )
    _print_json(progress)


def read_app_progress(arguments: list[str]) -> None:
    if len(arguments) != 1:
        raise TransactionError
    raw_root = arguments[0]
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if document is None:
            raise TransactionError
        transaction_descriptor = _open_child_directory(
            transactions_descriptor,
            document["transaction_id"],
            create=False,
        )
        try:
            progress, _progress_identity = _read_app_progress_from_descriptor(
                transaction_descriptor,
                allow_missing=False,
            )
            if (
                progress is None
                or progress["transaction_id"] != document["transaction_id"]
                or progress["target_commit"] != document["target_commit"]
            ):
                raise TransactionError
        finally:
            os.close(transaction_descriptor)
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    _print_json(progress)


def transition(arguments: list[str]) -> None:
    if len(arguments) not in {4, 5}:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, target_phase, *raw_outcome = arguments
    outcome = raw_outcome[0] if raw_outcome else None
    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        lambda value: _set_phase(value, target_phase, outcome),
    )
    _print_json(document)


def capture_baseline(arguments: list[str]) -> None:
    if len(arguments) != 3:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision = arguments
    captured = _require_exact_fields(
        _read_stdin_json(),
        {"kind", "control", "services"},
    )
    if captured["kind"] not in {"managed", "legacy_no_control"}:
        raise TransactionError
    if captured["control"] is not None:
        _validate_control_identity(captured["control"])
    if (captured["kind"] == "managed") != (captured["control"] is not None):
        raise TransactionError
    _validate_service_identities(captured["services"])
    if {service["name"] for service in captured["services"]} != APP_SERVICES:
        raise TransactionError
    desired = {
        "captured": True,
        "kind": captured["kind"],
        "control": captured["control"],
        "services": captured["services"],
    }

    def updater(document: dict[str, Any]) -> bool | None:
        if document["phase"] != "PREPARING" or document["operation"] is not None:
            raise TransactionError
        if document["baseline"]["kind"] != captured["kind"]:
            raise TransactionError
        if document["baseline"]["captured"]:
            if document["baseline"] != desired:
                raise TransactionError
            return False
        document["baseline"] = desired
        return None

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def capture_failed_forward(arguments: list[str]) -> None:
    if len(arguments) != 3:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision = arguments
    captured = _require_exact_fields(
        _read_stdin_json(),
        {"control", "services"},
    )
    if captured["control"] is not None:
        _validate_failed_forward_control(captured["control"])
    _validate_service_identities(captured["services"])
    if any(
        not service["existed"] or service["name"] not in APP_SERVICES
        for service in captured["services"]
    ):
        raise TransactionError
    desired = {
        "captured": True,
        "control": captured["control"],
        "services": captured["services"],
    }

    def updater(document: dict[str, Any]) -> bool | None:
        if document["phase"] not in {"PREPARING", "FORWARD_APPLYING"}:
            raise TransactionError
        if document["operation"] is not None:
            raise TransactionError
        if document["failed_forward"]["captured"]:
            if document["failed_forward"] != desired:
                raise TransactionError
            return False
        document["failed_forward"] = desired
        return None

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def allocate_rollback_attempt(arguments: list[str]) -> None:
    if len(arguments) != 3:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision = arguments

    def updater(document: dict[str, Any]) -> bool | None:
        if (
            document["phase"] != "ROLLBACK_PREPARING"
            or document["operation"] is not None
        ):
            raise TransactionError
        rollback = document["rollback"]
        if rollback["attempt"] != 0:
            return False
        attempt = 1
        transaction_hex = document["transaction_id"][3:]
        namespace_number = int(transaction_hex[:15], 16) + 10**17
        rollback["attempt"] = attempt
        rollback["namespace"] = f"rollback-{namespace_number}"
        rollback["marker_generation"] = (
            f"m-rb-{transaction_hex[:12]}-{attempt}"
        )
        return None

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def _record_selection(
    arguments: list[str],
    field: str,
    validator: Any,
) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, direction = arguments
    if direction == "forward":
        allowed_phases = {"PREPARING", "FORWARD_APPLYING"}
    elif direction == "rollback":
        allowed_phases = {"ROLLBACK_PREPARING", "ROLLBACK_APPLYING"}
    else:
        raise TransactionError
    selection = _read_stdin_json()
    validator(selection)

    def updater(document: dict[str, Any]) -> bool | None:
        if (
            document["phase"] not in allowed_phases
            or document["operation"] is not None
        ):
            raise TransactionError
        existing = document[direction][field]
        if existing is not None:
            if existing != selection:
                raise TransactionError
            return False
        document[direction][field] = selection
        return None

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def record_control_selection(arguments: list[str]) -> None:
    _record_selection(arguments, "control", _validate_control_identity)


def record_marker_selection(arguments: list[str]) -> None:
    _record_selection(arguments, "marker", _validate_marker_identity)


def record_janitor_service(arguments: list[str]) -> None:
    if len(arguments) != 3:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision = arguments
    service = _read_stdin_json()
    _validate_janitor_service(service)

    def updater(document: dict[str, Any]) -> bool | None:
        if (
            document["phase"]
            not in {"PREPARING", "FORWARD_APPLYING", "ROLLBACK_APPLYING"}
            or document["operation"] is not None
        ):
            raise TransactionError
        existing = document["janitor"]["service"]
        if existing is not None:
            if existing != service:
                raise TransactionError
            return False
        document["janitor"]["service"] = service
        return None

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def clear_janitor_service(arguments: list[str]) -> None:
    if len(arguments) != 3:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision = arguments
    service = _read_stdin_json()
    _validate_janitor_service(service)

    def updater(document: dict[str, Any]) -> bool | None:
        if (
            document["phase"] != "ROLLBACK_APPLYING"
            or document["operation"] is not None
        ):
            raise TransactionError
        existing = document["janitor"]["service"]
        if existing is None:
            return False
        if existing != service:
            raise TransactionError
        document["janitor"]["service"] = None
        return None

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def _worker_plan_direction(
    document: dict[str, Any],
    direction: str,
) -> list[dict[str, Any]]:
    if direction == "forward":
        allowed_phases = {"PREPARING", "FORWARD_APPLYING"}
    elif direction == "rollback":
        allowed_phases = {"ROLLBACK_PREPARING", "ROLLBACK_APPLYING"}
        if document["rollback"]["attempt"] == 0:
            raise TransactionError
    else:
        raise TransactionError
    if document["phase"] not in allowed_phases:
        raise TransactionError
    return document[direction]["workers"]


def record_worker_plan(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, direction = arguments
    plan = _require_exact_fields(
        _read_stdin_json(),
        {
            "service",
            "generation",
            "commit",
            "image",
            "target_spec_digest",
            "database_secret",
            "admission_secret",
        },
    )
    worker = {
        **plan,
        "docker_service_id": None,
        "applied_stage": "pending",
    }
    _validate_worker_identity(worker)

    def updater(document: dict[str, Any]) -> bool | None:
        if document["operation"] is not None:
            raise TransactionError
        workers = _worker_plan_direction(document, direction)
        matches = [
            existing
            for existing in workers
            if (
                existing["service"] == worker["service"]
                and existing["generation"] == worker["generation"]
            )
        ]
        if matches:
            if len(matches) != 1 or any(
                matches[0][field] != plan[field]
                for field in plan
                if field != "target_spec_digest"
            ):
                raise TransactionError
            return False
        workers.append(worker)
        return None

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def advance_worker_stage(arguments: list[str]) -> None:
    if len(arguments) != 10:
        raise TransactionError
    (
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        direction,
        service,
        raw_generation,
        expected_stage,
        target_stage,
        raw_service_id,
        raw_spec_digest,
    ) = arguments
    _require_string(service, r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    if re.fullmatch(r"[1-9][0-9]{0,18}", raw_generation) is None:
        raise TransactionError
    generation = int(raw_generation, 10)
    if WORKER_STAGE_SUCCESSORS.get(expected_stage) != target_stage:
        raise TransactionError
    service_id = None
    if raw_service_id != "-":
        service_id = _require_string(
            raw_service_id,
            r"[0-9a-z]{12,64}",
            maximum=64,
        )
    spec_digest = None
    if raw_spec_digest != "-":
        spec_digest = _require_string(
            raw_spec_digest,
            r"[0-9a-f]{64}",
            maximum=64,
        )

    def updater(document: dict[str, Any]) -> bool | None:
        if document["operation"] is not None:
            raise TransactionError
        workers = _worker_plan_direction(document, direction)
        matches = [
            worker
            for worker in workers
            if (
                worker["service"] == service
                and worker["generation"] == generation
            )
        ]
        if len(matches) != 1:
            raise TransactionError
        worker = matches[0]
        current_stage = worker["applied_stage"]
        if WORKER_STAGE_ORDER[current_stage] >= WORKER_STAGE_ORDER[target_stage]:
            if target_stage == "prepared":
                if service_id is not None or spec_digest is not None:
                    raise TransactionError
            elif (
                service_id != worker["docker_service_id"]
                or spec_digest != worker["target_spec_digest"]
            ):
                raise TransactionError
            return False
        if current_stage != expected_stage:
            raise TransactionError
        if target_stage == "prepared":
            if (
                service_id is not None
                or spec_digest is not None
                or worker["docker_service_id"] is not None
                or worker["target_spec_digest"] is not None
            ):
                raise TransactionError
        elif target_stage == "applied":
            if (
                service_id is None
                or spec_digest is None
                or worker["docker_service_id"] is not None
                or worker["target_spec_digest"] is not None
            ):
                raise TransactionError
            worker["docker_service_id"] = service_id
            worker["target_spec_digest"] = spec_digest
        elif (
            target_stage == "verified"
            and (
                service_id != worker["docker_service_id"]
                or spec_digest != worker["target_spec_digest"]
            )
        ):
            raise TransactionError
        worker["applied_stage"] = target_stage
        return None

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def record_runtime_secret(arguments: list[str]) -> None:
    if len(arguments) != 7:
        raise TransactionError
    (
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        role,
        runtime_generation,
        secret_name,
        docker_secret_id,
    ) = arguments
    _require_string(role, r"[a-z][a-z0-9_-]{0,63}", maximum=64)
    _require_string(
        runtime_generation,
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",
    )
    _require_string(secret_name, r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    _require_string(docker_secret_id, r"[a-z0-9]{20,64}", maximum=64)

    def updater(document: dict[str, Any]) -> None:
        if document["phase"] != "PREPARING" or document["operation"] is not None:
            raise TransactionError
        reference = {
            "runtime_generation": runtime_generation,
            "secret_name": secret_name,
            "docker_secret_id": docker_secret_id,
        }
        existing = document["runtime_redis"].get(role)
        if existing is not None and existing != reference:
            raise TransactionError
        document["runtime_redis"][role] = reference

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def prepare_vision_job(arguments: list[str]) -> None:
    if len(arguments) != 9:
        raise TransactionError
    (
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        mode,
        name,
        image,
        redis_role,
        database_secret_name,
        database_secret_id,
    ) = arguments
    if mode not in {"safety", "final-safety", "check", "reconcile"}:
        raise TransactionError
    _require_string(
        name,
        r"vp-vision-cutover-(safety|final-safety|check|reconcile)-[0-9a-f]{12}",
    )
    _require_string(image, r"[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,254}")
    if redis_role not in {"watcher", "control"}:
        raise TransactionError

    def updater(document: dict[str, Any]) -> None:
        if (
            document["phase"] not in {"PREPARING", "FORWARD_APPLYING"}
            or document["operation"] is not None
        ):
            raise TransactionError
        if (mode == "reconcile") != (redis_role == "control"):
            raise TransactionError
        runtime_reference = document["runtime_redis"].get(redis_role)
        if runtime_reference is None:
            raise TransactionError
        redis_secret = {
            "name": runtime_reference["secret_name"],
            "docker_secret_id": runtime_reference["docker_secret_id"],
            "service": "worker-redis-runtime",
            "generation": runtime_reference["runtime_generation"],
            "purpose": redis_role,
        }
        database_secret = None
        if mode in {"safety", "final-safety"}:
            database_purpose = (
                "final-safety-database"
                if mode == "final-safety"
                else "safety-database"
            )
            matches = [
                reference
                for reference in document["prepared_secrets"]
                if (
                    reference["name"] == database_secret_name
                    and reference["docker_secret_id"] == database_secret_id
                    and reference["service"] == "vision-cutover"
                    and reference["generation"] == document["transaction_id"]
                    and reference["purpose"] == database_purpose
                )
            ]
            if len(matches) != 1:
                raise TransactionError
            database_secret = dict(matches[0])
        elif database_secret_name != "-" or database_secret_id != "-":
            raise TransactionError
        job = {
            "mode": mode,
            "name": name,
            "image": image,
            "redis_secret": redis_secret,
            "database_secret": database_secret,
            "docker_service_id": None,
            "state": "planned",
            "exit_code": None,
        }
        matches = [item for item in document["vision_jobs"] if item["mode"] == mode]
        if matches:
            if len(matches) != 1 or any(
                matches[0][field] != job[field]
                for field in (
                    "mode", "name", "image", "redis_secret", "database_secret"
                )
            ):
                raise TransactionError
            return
        document["vision_jobs"].append(job)

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def record_vision_job_service(arguments: list[str]) -> None:
    if len(arguments) != 5:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, mode, service_id = arguments
    _require_string(service_id, r"[a-z0-9]{12,64}", maximum=64)

    def updater(document: dict[str, Any]) -> None:
        matches = [item for item in document["vision_jobs"] if item["mode"] == mode]
        if len(matches) != 1:
            raise TransactionError
        job = matches[0]
        if job["state"] == "planned" and job["docker_service_id"] is None:
            job["docker_service_id"] = service_id
            job["state"] = "created"
        elif job["docker_service_id"] != service_id:
            raise TransactionError

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def record_vision_job_terminal(arguments: list[str]) -> None:
    if len(arguments) != 5:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, mode, raw_exit_code = arguments
    exit_code = _parse_revision(raw_exit_code)
    if exit_code > 255:
        raise TransactionError

    def updater(document: dict[str, Any]) -> None:
        matches = [item for item in document["vision_jobs"] if item["mode"] == mode]
        if len(matches) != 1:
            raise TransactionError
        job = matches[0]
        if job["state"] == "created":
            job["state"] = "terminal"
            job["exit_code"] = exit_code
        elif job["state"] not in {"terminal", "removed"} or job["exit_code"] != exit_code:
            raise TransactionError

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def complete_vision_job_removal(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, mode = arguments

    def updater(document: dict[str, Any]) -> None:
        matches = [item for item in document["vision_jobs"] if item["mode"] == mode]
        if len(matches) != 1 or matches[0]["state"] != "terminal":
            raise TransactionError
        job = matches[0]
        if job["database_secret"] is not None:
            expected = job["database_secret"]
            document["prepared_secrets"] = [
                reference
                for reference in document["prepared_secrets"]
                if reference != expected
            ]
        job["state"] = "removed"

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def abort_vision_job_removal(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, mode = arguments

    def updater(document: dict[str, Any]) -> None:
        if document["phase"] not in {
            "PREPARING",
            "FORWARD_APPLYING",
            "ABORTING",
        } or document["operation"] is not None:
            raise TransactionError
        matches = [item for item in document["vision_jobs"] if item["mode"] == mode]
        if len(matches) != 1:
            raise TransactionError
        job = matches[0]
        if job["state"] == "removed":
            return
        if job["database_secret"] is not None:
            expected = job["database_secret"]
            document["prepared_secrets"] = [
                reference
                for reference in document["prepared_secrets"]
                if reference != expected
            ]
        job["state"] = "removed"
        job["exit_code"] = 255

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def lookup_vision_job(arguments: list[str]) -> None:
    if len(arguments) != 2:
        raise TransactionError
    raw_root, mode = arguments
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if document is None:
            raise TransactionError
        matches = [item for item in document["vision_jobs"] if item["mode"] == mode]
        if len(matches) > 1:
            raise TransactionError
        _print_json(matches[0] if matches else None)
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)


_REGISTERED_PROTOCOL: dict[str, Any] | None = None
REGISTERED_RECONCILE_NAME = "registered-reconcile.json"


def _validate_registered_jobs(value: object) -> None:
    record = _require_exact_fields(
        value,
        {"version", "baseline", "current", "run"}
        | (
            {"pins", "capture_read"}.intersection(value)
            if isinstance(value, dict)
            else set()
        ),
    )
    _require_exact_schema(record["version"], 1)
    reader = record.get("capture_read")
    if reader is not None:
        _require_exact_fields(reader, {"state", "id", "name", "sha256", "principal"})
        if reader["state"] not in {"creating", "present", "removing", "removed"}:
            raise TransactionError
        _require_optional_string(reader["id"], r"[a-z0-9]{25}")
        if reader["state"] != "creating" and reader["id"] is None:
            raise TransactionError
        _require_string(reader["name"], r"vp-registered-read-tx-[0-9a-f]{32}")
        _require_string(reader["sha256"], r"[0-9a-f]{64}")
        _require_string(reader["principal"], r"[A-Za-z_][A-Za-z0-9_.$@-]{0,127}")
    pins = record.get("pins")
    if pins is not None:
        _require_exact_fields(pins, {"state", "id", "name", "sha256"})
        if pins["state"] not in {"creating", "present", "removing", "removed"}:
            raise TransactionError
        _require_optional_string(pins["id"], r"[a-z0-9]{25}")
        if pins["state"] != "creating" and pins["id"] is None:
            raise TransactionError
        _require_string(pins["name"], r"vp-registered-pins-[0-9a-f-]{36}")
        _require_string(pins["sha256"], r"[0-9a-f]{64}")
    for stage in ("baseline", "current", "run"):
        job = record[stage]
        if job is None:
            continue
        _require_exact_fields(
            job,
            {
                "attempt_id",
                "files",
                "input_file",
                "input_sha256",
                "spec",
                "service_id",
                "task_id",
                "exit_code",
                "state",
                "result",
                "pins_secret_id",
            },
        )
        _require_string(
            job["attempt_id"],
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        )
        _require_string(job["input_sha256"], r"[0-9a-f]{64}")
        if job["state"] not in {
            "planned",
            "launching",
            "created",
            "terminal",
            "removing",
            "removed",
        }:
            raise TransactionError
        _require_optional_string(job["service_id"], r"[a-z0-9]{25}")
        _require_optional_string(job["task_id"], r"[a-z0-9]{25}")
        if job["state"] == "terminal" or job["exit_code"] is not None:
            _require_integer(job["exit_code"])
            if job["exit_code"] > 255:
                raise TransactionError
        if job["state"] == "created" and job["service_id"] is None:
            raise TransactionError
        if job["state"] == "planned" and job["service_id"] is not None:
            raise TransactionError
        if not isinstance(job["files"], dict) or not isinstance(job["spec"], dict):
            raise TransactionError


def _registered_gate(document: dict, *, success: bool) -> None:
    record = document.get("registered_reconcile")
    if record is None:
        return  # Explicit older-journal compatibility; never synthesize a result.
    jobs = [record[stage] for stage in ("baseline", "current", "run")]
    if any(job is not None and job["state"] != "removed" for job in jobs):
        raise TransactionError
    for key in ("pins", "capture_read"):
        if record.get(key) is not None and record[key]["state"] != "removed":
            raise TransactionError
    if success and (
        any(job is None or job["exit_code"] != 0 for job in jobs)
        or record["run"]["result"] is None
        or record.get("capture_read") is None
    ):
        raise TransactionError
    if success:
        try:
            baseline, current, run = jobs
            pins = record["pins"]
            result = _require_exact_fields(
                run["result"],
                {
                    "outcome",
                    "pin_sha256",
                    "attempted_streams",
                    "request_bytes",
                    "request_sha256",
                    "service_id",
                    "task_id",
                },
            )
            if (
                pins is None
                or pins["id"] != current["pins_secret_id"]
                or pins["id"] != run["pins_secret_id"]
                or pins["sha256"] != current["result"]["pins"]["pin_sha256"]
                or result["pin_sha256"] != pins["sha256"]
                or result["service_id"] != run["service_id"]
                or result["task_id"] != run["task_id"]
                or result["outcome"] not in {"reconciled", "already_absent"}
                or baseline["result"]["credentials"] != current["result"]["credentials"]
            ):
                raise TransactionError
            _require_string(result["request_sha256"], r"[0-9a-f]{64}")
            _require_integer(result["request_bytes"], 1)
            _require_string(result["service_id"], r"[a-z0-9]{25}")
            _require_string(result["task_id"], r"[a-z0-9]{25}")
            streams = result["attempted_streams"]
            if (
                type(streams) is not list
                or streams != sorted(set(streams))
                or not set(streams).issubset(current["result"]["pins"]["commands"])
                or (result["outcome"] == "already_absent") != (not streams)
            ):
                raise TransactionError
        except Exception:
            raise TransactionError from None


def _registered_credentials(document: dict) -> dict:
    control = document["forward"]["control"]
    if control is None:
        raise TransactionError
    matches = [item for item in control["secrets"] if item["purpose"] == "operator"]
    if (
        len(matches) != 1
        or matches[0]["name"] != "vp-wc-operator-" + control["generation"]
    ):
        raise TransactionError
    redis = _require_exact_fields(
        document["runtime_redis"].get("control"),
        {"runtime_generation", "secret_name", "docker_secret_id"},
    )
    _require_string(redis["runtime_generation"], r"[a-z0-9][a-z0-9-]{0,62}")
    _require_string(redis["secret_name"], r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}")
    _require_string(redis["docker_secret_id"], r"[a-z0-9]{25}")
    return dict(
        control_generation=control["generation"],
        database_secret_id=matches[0]["docker_secret_id"],
        redis_generation=redis["runtime_generation"],
        redis_secret_name=redis["secret_name"],
        redis_secret_id=redis["docker_secret_id"],
    )


def _registered_directory(raw_root: str, transaction_id: str, stage: str) -> Path:
    root, root_fd, transactions_fd = _open_transactions(raw_root, create=False)
    descriptor = None
    try:
        descriptor = _open_child_directory(transactions_fd, transaction_id, create=True)
        child = _open_child_directory(descriptor, "registered-" + stage, create=True)
        os.close(child)
        return root / TRANSACTIONS_NAME / transaction_id / ("registered-" + stage)
    finally:
        for fd in (descriptor, transactions_fd, root_fd):
            if fd is not None:
                os.close(fd)


def prepare_registered_capture(
    raw_root: str,
    raw_descriptor: str,
    raw_revision: str,
    stage: str,
    network_id: str,
    manager_node: str,
    manager_node_id: str,
) -> dict:
    def updater(document: dict) -> None:
        if (
            stage not in {"baseline", "current"}
            or document["phase"] != "FORWARD_APPLYING"
            or document["operation"] is not None
        ):
            raise TransactionError
        registered = document["registered_reconcile"]
        if registered[stage] is not None or not document["baseline"]["captured"]:
            raise TransactionError
        reader = registered.get("capture_read")
        if reader is None or reader["state"] != "present":
            raise TransactionError
        capture_read = {
            key: reader[key] for key in ("id", "name", "sha256", "principal")
        }
        workers = document["forward"]["workers"]
        expected = {"pending", "prepared"} if stage == "baseline" else {"verified"}
        if len(workers) != 4 or any(
            worker["applied_stage"] not in expected for worker in workers
        ):
            raise TransactionError
        baseline = None
        if stage == "current":
            prior = registered["baseline"]
            if (
                prior is None
                or prior["state"] != "removed"
                or prior["exit_code"] != 0
                or prior["result"] is None
            ):
                raise TransactionError
            baseline = prior["result"]["snapshot"]
        images = {
            worker["image"]
            for worker in workers
            if worker["service"] != "vp-ffmpeg-worker-go-swarm"
        }
        if len(images) != 1:
            raise TransactionError
        image = images.pop()
        if not image.endswith(":deploy-" + document["target_commit"][:12]):
            raise TransactionError
        protocol = _registered_protocol()
        directory = _registered_directory(raw_root, document["transaction_id"], stage)
        if stage == "baseline":
            descriptor = os.open(
                directory.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            )
            try:
                progress, _identity = _read_app_progress_from_descriptor(
                    descriptor, allow_missing=False
                )
                if (
                    progress["transaction_id"] != document["transaction_id"]
                    or progress["target_commit"] != document["target_commit"]
                    or progress["migration_state"] != "applied"
                    or RUNTIME_AUTHORITY_SERVICES.intersection(
                        progress["attempted_services"]
                    )
                ):
                    raise TransactionError
            finally:
                os.close(descriptor)
        files = protocol["prepare_files"](directory)
        attempt_id = str(uuid.uuid4())
        credentials = _registered_credentials(document)
        payload = dict(
            files=files,
            credentials=credentials,
            capture_read=capture_read,
            baseline=baseline,
            transaction_id=document["transaction_id"],
            revision=document["revision"],
            release_commit=document["target_commit"],
        )
        input_file = protocol["write_input"](directory / "input.json", payload)
        spec = protocol["capture_spec"](
            attempt_id=attempt_id,
            transaction_id=document["transaction_id"],
            files=files,
            credentials=credentials,
            capture_read=capture_read,
            image=image,
            network_id=network_id,
            manager_node=manager_node,
            manager_node_id=manager_node_id,
        )
        registered[stage] = dict(
            attempt_id=attempt_id,
            files=files,
            input_file=input_file,
            input_sha256=protocol["digest"](payload),
            spec=spec,
            service_id=None,
            task_id=None,
            exit_code=None,
            state="planned",
            result=None,
            pins_secret_id=None,
        )

    try:
        return _update_document(raw_root, raw_descriptor, raw_revision, updater)
    except Exception:
        raise TransactionError from None


def require_registered_outer_lock(path: str, descriptor: str, owner_pid: str) -> None:
    candidate = Path(path)
    probe = None
    try:
        if (
            not candidate.is_absolute()
            or candidate.name != "sync.lock"
            or candidate.resolve() != candidate
        ):
            raise TransactionError
        if int(owner_pid) != os.getppid():
            raise TransactionError
        opened, named = os.fstat(int(descriptor)), candidate.lstat()
        for metadata in (opened, named):
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                raise TransactionError
        if _identity(opened) != _identity(named):
            raise TransactionError
        probe = os.open(candidate, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Contention alone does not prove the inherited description owns it.
            fcntl.flock(int(descriptor), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        raise TransactionError
    except (OSError, ValueError):
        raise TransactionError from None
    finally:
        if probe is not None:
            os.close(probe)


def _registered_docker(
    arguments: list[str], *, timeout: float = 0.35, input_bytes: bytes | None = None
) -> str:
    try:
        result = subprocess.run(
            ["docker", *arguments],
            input=input_bytes,
            **({"stdin": subprocess.DEVNULL} if input_bytes is None else {}),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
            check=True,
        )
        if len(result.stdout) > MAX_DOCUMENT_BYTES:
            raise TransactionError
        return result.stdout.decode("utf-8").strip()
    except Exception:
        raise TransactionError from None


def _engine_exchange(request: bytes) -> bytes:
    process = subprocess.Popen(
        ["docker", "system", "dial-stdio"], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
    )
    try:
        deadline = time.monotonic() + 30
        output = bytearray()
        sent = 0
        with selectors.DefaultSelector() as ready:
            for stream, event in ((process.stdin, selectors.EVENT_WRITE), (process.stdout, selectors.EVENT_READ)):
                os.set_blocking(stream.fileno(), False)
                ready.register(stream, event)
            while ready.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TransactionError
                for key, event in ready.select(remaining):
                    try:
                        if event == selectors.EVENT_WRITE:
                            sent += os.write(key.fd, request[sent:sent + 65536])
                            if sent == len(request):
                                ready.unregister(key.fileobj)
                                key.fileobj.close()
                        else:
                            block = os.read(key.fd, min(65536, MAX_DOCUMENT_BYTES + 1 - len(output)))
                            if not block:
                                ready.unregister(key.fileobj)
                            output.extend(block)
                            if len(output) > MAX_DOCUMENT_BYTES:
                                raise TransactionError
                    except BlockingIOError:
                        continue
        if process.wait(timeout=max(0.01, deadline - time.monotonic())) != 0:
            raise TransactionError
        return bytes(output)
    finally:
        for stream in (process.stdin, process.stdout):
            stream.close()
        if process.poll() is None:
            process.kill()
        process.wait(timeout=2)


def _engine_service_post(path: str, spec: dict, status: int) -> dict:
    """Send only the two ID-pinned service mutations through the CLI context."""
    try:
        if not (
            (path == "/services/create" and status == 201)
            or (re.fullmatch(r"/services/[a-z0-9]{25}/update\?version=[0-9]+&registryAuthFrom=spec", path) and status == 200)
        ):
            raise TransactionError
        body = json.dumps(spec, separators=(",", ":"), allow_nan=False).encode()
        if len(body) > MAX_DOCUMENT_BYTES:
            raise TransactionError
        request = (
            f"POST /v1.52{path} HTTP/1.1\r\nHost: docker\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
        ).encode() + body
        raw = _engine_exchange(request)
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise TransactionError

        class ResponseSocket:
            def makefile(self, mode: str) -> io.BytesIO:
                return io.BytesIO(raw)

        response = http.client.HTTPResponse(ResponseSocket())
        try:
            response.begin()
            if response.status != status:
                raise TransactionError
            payload = response.read(MAX_DOCUMENT_BYTES + 1)
            if len(payload) > MAX_DOCUMENT_BYTES or response.length not in (None, 0):
                raise TransactionError
            value = json.loads(payload or b"{}")
            if type(value) is not dict:
                raise TransactionError
            return value
        finally:
            response.close()
    except Exception:
        raise TransactionError from None


def _owned_history_secret(reference: dict, user: str) -> dict:
    _require_exact_fields(reference, {"runtime_generation", "secret_name", "docker_secret_id"})
    _require_string(reference["runtime_generation"], r"[0-9a-f]{40}")
    _require_string(reference["docker_secret_id"], r"[a-z0-9]{25}")
    _require_string(reference["secret_name"], r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}")
    if user in {"", "0", "root"}:
        user = "0:0"
    _require_string(user, r"[0-9]+:[0-9]+")
    uid, gid = user.split(":")
    return {"SecretID": reference["docker_secret_id"], "SecretName": reference["secret_name"],
            "File": {"Name": "owned-history-redis-url", "UID": uid, "GID": gid, "Mode": 0o400}}


def _mount_owned_history(container: dict, image_user: str, reference: dict) -> None:
    expected = _owned_history_secret(reference, container.get("User") or image_user)
    target = expected["File"]["Name"]
    path = "/run/secrets/" + target
    env_key = "OWNED_HISTORY_REDIS_URL_FILE"
    env = container.get("Env") or []
    keys = [entry.split("=", 1)[0] for entry in env]
    configured = [entry for entry in env if entry.split("=", 1)[0] == env_key]
    secrets = container.get("Secrets") or []
    mounted = [entry for entry in secrets if entry.get("File", {}).get("Name") in {target, path}]
    if len(keys) != len(set(keys)) or configured not in ([], [env_key + "=" + path]):
        raise TransactionError
    if mounted not in ([], [expected]):
        raise TransactionError
    if any(entry.get("File", {}).get("Name") in {target, path} for entry in container.get("Configs", [])):
        raise TransactionError
    for mount in container.get("Mounts", []):
        mount_target = mount.get("Target", "").rstrip("/")
        if path == mount_target or path.startswith(mount_target + "/"):
            raise TransactionError
    container["Secrets"] = [entry for entry in secrets if entry not in mounted] + [expected]
    container["Env"] = [entry for entry in env if entry.split("=", 1)[0] != env_key] + [env_key + "=" + path]


def _owned_history_runner_update_spec(
    actual: dict, service_id: str, image: str, order: str,
    image_user: str, runtime_node: str, reference: dict,
) -> dict:
    try:
        _require_string(service_id, r"[a-z0-9]{25}")
        _require_string(runtime_node, r"[A-Za-z0-9][A-Za-z0-9.-]{0,62}")
        if (actual["ID"] != service_id or actual["Spec"]["Name"] != "vp-channel-agent-runner-swarm"
                or order not in {"start-first", "stop-first"}):
            raise TransactionError
        spec = copy.deepcopy(actual["Spec"])
        container = spec["TaskTemplate"]["ContainerSpec"]
        _mount_owned_history(container, image_user, reference)
        container["Image"] = image
        overrides = {"CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS", "CHANNELOPS_RUNNER_ID"}
        container["Env"] = [entry for entry in container["Env"] if entry.split("=", 1)[0] not in overrides] + [
            "CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS=120", "CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1",
        ]
        container.setdefault("Healthcheck", {}).update(
            Test=["CMD-SHELL", "wget -qO- http://127.0.0.1:8080/readyz >/dev/null || exit 1"],
            Interval=10_000_000_000, Timeout=3_000_000_000, Retries=6, StartPeriod=10_000_000_000,
        )
        spec["TaskTemplate"].setdefault("Placement", {})["Constraints"] = [
            "node.labels.vp.runtime==true", "node.hostname==" + runtime_node,
        ]
        spec.setdefault("UpdateConfig", {})["Order"] = order
        return spec
    except Exception:
        raise TransactionError from None


def _autoflow_update_spec(
    actual: dict, service_id: str, image: str, order: str, identity: str,
    image_user: str, health: str, runtime_node: str, *, owned_history: dict | None = None,
) -> dict:
    try:
        name, secret_id, generation = identity.split("|")
        _require_string(service_id, r"[a-z0-9]{25}")
        _require_string(secret_id, r"[a-z0-9]{25}")
        _require_string(generation, r"c-[0-9a-f]{20}")
        _require_string(runtime_node, r"[A-Za-z0-9][A-Za-z0-9.-]{0,62}")
        if name != "vp-wc-orchestrator-" + generation or order not in {"start-first", "stop-first"}:
            raise TransactionError
        if actual["ID"] != service_id or actual["Spec"]["Name"] != "vp-autoflow-api-swarm":
            raise TransactionError
        spec = copy.deepcopy(actual["Spec"])
        container = spec["TaskTemplate"]["ContainerSpec"]
        user = container.get("User") or image_user or "0:0"
        if user in {"0", "root"}:
            user = "0:0"
        _require_string(user, r"[0-9]+:[0-9]+")
        uid, gid = user.split(":")
        env = container.get("Env") or []
        keys = [entry.split("=", 1)[0] for entry in env]
        if len(keys) != len(set(keys)):
            raise TransactionError
        target = "worker-orchestrator-database-url"
        secrets = container.get("Secrets") or []
        old = [entry for entry in secrets if entry.get("File", {}).get("Name") == target]
        if len(old) > 1 or any(not entry.get("SecretName", "").startswith("vp-wc-orchestrator-") for entry in old):
            raise TransactionError
        if any(entry.get("SecretName", "").startswith("vp-wc-orchestrator-") and entry not in old for entry in secrets):
            raise TransactionError
        env_keys = {"WORKER_ORCHESTRATOR_DATABASE_URL_FILE", "WORKER_ORCHESTRATOR_CONTROL_GENERATION"}
        container["Env"] = [entry for entry in env if entry.split("=", 1)[0] not in env_keys] + [
            "WORKER_ORCHESTRATOR_DATABASE_URL_FILE=/run/secrets/" + target,
            "WORKER_ORCHESTRATOR_CONTROL_GENERATION=" + generation,
        ]
        container["Secrets"] = [entry for entry in secrets if entry not in old] + [{
            "SecretID": secret_id, "SecretName": name,
            "File": {"Name": target, "UID": uid, "GID": gid, "Mode": 0o400},
        }]
        if owned_history is not None:
            _mount_owned_history(container, image_user, owned_history)
        container["Image"] = image
        container.setdefault("Healthcheck", {}).update(
            Test=["CMD-SHELL", health], Interval=10_000_000_000,
            Timeout=3_000_000_000, Retries=6, StartPeriod=10_000_000_000,
        )
        spec["TaskTemplate"].setdefault("Placement", {})["Constraints"] = [
            "node.labels.vp.runtime==true", "node.hostname==" + runtime_node,
        ]
        spec.setdefault("UpdateConfig", {})["Order"] = order
        return spec
    except Exception:
        raise TransactionError from None


def autoflow_update(arguments: list[str], *, owned_history_runner: bool = False) -> int:
    attempted = False
    try:
        file_mode = len(arguments) == 12 and arguments[-1] == "owned-history-file"
        if owned_history_runner and not file_mode:
            raise TransactionError
        root, fd, owner, token, revision, service_id, image, order, identity, node, health = arguments[:-1] if file_mode else arguments

        def locked_document() -> dict:
            if fd != "19" or int(owner) != os.getppid() or acquire_lock(root, fd) != token:
                raise TransactionError
            document = _registered_document(root)
            if str(document["revision"]) != revision:
                raise TransactionError
            return document

        document = locked_document()
        rollback = document["phase"].startswith("ROLLBACK") or document.get("retiring_outcome") == "rolled_back"
        service = "vp-channel-agent-runner-swarm" if owned_history_runner else "vp-autoflow-api-swarm"
        baseline = [entry for entry in document["baseline"]["services"] if entry["name"] == service]
        if len(baseline) != 1 or not baseline[0]["existed"] or baseline[0]["docker_service_id"] != service_id:
            raise TransactionError
        if owned_history_runner:
            if identity != "-" or health != "-":
                raise TransactionError
            expected_image = "vp-channelops-runner-go:deploy-" + document["target_commit"][:12]
            if rollback:
                expected_image = baseline[0]["image"]
            elif document["phase"].startswith("CANDIDATE_RESTORE"):
                failed = [entry for entry in document["failed_forward"]["services"] if entry["name"] == service]
                if len(failed) != 1 or failed[0]["docker_service_id"] != service_id:
                    raise TransactionError
                expected_image = failed[0]["image"]
        else:
            selected = document["rollback" if rollback else "forward"]["control"]
            name, secret_id, generation = identity.split("|")
            if selected["generation"] != generation or not any(
                ref["name"] == name and ref["docker_secret_id"] == secret_id
                and ref["purpose"] == "orchestrator" for ref in selected["secrets"]
            ):
                raise TransactionError
            expected_image = "vp-backend-api:deploy-" + document["target_commit"][:12]
        if image != expected_image:
            raise TransactionError
        owned_history = document["runtime_redis"].get("control") if file_mode else None
        if file_mode:
            if owned_history is None:
                raise TransactionError
            mounted = _owned_history_secret(owned_history, "0:0")
            actual_secret = _registered_docker([
                "secret", "inspect", mounted["SecretID"], "--format", "{{.ID}}|{{.Spec.Name}}",
            ], timeout=5)
            if actual_secret != mounted["SecretID"] + "|" + mounted["SecretName"]:
                raise TransactionError

        def inspect(timeout: float = 5) -> dict:
            records = json.loads(_registered_docker(["service", "inspect", service_id], timeout=timeout))
            if type(records) is not list or len(records) != 1 or records[0]["ID"] != service_id:
                raise TransactionError
            return records[0]

        actual = inspect()
        version = actual["Version"]["Index"]
        if type(version) is not int or version < 0:
            raise TransactionError
        image_user = _registered_docker(["image", "inspect", image, "--format", "{{.Config.User}}"], timeout=5)
        if owned_history_runner:
            if owned_history is None:
                raise TransactionError
            spec = _owned_history_runner_update_spec(actual, service_id, image, order, image_user, node, owned_history)
        else:
            spec = _autoflow_update_spec(actual, service_id, image, order, identity, image_user, health, node,
                                        owned_history=owned_history)
        if locked_document() != document:
            raise TransactionError
        attempted = True
        _engine_service_post(f"/services/{service_id}/update?version={version}&registryAuthFrom=spec", spec, 200)
        # API acceptance replaces neither CLI convergence nor the caller's strict readiness.
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            locked_document()
            current = inspect(timeout=min(5, max(0.01, deadline - time.monotonic())))
            if current["Spec"] != spec:
                raise TransactionError
            state = current.get("UpdateStatus", {}).get("State")
            if state == "completed" or (state is None and actual["Spec"] == spec):
                return 0
            if state != "updating":
                raise TransactionError
            time.sleep(min(0.2, max(0, deadline - time.monotonic())))
        raise TransactionError
    except Exception:
        return 1 if attempted else 2


def _registered_document(raw_root: str) -> dict:
    _root, root_fd, transactions_fd = _open_transactions(raw_root, create=False)
    try:
        document, _identity = _read_active_from_descriptor(
            transactions_fd, allow_missing=False
        )
        if document is None:
            raise TransactionError
        return document
    finally:
        os.close(transactions_fd)
        os.close(root_fd)


def _registered_inspect(job: dict, *, task: bool = False) -> dict | None:
    protocol = _registered_protocol()
    reference = job["service_id"] or job["spec"]["Name"]
    values = json.loads(_registered_docker(["service", "inspect", reference]))
    if type(values) is not list or len(values) != 1:
        raise TransactionError
    actual = values[0]
    _require_string(actual["ID"], r"[a-z0-9]{25}")
    if job["service_id"] is not None and actual["ID"] != job["service_id"]:
        raise TransactionError
    protocol["validate_managed_spec"](actual["Spec"], job["spec"])
    if not task:
        return actual
    if job["task_id"] is None:
        raise TransactionError
    tasks = json.loads(_registered_docker(["inspect", job["task_id"]]))
    if type(tasks) is not list or len(tasks) != 1 or tasks[0]["ID"] != job["task_id"]:
        raise TransactionError
    protocol["task_exit"](tasks[0], actual["ID"], job["spec"])
    return tasks[0]


def _registered_input(job: dict) -> dict:
    protocol = _registered_protocol()
    value = protocol["read_input"](Path(job["input_file"]["path"]), job["input_file"])
    if protocol["digest"](value) != job["input_sha256"]:
        raise TransactionError
    return value


def launch_registered_job(raw_root: str, raw_descriptor: str, stage: str) -> dict:
    def consume(document: dict) -> None:
        if document["phase"] != "FORWARD_APPLYING" or document["operation"] is not None:
            raise TransactionError
        job = document["registered_reconcile"][stage]
        if job is None or job["state"] != "planned":
            raise TransactionError
        _registered_files(raw_root, {"files": job["files"]})
        _registered_input(job)
        job["state"] = "launching"

    document = _update_current_document(raw_root, raw_descriptor, consume)
    job = document["registered_reconcile"][stage]
    service_id = _engine_service_post("/services/create", job["spec"], 201).get("ID")
    _require_string(service_id, r"[a-z0-9]{25}")

    def bind(document: dict) -> None:
        current = document["registered_reconcile"][stage]
        if current != job:
            raise TransactionError
        current.update(service_id=service_id, state="created")

    return _update_current_document(raw_root, raw_descriptor, bind)


def _registered_capture_result(document: dict, stage: str, job: dict) -> dict:
    protocol = _registered_protocol()
    fd = protocol["open_checked"](job["files"]["request"], os.O_RDONLY, 0o602)
    try:
        result = protocol["decode"](_read_limited(fd))
    finally:
        os.close(fd)
    protocol["exact"](result, {"snapshot", "credentials", "pins"})
    credentials = protocol["exact"](
        result["credentials"], protocol["CREDENTIAL_FIELDS"]
    )
    if any(
        credentials[key] != value
        for key, value in _registered_credentials(document).items()
    ):
        raise TransactionError
    for key in ("database_secret_sha256", "redis_secret_sha256"):
        _require_string(credentials[key], r"[0-9a-f]{64}")
    _require_string(credentials["redis_username"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    snapshot = protocol["exact"](result["snapshot"], {"observed_at", "workers"})
    observed = datetime.datetime.fromisoformat(snapshot["observed_at"])
    if observed.tzinfo is None or len(snapshot["workers"]) != 4:
        raise TransactionError
    services = (
        "vp-ffmpeg-worker-go-swarm",
        "vp-ffmpeg-worker-gpu-swarm",
        "vp-vision-worker-swarm",
        "vp-youtube-publisher-swarm",
    )
    baseline = {item["name"]: item for item in document["baseline"]["services"]}
    targets = {item["service"]: item for item in document["forward"]["workers"]}
    for service, pin in zip(services, snapshot["workers"], strict=True):
        if pin is None:
            if stage != "baseline" or baseline[service]["existed"]:
                raise TransactionError
        elif (
            pin["service_name"] != service
            or pin["image_identity"]
            != (
                baseline[service]["image"]
                if stage == "baseline"
                else targets[service]["image"]
            )
            or (
                stage == "current"
                and (
                    pin["generation"] != targets[service]["generation"]
                    or pin["release_commit"] != document["target_commit"]
                )
            )
        ):
            raise TransactionError
    if stage == "baseline" and result["pins"] is not None:
        raise TransactionError
    if stage == "current":
        protocol["exact"](result["pins"], {"pin_json", "pin_sha256", "commands"})
        pin_document = json.loads(result["pins"]["pin_json"])
        supplied = _registered_input(job)
        if (
            pin_document["transaction_id"] != document["transaction_id"]
            or pin_document["revision"] != supplied["revision"]
            or pin_document["release_commit"] != document["target_commit"]
            or pin_document["workers"]
            != [
                dict(current=current, predecessor=old)
                for current, old in zip(
                    snapshot["workers"], supplied["baseline"]["workers"], strict=True
                )
            ]
            or hashlib.sha256(result["pins"]["pin_json"].encode("ascii")).hexdigest()
            != result["pins"]["pin_sha256"]
        ):
            raise TransactionError
    return result


def observe_registered_job(raw_root: str, raw_descriptor: str, stage: str) -> dict:
    document = _registered_document(raw_root)
    job = document["registered_reconcile"][stage]
    if job is None or job["state"] not in {"launching", "created", "terminal"}:
        raise TransactionError
    actual = _registered_inspect(job)
    task_ids = _registered_docker(
        ["service", "ps", actual["ID"], "--no-trunc", "--format", "{{.ID}}"]
    )
    ids = task_ids.splitlines()
    if len(ids) > 1 or (job["task_id"] is not None and ids != [job["task_id"]]):
        raise TransactionError
    task = None
    code = None
    if ids:
        _require_string(ids[0], r"[a-z0-9]{25}")
        task = _registered_inspect(
            {**job, "service_id": actual["ID"], "task_id": ids[0]}, task=True
        )
        code = _registered_protocol()["task_exit"](task, actual["ID"], job["spec"])

    def update(value: dict) -> None:
        current = value["registered_reconcile"][stage]
        if current != job:
            raise TransactionError
        current.update(
            service_id=actual["ID"],
            state="created" if code is None else "terminal",
            task_id=None if task is None else task["ID"],
            exit_code=code,
        )
        if code == 0 and stage != "run":
            try:
                current["result"] = _registered_capture_result(value, stage, current)
            except Exception:
                current["result"] = (
                    None  # Actual exit is still recorded; invalid output cannot qualify.
                )
        elif code == 0:
            try:
                current["result"] = registered_finish_receipt(raw_root, value, current)
            except Exception:
                current["result"] = None

    return _update_current_document(raw_root, raw_descriptor, update)


def prepare_registered_run(
    raw_root: str, raw_descriptor: str, *, verify_owner: Any
) -> dict:
    def prepare(document: dict) -> None:
        registered = document["registered_reconcile"]
        current = registered["current"]
        if (
            document["phase"] != "FORWARD_APPLYING"
            or document["operation"] is not None
            or registered["run"] is not None
            or current is None
            or current["state"] != "removed"
            or current["exit_code"] != 0
            or current["result"] is None
        ):
            raise TransactionError
        _require_string(current["pins_secret_id"], r"[a-z0-9]{25}")
        protocol = _registered_protocol()
        directory = _registered_directory(raw_root, document["transaction_id"], "run")
        files = protocol["prepare_files"](directory)
        result = current["result"]
        pin_fields = result["pins"]
        pins = json.loads(pin_fields["pin_json"])
        binding = dict(
            version=1,
            attempt_id=current["attempt_id"],
            replay_only=False,
            transaction_id=document["transaction_id"],
            release_commit=document["target_commit"],
            binding_revision=pins["revision"],
            **pin_fields,
            targets={
                item["current"]["service_name"]: [
                    item["current"]["generation"],
                    item["current"]["image_identity"],
                ]
                for item in pins["workers"]
            },
            credentials=result["credentials"],
            files=files,
            descriptor_sha256="0" * 64,
        )
        _registered_selection(document, binding, None, verify_owner)
        spec = current["spec"]
        expected = protocol["managed_spec"](
            binding,
            image=spec["TaskTemplate"]["ContainerSpec"]["Image"],
            network_id=spec["TaskTemplate"]["Networks"][0]["Target"],
            manager_node=spec["TaskTemplate"]["Placement"]["Constraints"][0].split(
                "==", 1
            )[1],
            pins_secret_id=current["pins_secret_id"],
            manager_node_id=spec["Labels"]["vp.manager-node-id"],
        )
        binding["descriptor_sha256"] = protocol["digest"](expected)
        payload = {"binding": binding}
        input_file = protocol["write_input"](directory / "input.json", payload)
        registered["run"] = dict(
            attempt_id=current["attempt_id"],
            files=files,
            input_file=input_file,
            input_sha256=protocol["digest"](payload),
            spec=expected,
            service_id=None,
            task_id=None,
            exit_code=None,
            state="planned",
            result=None,
            pins_secret_id=current["pins_secret_id"],
        )

    try:
        return _update_current_document(raw_root, raw_descriptor, prepare)
    except Exception:
        raise TransactionError from None


def create_registered_pins(raw_root: str, raw_descriptor: str) -> dict:
    def intent(document: dict) -> None:
        record = document["registered_reconcile"]
        current = record["current"]
        if (
            document["phase"] != "FORWARD_APPLYING"
            or document["operation"] is not None
            or record.get("pins") is not None
            or current is None
            or current["state"] != "removed"
            or current["exit_code"] != 0
            or current["result"] is None
        ):
            raise TransactionError
        pins = current["result"]["pins"]
        if (
            hashlib.sha256(pins["pin_json"].encode("ascii")).hexdigest()
            != pins["pin_sha256"]
        ):
            raise TransactionError
        record["pins"] = dict(
            state="creating",
            id=None,
            name="vp-registered-pins-" + current["attempt_id"],
            sha256=pins["pin_sha256"],
        )

    document = _update_current_document(raw_root, raw_descriptor, intent)
    record = document["registered_reconcile"]
    pins = record["pins"]
    identity = _registered_docker(
        [
            "secret",
            "create",
            "--label",
            "vp.transaction=" + document["transaction_id"],
            "--label",
            "vp.pin_sha256=" + pins["sha256"],
            pins["name"],
            "-",
        ],
        timeout=2,
        input_bytes=record["current"]["result"]["pins"]["pin_json"].encode("ascii"),
    )
    _require_string(identity, r"[a-z0-9]{25}")

    def bind(value: dict) -> None:
        current = value["registered_reconcile"]
        if current["pins"] != pins:
            raise TransactionError
        current["pins"].update(state="present", id=identity)
        current["current"]["pins_secret_id"] = identity

    return _update_current_document(raw_root, raw_descriptor, bind)


def create_registered_read(raw_root: str, raw_descriptor: str) -> dict:
    """Copy only the captured deploy-read URL; persist no URL in the journal."""
    _require_writer_lock(raw_root, raw_descriptor)
    document = _registered_document(raw_root)
    source = document["database_credentials"]["deploy_read"]
    expected = _capture_credential(
        source["canonical_path"], source["expected_principal"]
    )
    if source != expected:
        raise TransactionError
    descriptor = os.open(source["canonical_path"], _read_file_flags())
    try:
        before = os.fstat(descriptor)
        _require_regular(before, CREDENTIAL_MODE, single_link=False)
        if (before.st_dev, before.st_ino) != (source["device"], source["inode"]):
            raise TransactionError
        raw = os.read(descriptor, 32769)
        after = os.fstat(descriptor)
        if (
            not 0 < len(raw) <= 32768
            or any(
                getattr(before, field) != getattr(after, field)
                for field in (
                    "st_dev",
                    "st_ino",
                    "st_mode",
                    "st_uid",
                    "st_gid",
                    "st_size",
                    "st_mtime_ns",
                    "st_ctime_ns",
                )
            )
            or len(raw) != before.st_size
        ):
            raise TransactionError
    finally:
        os.close(descriptor)
    from urllib.parse import unquote, urlsplit

    line = raw.decode("utf-8").removesuffix("\n")
    parsed = urlsplit(line)
    if (
        not line
        or any(char.isspace() for char in line)
        or parsed.scheme not in {"postgresql", "postgres", "postgresql+asyncpg"}
        or not parsed.password
        or unquote(parsed.username or "") != source["expected_principal"]
    ):
        raise TransactionError

    def intent(value: dict) -> None:
        record = value["registered_reconcile"]
        if (
            value["phase"] != "FORWARD_APPLYING"
            or value["operation"] is not None
            or record.get("capture_read") is not None
            or record["baseline"] is not None
            or value["database_credentials"]["deploy_read"] != source
            or not value["baseline"]["captured"]
            or any(
                worker["applied_stage"] not in {"pending", "prepared"}
                for worker in value["forward"]["workers"]
            )
        ):
            raise TransactionError
        record["capture_read"] = dict(
            state="creating",
            id=None,
            name="vp-registered-read-" + value["transaction_id"],
            sha256=hashlib.sha256(raw).hexdigest(),
            principal=source["expected_principal"],
        )

    document = _update_current_document(raw_root, raw_descriptor, intent)
    reader = document["registered_reconcile"]["capture_read"]
    identity = _registered_docker(
        [
            "secret",
            "create",
            "--label",
            "vp.transaction=" + document["transaction_id"],
            "--label",
            "vp.credential_sha256=" + reader["sha256"],
            reader["name"],
            "-",
        ],
        timeout=2,
        input_bytes=raw,
    )
    _require_string(identity, r"[a-z0-9]{25}")

    def bind(value: dict) -> None:
        if value["registered_reconcile"]["capture_read"] != reader:
            raise TransactionError
        value["registered_reconcile"]["capture_read"].update(
            state="present", id=identity
        )

    return _update_current_document(raw_root, raw_descriptor, bind)


def cleanup_registered_read(raw_root: str, raw_descriptor: str) -> dict:
    return _cleanup_registered_secret(raw_root, raw_descriptor, "capture_read")


def cleanup_registered_pins(raw_root: str, raw_descriptor: str) -> dict:
    return _cleanup_registered_secret(raw_root, raw_descriptor, "pins")


def _cleanup_registered_secret(raw_root: str, raw_descriptor: str, key: str) -> dict:
    _require_writer_lock(raw_root, raw_descriptor)
    document = _registered_document(raw_root)
    record = document.get("registered_reconcile")
    if key not in {"pins", "capture_read"}:
        raise TransactionError
    if record is None or record.get(key) is None:
        return document
    pins = record[key]
    if pins["state"] == "creating" or any(
        record[stage] is not None and record[stage]["state"] != "removed"
        for stage in ("baseline", "current", "run")
    ):
        raise TransactionError

    def inventory() -> dict:
        result = {}
        for line in _registered_docker(
            ["secret", "ls", "--format", "{{.ID}} {{.Name}}"]
        ).splitlines():
            parts = line.split()
            if len(parts) != 2 or parts[0] in result:
                raise TransactionError
            _require_string(parts[0], r"[a-z0-9]{25}")
            result[parts[0]] = parts[1]
        return result

    secrets = inventory()
    if pins["name"] in secrets.values() and secrets.get(pins["id"]) != pins["name"]:
        raise TransactionError
    if pins["id"] in secrets:
        if pins["state"] == "removed":
            raise TransactionError
        actual = json.loads(_registered_docker(["secret", "inspect", pins["id"]]))
        if (
            type(actual) is not list
            or len(actual) != 1
            or actual[0]["ID"] != pins["id"]
            or actual[0]["Spec"]
            != {
                "Name": pins["name"],
                "Labels": {
                    "vp.transaction": document["transaction_id"],
                    (
                        "vp.pin_sha256" if key == "pins" else "vp.credential_sha256"
                    ): pins["sha256"],
                },
            }
        ):
            raise TransactionError
        _registered_docker(["secret", "rm", pins["id"]], timeout=2)
    remaining = inventory()
    if pins["id"] in remaining or pins["name"] in remaining.values():
        raise TransactionError

    def removed(value: dict) -> None:
        if value["registered_reconcile"][key] != pins:
            raise TransactionError
        value["registered_reconcile"][key]["state"] = "removed"

    return _update_current_document(raw_root, raw_descriptor, removed)


def _registered_services() -> dict[str, str]:
    # service ls truncates IDs and has no --no-trunc flag. Resolve names with
    # inspect, refusing a partial or changed batch instead of inferring absence.
    names = _registered_docker(
        ["service", "ls", "--format", "{{.Name}}"]
    ).splitlines()
    for name in names:
        _require_string(name, r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}")
    expected_names = set(names)
    if len(expected_names) != len(names):
        raise TransactionError
    if not names:
        return {}
    rows = _registered_docker([
        "service", "inspect", "--format", "{{.ID}} {{.Spec.Name}}", *names,
    ])
    result = {}
    observed_names = set()
    for row in rows.splitlines():
        values = row.split()
        if (
            len(values) != 2
            or values[0] in result
            or values[1] not in expected_names
            or values[1] in observed_names
        ):
            raise TransactionError
        _require_string(values[0], r"[a-z0-9]{25}")
        result[values[0]] = values[1]
        observed_names.add(values[1])
    if observed_names != expected_names:
        raise TransactionError
    return result


def _registered_absent(job: dict) -> None:
    services = _registered_services()
    if job["service_id"] in services or job["spec"]["Name"] in services.values():
        raise TransactionError
    if job["service_id"] is None:
        return
    containers = _registered_docker(
        [
            "container",
            "ls",
            "--all",
            "--no-trunc",
            "--format",
            "{{.ID}}",
            "--filter",
            "label=com.docker.swarm.service.id=" + job["service_id"],
        ]
    ).splitlines()
    if len(set(containers)) != len(containers):
        raise TransactionError
    for container_id in containers:
        _require_string(container_id, r"[0-9a-f]{64}")
        values = json.loads(_registered_docker(["container", "inspect", container_id]))
        if (
            type(values) is not list
            or len(values) != 1
            or values[0].get("Id") != container_id
            or values[0]["Config"]["Labels"].get("com.docker.swarm.service.id")
            != job["service_id"]
            or values[0]["State"].get("Running") is not False
            or values[0]["State"].get("Status") not in {"exited", "dead"}
        ):
            raise TransactionError


def cleanup_registered_job(raw_root: str, raw_descriptor: str, stage: str) -> dict:
    """Stop only the pinned job; preserve bindings on any ambiguous observation."""
    _require_writer_lock(raw_root, raw_descriptor)
    document = _registered_document(raw_root)
    job = document["registered_reconcile"][stage]
    if job is None:
        return document
    if job["state"] == "removed":
        _registered_absent(job)
        _registered_protocol()["retain_managed_files"](job["files"], job["input_file"])
        return document
    if job["state"] == "launching" and job["service_id"] is None:
        # Discover an exact existing attempt only to stop it, never relaunch it.
        actual = _registered_inspect(job)

        def discovered(value: dict) -> None:
            current = value["registered_reconcile"][stage]
            if current != job:
                raise TransactionError
            current.update(service_id=actual["ID"], state="created")

        document = _update_current_document(raw_root, raw_descriptor, discovered)
        job = document["registered_reconcile"][stage]
    if job["state"] != "removing":
        if job["service_id"] is not None:
            _registered_inspect(job)
        else:
            _registered_absent(job)

        def removing(value: dict) -> None:
            current = value["registered_reconcile"][stage]
            if current != job:
                raise TransactionError
            current["state"] = "removing"

        document = _update_current_document(raw_root, raw_descriptor, removing)
        job = document["registered_reconcile"][stage]
    services = _registered_services()
    if (
        job["spec"]["Name"] in services.values()
        and services.get(job["service_id"]) != job["spec"]["Name"]
    ):
        raise TransactionError
    if job["service_id"] in services:
        _registered_inspect(job)
        _registered_docker(["service", "rm", job["service_id"]], timeout=2)
    _registered_absent(job)
    _registered_protocol()["retain_managed_files"](job["files"], job["input_file"])

    def removed(value: dict) -> None:
        current = value["registered_reconcile"][stage]
        if current != job:
            raise TransactionError
        current["state"] = "removed"

    return _update_current_document(raw_root, raw_descriptor, removed)


def _registered_protocol() -> dict[str, Any]:
    global _REGISTERED_PROTOCOL
    if _REGISTERED_PROTOCOL is None:
        _REGISTERED_PROTOCOL = runpy.run_path(str(
            Path(__file__).resolve().parents[2]
            / "backend/app/services/registered_consumer_reconcile_job.py"
        ))
    return _REGISTERED_PROTOCOL


@contextmanager
def _registered_context(raw_root: str, raw_descriptor: str, raw_revision: str):
    """Same writer lock/CAS as the main journal; no deployment activation."""
    root_fd = transactions_fd = transaction_fd = None
    try:
        _require_writer_lock(raw_root, raw_descriptor)
        _root, root_fd, transactions_fd = _open_transactions(raw_root, create=False)
        document, _identity = _read_active_from_descriptor(transactions_fd, allow_missing=False)
        if (document is None or document["phase"] != "FORWARD_APPLYING"
                or document["operation"] is not None
                or document["revision"] != _parse_revision(raw_revision)):
            raise TransactionError
        transaction_fd = _open_child_directory(
            transactions_fd, document["transaction_id"], create=True,
        )
        yield document, transaction_fd
    except Exception:
        raise TransactionError from None
    finally:
        for descriptor in (transaction_fd, transactions_fd, root_fd):
            if descriptor is not None:
                os.close(descriptor)


def _registered_selection(document: dict, binding: dict, service_id: str | None,
                          verify_owner: Any) -> None:
    """Task2 must supply live owning-shell FD9/FD19 and exact-job verification.

    There is deliberately no default verifier, CLI dispatch, or stale descriptor
    boolean. The callback must raise on refusal and return None only on success.
    """
    _registered_protocol()["validate_binding"](binding)
    if (binding["transaction_id"] != document["transaction_id"]
            or binding["release_commit"] != document["target_commit"]
            or binding["binding_revision"] > document["revision"]):
        raise TransactionError
    workers = document["forward"]["workers"]
    if (len(workers) != 4
            or any(worker["applied_stage"] != "verified"
                   or worker["commit"] != binding["release_commit"] for worker in workers)
            or {worker["service"]: [worker["generation"], worker["image"]]
                for worker in workers} != binding["targets"]):
        raise TransactionError
    control = document["forward"]["control"]
    credentials = binding["credentials"]
    if (control is None or control["generation"] != credentials["control_generation"]
            or not any(secret["purpose"] == "operator"
                       and secret["docker_secret_id"] == credentials["database_secret_id"]
                       and secret["name"] == "vp-wc-operator-" + credentials["control_generation"]
                       for secret in control["secrets"])):
        raise TransactionError
    if document["runtime_redis"].get("control") != {
        "runtime_generation": credentials["redis_generation"],
        "secret_name": credentials["redis_secret_name"],
        "docker_secret_id": credentials["redis_secret_id"],
    }:
        raise TransactionError
    if not callable(verify_owner) or verify_owner(binding, service_id) is not None:
        raise TransactionError


def _registered_files(raw_root: str, binding: dict) -> None:
    files = binding["files"]
    request, replies = (Path(files[key]["path"]) for key in ("request", "replies"))
    if request.name != "requests" or replies.name != "replies" or request.parent != replies.parent:
        raise TransactionError
    relative = request.parent.relative_to(Path(raw_root))
    if not relative.parts or any(part in {".", ".."} for part in relative.parts):
        raise TransactionError
    _root, descriptor = _open_admission_root(raw_root)
    try:
        for part in relative.parts:
            child = _open_child_directory(descriptor, part, create=False)
            os.close(descriptor)
            descriptor = child
        for key, mode, directory in (("request", 0o602, False), ("replies", 0o755, True)):
            if (files[key]["uid"], files[key]["gid"]) != (os.getuid(), os.getgid()):
                raise TransactionError
            fd = _registered_protocol()["open_checked"](files[key], os.O_RDONLY, mode, directory=directory)
            os.close(fd)
    finally:
        os.close(descriptor)


def _read_registered_record(descriptor: int) -> tuple[dict, tuple[int, int]]:
    before = os.stat(REGISTERED_RECONCILE_NAME, dir_fd=descriptor, follow_symlinks=False)
    _require_regular(before, FILE_MODE, single_link=True)
    fd = os.open(REGISTERED_RECONCILE_NAME, _read_file_flags(), dir_fd=descriptor)
    try:
        opened = os.fstat(fd)
        _require_regular(opened, FILE_MODE, single_link=True)
        if _identity(before) != _identity(opened):
            raise TransactionError
        record = _decode_canonical(_read_limited(fd))
        _registered_protocol()["validate_record"](record)
        return record, _identity(opened)
    finally:
        os.close(fd)


def prepare_registered_reconcile(
    raw_root: str,
    raw_descriptor: str,
    raw_revision: str,
    binding: dict,
    *,
    verify_owner: Any,
) -> None:
    with _registered_context(raw_root, raw_descriptor, raw_revision) as (
        document,
        descriptor,
    ):
        _registered_selection(document, binding, None, verify_owner)
        if binding["binding_revision"] != document["revision"]:
            run = document.get("registered_reconcile", {}).get("run")
            if (
                run is None
                or run["state"] != "planned"
                or _registered_input(run) != {"binding": binding}
            ):
                raise TransactionError
        _registered_files(raw_root, binding)
        protocol = _registered_protocol()
        record = protocol["new_record"](binding)
        if (
            protocol["read_request"](record) is not None
            or os.stat(binding["files"]["request"]["path"]).st_size
        ):
            raise TransactionError
        _write_document(
            descriptor,
            REGISTERED_RECONCILE_NAME,
            record,
            expected_identity=None,
            validator=protocol["validate_record"],
        )


def bind_registered_reconcile_job(raw_root: str, raw_descriptor: str, raw_revision: str,
                                  attempt_id: str, service_id: str, descriptor_sha256: str,
                                  *, verify_owner: Any) -> None:
    with _registered_context(raw_root, raw_descriptor, raw_revision) as (document, descriptor):
        record, identity = _read_registered_record(descriptor)
        binding = record["binding"]
        _require_string(service_id, r"[a-z0-9]{12,64}", maximum=64)
        if (attempt_id != binding["attempt_id"] or descriptor_sha256 != binding["descriptor_sha256"]
                or record["service_id"] not in {None, service_id} or record["sequence"] != 0):
            raise TransactionError
        _registered_selection(document, binding, service_id, verify_owner)
        record["service_id"] = service_id
        _write_document(descriptor, REGISTERED_RECONCILE_NAME, record, expected_identity=identity,
                        validator=_registered_protocol()["validate_record"])


def answer_registered_reconcile(raw_root: str, raw_descriptor: str, raw_revision: str,
                                *, verify_owner: Any) -> bool:
    """Consume and fsync before replying. An acknowledged prefix is never replayed."""
    with _registered_context(raw_root, raw_descriptor, raw_revision) as (document, descriptor):
        protocol = _registered_protocol()
        record, identity = _read_registered_record(descriptor)
        if record["service_id"] is None:
            raise TransactionError
        _registered_files(raw_root, record["binding"])
        found = protocol["read_request"](record)
        if found is None:
            return False
        _registered_selection(document, record["binding"], record["service_id"], verify_owner)
        if protocol["read_request"](record) != found:
            raise TransactionError
        request, prefix = found
        advanced = protocol["advance_record"](record, request, prefix)
        _write_document(descriptor, REGISTERED_RECONCILE_NAME, advanced,
                        expected_identity=identity, validator=protocol["validate_record"])
        protocol["write_reply"](record["binding"]["files"], request)
        return True


def registered_finish_receipt(raw_root: str, document: dict, job: dict) -> dict:
    root_fd = transactions_fd = descriptor = None
    try:
        _root, root_fd, transactions_fd = _open_transactions(raw_root, create=False)
        descriptor = _open_child_directory(
            transactions_fd, document["transaction_id"], create=False
        )
        record, _identity = _read_registered_record(descriptor)
        if (
            record["binding"]["attempt_id"] != job["attempt_id"]
            or record["service_id"] != job["service_id"]
            or record["binding"]["files"] != job["files"]
            or record["last_request"] is None
            or record["last_request"]["action"] != "finished"
        ):
            raise TransactionError
        metadata = dict(job["files"]["request"])
        if job["state"] == "removed":
            path = Path(metadata["path"])
            metadata["path"] = str(path.with_name("retained-" + path.name))
        fd = _registered_protocol()["open_checked"](metadata, os.O_RDONLY, 0o602)
        try:
            raw = _read_limited(fd)
        finally:
            os.close(fd)
        if (
            len(raw) != record["prefix_length"]
            or hashlib.sha256(raw).hexdigest() != record["prefix_sha256"]
        ):
            raise TransactionError
        return dict(
            outcome=record["last_request"]["outcome"],
            pin_sha256=record["binding"]["pin_sha256"],
            attempted_streams=sorted(
                stream
                for stream, value in record["streams"].items()
                if value != "unused"
            ),
            request_bytes=len(raw),
            request_sha256=record["prefix_sha256"],
            service_id=job["service_id"],
            task_id=job["task_id"],
        )
    except Exception:
        raise TransactionError from None
    finally:
        for fd in (descriptor, transactions_fd, root_fd):
            if fd is not None:
                os.close(fd)


def registered_job_action(arguments: list[str]) -> int:
    """One fixed owning-shell action; callbacks never run the replay-plan pipeline."""
    try:
        if len(arguments) < 8:
            raise TransactionError
        root, fd, outer_path, outer_fd, owner_pid, token, action, stage, *extra = (
            arguments
        )
        if stage not in {"baseline", "current", "run", "all"}:
            raise TransactionError

        def locks() -> None:
            require_registered_outer_lock(outer_path, outer_fd, owner_pid)
            if fd != "19" or outer_fd != "9" or acquire_lock(root, fd) != token:
                raise TransactionError

        locks()
        document = _registered_document(root)
        record = document.get("registered_reconcile")
        if record is None:
            if action in {"cleanup", "verify"} and not extra:
                return 0
            raise TransactionError

        def owner(binding: dict, service_id: str | None) -> None:
            locks()
            current = _registered_document(root)
            job = current["registered_reconcile"]["run"]
            if service_id is None:
                if job is not None and _registered_input(job) != {"binding": binding}:
                    raise TransactionError
                return
            if (
                job is None
                or job["service_id"] != service_id
                or _registered_input(job) != {"binding": binding}
                or _registered_protocol()["digest"](job["spec"])
                != binding["descriptor_sha256"]
            ):
                raise TransactionError
            actual = _registered_inspect(job, task=job["task_id"] is not None)
            if job["task_id"] is not None and actual["Status"]["State"] != "running":
                raise TransactionError

        if action == "prepare" and stage in {"baseline", "current"} and len(extra) == 2:
            network_id, manager = extra
            if manager != "ccttww-lap":
                raise TransactionError
            network = json.loads(_registered_docker(["network", "inspect", network_id]))
            info = json.loads(_registered_docker(["info", "--format", "{{json .}} "]))
            if (
                len(network) != 1
                or network[0]["Id"] != network_id
                or network[0]["Name"] != "vp-pipeline-net"
                or network[0]["Driver"] != "overlay"
                or network[0]["Scope"] != "swarm"
                or info["Name"] != manager
                or info["OSType"] != "linux"
                or info["Swarm"]["ControlAvailable"] is not True
            ):
                raise TransactionError
            if stage == "baseline":
                document = create_registered_read(root, fd)
            prepare_registered_capture(
                root,
                fd,
                str(document["revision"]),
                stage,
                network_id,
                manager,
                info["Swarm"]["NodeID"],
            )
        elif action == "prepare" and stage == "run" and not extra:
            create_registered_pins(root, fd)
            document = prepare_registered_run(root, fd, verify_owner=owner)
            binding = _registered_input(document["registered_reconcile"]["run"])[
                "binding"
            ]
            prepare_registered_reconcile(
                root, fd, str(document["revision"]), binding, verify_owner=owner
            )
        elif action == "launch" and stage != "all" and not extra:
            document = launch_registered_job(root, fd, stage)
            if stage == "run":
                job = document["registered_reconcile"][stage]
                binding = _registered_input(job)["binding"]
                bind_registered_reconcile_job(
                    root,
                    fd,
                    str(document["revision"]),
                    job["attempt_id"],
                    job["service_id"],
                    binding["descriptor_sha256"],
                    verify_owner=owner,
                )
        elif action in {"poll", "observe"} and stage != "all" and not extra:
            job = record[stage]
            if job is None:
                raise TransactionError
            if job["state"] != "terminal" and (
                action == "observe" or job["task_id"] is None
            ):
                document = observe_registered_job(root, fd, stage)
                job = document["registered_reconcile"][stage]
            if job["state"] == "terminal":
                return 10 if job["exit_code"] == 0 and job["result"] is not None else 11
            if stage == "run" and job["task_id"] is not None:
                answer_registered_reconcile(
                    root, fd, str(document["revision"]), verify_owner=owner
                )
        elif action == "cleanup" and not extra:
            for selected in (
                ("run", "current", "baseline") if stage == "all" else (stage,)
            ):
                cleanup_registered_job(root, fd, selected)
            if stage == "all":
                cleanup_registered_pins(root, fd)
                cleanup_registered_read(root, fd)
        elif action == "verify" and stage == "all" and not extra:
            _registered_gate(document, success=True)
            for job in (record["baseline"], record["current"], record["run"]):
                _registered_absent(job)
                _registered_protocol()["retain_managed_files"](
                    job["files"], job["input_file"]
                )
            if (
                registered_finish_receipt(root, document, record["run"])
                != record["run"]["result"]
            ):
                raise TransactionError
            cleanup_registered_pins(root, fd)
            cleanup_registered_read(root, fd)
        else:
            raise TransactionError
        return 0
    except Exception:
        raise TransactionError from None


def queue_retirement(arguments: list[str]) -> None:
    if len(arguments) != 5:
        raise TransactionError
    (
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        retirement_id,
        identity_path,
    ) = arguments
    _require_string(retirement_id, r"retirement-[0-9a-f]{32}", maximum=43)
    identity = _load_identity_file(identity_path)

    def updater(document: dict[str, Any]) -> None:
        if document["phase"] in {"PREPARING", "DONE"}:
            raise TransactionError
        document["pending_retirements"].append(
            {"retirement_id": retirement_id, "identity": identity}
        )

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def complete_retirement(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, retirement_id = arguments

    def updater(document: dict[str, Any]) -> None:
        if document["phase"] != "RETIRING":
            raise TransactionError
        matches = [
            item
            for item in document["pending_retirements"]
            if item["retirement_id"] == retirement_id
        ]
        if len(matches) != 1:
            raise TransactionError
        document["pending_retirements"] = [
            item
            for item in document["pending_retirements"]
            if item["retirement_id"] != retirement_id
        ]

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def intent(arguments: list[str]) -> None:
    if len(arguments) != 5:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, kind, identity_path = arguments
    if kind not in INTENT_PHASES or kind == "REMOVE_PREPARED_SECRET":
        raise TransactionError
    identity = _load_identity_file(identity_path)

    def updater(document: dict[str, Any]) -> None:
        current_phase, target_phase = INTENT_PHASES[kind]
        if document["phase"] != current_phase or document["operation"] is not None:
            raise TransactionError
        if kind in {"PROMOTE_WORKERS", "PROMOTE_MARKER", "PROMOTE_CONTROL"}:
            _registered_gate(document, success=True)
        document["operation"] = {
            "operation_id": f"operation-{secrets.token_hex(16)}",
            "kind": kind,
            "target_phase": target_phase,
            "identity": identity,
        }

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def complete_intent(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, operation_id = arguments

    def updater(document: dict[str, Any]) -> None:
        operation = document["operation"]
        if (
            operation is None
            or operation["operation_id"] != operation_id
            or operation["kind"] == "REMOVE_PREPARED_SECRET"
        ):
            raise TransactionError
        document["operation"] = None
        _set_phase(document, operation["target_phase"], None)

    document = _update_document(
        raw_root,
        raw_lock_descriptor,
        raw_revision,
        updater,
    )
    _print_json(document)


def replay_plan(raw_root: str) -> None:
    try:
        _root, root_descriptor, transactions_descriptor = _open_transactions(
            raw_root,
            create=False,
        )
    except FileNotFoundError:
        _print_json(
            {
                "active": False,
                "allow_new_candidate": True,
                "allow_stale_cleanup": True,
                "namespace": None,
                "next_action": "BEGIN",
                "pending_operation": None,
                "phase": None,
                "retirements": [],
                "revision": None,
                "transaction_id": None,
            }
        )
        return
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=True,
            allow_legacy_quarantine=True,
        )
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    if document is None:
        _print_json(
            {
                "active": False,
                "allow_new_candidate": True,
                "allow_stale_cleanup": True,
                "namespace": None,
                "next_action": "BEGIN",
                "pending_operation": None,
                "phase": None,
                "retirements": [],
                "revision": None,
                "transaction_id": None,
            }
        )
        return
    if isinstance(document, LegacySchema1Quarantine):
        _print_json(
            {
                "active": True,
                "allow_new_candidate": False,
                "allow_stale_cleanup": False,
                "journal_sha256": document.journal_sha256,
                "namespace": None,
                "next_action": "QUARANTINE_LEGACY_SCHEMA_1",
                "pending_operation": None,
                "phase": document.phase,
                "reason_code": (
                    "legacy_schema_1_authority_context_unavailable"
                ),
                "retirements": [],
                "revision": document.revision,
                "transaction_id": document.transaction_id,
            }
        )
        return
    operation = document["operation"]
    if operation is None:
        next_action = REPLAY_ACTIONS[document["phase"]]
    else:
        next_action = f"VERIFY_{operation['kind']}"
    retirements = (
        document["pending_retirements"]
        if document["phase"] == "RETIRING" and operation is None
        else []
    )
    _print_json(
        {
            "active": True,
            "allow_new_candidate": False,
            "allow_stale_cleanup": False,
            "namespace": document["forward"]["namespace"],
            "next_action": next_action,
            "pending_operation": operation,
            "phase": document["phase"],
            "retirements": retirements,
            "revision": document["revision"],
            "transaction_id": document["transaction_id"],
        }
    )


def replay_state(raw_root: str) -> None:
    _root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, _active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if document is None:
            raise TransactionError
        transaction_descriptor = _open_child_directory(
            transactions_descriptor,
            document["transaction_id"],
            create=False,
        )
        try:
            progress, _progress_identity = _read_app_progress_from_descriptor(
                transaction_descriptor,
                allow_missing=True,
            )
            if progress is not None and (
                progress["transaction_id"] != document["transaction_id"]
                or progress["target_commit"] != document["target_commit"]
            ):
                raise TransactionError
        finally:
            os.close(transaction_descriptor)
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    replay = {**document, "app_progress": progress}
    _print_json(replay)


def archive(arguments: list[str]) -> None:
    if len(arguments) != 3:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision = arguments
    _require_writer_lock(raw_root, raw_lock_descriptor)
    expected_revision = _parse_revision(raw_revision)
    root, root_descriptor, transactions_descriptor = _open_transactions(
        raw_root,
        create=False,
    )
    try:
        document, active_identity = _read_active_from_descriptor(
            transactions_descriptor,
            allow_missing=False,
        )
        if (
            document is None
            or active_identity is None
            or document["revision"] != expected_revision
            or document["phase"] != "DONE"
            or document["pending_retirements"]
            or document["operation"] is not None
        ):
            raise TransactionError
        transaction_id = document["transaction_id"]
        transaction_descriptor = _open_child_directory(
            transactions_descriptor,
            transaction_id,
            create=False,
        )
        try:
            try:
                os.stat(
                    "done.json",
                    dir_fd=transaction_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                pass
            else:
                raise TransactionError
            current = os.stat(
                ACTIVE_NAME,
                dir_fd=transactions_descriptor,
                follow_symlinks=False,
            )
            _require_regular(current, FILE_MODE, single_link=True)
            if _identity(current) != active_identity:
                raise TransactionError
            os.replace(
                ACTIVE_NAME,
                "done.json",
                src_dir_fd=transactions_descriptor,
                dst_dir_fd=transaction_descriptor,
            )
            os.fsync(transaction_descriptor)
            os.fsync(transactions_descriptor)
        finally:
            os.close(transaction_descriptor)
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    print(root / TRANSACTIONS_NAME / transaction_id / "done.json")


def main(arguments: list[str]) -> int:
    if arguments and arguments[0] == "autoflow-update":
        return autoflow_update(arguments[1:])
    if arguments and arguments[0] == "owned-history-runner-update":
        return autoflow_update(arguments[1:], owned_history_runner=True)
    if arguments and arguments[0] == "registered-job":
        return registered_job_action(arguments[1:])
    if len(arguments) == 2 and arguments[0] == "lock-prepare":
        prepare_lock(arguments[1])
        return 0
    if len(arguments) == 3 and arguments[0] == "lock-acquire":
        acquire_lock(arguments[1], arguments[2])
        return 0
    if len(arguments) == 3 and arguments[0] == "lock-token":
        print(acquire_lock(arguments[1], arguments[2]))
        return 0
    if arguments and arguments[0] == "begin":
        begin(arguments[1:])
        return 0
    if arguments and arguments[0] == "verify-preparing":
        verify_preparing(arguments[1:])
        return 0
    if arguments and arguments[0] == "validate-credentials":
        validate_credentials(arguments[1:])
        return 0
    if arguments and arguments[0] == "verify-credential-record":
        verify_credential_record(arguments[1:])
        return 0
    if arguments and arguments[0] == "verify-credential":
        verify_credential(arguments[1:])
        return 0
    if arguments and arguments[0] == "failed-control":
        try:
            failed_control(arguments[1:])
        except (OSError, UnicodeError, ValueError, TypeError, KeyError):
            raise TransactionError from None
        return 0
    if arguments and arguments[0] == "record-prepared-secret":
        record_prepared_secret(arguments[1:])
        return 0
    if arguments and arguments[0] == "lookup-prepared-secret":
        lookup_prepared_secret(arguments[1:])
        return 0
    if arguments and arguments[0] == "record-authority-intent":
        record_authority_intent(arguments[1:])
        return 0
    if arguments and arguments[0] == "mark-authority-provisioning":
        mark_authority_provisioning(arguments[1:])
        return 0
    if arguments and arguments[0] == "mark-authority-provisioned":
        mark_authority_provisioned(arguments[1:])
        return 0
    if arguments and arguments[0] == "begin-abort":
        begin_abort(arguments[1:])
        return 0
    if arguments and arguments[0] == "resume-legacy-forward":
        resume_legacy_forward(arguments[1:])
        return 0
    if arguments and arguments[0] == "list-abort":
        list_abort(arguments[1:])
        return 0
    if arguments and arguments[0] == "intent-prepared-secret-removal":
        intent_prepared_secret_removal(arguments[1:])
        return 0
    if arguments and arguments[0] == "complete-prepared-secret-removal":
        complete_prepared_secret_removal(arguments[1:])
        return 0
    if arguments and arguments[0] == "complete-abort-authority":
        complete_abort_authority(arguments[1:])
        return 0
    if arguments and arguments[0] == "finish-abort":
        finish_abort(arguments[1:])
        return 0
    if arguments and arguments[0] == "init-app-progress":
        init_app_progress(arguments[1:])
        return 0
    if arguments and arguments[0] == "record-app-attempt":
        record_app_attempt(arguments[1:])
        return 0
    if arguments and arguments[0] == "remove-app-attempt":
        remove_app_attempt(arguments[1:])
        return 0
    if arguments and arguments[0] == "advance-migration-state":
        advance_migration_state(arguments[1:])
        return 0
    if arguments and arguments[0] == "read-app-progress":
        read_app_progress(arguments[1:])
        return 0
    if arguments and arguments[0] == "transition":
        transition(arguments[1:])
        return 0
    if arguments and arguments[0] == "capture-baseline":
        capture_baseline(arguments[1:])
        return 0
    if arguments and arguments[0] == "capture-failed-forward":
        capture_failed_forward(arguments[1:])
        return 0
    if arguments and arguments[0] == "allocate-rollback-attempt":
        allocate_rollback_attempt(arguments[1:])
        return 0
    if arguments and arguments[0] == "record-control-selection":
        record_control_selection(arguments[1:])
        return 0
    if arguments and arguments[0] == "record-marker-selection":
        record_marker_selection(arguments[1:])
        return 0
    if arguments and arguments[0] == "record-janitor-service":
        record_janitor_service(arguments[1:])
        return 0
    if arguments and arguments[0] == "clear-janitor-service":
        clear_janitor_service(arguments[1:])
        return 0
    if arguments and arguments[0] == "record-worker-plan":
        record_worker_plan(arguments[1:])
        return 0
    if arguments and arguments[0] == "advance-worker-stage":
        advance_worker_stage(arguments[1:])
        return 0
    if arguments and arguments[0] == "record-runtime-secret":
        record_runtime_secret(arguments[1:])
        return 0
    if arguments and arguments[0] == "prepare-vision-job":
        prepare_vision_job(arguments[1:])
        return 0
    if arguments and arguments[0] == "record-vision-job-service":
        record_vision_job_service(arguments[1:])
        return 0
    if arguments and arguments[0] == "record-vision-job-terminal":
        record_vision_job_terminal(arguments[1:])
        return 0
    if arguments and arguments[0] == "complete-vision-job-removal":
        complete_vision_job_removal(arguments[1:])
        return 0
    if arguments and arguments[0] == "abort-vision-job-removal":
        abort_vision_job_removal(arguments[1:])
        return 0
    if arguments and arguments[0] == "lookup-vision-job":
        lookup_vision_job(arguments[1:])
        return 0
    if arguments and arguments[0] == "queue-retirement":
        queue_retirement(arguments[1:])
        return 0
    if arguments and arguments[0] == "complete-retirement":
        complete_retirement(arguments[1:])
        return 0
    if arguments and arguments[0] == "intent":
        intent(arguments[1:])
        return 0
    if arguments and arguments[0] == "complete-intent":
        complete_intent(arguments[1:])
        return 0
    if len(arguments) == 2 and arguments[0] == "replay-plan":
        replay_plan(arguments[1])
        return 0
    if len(arguments) == 2 and arguments[0] == "replay-state":
        replay_state(arguments[1])
        return 0
    if arguments and arguments[0] == "archive":
        archive(arguments[1:])
        return 0
    raise TransactionError


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except TransactionError:
        raise SystemExit(1)
