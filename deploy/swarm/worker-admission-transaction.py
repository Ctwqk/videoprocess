#!/usr/bin/env python3
"""Durable worker-admission transaction and single-writer lock helper."""

from __future__ import annotations

import datetime
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import sys
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
    expected_control_operator_reference = (
        f"control/{expected_control_generation}/"
        "worker-registration-operator-database-url"
    )
    expected_control_image = (
        f"vp-ffmpeg-worker-python:deploy-{target_commit[:12]}"
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
) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise TransactionError
    identities: set[tuple[str, str, str]] = set()
    control_images: set[str] = set()
    authorities: list[dict[str, str]] = []
    for item in value:
        authority = _validate_authority(item, target_commit)
        identity = (
            authority["kind"],
            authority["service"],
            authority["generation"],
        )
        if identity in identities:
            raise TransactionError
        identities.add(identity)
        control_images.add(authority["control_image"])
        authorities.append(authority)
    if len(control_images) > 1:
        raise TransactionError
    return authorities


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
    document = _require_exact_fields(value, TOP_LEVEL_FIELDS)
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

    authorities = _validate_authorities(
        document["authorities"],
        document["target_commit"],
    )
    if document["phase"] in {"PREPARING", "FORWARD_APPLYING"}:
        if any(authority["state"] == "revoked" for authority in authorities):
            raise TransactionError
    elif document["phase"] == "ABORTING" or (
        document["phase"] == "DONE" and document["outcome"] == "aborted"
    ):
        pass
    elif any(
        authority["state"] != "provisioned" for authority in authorities
    ):
        raise TransactionError

    _validate_secret_refs(document["prepared_secrets"])
    for reference in document["prepared_secrets"]:
        if (
            reference["service"] == "vision-cutover"
            and reference["generation"] == document["transaction_id"]
            and reference["purpose"]
            in {"safety-database", "final-safety-database"}
        ):
            continue
        if reference["service"] == "vp-worker-control":
            expected_kind = "control"
        elif reference["service"] == "worker-redis-marker-control":
            expected_kind = "marker"
        else:
            expected_kind = "runtime"
        matches = [
            authority
            for authority in authorities
            if (
                authority["kind"] == expected_kind
                and authority["service"] == reference["service"]
                and authority["generation"] == reference["generation"]
                and authority["state"] == "provisioned"
            )
        ]
        if len(matches) != 1:
            raise TransactionError

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

    def updater(document: dict[str, Any]) -> None:
        if document["phase"] not in {"PREPARING", "FORWARD_APPLYING"}:
            raise TransactionError
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
        )
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
            return
        document["authorities"].append(authority)

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

    def updater(document: dict[str, Any]) -> None:
        if document["phase"] not in {"PREPARING", "FORWARD_APPLYING"}:
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

    def updater(document: dict[str, Any]) -> None:
        if document["phase"] not in {"PREPARING", "FORWARD_APPLYING"}:
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
                return
            if (
                existing["name"] == reference["name"]
                or existing["docker_secret_id"]
                == reference["docker_secret_id"]
                or existing_key == logical_key
            ):
                raise TransactionError
        document["prepared_secrets"].append(reference)

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
        if document is None or document["phase"] not in {
            "PREPARING",
            "FORWARD_APPLYING",
        }:
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
        if len(matches) > 1:
            raise TransactionError
    finally:
        os.close(transactions_descriptor)
        os.close(root_descriptor)
    print(matches[0]["docker_secret_id"] if matches else "-")


def _set_phase(document: dict[str, Any], target_phase: str, outcome: str | None) -> None:
    current_phase = document["phase"]
    if (
        target_phase not in PHASES
        or target_phase not in LEGAL_TRANSITIONS[current_phase]
        or document["operation"] is not None
    ):
        raise TransactionError
    if target_phase == "FORWARD_APPLYING" and not document["baseline"]["captured"]:
        raise TransactionError
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


def begin_abort(arguments: list[str]) -> None:
    if len(arguments) != 4:
        raise TransactionError
    raw_root, raw_lock_descriptor, raw_revision, reason = arguments
    _require_string(reason, r"[a-z][a-z0-9_]{0,63}", maximum=64)

    def updater(document: dict[str, Any]) -> None:
        aborting_forward = document["phase"] == "FORWARD_APPLYING"
        if (
            document["phase"] not in {"PREPARING", "FORWARD_APPLYING"}
            or document["operation"] is not None
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
