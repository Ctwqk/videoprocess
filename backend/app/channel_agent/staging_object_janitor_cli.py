from __future__ import annotations

import argparse
import asyncio
import json
import os
import stat
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Never

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings
from app.services.staging_janitor_status import StagingJanitorStatusStore
from app.services.staging_object_janitor import (
    STAGING_GRACE_SECONDS,
    StagingObjectJanitor,
)
from app.storage.minio_backend import MinioStorageBackend
from worker.secret_config import read_mode_0400_secret


_EVIDENCE_DIRECTORY = Path("/run/videoprocess/staging-janitor")
_STATUS_FILENAME = "status.json"
_ROOT_UID = 0
_ROOT_GID = 0
_RUNTIME_UID = 10001
_RUNTIME_GID = 10001


class _CLIUsageError(ValueError):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise _CLIUsageError(message)

    def exit(self, status: int = 0, message: str | None = None) -> Never:
        raise _CLIUsageError(message or "invalid arguments")


async def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
    except (argparse.ArgumentError, _CLIUsageError):
        _emit({"status": "failed", "code": "invalid_arguments"})
        return 3
    if args.action == "prepare-evidence":
        if args.status_file is not None:
            _emit({"status": "failed", "code": "invalid_arguments"})
            return 3
        try:
            _prepare_evidence_volume()
        except Exception:
            _emit({"status": "failed", "code": "evidence_prepare_failed"})
            return 3
        _emit({"status": "ok", "action": "prepare-evidence"})
        return 0
    status_file = Path(
        args.status_file
        or os.environ.get(
            "VP_STAGING_JANITOR_STATUS_FILE",
            "/run/videoprocess/staging-janitor/status.json",
        )
    )
    runner_id = os.environ.get(
        "VP_STAGING_JANITOR_RUNNER_ID",
        "",
    ).strip()
    if not runner_id or len(runner_id) > 255:
        _emit({"status": "failed", "code": "janitor_failed"})
        return 3
    run_id = uuid.uuid4()
    engine: AsyncEngine | None = None
    status_store: StagingJanitorStatusStore | None = None
    begin_attempted = False
    try:
        engine, session_factory = _database_resources(os.environ)
        status_store = StagingJanitorStatusStore(session_factory)
        begin_attempted = True
        begin_outcome = await status_store.begin(
            run_id,
            runner_id=runner_id,
        )
        if begin_outcome == "overlap":
            _emit({"status": "overlap_skipped"})
            return 0
        minio_access_key, minio_secret_key = _load_minio_credentials(
            os.environ
        )
        storage = MinioStorageBackend(
            endpoint=settings.minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            bucket=settings.minio_bucket,
            secure=settings.minio_secure,
            create_bucket=False,
            connect_timeout_seconds=(
                settings.minio_connect_timeout_seconds
            ),
            read_timeout_seconds=settings.minio_read_timeout_seconds,
            max_retries=settings.minio_max_retries,
            operation_timeout_seconds=(
                settings.minio_operation_timeout_seconds
            ),
        )
        result = await StagingObjectJanitor(
            session_factory,
            client=storage.client,
            bucket=storage.bucket,
            status_file=status_file,
            grace_seconds=STAGING_GRACE_SECONDS,
        ).run_once()
        succeeded = result["errors"] == 0
        effective_success = await status_store.finish(
            run_id,
            result=result,
            succeeded=succeeded,
        )
        if effective_success is not succeeded:
            raise RuntimeError("janitor durable result mismatch")
    except Exception:
        if status_store is not None and begin_attempted:
            try:
                await status_store.finish(
                    run_id,
                    result={
                        "scanned": 0,
                        "deleted": 0,
                        "protected": 0,
                        "too_young": 0,
                        "invalid": 0,
                        "errors": 1,
                    },
                    succeeded=False,
                )
            except Exception:
                pass
        _emit({"status": "failed", "code": "janitor_failed"})
        return 3
    finally:
        if engine is not None:
            try:
                await engine.dispose()
            except Exception:
                pass
    if result["errors"]:
        _emit({"status": "failed", "code": "janitor_failed"})
        return 3
    _emit({"status": "ok", **result})
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="staging-object-janitor",
        add_help=False,
        exit_on_error=False,
    )
    parser.add_argument(
        "action",
        nargs="?",
        choices=("run", "prepare-evidence"),
        default="run",
    )
    parser.add_argument("--status-file")
    return parser


def _prepare_evidence_volume() -> None:
    if os.geteuid() != _ROOT_UID or os.getegid() != _ROOT_GID:
        raise RuntimeError("evidence preparation requires root")

    directory_flags = (
        os.O_RDONLY
        | os.O_DIRECTORY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        directory_descriptor = os.open(
            _EVIDENCE_DIRECTORY,
            directory_flags,
        )
    except OSError as exc:
        raise RuntimeError("evidence directory is invalid") from exc

    status_descriptor: int | None = None
    try:
        directory_metadata = os.fstat(directory_descriptor)
        _require_migratable_evidence_directory(directory_metadata)
        _require_path_identity(
            _EVIDENCE_DIRECTORY,
            directory_metadata,
        )
        status_descriptor = _open_migratable_status_file(
            directory_descriptor
        )

        if status_descriptor is not None:
            os.fchown(
                status_descriptor,
                _RUNTIME_UID,
                _RUNTIME_GID,
            )
            os.fchmod(status_descriptor, 0o600)
            status_metadata = os.fstat(status_descriptor)
            _require_runtime_status_file(status_metadata)
            _require_status_path_identity(
                directory_descriptor,
                status_metadata,
            )

        os.fchmod(directory_descriptor, 0o700)
        os.fchown(
            directory_descriptor,
            _RUNTIME_UID,
            _RUNTIME_GID,
        )
        runtime_directory_metadata = os.fstat(directory_descriptor)
        if (
            not stat.S_ISDIR(runtime_directory_metadata.st_mode)
            or stat.S_IMODE(runtime_directory_metadata.st_mode) != 0o700
            or runtime_directory_metadata.st_uid != _RUNTIME_UID
            or runtime_directory_metadata.st_gid != _RUNTIME_GID
        ):
            raise RuntimeError("evidence directory migration failed")
        _require_path_identity(
            _EVIDENCE_DIRECTORY,
            runtime_directory_metadata,
        )
        if status_descriptor is not None:
            _require_status_path_identity(
                directory_descriptor,
                os.fstat(status_descriptor),
            )
    finally:
        if status_descriptor is not None:
            os.close(status_descriptor)
        os.close(directory_descriptor)


def _require_migratable_evidence_directory(metadata: os.stat_result) -> None:
    identity = (
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
    )
    allowed_identities = {
        (_ROOT_UID, _ROOT_GID, 0o755),
        (_ROOT_UID, _ROOT_GID, 0o700),
        (_RUNTIME_UID, _RUNTIME_GID, 0o700),
    }
    if not stat.S_ISDIR(metadata.st_mode) or identity not in allowed_identities:
        raise RuntimeError("evidence directory identity is invalid")


def _open_migratable_status_file(
    directory_descriptor: int,
) -> int | None:
    try:
        path_metadata = os.stat(
            _STATUS_FILENAME,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None
    _require_migratable_status_file(path_metadata)
    flags = (
        os.O_RDONLY
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(
            _STATUS_FILENAME,
            flags,
            dir_fd=directory_descriptor,
        )
    except OSError as exc:
        raise RuntimeError("evidence status file is invalid") from exc
    try:
        descriptor_metadata = os.fstat(descriptor)
        _require_migratable_status_file(descriptor_metadata)
        if not _same_file_identity(path_metadata, descriptor_metadata):
            raise RuntimeError("evidence status file identity changed")
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def _require_migratable_status_file(metadata: os.stat_result) -> None:
    owner = (metadata.st_uid, metadata.st_gid)
    allowed_owners = {
        (_ROOT_UID, _ROOT_GID),
        (_RUNTIME_UID, _RUNTIME_GID),
    }
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or owner not in allowed_owners
    ):
        raise RuntimeError("evidence status file identity is invalid")


def _require_runtime_status_file(metadata: os.stat_result) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_uid != _RUNTIME_UID
        or metadata.st_gid != _RUNTIME_GID
    ):
        raise RuntimeError("evidence status file migration failed")


def _require_path_identity(
    path: Path,
    expected: os.stat_result,
) -> None:
    try:
        actual = path.lstat()
    except OSError as exc:
        raise RuntimeError("evidence directory identity changed") from exc
    if not _same_file_identity(expected, actual):
        raise RuntimeError("evidence directory identity changed")


def _require_status_path_identity(
    directory_descriptor: int,
    expected: os.stat_result,
) -> None:
    try:
        actual = os.stat(
            _STATUS_FILENAME,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise RuntimeError("evidence status file identity changed") from exc
    if not _same_file_identity(expected, actual):
        raise RuntimeError("evidence status file identity changed")


def _same_file_identity(
    left: os.stat_result,
    right: os.stat_result,
) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _database_resources(
    env: Mapping[str, str],
) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    if env.get("DATABASE_URL", "").strip():
        raise RuntimeError(
            "staging janitor database URL must not be supplied in environment"
        )
    secret_path = env.get(
        "VP_STAGING_JANITOR_DATABASE_URL_FILE",
        "",
    ).strip()
    if not secret_path:
        raise RuntimeError(
            "staging janitor database URL file is required"
        )
    database_url = read_mode_0400_secret(
        secret_path,
        label="staging janitor database URL",
    )
    engine = create_async_engine(
        database_url,
        pool_pre_ping=True,
    )
    return engine, async_sessionmaker(
        engine,
        expire_on_commit=False,
    )


def _load_minio_credentials(
    env: Mapping[str, str],
) -> tuple[str, str]:
    if (
        str(env.get("MINIO_ACCESS_KEY", "")).strip()
        or str(env.get("MINIO_SECRET_KEY", "")).strip()
    ):
        raise RuntimeError(
            "staging janitor MinIO credentials must not be supplied "
            "in environment"
        )
    access_path = str(
        env.get("VP_STAGING_JANITOR_MINIO_ACCESS_KEY_FILE", "")
    ).strip()
    secret_path = str(
        env.get("VP_STAGING_JANITOR_MINIO_SECRET_KEY_FILE", "")
    ).strip()
    if (
        not access_path.startswith("/")
        or not secret_path.startswith("/")
        or access_path == secret_path
    ):
        raise RuntimeError(
            "staging janitor MinIO credential files are required"
        )
    access_key = read_mode_0400_secret(
        access_path,
        label="staging janitor MinIO access key",
    )
    secret_key = read_mode_0400_secret(
        secret_path,
        label="staging janitor MinIO secret key",
    )
    return access_key, secret_key


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
