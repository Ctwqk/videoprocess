from __future__ import annotations

import argparse
import asyncio
import json
import os
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

from app.services.staging_janitor_status import StagingJanitorStatusStore
from app.services.staging_object_janitor import (
    STAGING_GRACE_SECONDS,
    StagingObjectJanitor,
)
from app.storage.manager import get_storage
from app.storage.minio_backend import MinioStorageBackend
from worker.secret_config import read_mode_0400_secret


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
        storage = get_storage("minio", create_bucket=False)
        if not isinstance(storage, MinioStorageBackend):
            raise RuntimeError("MinIO storage backend is unavailable")
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
    parser.add_argument("--status-file")
    return parser


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


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
