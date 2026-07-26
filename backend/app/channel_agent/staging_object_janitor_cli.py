from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Never

from app.db import async_session
from app.services.staging_object_janitor import (
    STAGING_GRACE_SECONDS,
    StagingObjectJanitor,
)
from app.storage.manager import get_storage
from app.storage.minio_backend import MinioStorageBackend


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
    try:
        storage = get_storage("minio", create_bucket=False)
        if not isinstance(storage, MinioStorageBackend):
            raise RuntimeError("MinIO storage backend is unavailable")
        result = await StagingObjectJanitor(
            async_session,
            client=storage.client,
            bucket=storage.bucket,
            status_file=status_file,
            grace_seconds=STAGING_GRACE_SECONDS,
        ).run_once()
    except Exception:
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


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
