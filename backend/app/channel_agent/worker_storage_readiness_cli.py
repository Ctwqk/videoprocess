from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Sequence
from typing import Never

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.services.staging_janitor_status import StagingJanitorStatusStore
from app.services.worker_storage_readiness import (
    ReadinessFailure,
    probe_worker_storage,
)
from worker.secret_config import load_worker_database_url


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

    engine = None
    try:
        if (
            os.environ.get("VP_REQUIRE_STAGING_JANITOR", "")
            .strip()
            .lower()
            == "true"
        ):
            database_url = load_worker_database_url(os.environ)
            engine = create_async_engine(
                database_url,
                pool_pre_ping=True,
            )
            session_factory = async_sessionmaker(
                engine,
                expire_on_commit=False,
            )
            result = await probe_worker_storage(
                os.environ,
                require_artifact_api=args.require_artifact_api,
                staging_janitor_probe=(
                    StagingJanitorStatusStore(
                        session_factory
                    ).readiness
                ),
            )
        else:
            result = await probe_worker_storage(
                os.environ,
                require_artifact_api=args.require_artifact_api,
            )
    except ReadinessFailure as exc:
        _emit({"status": "failed", "code": exc.code})
        return 3
    except Exception:
        _emit({"status": "failed", "code": "unexpected_failure"})
        return 3
    finally:
        if engine is not None:
            try:
                await engine.dispose()
            except Exception:
                pass

    _emit(result)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="worker-storage-readiness",
        add_help=False,
        exit_on_error=False,
    )
    parser.add_argument("--require-artifact-api", action="store_true")
    return parser


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
