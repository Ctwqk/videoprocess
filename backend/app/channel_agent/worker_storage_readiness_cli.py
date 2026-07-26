from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Sequence
from typing import Never

from app.services.worker_storage_readiness import (
    ReadinessFailure,
    probe_worker_storage,
)


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

    try:
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
