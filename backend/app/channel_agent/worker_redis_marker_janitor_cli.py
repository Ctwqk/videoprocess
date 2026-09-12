from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from collections.abc import Sequence
from typing import Any, Never

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.services.worker_redis_marker_control import (
    load_marker_control_config,
    run_worker_redis_marker_janitor,
)


class _CLIUsageError(ValueError):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise _CLIUsageError(message)

    def exit(self, status: int = 0, message: str | None = None) -> Never:
        raise _CLIUsageError(message or "invalid arguments")


def create_database_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


def create_redis_client(redis_url: str) -> Any:
    return aioredis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=30,
        health_check_interval=30,
    )


def create_session_factory(
    engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def run(argv: Sequence[str] | None = None) -> int:
    try:
        _parser().parse_args(argv)
    except (argparse.ArgumentError, _CLIUsageError):
        _emit({"status": "failed", "code": "invalid_arguments"})
        return 3
    try:
        config = load_marker_control_config(production=True)
    except Exception:
        _emit({"status": "failed", "code": "marker_control_config_invalid"})
        return 3

    engine: AsyncEngine | Any | None = None
    redis_client: Any | None = None
    try:
        engine = create_database_engine(config.database_url)
        redis_client = create_redis_client(config.redis_url)
        session_factory = create_session_factory(engine)
        async with session_factory() as db:
            result = await run_worker_redis_marker_janitor(
                db,
                redis_client,
                uuid.uuid4(),
            )
    except Exception:
        _emit({"status": "failed", "code": "marker_cleanup_failed"})
        return 3
    finally:
        if redis_client is not None:
            try:
                await redis_client.aclose()
            except Exception:
                pass
        if engine is not None:
            try:
                await engine.dispose()
            except Exception:
                pass

    failed = result["conflict"] > 0
    _emit(
        {
            "status": "failed" if failed else "ok",
            "code": "marker_cleanup_conflict" if failed else "ready",
            **result,
        }
    )
    return 3 if failed else 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="worker-redis-marker-janitor",
        add_help=False,
        exit_on_error=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("run", add_help=False, exit_on_error=False)
    return parser


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
