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
    audit_worker_redis_markers,
    load_marker_control_config,
    promote_prepared_worker_event,
    restore_worker_redis_marker,
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
        args = _parser().parse_args(argv)
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
            if args.command == "audit":
                audits = await audit_worker_redis_markers(db, redis_client)
                payload = {"status": "ok", "count": len(audits), "markers": audits}
                exit_code = 0
            elif args.command == "restore-marker":
                code = await restore_worker_redis_marker(
                    db,
                    redis_client,
                    args.source_id,
                    apply=args.apply,
                )
                payload = {
                    "status": "ok" if code in {"dry_run_ready", "restore_applied"} else "failed",
                    "code": code,
                }
                exit_code = 0 if payload["status"] == "ok" else 3
            else:
                code = await promote_prepared_worker_event(
                    db,
                    redis_client,
                    args.emission_id,
                    apply=args.apply,
                )
                payload = {
                    "status": "ok" if code in {"dry_run_ready", "promotion_applied"} else "failed",
                    "code": code,
                }
                exit_code = 0 if payload["status"] == "ok" else 3
    except Exception:
        _emit({"status": "failed", "code": "marker_repair_failed"})
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

    _emit(payload)
    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="worker-redis-marker-repair",
        add_help=False,
        exit_on_error=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("audit", add_help=False, exit_on_error=False)
    restore = subparsers.add_parser(
        "restore-marker",
        add_help=False,
        exit_on_error=False,
    )
    restore.add_argument("--source-id", required=True, type=uuid.UUID)
    restore.add_argument("--apply", action="store_true")
    promote = subparsers.add_parser(
        "promote-prepared",
        add_help=False,
        exit_on_error=False,
    )
    promote.add_argument("--emission-id", required=True, type=uuid.UUID)
    promote.add_argument("--apply", action="store_true")
    return parser


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
