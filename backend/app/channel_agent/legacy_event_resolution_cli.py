from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Never

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.services.legacy_worker_event_resolution import (
    LegacyEventResolutionError,
    LegacyEventResolutionReport,
    LegacyEventResolutionRequest,
    parse_expected_events,
    resolve_legacy_worker_events,
)


class _CLIUsageError(ValueError):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise _CLIUsageError(message)


def create_database_engine(database_url: str) -> AsyncEngine:
    from sqlalchemy.ext.asyncio import create_async_engine

    return create_async_engine(database_url, pool_pre_ping=True)


def create_redis_client(redis_url: str) -> Any:
    import redis.asyncio as aioredis

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


async def execute_request(
    database_url: str,
    redis_url: str,
    request: LegacyEventResolutionRequest,
) -> LegacyEventResolutionReport:
    engine = create_database_engine(database_url)
    redis_client = None
    try:
        redis_client = create_redis_client(redis_url)
        session_factory = create_session_factory(engine)
        async with session_factory() as db:
            return await resolve_legacy_worker_events(db, redis_client, request)
    finally:
        try:
            if redis_client is not None:
                await redis_client.aclose()
        finally:
            await engine.dispose()


async def run(argv: Sequence[str] | None = None) -> int:
    started_at = _utc_now()
    try:
        args = _parser().parse_args(argv)
        expected_events = parse_expected_events(tuple(args.expected_event))
        operator_id = (args.operator or "").strip()
        if args.apply and not operator_id:
            raise _CLIUsageError("--operator is required with --apply")
        if len(operator_id) > 255:
            raise _CLIUsageError("--operator is too long")
        operator_id = operator_id or "dry-run"
    except (argparse.ArgumentError, _CLIUsageError, LegacyEventResolutionError):
        return 2

    evidence_path = Path(args.evidence)
    mode = "apply" if args.apply else "dry_run"
    evidence: dict[str, Any] = {
        "mode": mode,
        "status": "running",
        "started_at": started_at.isoformat(),
    }
    database_url = os.environ.get("DATABASE_URL", "")
    redis_url = os.environ.get("REDIS_URL", "")
    if not database_url or not redis_url:
        evidence.update(
            {
                "status": "failed",
                "failure": {
                    "type": "ConfigurationError",
                    "message": "DATABASE_URL and REDIS_URL are required",
                },
                "completed_at": _utc_now().isoformat(),
            }
        )
        return _write_evidence_or_error(evidence_path, evidence, failure_code=3)

    request = LegacyEventResolutionRequest(
        expected_events=expected_events,
        operator_id=operator_id,
        apply=args.apply,
    )
    try:
        report = await execute_request(database_url, redis_url, request)
    except LegacyEventResolutionError as exc:
        evidence.update(
            {
                "status": "failed",
                "failure": {
                    "type": type(exc).__name__,
                    "message": str(exc),
                },
                "completed_at": _utc_now().isoformat(),
            }
        )
        return _write_evidence_or_error(evidence_path, evidence, failure_code=3)
    except Exception as exc:
        evidence.update(
            {
                "status": "failed",
                "failure": {
                    "type": type(exc).__name__,
                    "message": "internal dependency failure",
                },
                "completed_at": _utc_now().isoformat(),
            }
        )
        return _write_evidence_or_error(evidence_path, evidence, failure_code=3)

    evidence.update(_report_payload(report))
    evidence["status"] = "succeeded"
    evidence["completed_at"] = _utc_now().isoformat()
    return _write_evidence_or_error(evidence_path, evidence, failure_code=0)


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="legacy-worker-event-resolution",
        exit_on_error=False,
    )
    parser.add_argument(
        "--expected-event",
        action="append",
        required=True,
        help="exact Redis message-id=payload-sha256 expectation",
    )
    parser.add_argument("--operator")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--evidence", required=True)
    return parser


def _report_payload(report: LegacyEventResolutionReport) -> dict[str, Any]:
    return {
        "applied": report.applied,
        "candidates": [
            {
                "message_id": candidate.message_id,
                "payload_sha256": candidate.payload_sha256,
                "job_id": str(candidate.job_id),
                "node_execution_id": str(candidate.node_execution_id),
            }
            for candidate in report.candidates
        ],
        "resolution_ids": [
            str(resolution_id) for resolution_id in report.resolution_ids
        ],
        "xack_count": report.xack_count,
        "final_pending": report.final_pending,
    }


def _write_evidence_or_error(
    path: Path,
    payload: dict[str, Any],
    *,
    failure_code: int,
) -> int:
    try:
        _write_evidence(path, payload)
    except OSError:
        return 4
    return failure_code


def _write_evidence(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        path.chmod(0o600)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        temporary_path.unlink(missing_ok=True)
        raise


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main())
