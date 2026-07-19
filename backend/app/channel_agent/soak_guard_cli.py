from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, Never

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.channelops_soak_guard import (
    ALLOWED_EXTERNAL_CONDITIONS,
    SOAK_GUARD_REASON,
    SoakGuardPolicy,
    assess_channelops_soak,
)


class _CLIUsageError(ValueError):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise _CLIUsageError(message)


def get_session_factory():
    from app.db import async_session

    return async_session


async def quarantine_channelops_backlog(
    db: AsyncSession,
    channel_id: uuid.UUID,
    *,
    apply: bool,
    reason: str,
    close_schedule: bool,
    now: datetime,
) -> dict[str, Any]:
    from app.services.channelops_quarantine import quarantine_channelops_backlog as quarantine

    return await quarantine(
        db,
        channel_id,
        apply=apply,
        reason=reason,
        close_schedule=close_schedule,
        now=now,
    )


async def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        policy = SoakGuardPolicy(
            channel_id=args.channel_id,
            started_at=args.started_at,
            max_publications_per_24h=args.max_publications_per_24h,
            upload_stale_minutes=args.upload_stale_minutes,
            feedback_grace_hours=args.feedback_grace_hours,
        )
    except (argparse.ArgumentError, _CLIUsageError, ValueError):
        _emit("invalid_arguments")
        return 2

    assessed_at = datetime.now(timezone.utc)
    try:
        session_factory = get_session_factory()
        async with session_factory() as db:
            assessment = await assess_channelops_soak(
                db,
                policy,
                external_conditions=tuple(args.external_condition),
                now=assessed_at,
            )
    except Exception:
        _emit("database_error")
        return 3

    payload: dict[str, Any] = {
        "status": "healthy" if assessment.healthy else "critical",
        "critical_codes": list(assessment.critical_codes),
        "metrics": dict(assessment.metrics),
    }
    if assessment.healthy:
        _emit_payload(payload)
        return 0

    if args.apply:
        try:
            async with session_factory() as db:
                quarantine = await quarantine_channelops_backlog(
                    db,
                    policy.channel_id,
                    apply=True,
                    reason=SOAK_GUARD_REASON,
                    close_schedule=True,
                    now=assessed_at,
                )
        except Exception:
            _emit("quarantine_error")
            return 3
        payload["status"] = "quarantined"
        payload["quarantine_counts"] = quarantine["counts"]

    _emit_payload(payload)
    return 20


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(prog="channelops-soak-guard", exit_on_error=False)
    parser.add_argument("--channel-id", required=True, type=_uuid)
    parser.add_argument("--started-at", required=True, type=_rfc3339)
    parser.add_argument(
        "--max-publications-per-24h",
        type=_positive_integer,
        default=1,
    )
    parser.add_argument("--upload-stale-minutes", type=_positive_integer, default=45)
    parser.add_argument("--feedback-grace-hours", type=_positive_integer, default=30)
    parser.add_argument(
        "--external-condition",
        action="append",
        choices=sorted(ALLOWED_EXTERNAL_CONDITIONS),
        default=[],
    )
    parser.add_argument("--apply", action="store_true")
    return parser


def _uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a UUID") from exc


def _rfc3339(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an RFC3339 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("must include a timezone")
    return parsed.astimezone(timezone.utc)


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _emit(status: str) -> None:
    _emit_payload({"status": status, "critical_codes": [], "metrics": {}})


def _emit_payload(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
