#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.services.channelops_quarantine import (  # noqa: E402
    UnknownChannelError,
    quarantine_channelops_backlog,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run or apply a channel-specific ChannelOps quarantine")
    parser.add_argument("--channel-id", required=True, type=uuid.UUID)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--apply", action="store_true", default=False)
    return parser.parse_args()


def async_database_url(value: str) -> str:
    if value.startswith("postgresql+asyncpg://"):
        return value
    if value.startswith("postgresql://"):
        return value.replace("postgresql://", "postgresql+asyncpg://", 1)
    if value.startswith("postgres://"):
        return value.replace("postgres://", "postgresql+asyncpg://", 1)
    return value


def atomic_write_json(path: Path, payload: dict) -> None:
    created_parent = False
    if not path.parent.exists():
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=False)
            created_parent = True
        except FileExistsError:
            pass
    if created_parent:
        os.chmod(path.parent, 0o700)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        os.chmod(path, 0o600)
    except BaseException:
        Path(temporary_name).unlink(missing_ok=True)
        raise


async def run(args: argparse.Namespace, database_url: str) -> dict:
    engine = create_async_engine(async_database_url(database_url), pool_pre_ping=True)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            return await quarantine_channelops_backlog(
                session,
                args.channel_id,
                apply=args.apply,
            )
    finally:
        await engine.dispose()


def main() -> int:
    args = parse_args()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 2
    try:
        report = asyncio.run(run(args, database_url))
    except UnknownChannelError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"quarantine failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    atomic_write_json(args.evidence, report)
    print(
        f"quarantine {'applied' if args.apply else 'dry-run'}: "
        f"channel={report['channel_id']} evidence={args.evidence}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
