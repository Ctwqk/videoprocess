from __future__ import annotations

import asyncio
import json
import os
import re
import stat
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.artifact import Artifact, IntermediateArtifactCache


STAGING_PREFIX = "staging/artifacts/"
STAGING_GRACE_SECONDS = 24 * 60 * 60
_CLAIM_STAGING_PATH = re.compile(
    r"^staging/artifacts/"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}/"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}-[0-9a-f]{16}\.[A-Za-z0-9]+$"
)


class StagingObjectJanitor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        client: Any,
        bucket: str,
        status_file: Path,
        grace_seconds: int = STAGING_GRACE_SECONDS,
    ) -> None:
        if grace_seconds < STAGING_GRACE_SECONDS:
            raise ValueError("staging grace is below the reviewed minimum")
        self._session_factory = session_factory
        self._client = client
        self._bucket = bucket
        self._status_file = status_file
        self._grace_seconds = grace_seconds

    async def run_once(
        self,
        *,
        now: datetime | None = None,
    ) -> dict[str, int]:
        checked_at = _utc(now or datetime.now(timezone.utc))
        async with self._session_factory() as db:
            protected = set(
                (
                    await db.execute(
                        select(Artifact.storage_path).where(
                            Artifact.storage_path.like(
                                f"{STAGING_PREFIX}%"
                            )
                        )
                    )
                ).scalars()
            )
            cached_paths = (
                await db.execute(
                    select(
                        IntermediateArtifactCache.storage_path
                    ).where(
                        IntermediateArtifactCache.storage_path.like(
                            f"{STAGING_PREFIX}%"
                        )
                    )
                )
            ).scalars()
            protected.update(
                path for path in cached_paths if path is not None
            )
        objects = await asyncio.to_thread(
            lambda: list(
                self._client.list_objects(
                    self._bucket,
                    prefix=STAGING_PREFIX,
                    recursive=True,
                )
            )
        )
        result = {
            "scanned": 0,
            "deleted": 0,
            "protected": 0,
            "too_young": 0,
            "invalid": 0,
            "errors": 0,
        }
        for item in objects:
            result["scanned"] += 1
            path = getattr(item, "object_name", None)
            last_modified = getattr(item, "last_modified", None)
            if (
                not isinstance(path, str)
                or _CLAIM_STAGING_PATH.fullmatch(path) is None
                or not isinstance(last_modified, datetime)
            ):
                result["invalid"] += 1
                continue
            if path in protected:
                result["protected"] += 1
                continue
            age = (checked_at - _utc(last_modified)).total_seconds()
            if age <= self._grace_seconds:
                result["too_young"] += 1
                continue
            try:
                await asyncio.to_thread(
                    self._client.remove_object,
                    self._bucket,
                    path,
                )
            except Exception:
                result["errors"] += 1
            else:
                result["deleted"] += 1
        self._write_status(checked_at, result)
        return result

    def _write_status(
        self,
        checked_at: datetime,
        result: dict[str, int],
    ) -> None:
        payload = {
            "schema_version": 1,
            "checked_at": checked_at.isoformat(),
            "grace_seconds": self._grace_seconds,
            **result,
        }
        parent = self._status_file.parent
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(parent, 0o700, follow_symlinks=False)
        parent_metadata = parent.lstat()
        if (
            not stat.S_ISDIR(parent_metadata.st_mode)
            or stat.S_IMODE(parent_metadata.st_mode) != 0o700
            or parent_metadata.st_uid != os.geteuid()
        ):
            raise RuntimeError("janitor evidence directory is invalid")
        _require_replaceable_status_file(self._status_file)

        directory_descriptor = os.open(
            parent,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC,
        )
        temporary_path: str | None = None
        try:
            descriptor, temporary_path = tempfile.mkstemp(
                dir=parent,
                prefix=f".{self._status_file.name}.",
                suffix=".tmp",
            )
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            _require_replaceable_status_file(self._status_file)
            os.replace(temporary_path, self._status_file)
            temporary_path = None
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
            if temporary_path is not None:
                try:
                    os.unlink(temporary_path)
                except FileNotFoundError:
                    pass


def staging_janitor_ready(
    status_file: Path,
    *,
    now: datetime | None = None,
    max_age_seconds: int,
) -> bool:
    if max_age_seconds <= 0:
        return False
    try:
        payload = json.loads(status_file.read_text(encoding="utf-8"))
        checked_at = datetime.fromisoformat(payload["checked_at"])
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        return False
    checked = _utc(checked_at)
    current = _utc(now or datetime.now(timezone.utc))
    return (
        payload.get("schema_version") == 1
        and payload.get("grace_seconds") == STAGING_GRACE_SECONDS
        and payload.get("errors") == 0
        and 0 <= (current - checked).total_seconds() <= max_age_seconds
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _require_replaceable_status_file(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_uid != os.geteuid()
    ):
        raise RuntimeError("janitor evidence file is invalid")
