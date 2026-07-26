from __future__ import annotations

import json
import uuid
from collections.abc import Mapping

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.services.staging_object_janitor import STAGING_GRACE_SECONDS


STAGING_JANITOR_MAX_AGE_SECONDS = 15 * 60
STAGING_JANITOR_STALE_RUN_SECONDS = 10 * 60
_RESULT_KEYS = {
    "scanned",
    "deleted",
    "protected",
    "too_young",
    "invalid",
    "errors",
}
_BEGIN_OUTCOMES = {"started", "recovered_stale", "overlap"}
_READINESS_OUTCOMES = {
    "ready",
    "missing",
    "stale_success",
    "latest_error",
    "active_stale",
}


class StagingJanitorStatusError(RuntimeError):
    """The durable janitor status contract returned an invalid result."""


class StagingJanitorStatusStore:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def begin(
        self,
        run_id: uuid.UUID,
        *,
        runner_id: str,
    ) -> str:
        if (
            not isinstance(run_id, uuid.UUID)
            or not isinstance(runner_id, str)
            or not runner_id
            or runner_id != runner_id.strip()
            or len(runner_id) > 255
        ):
            raise StagingJanitorStatusError(
                "janitor begin identity is invalid"
            )
        async with self._session_factory() as db:
            async with db.begin():
                outcome = await db.scalar(
                    text(
                        """
                        SELECT public.vp_begin_staging_janitor_run(
                            :run_id,
                            :runner_id,
                            :stale_run_seconds
                        )
                        """
                    ),
                    {
                        "run_id": run_id,
                        "runner_id": runner_id,
                        "stale_run_seconds": (
                            STAGING_JANITOR_STALE_RUN_SECONDS
                        ),
                    },
                )
        if outcome not in _BEGIN_OUTCOMES:
            raise StagingJanitorStatusError(
                "janitor begin result is invalid"
            )
        return outcome

    async def finish(
        self,
        run_id: uuid.UUID,
        *,
        result: Mapping[str, int],
        succeeded: bool,
    ) -> bool:
        if (
            not isinstance(run_id, uuid.UUID)
            or not isinstance(succeeded, bool)
            or set(result) != _RESULT_KEYS
            or any(
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                for value in result.values()
            )
        ):
            raise StagingJanitorStatusError(
                "janitor finish result is invalid"
            )
        canonical_result = {
            "schema_version": 1,
            "grace_seconds": STAGING_GRACE_SECONDS,
            **result,
        }
        async with self._session_factory() as db:
            async with db.begin():
                effective_success = await db.scalar(
                    text(
                        """
                        SELECT public.vp_finish_staging_janitor_run(
                            :run_id,
                            CAST(:result_json AS jsonb),
                            :succeeded
                        )
                        """
                    ),
                    {
                        "run_id": run_id,
                        "result_json": json.dumps(
                            canonical_result,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        "succeeded": succeeded,
                    },
                )
        if not isinstance(effective_success, bool):
            raise StagingJanitorStatusError(
                "janitor finish result is invalid"
            )
        return effective_success

    async def readiness(
        self,
        *,
        max_age_seconds: int = STAGING_JANITOR_MAX_AGE_SECONDS,
        stale_run_seconds: int = STAGING_JANITOR_STALE_RUN_SECONDS,
    ) -> str:
        async with self._session_factory() as db:
            async with db.begin():
                outcome = await db.scalar(
                    text(
                        """
                        SELECT public.vp_staging_janitor_readiness(
                            :max_age_seconds,
                            :stale_run_seconds
                        )
                        """
                    ),
                    {
                        "max_age_seconds": max_age_seconds,
                        "stale_run_seconds": stale_run_seconds,
                    },
                )
        if outcome not in _READINESS_OUTCOMES:
            raise StagingJanitorStatusError(
                "janitor readiness result is invalid"
            )
        return outcome
