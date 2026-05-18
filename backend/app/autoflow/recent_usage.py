from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.autoflow import AutoFlowUsedClip
from app.schemas.autoflow import AutoFlowClipCandidate


class RecentClipUsageStore:
    def __init__(self, *, now: Callable[[], datetime] | None = None, lookback_days: int = 7) -> None:
        self._now = now or (lambda: datetime.now(timezone.utc))
        self.lookback_days = lookback_days

    async def load_recent_asset_ids(self, db: AsyncSession) -> set[str]:
        threshold = self._now() - timedelta(days=self.lookback_days)
        result = await db.execute(
            select(AutoFlowUsedClip.asset_id).where(AutoFlowUsedClip.selected_at >= threshold)
        )
        return {asset_id for asset_id in result.scalars().all() if asset_id}

    async def record_selected_clips(
        self,
        db: AsyncSession,
        *,
        run_id: str,
        candidates: Iterable[AutoFlowClipCandidate],
    ) -> None:
        run_uuid = uuid.UUID(run_id)
        selected_at = self._now()
        seen: set[str] = set()
        for candidate in candidates:
            if not candidate.asset_id or candidate.asset_id in seen:
                continue
            seen.add(candidate.asset_id)
            db.add(
                AutoFlowUsedClip(
                    run_id=run_uuid,
                    asset_id=candidate.asset_id,
                    source_platform=_source_platform(candidate),
                    candidate_title=candidate.title,
                    selected_at=selected_at,
                    metadata_json=_candidate_metadata(candidate),
                )
            )
        await db.flush()


def _source_platform(candidate: AutoFlowClipCandidate) -> str | None:
    metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
    value = metadata.get("source_platform") or metadata.get("platform") or candidate.source_type
    return str(value) if value else None


def _candidate_metadata(candidate: AutoFlowClipCandidate) -> dict[str, Any]:
    return {
        "candidate_id": candidate.id,
        "source_type": candidate.source_type,
        "score": candidate.score,
        "score_breakdown": dict(candidate.score_breakdown),
        "start_sec": candidate.start_sec,
        "end_sec": candidate.end_sec,
    }
