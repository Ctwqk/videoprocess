from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from feature_aggregator.schemas import FeatureResponse, PDSDecisionEvent, VPActorActionEvent
from feature_aggregator.store import dedupe_key
from feature_aggregator.windows import WindowAggregator


DEFAULT_DEDUPE_TTL = timedelta(days=7)


class MemoryFeatureStore:
    def __init__(
        self,
        aggregator: WindowAggregator | None = None,
        *,
        now: Callable[[], datetime] | None = None,
        dedupe_ttl: timedelta = DEFAULT_DEDUPE_TTL,
    ) -> None:
        self._now = now or (lambda: datetime.now(timezone.utc))
        self.dedupe_ttl = dedupe_ttl
        self.aggregator = aggregator or WindowAggregator(now=self._now)
        self.seen_event_ids: dict[str, datetime] = {}

    async def apply_vp_action(self, event: VPActorActionEvent) -> bool:
        key = dedupe_key(event.topic_version, event.event_id)
        if self._is_duplicate(key):
            return False
        self.seen_event_ids[key] = self._normalize_now()
        self.aggregator.apply_vp_action(event)
        return True

    async def apply_pds_decision(self, event: PDSDecisionEvent) -> bool:
        key = dedupe_key(event.topic_version, event.event_id)
        if self._is_duplicate(key):
            return False
        self.seen_event_ids[key] = self._normalize_now()
        self.aggregator.apply_pds_decision(event)
        return True

    async def features_for(self, actor_id: str) -> FeatureResponse:
        return self.aggregator.features_for(actor_id)

    def _is_duplicate(self, key: str) -> bool:
        now = self._normalize_now()
        self._prune_seen(now)
        return key in self.seen_event_ids

    def _prune_seen(self, now: datetime) -> None:
        cutoff = now - self.dedupe_ttl
        self.seen_event_ids = {
            key: seen_at for key, seen_at in self.seen_event_ids.items() if seen_at >= cutoff
        }

    def _normalize_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
