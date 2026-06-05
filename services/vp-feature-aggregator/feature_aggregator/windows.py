from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from feature_aggregator.schemas import FeatureResponse, PDSDecisionEvent, VPActorActionEvent


PUBLISH_ACTIONS = {"candidate_accepted", "publication_scheduled"}
COMMENT_ACTIONS = {"post_comment"}


@dataclass
class ActorCounters:
    publishes: list[datetime] = field(default_factory=list)
    blocks: list[datetime] = field(default_factory=list)
    flags: list[datetime] = field(default_factory=list)
    comments: list[datetime] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.publishes and not self.blocks and not self.flags and not self.comments


class WindowAggregator:
    def __init__(self, *, now: Callable[[], datetime] | None = None) -> None:
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._actors: dict[str, ActorCounters] = defaultdict(ActorCounters)

    def apply_vp_action(self, event: VPActorActionEvent) -> None:
        now = self._normalize_now()
        if event.action_type in PUBLISH_ACTIONS:
            counters = self._actors[event.actor_id]
            counters.publishes.append(_to_utc(event.occurred_at))
            self._prune_actor(event.actor_id, counters, now)
        elif event.action_type in COMMENT_ACTIONS:
            counters = self._actors[event.actor_id]
            counters.comments.append(_to_utc(event.occurred_at))
            self._prune_actor(event.actor_id, counters, now)
        self._prune_all(now)

    def apply_pds_decision(self, event: PDSDecisionEvent) -> None:
        now = self._normalize_now()
        if event.verdict == "block":
            counters = self._actors[event.actor_id]
            counters.blocks.append(_to_utc(event.occurred_at))
            self._prune_actor(event.actor_id, counters, now)
        elif event.verdict == "flag":
            counters = self._actors[event.actor_id]
            counters.flags.append(_to_utc(event.occurred_at))
            self._prune_actor(event.actor_id, counters, now)
        self._prune_all(now)

    def features_for(self, actor_id: str) -> FeatureResponse:
        now = self._normalize_now()
        counters = self._actors.get(actor_id)
        if counters is None:
            counters = ActorCounters()
        return FeatureResponse(
            actor_id=actor_id,
            publishes_5m=_count_since(counters.publishes, now - timedelta(minutes=5)),
            publishes_1h=_count_since(counters.publishes, now - timedelta(hours=1)),
            publishes_24h=_count_since(counters.publishes, now - timedelta(hours=24)),
            blocks_24h=_count_since(counters.blocks, now - timedelta(hours=24)),
            flags_7d=_count_since(counters.flags, now - timedelta(days=7)),
            comment_burst_1m=_count_since(counters.comments, now - timedelta(minutes=1)),
            as_of=now,
            from_cache=False,
        )

    def _normalize_now(self) -> datetime:
        return _to_utc(self._now())

    def _prune_actor(self, actor_id: str, counters: ActorCounters, now: datetime) -> None:
        _prune(counters.publishes, now - timedelta(hours=24))
        _prune(counters.blocks, now - timedelta(hours=24))
        _prune(counters.flags, now - timedelta(days=7))
        _prune(counters.comments, now - timedelta(minutes=1))
        if counters.is_empty():
            self._actors.pop(actor_id, None)

    def _prune_all(self, now: datetime) -> None:
        for actor_id, counters in list(self._actors.items()):
            self._prune_actor(actor_id, counters, now)


def _count_since(values: list[datetime], since: datetime) -> int:
    return sum(1 for value in values if value >= since)


def _prune(values: list[datetime], since: datetime) -> None:
    values[:] = [value for value in values if value >= since]


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
