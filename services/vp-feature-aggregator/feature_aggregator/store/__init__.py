from __future__ import annotations

from typing import Protocol

from feature_aggregator.schemas import FeatureResponse, PDSDecisionEvent, VPActorActionEvent


class FeatureStore(Protocol):
    async def apply_vp_action(self, event: VPActorActionEvent) -> bool:
        ...

    async def apply_pds_decision(self, event: PDSDecisionEvent) -> bool:
        ...

    async def features_for(self, actor_id: str) -> FeatureResponse:
        ...


def dedupe_key(topic_version: str, event_id: str) -> str:
    return f"{topic_version}:{event_id}"
