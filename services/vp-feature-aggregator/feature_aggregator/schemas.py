from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class FeatureResponse(BaseModel):
    actor_id: str
    publishes_5m: int = 0
    publishes_1h: int = 0
    publishes_24h: int = 0
    blocks_24h: int = 0
    flags_7d: int = 0
    comment_burst_1m: int = 0
    as_of: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    from_cache: bool = False


class VPActorActionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    topic_version: Literal["vp.actor.actions.v1"]
    actor_id: str = Field(min_length=1)
    action_type: Literal[
        "candidate_accepted",
        "candidate_blocked",
        "candidate_flagged",
        "post_comment",
        "publication_promotion_attempted",
        "publication_promotion_blocked",
        "publication_scheduled",
    ]
    platform: str = ""
    occurred_at: datetime
    source: Literal["videoprocess.channel_ops"]
    metadata: dict[str, Any] = Field(default_factory=dict)


class PDSDecisionReason(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    rule: str = ""
    detail: str = ""


class PDSDecisionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    topic_version: Literal["pds.decisions.v1"]
    actor_id: str = Field(min_length=1)
    action_type: str = Field(min_length=1)
    platform: str = ""
    verdict: Literal["allow", "flag", "block"]
    score: float
    reasons: list[PDSDecisionReason] = Field(default_factory=list)
    decision_id: str = Field(min_length=1)
    client: str = ""
    occurred_at: datetime
