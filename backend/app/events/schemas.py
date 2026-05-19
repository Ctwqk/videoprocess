from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any


TOPIC_VP_ACTIONS = "vp.actor.actions.v1"
SOURCE_CHANNEL_OPS = "videoprocess.channel_ops"


def build_actor_action_event(
    *,
    actor_id: str,
    action_type: str,
    platform: str = "",
    metadata: dict[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    occurred_at = occurred_at or datetime.now(timezone.utc)
    return {
        "event_id": str(uuid.uuid4()),
        "topic_version": TOPIC_VP_ACTIONS,
        "actor_id": actor_id,
        "action_type": action_type,
        "platform": platform,
        "occurred_at": occurred_at.isoformat(),
        "source": SOURCE_CHANNEL_OPS,
        "metadata": dict(metadata or {}),
    }
