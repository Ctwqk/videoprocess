from __future__ import annotations

QUEUE_QUEUED = "queued"
QUEUE_RUNNING = "running"
QUEUE_SUCCEEDED = "succeeded"
QUEUE_FAILED = "failed"
QUEUE_HELD = "held"
QUEUE_CANCELLED = "cancelled"
QUEUE_DEAD_LETTERED = "dead_lettered"

TASK_SEEDED = "seeded"
TASK_SELECTED = "selected"
TASK_PLANNING = "planning"
TASK_PRODUCING = "producing"
TASK_UPLOADED_PRIVATE = "uploaded_private"
TASK_SCHEDULED = "scheduled"
TASK_PUBLISHED = "published"
TASK_MEASURED = "measured"
TASK_HELD = "held"
TASK_FAILED = "failed"
TASK_CANCELLED = "cancelled"
TASK_REJECTED = "rejected"

ACTIVE_TASK_STATES = {
    TASK_SELECTED,
    TASK_PLANNING,
    TASK_PRODUCING,
    TASK_UPLOADED_PRIVATE,
    TASK_HELD,
    TASK_SCHEDULED,
}
TERMINAL_TASK_STATES = {
    TASK_FAILED,
    TASK_REJECTED,
    TASK_CANCELLED,
    TASK_PUBLISHED,
    TASK_MEASURED,
}
UPLOAD_FAILURE_KEYWORDS = {
    "upload",
    "publish",
    "youtube",
    "quota",
    "oauth",
    "video_id",
    "thumbnail",
}

ALERT_CONSECUTIVE_UPLOAD_FAILURE = "consecutive_upload_failure"
ALERT_TOKEN_EXPIRING = "token_expiring_24h"
ALERT_QUOTA_LOW = "quota_below_20pct"
ALERT_TAKEDOWN = "takedown_event_logged"
ALERT_MATERIAL_SUPPLY_LOW = "material_supply_low"
