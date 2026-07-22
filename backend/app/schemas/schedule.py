from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class VideoScheduleStatusResponse(BaseModel):
    service_name: str
    state: str
    guarded_job_id: str | None = None
    waiting_jobs: int
    active_jobs: int
    queued_nodes: int
    running_nodes: int
    updated_at: datetime | None
    updated_by: str | None
    released_jobs: int = 0
