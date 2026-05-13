from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel


class ArtifactResponse(BaseModel):
    id: str
    job_id: str
    node_execution_id: str
    kind: str
    filename: str
    mime_type: str | None = None
    file_size: int | None = None
    created_at: datetime

    model_config = {"from_attributes": True}
