from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel


class AssetResponse(BaseModel):
    id: str
    filename: str
    original_name: str
    mime_type: str | None = None
    file_size: int | None = None
    media_info: dict | None = None
    uploaded_at: datetime

    model_config = {"from_attributes": True}


class AssetListResponse(BaseModel):
    items: list[AssetResponse]
    total: int
