from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class MaterialLibraryCreate(BaseModel):
    name: str
    description: str = ""


class MaterialLibraryResponse(BaseModel):
    id: uuid.UUID
    name: str
    description: str
    is_disabled: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class MaterialLibraryListResponse(BaseModel):
    items: list[MaterialLibraryResponse]
    total: int


class MaterialClipResponse(BaseModel):
    id: uuid.UUID
    library_id: uuid.UUID
    parent_material_item_id: uuid.UUID | None = None
    source_asset_id: uuid.UUID
    clip_id: str
    start_sec: float
    end_sec: float
    subtitle_text: str
    ocr_text: str | None = None
    caption: str | None = None
    neighbor_clip_ids: list[str] = Field(default_factory=list)
    clip_kind: str
    storage_asset_id: uuid.UUID | None = None
    metadata: dict[str, Any] | None = None
    created_at: datetime


class MaterialClipListResponse(BaseModel):
    items: list[MaterialClipResponse]
    total: int


class MaterialSearchRequest(BaseModel):
    query: str
    source_library_ids: list[str]
    result_library_ids: list[str] = Field(default_factory=list)
    top_k: int = 50
    merge_gap: float = 5.0
    expand_left: float = 4.0
    expand_right: float = 4.0
    rerank_top_m: int = 8
    min_duration: float = 1.5
    max_duration: float = 20.0
    dedupe_overlap_threshold: float = 0.6


class MaterialSearchResultResponse(BaseModel):
    id: str
    title: str
    asset_id: uuid.UUID | None = None
    source_asset_id: uuid.UUID
    library_id: uuid.UUID
    start_sec: float
    end_sec: float
    subtitle_text: str = ""
    coarse_score: float | None = None
    lighthouse_score: float | None = None
    confidence: float | None = None
    metadata: dict[str, Any] | None = None


class MaterialSearchResponse(BaseModel):
    query_id: uuid.UUID
    results: list[MaterialSearchResultResponse]
