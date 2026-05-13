from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.schemas.material import (
    MaterialClipListResponse,
    MaterialClipResponse,
    MaterialLibraryCreate,
    MaterialLibraryListResponse,
    MaterialLibraryResponse,
    MaterialSearchRequest,
    MaterialSearchResponse,
    MaterialSearchResultResponse,
)
from app.services.material_service import (
    MaterialLibraryConflictError,
    create_material_library,
    get_material_library,
    list_material_clips,
    list_material_libraries,
    materialize_material_search,
    preview_material_search,
)

router = APIRouter(prefix="/api/v1", tags=["materials"])


@router.get("/material-libraries", response_model=MaterialLibraryListResponse)
async def list_libraries(skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db)):
    items, total = await list_material_libraries(db, skip, limit)
    return MaterialLibraryListResponse(
        items=[MaterialLibraryResponse.model_validate(item) for item in items],
        total=total,
    )


@router.post("/material-libraries", response_model=MaterialLibraryResponse)
async def create_library(payload: MaterialLibraryCreate, db: AsyncSession = Depends(get_db)):
    try:
        item = await create_material_library(db, payload.name, payload.description)
    except MaterialLibraryConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return MaterialLibraryResponse.model_validate(item)


@router.get("/material-libraries/{library_id}", response_model=MaterialLibraryResponse)
async def get_library(library_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    item = await get_material_library(db, library_id)
    if not item:
        raise HTTPException(status_code=404, detail="Material library not found")
    return MaterialLibraryResponse.model_validate(item)


@router.get("/material-libraries/{library_id}/clips", response_model=MaterialClipListResponse)
async def get_library_clips(library_id: uuid.UUID, skip: int = 0, limit: int = 100, db: AsyncSession = Depends(get_db)):
    item = await get_material_library(db, library_id)
    if not item:
        raise HTTPException(status_code=404, detail="Material library not found")
    clips, total = await list_material_clips(db, library_id, skip, limit)
    return MaterialClipListResponse(
        items=[
            MaterialClipResponse(
                id=str(clip.id),
                library_id=str(clip.library_id),
                parent_material_item_id=str(clip.parent_material_item_id) if clip.parent_material_item_id else None,
                source_asset_id=str(clip.source_asset_id),
                clip_id=clip.clip_id,
                start_sec=clip.start_sec,
                end_sec=clip.end_sec,
                subtitle_text=clip.subtitle_text,
                ocr_text=clip.ocr_text,
                caption=clip.caption,
                neighbor_clip_ids=list(clip.neighbor_clip_ids or []),
                clip_kind=clip.clip_kind,
                storage_asset_id=str(clip.storage_asset_id) if clip.storage_asset_id else None,
                metadata=clip.metadata_json,
                created_at=clip.created_at,
            )
            for clip in clips
        ],
        total=total,
    )


@router.post("/material-search/preview", response_model=MaterialSearchResponse)
async def preview_search(payload: MaterialSearchRequest, db: AsyncSession = Depends(get_db)):
    query_row, results = await preview_material_search(db, payload)
    return MaterialSearchResponse(
        query_id=str(query_row.id),
        results=[
            MaterialSearchResultResponse(
                id=f"preview-{index}",
                title=f"Preview {index}",
                asset_id=None,
                source_asset_id=result["source_asset_id"],
                library_id=result["library_id"],
                start_sec=result["start_sec"],
                end_sec=result["end_sec"],
                subtitle_text=result["subtitle_text"],
                coarse_score=result["coarse_score"],
                lighthouse_score=result["lighthouse_score"],
                confidence=result["confidence"],
                metadata={
                    "member_clip_ids": result["member_clip_ids"],
                    "neighbor_clip_ids": result["neighbor_clip_ids"],
                },
            )
            for index, result in enumerate(results, start=1)
        ],
    )


@router.post("/material-search/materialize", response_model=MaterialSearchResponse)
async def materialize_search(payload: MaterialSearchRequest, db: AsyncSession = Depends(get_db)):
    query_row, results = await materialize_material_search(db, payload)
    return MaterialSearchResponse(
        query_id=str(query_row.id),
        results=[MaterialSearchResultResponse(**result) for result in results],
    )
