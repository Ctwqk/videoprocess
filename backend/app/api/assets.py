from __future__ import annotations
import uuid
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.downloads import build_download_response
from app.db import get_db
from app.schemas.asset import AssetResponse, AssetListResponse
from app.services.asset_service import upload_asset, get_asset, list_assets, delete_asset
from app.storage.manager import get_storage

router = APIRouter(prefix="/api/v1/assets", tags=["assets"])


@router.post("/upload", response_model=AssetResponse)
async def upload(file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    asset = await upload_asset(db, file)
    return AssetResponse(
        id=str(asset.id),
        filename=asset.filename,
        original_name=asset.original_name,
        mime_type=asset.mime_type,
        file_size=asset.file_size,
        media_info=asset.media_info,
        uploaded_at=asset.uploaded_at,
    )


@router.get("", response_model=AssetListResponse)
async def list_all(skip: int = 0, limit: int = 50, db: AsyncSession = Depends(get_db)):
    items, total = await list_assets(db, skip, limit)
    return AssetListResponse(
        items=[
            AssetResponse(
                id=str(a.id),
                filename=a.filename,
                original_name=a.original_name,
                mime_type=a.mime_type,
                file_size=a.file_size,
                media_info=a.media_info,
                uploaded_at=a.uploaded_at,
            )
            for a in items
        ],
        total=total,
    )


@router.get("/{asset_id}", response_model=AssetResponse)
async def get_one(asset_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    asset = await get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    return AssetResponse(
        id=str(asset.id),
        filename=asset.filename,
        original_name=asset.original_name,
        mime_type=asset.mime_type,
        file_size=asset.file_size,
        media_info=asset.media_info,
        uploaded_at=asset.uploaded_at,
    )


@router.get("/{asset_id}/download")
async def download(asset_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    asset = await get_asset(db, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")

    storage = get_storage(asset.storage_backend)
    return await build_download_response(
        storage=storage,
        storage_path=asset.storage_path,
        filename=asset.original_name,
        media_type=asset.mime_type,
    )


@router.delete("/{asset_id}")
async def delete(asset_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    deleted = await delete_asset(db, asset_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Asset not found")
    return {"status": "deleted"}
