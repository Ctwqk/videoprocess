from __future__ import annotations
import asyncio
import json
import logging
import mimetypes
import uuid
from pathlib import Path
from fastapi import UploadFile
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.models.asset import Asset
from app.storage.manager import get_storage

logger = logging.getLogger(__name__)


async def _extract_media_info(local_path: str) -> dict | None:
    """Run ffprobe to extract media info (duration, resolution, codecs)."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_format", "-show_streams",
            local_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0:
            return None
        raw = json.loads(stdout.decode())
        fmt = raw.get("format", {})
        streams = raw.get("streams", [])
        video = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
        info: dict = {
            "duration": float(fmt.get("duration", 0)),
            "format_name": fmt.get("format_name"),
            "bit_rate": int(fmt.get("bit_rate", 0)),
        }
        if video:
            info["video"] = {
                "codec": video.get("codec_name"),
                "width": video.get("width"),
                "height": video.get("height"),
                "fps": video.get("r_frame_rate"),
            }
        if audio:
            info["audio"] = {
                "codec": audio.get("codec_name"),
                "sample_rate": audio.get("sample_rate"),
                "channels": audio.get("channels"),
            }
        return info
    except Exception:
        logger.exception("Failed to extract media info")
        return None


async def upload_asset(db: AsyncSession, file: UploadFile) -> Asset:
    storage = get_storage()

    original_name = file.filename or "unknown"
    ext = Path(original_name).suffix
    unique_name = f"{uuid.uuid4().hex}{ext}"
    storage_path = f"assets/{unique_name}"

    mime_type = file.content_type or mimetypes.guess_type(original_name)[0]
    file_size = await storage.save(storage_path, file.file)

    media_info = None
    local_path = storage.get_local_path(storage_path)
    if local_path:
        media_info = await _extract_media_info(local_path)

    asset = Asset(
        filename=unique_name,
        original_name=original_name,
        mime_type=mime_type,
        file_size=file_size,
        storage_backend=settings.storage_backend,
        storage_path=storage_path,
        media_info=media_info,
    )
    db.add(asset)
    await db.commit()
    await db.refresh(asset)
    return asset


async def create_asset_from_local_file(
    db: AsyncSession,
    local_path: str,
    *,
    original_name: str,
    mime_type: str | None = None,
    uploaded_by: str = "system",
) -> Asset:
    storage = get_storage()
    ext = Path(original_name).suffix or Path(local_path).suffix
    unique_name = f"{uuid.uuid4().hex}{ext}"
    storage_path = f"assets/{unique_name}"

    guessed_mime = mime_type or mimetypes.guess_type(original_name)[0]
    with open(local_path, "rb") as handle:
        file_size = await storage.save(storage_path, handle)

    media_info = None
    storage_local_path = storage.get_local_path(storage_path)
    if storage_local_path:
        media_info = await _extract_media_info(storage_local_path)

    asset = Asset(
        filename=unique_name,
        original_name=original_name,
        mime_type=guessed_mime,
        file_size=file_size,
        storage_backend=settings.storage_backend,
        storage_path=storage_path,
        media_info=media_info,
        uploaded_by=uploaded_by,
    )
    db.add(asset)
    await db.commit()
    await db.refresh(asset)
    return asset


async def get_asset(db: AsyncSession, asset_id: uuid.UUID) -> Asset | None:
    return await db.get(Asset, asset_id)


async def list_assets(db: AsyncSession, skip: int = 0, limit: int = 50) -> tuple[list[Asset], int]:
    total_stmt = select(func.count()).select_from(Asset)
    total = (await db.execute(total_stmt)).scalar() or 0

    stmt = select(Asset).order_by(Asset.uploaded_at.desc()).offset(skip).limit(limit)
    result = await db.execute(stmt)
    return list(result.scalars().all()), total


async def delete_asset(db: AsyncSession, asset_id: uuid.UUID) -> bool:
    asset = await db.get(Asset, asset_id)
    if not asset:
        return False

    storage = get_storage(asset.storage_backend)
    await storage.delete(asset.storage_path)
    await db.delete(asset)
    await db.commit()
    return True
