from __future__ import annotations

from fastapi.responses import FileResponse, StreamingResponse

from app.storage.base import StorageBackend


async def build_download_response(
    *,
    storage: StorageBackend,
    storage_path: str,
    filename: str,
    media_type: str | None,
):
    local_path = storage.get_local_path(storage_path)

    if local_path:
        return FileResponse(
            path=local_path,
            filename=filename,
            media_type=media_type or "application/octet-stream",
        )

    content = await storage.read(storage_path)
    return StreamingResponse(
        iter([content]),
        media_type=media_type or "application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
