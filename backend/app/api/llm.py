from fastapi import APIRouter, HTTPException, Query
import httpx

from app.config import settings


router = APIRouter(prefix="/api/v1/llm", tags=["llm"])


@router.get("/provider-models")
async def get_provider_models(
    provider_config_id: str = Query(..., min_length=1),
):
    watchdog_base = settings.exo_watchdog_url.rstrip("/")
    url = f"{watchdog_base}/router/provider-models"

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                url,
                params={"provider_config_id": provider_config_id},
            )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to load provider models from watchdog: {exc}",
        ) from exc

    payload = response.json()
    return {
        "provider_config_id": provider_config_id,
        "blank_label": payload.get("blank_label", "Select provider model"),
        "models": [str(model) for model in payload.get("models", [])],
        "source": payload.get("source"),
        "ok": payload.get("ok"),
    }
