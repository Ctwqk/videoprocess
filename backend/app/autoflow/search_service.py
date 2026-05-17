from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowRequest
from app.schemas.material import MaterialSearchRequest
from app.services import material_service


logger = logging.getLogger(__name__)


class SearchService:
    async def search_material(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        db: AsyncSession | None = None,
        max_results: int = 8,
    ) -> list[AutoFlowClipCandidate]:
        if db is None or not request.material_library_ids:
            return []

        source_library_ids = _valid_uuid_strings(request.material_library_ids)
        if not source_library_ids:
            logger.warning("AutoFlow material search skipped because material_library_ids are not UUIDs")
            return []

        result_library_ids = _valid_uuid_strings(
            list(request.user_constraints.get("result_material_library_ids") or request.material_library_ids)
        )
        if not result_library_ids:
            result_library_ids = source_library_ids

        payload = _material_search_request(intent, request, max_results, source_library_ids, result_library_ids)
        try:
            _query, results = await _materialize_material_search(db, payload)
        except Exception as exc:
            logger.warning("Material materialization failed; falling back to preview search: %s", exc)
            _query, results = await _preview_material_search(db, payload)

        return [
            _candidate_from_material_result(result, index)
            for index, result in enumerate(results[:max_results], start=1)
        ]

    async def search_external(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        max_results: int = 8,
    ) -> list[AutoFlowClipCandidate]:
        return []

    async def search_youtube(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return []

    async def search_x(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return await self._platform_stubs("x", query, max_results)

    async def search_xiaohongshu(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return await self._platform_stubs("xiaohongshu", query, max_results)

    async def search_bilibili(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return await self._platform_stubs("bilibili", query, max_results)

    async def _platform_stubs(
        self,
        platform: str,
        query: str,
        max_results: int,
    ) -> list[AutoFlowClipCandidate]:
        return []


async def _materialize_material_search(db: AsyncSession, payload: MaterialSearchRequest):
    return await materialize_material_search(db, payload)


async def _preview_material_search(db: AsyncSession, payload: MaterialSearchRequest):
    return await preview_material_search(db, payload)


async def materialize_material_search(db: AsyncSession, payload: MaterialSearchRequest):
    return await material_service.materialize_material_search(db, payload)


async def preview_material_search(db: AsyncSession, payload: MaterialSearchRequest):
    return await material_service.preview_material_search(db, payload)


def _material_search_request(
    intent: AutoFlowIntent,
    request: AutoFlowRequest,
    max_results: int,
    source_library_ids: list[str],
    result_library_ids: list[str],
) -> MaterialSearchRequest:
    top_k = max(1, int(max_results))
    max_duration = float(request.user_constraints.get("max_clip_duration") or 20.0)
    min_duration = float(request.user_constraints.get("min_clip_duration") or 1.5)
    return MaterialSearchRequest(
        query=_material_query(intent, request),
        source_library_ids=source_library_ids,
        result_library_ids=result_library_ids,
        top_k=top_k,
        rerank_top_m=min(top_k, 8),
        min_duration=min_duration,
        max_duration=max(max_duration, min_duration),
    )


def _material_query(intent: AutoFlowIntent, request: AutoFlowRequest) -> str:
    terms: list[str] = []
    for value in [intent.subject, *intent.keywords]:
        cleaned = str(value or "").strip()
        if cleaned and cleaned not in terms:
            terms.append(cleaned)
    if not terms:
        terms.append(request.prompt)
    return " ".join(terms)


def _valid_uuid_strings(values: list[str]) -> list[str]:
    valid: list[str] = []
    for value in values:
        try:
            valid.append(str(uuid.UUID(str(value))))
        except (TypeError, ValueError, AttributeError):
            continue
    return valid


def _candidate_from_material_result(
    result: dict[str, Any],
    index: int,
) -> AutoFlowClipCandidate:
    source_asset_id = _string_or_none(result.get("source_asset_id"))
    materialized_asset_id = _string_or_none(result.get("asset_id"))
    asset_id = materialized_asset_id or source_asset_id
    start_sec = _float_or_none(result.get("start_sec"))
    end_sec = _float_or_none(result.get("end_sec"))
    return AutoFlowClipCandidate(
        id=_string_or_none(result.get("id")) or _material_result_id(index, source_asset_id, start_sec, end_sec),
        title=str(result.get("title") or result.get("subtitle_text") or f"Material clip {index}"),
        source_type="material",
        asset_id=asset_id,
        start_sec=start_sec,
        end_sec=end_sec,
        rights_status="allowed",
        metadata=_material_metadata(result, materialized_asset_id, source_asset_id),
    )


def _material_metadata(
    result: dict[str, Any],
    materialized_asset_id: str | None,
    source_asset_id: str | None,
) -> dict[str, Any]:
    raw_metadata = result.get("metadata")
    if not isinstance(raw_metadata, dict):
        raw_metadata = {}

    metadata: dict[str, Any] = {}
    _put_if_present(metadata, "library_id", _string_or_none(result.get("library_id")))
    _put_if_present(metadata, "source_asset_id", source_asset_id)
    _put_if_present(metadata, "asset_id", materialized_asset_id)
    _put_if_present(metadata, "coarse", result.get("coarse") or result.get("coarse_score"))
    _put_if_present(metadata, "lighthouse", result.get("lighthouse") or result.get("lighthouse_score"))
    _put_if_present(metadata, "confidence", result.get("confidence"))
    _put_if_present(metadata, "subtitle", result.get("subtitle") or result.get("subtitle_text"))
    _put_if_present(metadata, "visual", result.get("visual") or raw_metadata.get("visual"))
    return metadata


def _put_if_present(target: dict[str, Any], key: str, value: Any) -> None:
    if value is not None and value != "":
        target[key] = value


def _material_result_id(index: int, source_asset_id: str | None, start_sec: float | None, end_sec: float | None) -> str:
    if source_asset_id:
        return f"material-{source_asset_id}-{start_sec or 0:g}-{end_sec or 0:g}"
    return f"material-{index}"


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)
