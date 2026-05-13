import asyncio
import hashlib
import os
import re
import tempfile
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.asset import Asset
from app.models.material import (
    MaterialClip,
    MaterialItem,
    MaterialLibrary,
    MaterialQuery,
    MaterialQueryResult,
)
from app.services.asset_service import _extract_media_info, create_asset_from_local_file
from app.storage.manager import get_storage


DEFAULT_VECTOR_SIZE = 1024


class MaterialLibraryConflictError(ValueError):
    """Raised when attempting to create a duplicate material library."""


@dataclass
class CandidateWindow:
    source_asset_id: str
    library_id: str
    start_sec: float
    end_sec: float
    coarse_score: float
    subtitle_text: str
    neighbor_clip_ids: list[str]
    member_clip_ids: list[str]
    clips: list[dict[str, Any]]
    lighthouse_score: float = 0.0


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[\w\u4e00-\u9fff]+", text.lower())
        if token
    }


def _overlap_score(query: str, text: str) -> float:
    query_tokens = _tokenize(query)
    text_tokens = _tokenize(text)
    if not query_tokens or not text_tokens:
        return 0.0
    return len(query_tokens & text_tokens) / max(1, len(query_tokens))


async def list_material_libraries(db: AsyncSession, skip: int = 0, limit: int = 100) -> tuple[list[MaterialLibrary], int]:
    total = (await db.execute(select(func.count()).select_from(MaterialLibrary))).scalar() or 0
    result = await db.execute(
        select(MaterialLibrary).order_by(MaterialLibrary.updated_at.desc()).offset(skip).limit(limit)
    )
    return list(result.scalars().all()), total


async def create_material_library(db: AsyncSession, name: str, description: str = "") -> MaterialLibrary:
    library = MaterialLibrary(name=name.strip(), description=description.strip())
    db.add(library)
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise MaterialLibraryConflictError(f"Material library '{library.name}' already exists") from exc
    await db.refresh(library)
    return library


async def get_material_library(db: AsyncSession, library_id: uuid.UUID) -> MaterialLibrary | None:
    return await db.get(MaterialLibrary, library_id)


async def list_material_clips(
    db: AsyncSession,
    library_id: uuid.UUID,
    skip: int = 0,
    limit: int = 100,
) -> tuple[list[MaterialClip], int]:
    total = (
        await db.execute(
            select(func.count()).select_from(MaterialClip).where(MaterialClip.library_id == library_id)
        )
    ).scalar() or 0
    result = await db.execute(
        select(MaterialClip)
        .where(MaterialClip.library_id == library_id)
        .order_by(MaterialClip.created_at.desc())
        .offset(skip)
        .limit(limit)
    )
    return list(result.scalars().all()), total


async def _embed_texts(texts: list[str]) -> list[list[float]]:
    cleaned = [text.strip() if text else " " for text in texts]
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{settings.embedding_gateway_url.rstrip('/')}/embed",
                json={"texts": cleaned},
            )
            response.raise_for_status()
            payload = response.json()
        embeddings = payload.get("embeddings") or []
        normalized = [list(map(float, item)) for item in embeddings if item]
        if len(normalized) != len(cleaned):
            raise ValueError(
                f"embedding-gateway returned {len(normalized)} embeddings for {len(cleaned)} texts"
            )
        return normalized
    except Exception:
        return [_hash_embedding(text) for text in cleaned]


def _hash_embedding(text: str, size: int = DEFAULT_VECTOR_SIZE) -> list[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    values: list[float] = []
    for index in range(size):
        byte = digest[index % len(digest)]
        values.append((byte / 255.0) * 2.0 - 1.0)
    return values


async def _ensure_material_collection(vector_size: int) -> None:
    base_url = settings.qdrant_url.rstrip("/")
    collection = settings.material_qdrant_collection
    async with httpx.AsyncClient(timeout=60.0) as client:
        existing = await client.get(f"{base_url}/collections/{collection}")
        if existing.status_code == 200:
            payload = existing.json().get("result") or {}
            vectors = (((payload.get("config") or {}).get("params") or {}).get("vectors") or {})
            if isinstance(vectors, dict):
                for vector_name in ("content", "subtitle"):
                    current_size = int(((vectors.get(vector_name) or {}).get("size") or 0) or 0)
                    if current_size and current_size != vector_size:
                        raise ValueError(
                            f"Qdrant collection '{collection}' vector '{vector_name}' has size {current_size}, expected {vector_size}"
                        )
            return
        response = await client.put(
            f"{base_url}/collections/{collection}",
            json={
                "vectors": {
                    "content": {"size": vector_size, "distance": "Cosine"},
                    "subtitle": {"size": vector_size, "distance": "Cosine"},
                }
            },
        )
        response.raise_for_status()


async def _upsert_material_points(points: list[dict[str, Any]]) -> None:
    if not points:
        return
    first_vector = (points[0].get("vector") or {}).get("content") or []
    await _ensure_material_collection(len(first_vector) or DEFAULT_VECTOR_SIZE)
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.put(
            f"{settings.qdrant_url.rstrip('/')}/collections/{settings.material_qdrant_collection}/points?wait=false",
            json={"points": points},
        )
        response.raise_for_status()


async def _delete_material_points(point_ids: list[str]) -> None:
    if not point_ids:
        return
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(
            f"{settings.qdrant_url.rstrip('/')}/collections/{settings.material_qdrant_collection}/points/delete?wait=false",
            json={"points": point_ids},
        )
        response.raise_for_status()


async def ingest_material_asset(
    db: AsyncSession,
    *,
    asset_id: uuid.UUID,
    library_ids: list[uuid.UUID],
    clip_len: float,
    stride: float,
    subtitle_mode: str,
    subtitle_cues: list[Any],
    fallback_media_info: dict[str, Any] | None = None,
    store_neighbors: bool = True,
) -> dict[str, Any]:
    asset = await db.get(Asset, asset_id)
    if not asset:
        raise ValueError(f"Asset {asset_id} not found")

    media_info = dict(asset.media_info or {})
    duration = float(media_info.get("duration") or 0.0)
    media_info_updated = False
    if duration <= 0:
        fallback_media_info = fallback_media_info or {}
        duration = float(fallback_media_info.get("duration") or 0.0)
        if duration > 0:
            media_info.update(fallback_media_info)
            media_info_updated = True

    if duration <= 0:
        storage = get_storage(asset.storage_backend)
        local_path = storage.get_local_path(asset.storage_path)
        temp_path: str | None = None
        try:
            if not local_path:
                suffix = Path(asset.original_name or asset.filename or "asset.bin").suffix or ".bin"
                fd, temp_path = tempfile.mkstemp(prefix="material_asset_probe_", suffix=suffix)
                os.close(fd)
                with open(temp_path, "wb") as handle:
                    handle.write(await storage.read(asset.storage_path))
                local_path = temp_path

            probed_media_info = await _extract_media_info(local_path)
            duration = float((probed_media_info or {}).get("duration") or 0.0)
            if duration > 0:
                media_info.update(probed_media_info or {})
                media_info_updated = True
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

    if duration <= 0:
        raise ValueError("Source asset is missing a valid duration")

    if media_info_updated:
        asset.media_info = media_info
        await db.flush()

    windows: list[tuple[float, float]] = []
    cursor = 0.0
    while cursor < duration:
        start = max(0.0, cursor)
        end = min(duration, cursor + clip_len)
        windows.append((start, end))
        if end >= duration:
            break
        cursor += stride

    clips_payload: list[dict[str, Any]] = []
    content_texts: list[str] = []
    subtitle_texts: list[str] = []
    for index, (start, end) in enumerate(windows):
        overlapping = [
            cue
            for cue in subtitle_cues
            if cue.end_seconds > start and cue.start_seconds < end
        ]
        subtitle_text = "\n".join(cue.text.strip() for cue in overlapping if cue.text.strip()).strip()
        previous_text = subtitle_cues[max(0, index - 1)].text.strip() if subtitle_cues and index - 1 < len(subtitle_cues) else ""
        next_text = subtitle_cues[min(len(subtitle_cues) - 1, index + 1)].text.strip() if subtitle_cues else ""
        content_text = " ".join(part for part in [previous_text, subtitle_text, next_text] if part).strip() or subtitle_text or f"clip {index + 1}"
        neighbor_ids: list[str] = []
        if store_neighbors:
            if index > 0:
                neighbor_ids.append(f"{asset_id}:{index}")
            if index + 1 < len(windows):
                neighbor_ids.append(f"{asset_id}:{index + 2}")
        clip_id = f"{asset_id}:{index + 1}"
        clips_payload.append(
            {
                "clip_id": clip_id,
                "start_sec": float(start),
                "end_sec": float(end),
                "subtitle_text": subtitle_text,
                "neighbor_clip_ids": neighbor_ids,
            }
        )
        content_texts.append(content_text)
        subtitle_texts.append(subtitle_text or content_text)

    content_embeddings = await _embed_texts(content_texts)
    subtitle_embeddings = await _embed_texts(subtitle_texts)

    points: list[dict[str, Any]] = []
    stale_point_ids: list[str] = []
    for library_id in library_ids:
        existing_item = (
            await db.execute(
                select(MaterialItem).where(
                    MaterialItem.library_id == library_id,
                    MaterialItem.asset_id == asset_id,
                )
            )
        ).scalar_one_or_none()

        if existing_item:
            existing_clips = (
                await db.execute(
                    select(MaterialClip.id).where(MaterialClip.parent_material_item_id == existing_item.id)
                )
            ).scalars().all()
            stale_point_ids.extend(str(point_id) for point_id in existing_clips)
            await db.execute(delete(MaterialClip).where(MaterialClip.parent_material_item_id == existing_item.id))
            item = existing_item
            item.status = "READY"
            item.duration = duration
            item.subtitle_source = subtitle_mode
            item.metadata_json = {"clip_len": clip_len, "stride": stride}
        else:
            item = MaterialItem(
                library_id=library_id,
                asset_id=asset_id,
                status="READY",
                duration=duration,
                subtitle_source=subtitle_mode,
                metadata_json={"clip_len": clip_len, "stride": stride},
            )
            db.add(item)
            await db.flush()

        for clip_index, payload in enumerate(clips_payload):
            clip = MaterialClip(
                library_id=library_id,
                parent_material_item_id=item.id,
                source_asset_id=asset_id,
                clip_id=payload["clip_id"],
                start_sec=payload["start_sec"],
                end_sec=payload["end_sec"],
                subtitle_text=payload["subtitle_text"],
                neighbor_clip_ids=payload["neighbor_clip_ids"],
                clip_kind="coarse_window",
                metadata_json={"index": clip_index},
            )
            db.add(clip)
            await db.flush()
            points.append(
                {
                    "id": str(clip.id),
                    "vector": {
                        "content": content_embeddings[clip_index],
                        "subtitle": subtitle_embeddings[clip_index],
                    },
                    "payload": {
                        "library_id": str(library_id),
                        "source_asset_id": str(asset_id),
                        "clip_id": payload["clip_id"],
                        "start_sec": payload["start_sec"],
                        "end_sec": payload["end_sec"],
                        "subtitle_text": payload["subtitle_text"],
                        "neighbor_clip_ids": payload["neighbor_clip_ids"],
                    },
                }
            )

    await db.commit()
    qdrant_indexed = True
    try:
        await _delete_material_points(stale_point_ids)
        await _upsert_material_points(points)
    except Exception:
        qdrant_indexed = False

    return {
        "library_ids": [str(library_id) for library_id in library_ids],
        "source_asset_id": str(asset_id),
        "clip_count": len(clips_payload),
        "duration": duration,
        "clip_len": clip_len,
        "stride": stride,
        "qdrant_indexed": qdrant_indexed,
    }


async def _qdrant_search(vector_name: str, vector: list[float], source_library_ids: list[str], limit: int) -> list[dict[str, Any]]:
    must = [{
        "key": "library_id",
        "match": {"any": source_library_ids},
    }]
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(
            f"{settings.qdrant_url.rstrip('/')}/collections/{settings.material_qdrant_collection}/points/search",
            json={
                "vector": {"name": vector_name, "vector": vector},
                "limit": limit,
                "with_payload": True,
                "filter": {"must": must},
            },
        )
        response.raise_for_status()
        payload = response.json()
    return payload.get("result") or []


async def _fallback_db_search(db: AsyncSession, query: str, source_library_ids: list[str], limit: int) -> list[dict[str, Any]]:
    rows = (
        await db.execute(
            select(MaterialClip)
            .where(
                MaterialClip.library_id.in_([uuid.UUID(item) for item in source_library_ids]),
                MaterialClip.clip_kind == "coarse_window",
            )
            .order_by(MaterialClip.created_at.desc())
            .limit(max(limit * 4, 100))
        )
    ).scalars().all()
    scored = sorted(
        rows,
        key=lambda clip: _overlap_score(query, clip.subtitle_text or ""),
        reverse=True,
    )[:limit]
    return [
        {
            "id": str(clip.id),
            "score": _overlap_score(query, clip.subtitle_text or ""),
            "payload": {
                "library_id": str(clip.library_id),
                "source_asset_id": str(clip.source_asset_id),
                "clip_id": clip.clip_id,
                "start_sec": clip.start_sec,
                "end_sec": clip.end_sec,
                "subtitle_text": clip.subtitle_text,
                "neighbor_clip_ids": clip.neighbor_clip_ids,
            },
        }
        for clip in scored
    ]


def _merge_candidates(raw_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in raw_results:
        payload = item.get("payload") or {}
        point_id = str(item.get("id"))
        score = float(item.get("score") or 0.0)
        existing = merged.get(point_id)
        if existing is None or score > existing["coarse_score"]:
            merged[point_id] = {
                "id": point_id,
                "library_id": str(payload.get("library_id")),
                "source_asset_id": str(payload.get("source_asset_id")),
                "clip_id": str(payload.get("clip_id")),
                "start_sec": float(payload.get("start_sec") or 0.0),
                "end_sec": float(payload.get("end_sec") or 0.0),
                "subtitle_text": str(payload.get("subtitle_text") or ""),
                "neighbor_clip_ids": payload.get("neighbor_clip_ids") or [],
                "coarse_score": score,
            }
    return sorted(merged.values(), key=lambda item: item["coarse_score"], reverse=True)


def _cluster_candidates(candidates: list[dict[str, Any]], merge_gap: float) -> list[CandidateWindow]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        grouped[candidate["source_asset_id"]].append(candidate)

    windows: list[CandidateWindow] = []
    for source_asset_id, group in grouped.items():
        group.sort(key=lambda item: item["start_sec"])
        current: list[dict[str, Any]] = []
        current_end = -1.0
        for candidate in group:
            if not current:
                current = [candidate]
                current_end = candidate["end_sec"]
                continue
            gap = candidate["start_sec"] - current_end
            if candidate["start_sec"] <= current_end or gap <= merge_gap:
                current.append(candidate)
                current_end = max(current_end, candidate["end_sec"])
                continue
            windows.append(_candidate_window_from_cluster(current))
            current = [candidate]
            current_end = candidate["end_sec"]
        if current:
            windows.append(_candidate_window_from_cluster(current))
    return windows


def _candidate_window_from_cluster(cluster: list[dict[str, Any]]) -> CandidateWindow:
    start_sec = min(item["start_sec"] for item in cluster)
    end_sec = max(item["end_sec"] for item in cluster)
    subtitle_text = "\n".join(item["subtitle_text"] for item in cluster if item["subtitle_text"]).strip()
    member_clip_ids = [item["clip_id"] for item in cluster]
    neighbor_clip_ids: list[str] = []
    for item in cluster:
        neighbor_clip_ids.extend(item.get("neighbor_clip_ids") or [])
    return CandidateWindow(
        source_asset_id=cluster[0]["source_asset_id"],
        library_id=cluster[0]["library_id"],
        start_sec=start_sec,
        end_sec=end_sec,
        coarse_score=max(item["coarse_score"] for item in cluster),
        subtitle_text=subtitle_text,
        neighbor_clip_ids=sorted(set(neighbor_clip_ids)),
        member_clip_ids=member_clip_ids,
        clips=cluster,
        lighthouse_score=0.0,
    )


def _expand_windows(windows: list[CandidateWindow], expand_left: float, expand_right: float, max_duration: float) -> list[CandidateWindow]:
    expanded: list[CandidateWindow] = []
    for window in windows:
        start = max(0.0, window.start_sec - expand_left)
        end = window.end_sec + expand_right
        if end - start > max_duration:
            end = start + max_duration
        expanded.append(
            CandidateWindow(
                source_asset_id=window.source_asset_id,
                library_id=window.library_id,
                start_sec=start,
                end_sec=end,
                coarse_score=window.coarse_score,
                subtitle_text=window.subtitle_text,
                neighbor_clip_ids=window.neighbor_clip_ids,
                member_clip_ids=window.member_clip_ids,
                clips=window.clips,
            )
        )
    return expanded


async def _lighthouse_rerank(query: str, windows: list[CandidateWindow], top_m: int) -> list[CandidateWindow]:
    if settings.material_lighthouse_url:
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    settings.material_lighthouse_url.rstrip("/") + "/rerank",
                    json={
                        "query": query,
                        "items": [
                            {
                                "source_asset_id": item.source_asset_id,
                                "subtitle_text": item.subtitle_text,
                                "start_sec": item.start_sec,
                                "end_sec": item.end_sec,
                            }
                            for item in windows
                        ],
                    },
                )
                response.raise_for_status()
                scores = response.json().get("scores") or []
                if len(scores) == len(windows):
                    keyed = []
                    for item, score in zip(windows, scores):
                        item.lighthouse_score = float(score)
                        keyed.append(item)
                    keyed.sort(key=lambda item: item.lighthouse_score, reverse=True)
                    return keyed[:top_m]
        except Exception:
            pass

    for item in windows:
        item.lighthouse_score = (_overlap_score(query, item.subtitle_text) * 0.7) + (item.coarse_score * 0.3)
    ranked = sorted(windows, key=lambda item: item.lighthouse_score, reverse=True)
    return ranked[:top_m]


async def _univtg_refine(query: str, window: CandidateWindow, min_duration: float, max_duration: float) -> tuple[float, float, float]:
    if settings.material_univtg_url:
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    settings.material_univtg_url.rstrip("/") + "/locate",
                    json={
                        "query": query,
                        "window": {
                            "start_sec": window.start_sec,
                            "end_sec": window.end_sec,
                            "subtitle_text": window.subtitle_text,
                        },
                    },
                )
                response.raise_for_status()
                payload = response.json()
                start_offset = float(payload.get("start_offset") or 0.0)
                end_offset = float(payload.get("end_offset") or (window.end_sec - window.start_sec))
                confidence = float(payload.get("confidence") or 0.0)
                return window.start_sec + start_offset, window.start_sec + end_offset, confidence
        except Exception:
            pass

    scored_clips = sorted(
        window.clips,
        key=lambda clip: (_overlap_score(query, clip["subtitle_text"]) * 0.8) + (clip["coarse_score"] * 0.2),
        reverse=True,
    )
    if not scored_clips:
        return window.start_sec, min(window.end_sec, window.start_sec + max_duration), 0.0
    top = scored_clips[0]
    start_sec = top["start_sec"]
    end_sec = top["end_sec"]
    duration = end_sec - start_sec
    if duration < min_duration:
        pad = (min_duration - duration) / 2
        start_sec = max(window.start_sec, start_sec - pad)
        end_sec = min(window.end_sec, end_sec + pad)
    if (end_sec - start_sec) > max_duration:
        end_sec = start_sec + max_duration
    return start_sec, end_sec, _overlap_score(query, top["subtitle_text"])


def _overlap_ratio(start_a: float, end_a: float, start_b: float, end_b: float) -> float:
    intersection = max(0.0, min(end_a, end_b) - max(start_a, start_b))
    union = max(end_a, end_b) - min(start_a, start_b)
    if union <= 0:
        return 0.0
    return intersection / union


async def _cut_asset_clip(source_asset: Asset, start_sec: float, end_sec: float, title_hint: str, db: AsyncSession) -> Asset:
    storage = get_storage(source_asset.storage_backend)
    local_path = storage.get_local_path(source_asset.storage_path)
    temp_source_path = None
    if not local_path:
        ext = Path(source_asset.original_name).suffix or ".mp4"
        fd, temp_source_path = tempfile.mkstemp(prefix="material_source_", suffix=ext)
        os.close(fd)
        with open(temp_source_path, "wb") as handle:
            handle.write(await storage.read(source_asset.storage_path))
        local_path = temp_source_path

    fd, output_path = tempfile.mkstemp(prefix="material_clip_", suffix=".mp4")
    os.close(fd)
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-ss",
            f"{start_sec:.3f}",
            "-to",
            f"{end_sec:.3f}",
            "-i",
            local_path,
            "-c",
            "copy",
            output_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(stderr.decode("utf-8", errors="replace")[-2000:])
        asset = await create_asset_from_local_file(
            db,
            output_path,
            original_name=f"{title_hint}.mp4",
            mime_type="video/mp4",
            uploaded_by="material-search",
        )
        return asset
    finally:
        if temp_source_path:
            try:
                os.unlink(temp_source_path)
            except OSError:
                pass
        try:
            os.unlink(output_path)
        except OSError:
            pass


async def preview_material_search(db: AsyncSession, request) -> tuple[MaterialQuery, list[dict[str, Any]]]:
    query_embedding = (await _embed_texts([request.query]))[0]

    try:
        content_hits = await _qdrant_search("content", query_embedding, request.source_library_ids, request.top_k)
        subtitle_hits = await _qdrant_search("subtitle", query_embedding, request.source_library_ids, request.top_k)
    except Exception:
        content_hits = await _fallback_db_search(db, request.query, request.source_library_ids, request.top_k)
        subtitle_hits = []
    candidates = _merge_candidates([*content_hits, *subtitle_hits])[: request.top_k]
    windows = _cluster_candidates(candidates, request.merge_gap)
    windows = _expand_windows(windows, request.expand_left, request.expand_right, request.max_duration)
    ranked = await _lighthouse_rerank(request.query, windows, request.rerank_top_m)

    query_row = MaterialQuery(
        query_text=request.query,
        source_library_ids=request.source_library_ids,
        result_library_ids=request.result_library_ids,
        config_json=request.model_dump(),
    )
    db.add(query_row)
    await db.flush()

    preview_results: list[dict[str, Any]] = []
    for rank, window in enumerate(ranked, start=1):
        start_sec, end_sec, confidence = await _univtg_refine(
            request.query,
            window,
            request.min_duration,
            request.max_duration,
        )
        preview_results.append(
            {
                "rank": rank,
                "library_id": window.library_id,
                "source_asset_id": window.source_asset_id,
                "start_sec": start_sec,
                "end_sec": end_sec,
                "subtitle_text": window.subtitle_text,
                "coarse_score": window.coarse_score,
                "lighthouse_score": window.lighthouse_score,
                "confidence": confidence,
                "member_clip_ids": window.member_clip_ids,
                "neighbor_clip_ids": window.neighbor_clip_ids,
            }
        )
    await db.commit()
    await db.refresh(query_row)
    return query_row, preview_results


async def materialize_material_search(db: AsyncSession, request) -> tuple[MaterialQuery, list[dict[str, Any]]]:
    query_row, preview_results = await preview_material_search(db, request)

    source_assets = {
        str(asset.id): asset
        for asset in (
            await db.execute(
                select(Asset).where(Asset.id.in_([uuid.UUID(item["source_asset_id"]) for item in preview_results]))
            )
        ).scalars().all()
    }

    final_results: list[dict[str, Any]] = []
    accepted_ranges: dict[str, list[tuple[float, float]]] = defaultdict(list)
    result_libraries = [uuid.UUID(item) for item in (request.result_library_ids or request.source_library_ids)]

    for rank, item in enumerate(preview_results, start=1):
        source_asset_id = item["source_asset_id"]
        if any(
            _overlap_ratio(item["start_sec"], item["end_sec"], existing_start, existing_end) >= request.dedupe_overlap_threshold
            for existing_start, existing_end in accepted_ranges[source_asset_id]
        ):
            continue
        accepted_ranges[source_asset_id].append((item["start_sec"], item["end_sec"]))
        source_asset = source_assets[source_asset_id]
        title_hint = f"material_{source_asset_id[:8]}_{int(item['start_sec'] * 1000)}_{int(item['end_sec'] * 1000)}"
        final_asset = await _cut_asset_clip(source_asset, item["start_sec"], item["end_sec"], title_hint, db)

        created_clips: list[MaterialClip] = []
        for library_id in result_libraries:
            clip = MaterialClip(
                library_id=library_id,
                parent_material_item_id=None,
                source_asset_id=uuid.UUID(source_asset_id),
                clip_id=f"{source_asset_id}:{item['start_sec']:.3f}:{item['end_sec']:.3f}",
                start_sec=item["start_sec"],
                end_sec=item["end_sec"],
                subtitle_text=item["subtitle_text"],
                neighbor_clip_ids=item["neighbor_clip_ids"],
                clip_kind="final_refined",
                storage_asset_id=final_asset.id,
                metadata_json={
                    "query": request.query,
                    "coarse_score": item["coarse_score"],
                    "lighthouse_score": item["lighthouse_score"],
                    "confidence": item["confidence"],
                },
            )
            db.add(clip)
            await db.flush()
            created_clips.append(clip)

        for clip in created_clips:
            query_result = MaterialQueryResult(
                query_id=query_row.id,
                source_asset_id=uuid.UUID(source_asset_id),
                material_clip_id=clip.id,
                rank=rank,
                coarse_score=item["coarse_score"],
                lighthouse_score=item["lighthouse_score"],
                confidence=item["confidence"],
                start_sec=item["start_sec"],
                end_sec=item["end_sec"],
                metadata_json={
                    "member_clip_ids": item["member_clip_ids"],
                    "neighbor_clip_ids": item["neighbor_clip_ids"],
                    "storage_asset_id": str(final_asset.id),
                    "result_library_id": str(clip.library_id),
                    "material_clip_id": str(clip.id),
                },
            )
            db.add(query_result)
            await db.flush()
            final_results.append(
                {
                    "id": str(query_result.id),
                    "title": source_asset.original_name,
                    "asset_id": str(final_asset.id),
                    "source_asset_id": source_asset_id,
                    "library_id": str(clip.library_id),
                    "start_sec": item["start_sec"],
                    "end_sec": item["end_sec"],
                    "subtitle_text": item["subtitle_text"],
                    "coarse_score": item["coarse_score"],
                    "lighthouse_score": item["lighthouse_score"],
                    "confidence": item["confidence"],
                    "metadata": {
                        "storage_asset_id": str(final_asset.id),
                        "material_clip_id": str(clip.id),
                        "result_library_id": str(clip.library_id),
                    },
                }
            )

    await db.commit()
    return query_row, final_results
