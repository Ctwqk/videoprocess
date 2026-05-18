from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.artifact import Artifact, IntermediateArtifactCache


CACHE_SCHEMA_VERSION = 1
DETERMINISTIC_NODE_TYPES = {
    "trim",
    "transcode",
    "vertical_crop",
    "concat_many",
    "montage_assembler",
    "subtitle",
    "title_overlay",
    "bgm",
    "watermark",
    "concat_timeline",
    "concat_vertical_timeline",
}
TRANSIENT_CONFIG_KEYS = {
    "disable_cache",
    "cache_key",
    "retry_count",
    "worker_id",
    "queued_at",
    "started_at",
    "completed_at",
    "debug",
}


class IntermediateArtifactCacheService:
    def is_cache_eligible(self, node_type: str, node_config: dict[str, Any], input_handles: Iterable[str]) -> bool:
        if node_type not in DETERMINISTIC_NODE_TYPES:
            return False
        if _truthy(node_config.get("disable_cache")):
            return False
        return bool(list(input_handles))

    def cache_key(
        self,
        node_type: str,
        node_config: dict[str, Any],
        input_artifacts: Mapping[str, Artifact],
    ) -> str:
        payload = {
            "schema_version": CACHE_SCHEMA_VERSION,
            "node_type": node_type,
            "node_config": _stable_config(node_config),
            "inputs": _input_signature(input_artifacts),
        }
        return _sha256_json(payload)

    def node_config_hash(self, node_config: dict[str, Any]) -> str:
        return _sha256_json(_stable_config(node_config))

    def input_signature_hash(self, input_artifacts: Mapping[str, Artifact]) -> str:
        return _sha256_json(_input_signature(input_artifacts))

    async def lookup(
        self,
        db: AsyncSession,
        *,
        node_type: str,
        node_config: dict[str, Any],
        input_artifacts: Mapping[str, Artifact],
    ) -> IntermediateArtifactCache | None:
        if not self.is_cache_eligible(node_type, node_config, input_artifacts.keys()):
            return None
        cache_key = self.cache_key(node_type, node_config, input_artifacts)
        entry = (
            await db.execute(select(IntermediateArtifactCache).where(IntermediateArtifactCache.cache_key == cache_key))
        ).scalar_one_or_none()
        if entry is None:
            return None
        output_artifact = await db.get(Artifact, entry.output_artifact_id)
        if output_artifact is None:
            await db.delete(entry)
            await db.flush()
            return None
        return entry

    async def record_hit(self, db: AsyncSession, entry: IntermediateArtifactCache) -> None:
        entry.hit_count = int(entry.hit_count or 0) + 1
        entry.last_used_at = datetime.now(timezone.utc)
        await db.flush()

    async def store(
        self,
        db: AsyncSession,
        *,
        node_type: str,
        node_config: dict[str, Any],
        input_artifacts: Mapping[str, Artifact],
        output_artifact: Artifact,
        node_id: str,
        job_id: uuid.UUID,
    ) -> None:
        if not self.is_cache_eligible(node_type, node_config, input_artifacts.keys()):
            return
        cache_key = self.cache_key(node_type, node_config, input_artifacts)
        entry = (
            await db.execute(select(IntermediateArtifactCache).where(IntermediateArtifactCache.cache_key == cache_key))
        ).scalar_one_or_none()
        metadata = {
            "cache_schema_version": CACHE_SCHEMA_VERSION,
            "node_id": node_id,
            "input_artifact_ids": [str(artifact.id) for _handle, artifact in _ordered_inputs(input_artifacts)],
            "config_keys": sorted(_stable_config(node_config).keys()),
            "created_by_job_id": str(job_id),
        }
        if entry is None:
            db.add(
                IntermediateArtifactCache(
                    cache_key=cache_key,
                    node_type=node_type,
                    node_config_hash=self.node_config_hash(node_config),
                    input_signature_hash=self.input_signature_hash(input_artifacts),
                    output_artifact_id=output_artifact.id,
                    metadata_json=metadata,
                )
            )
            await db.flush()
            return

        entry.node_type = node_type
        entry.node_config_hash = self.node_config_hash(node_config)
        entry.input_signature_hash = self.input_signature_hash(input_artifacts)
        entry.output_artifact_id = output_artifact.id
        entry.metadata_json = metadata
        await db.flush()


def _stable_config(config: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in config.items():
        if key.startswith("_") or key in TRANSIENT_CONFIG_KEYS:
            continue
        result[key] = value
    return result


def _input_signature(input_artifacts: Mapping[str, Artifact]) -> list[dict[str, Any]]:
    return [
        {
            "handle": handle,
            "artifact_id": str(artifact.id),
            "storage_backend": artifact.storage_backend,
            "storage_path": artifact.storage_path,
            "file_size": artifact.file_size,
            "media_info_hash": _sha256_json(artifact.media_info or {}),
        }
        for handle, artifact in _ordered_inputs(input_artifacts)
    ]


def _ordered_inputs(input_artifacts: Mapping[str, Artifact]) -> list[tuple[str, Artifact]]:
    return sorted(input_artifacts.items(), key=lambda item: _natural_key(item[0]))


def _natural_key(value: str) -> tuple[Any, ...]:
    parts: list[Any] = []
    for part in re.split(r"(\d+)", value):
        if not part:
            continue
        parts.append(int(part) if part.isdigit() else part)
    return tuple(parts)


def _sha256_json(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)
