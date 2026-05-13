from __future__ import annotations
import logging
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.downloads import build_download_response
from app.db import get_db
from app.models.asset import Asset
from app.models.artifact import Artifact, ArtifactKind
from app.models.job import Job, JobStatus, NodeExecution
from app.schemas.artifact import ArtifactResponse
from app.storage.manager import get_storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/artifacts", tags=["artifacts"])


@router.get("/{artifact_id}", response_model=ArtifactResponse)
async def get_artifact(artifact_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    artifact = await db.get(Artifact, artifact_id)
    if not artifact:
        raise HTTPException(status_code=404, detail="Artifact not found")
    return ArtifactResponse(
        id=str(artifact.id),
        job_id=str(artifact.job_id),
        node_execution_id=str(artifact.node_execution_id),
        kind=artifact.kind.value,
        filename=artifact.filename,
        mime_type=artifact.mime_type,
        file_size=artifact.file_size,
        created_at=artifact.created_at,
    )


@router.delete("/cleanup", status_code=200)
async def cleanup_intermediates(
    job_id: str | None = Query(default=None, description="Clean up intermediates for a specific job"),
    db: AsyncSession = Depends(get_db),
):
    """Delete intermediate artifacts (files + DB records) for completed jobs."""
    job_filter = uuid.UUID(job_id) if job_id else None

    # Build query for intermediate artifacts of completed jobs.
    stmt = (
        select(Artifact, Job, NodeExecution)
        .join(Job, Artifact.job_id == Job.id)
        .join(NodeExecution, Artifact.node_execution_id == NodeExecution.id)
        .where(
            Artifact.kind == ArtifactKind.INTERMEDIATE,
            Job.status.in_([JobStatus.SUCCEEDED, JobStatus.FAILED,
                           JobStatus.CANCELLED, JobStatus.PARTIALLY_FAILED]),
        )
    )
    if job_filter:
        stmt = stmt.where(Artifact.job_id == job_filter)

    result = await db.execute(stmt)
    rows = list(result.all())

    terminal_cache: dict[uuid.UUID, set[str]] = {}
    node_ids_by_job: dict[uuid.UUID, set[str]] = {}
    for _, _, node in rows:
        node_ids_by_job.setdefault(node.job_id, set()).add(node.node_id)

    deleted_count = 0
    freed_bytes = 0
    for artifact, job, node_execution in rows:
        terminal_nodes = terminal_cache.get(job.id)
        if terminal_nodes is None:
            snapshot = job.pipeline_snapshot or {}
            edges = snapshot.get("edges", [])
            edge_sources = {edge.get("source") for edge in edges if isinstance(edge, dict)}
            node_ids = node_ids_by_job.get(job.id, set())
            terminal_nodes = {node_id for node_id in node_ids if node_id not in edge_sources}
            terminal_cache[job.id] = terminal_nodes

        # Preserve terminal-node outputs even if kind has not been promoted to FINAL yet.
        if node_execution.node_id in terminal_nodes:
            continue

        # Download cache objects are intentionally shared across jobs and should
        # outlive individual artifact records so future URL downloads can reuse them.
        if artifact.storage_path.startswith("download-cache/"):
            node_execution.output_artifact_id = None
            await db.delete(artifact)
            deleted_count += 1
            freed_bytes += artifact.file_size or 0
            continue

        shared_asset = await db.execute(
            select(Asset.id)
            .where(
                Asset.storage_backend == artifact.storage_backend,
                Asset.storage_path == artifact.storage_path,
            )
            .limit(1)
        )
        shared_artifact = await db.execute(
            select(Artifact.id)
            .where(
                Artifact.id != artifact.id,
                Artifact.storage_backend == artifact.storage_backend,
                Artifact.storage_path == artifact.storage_path,
            )
            .limit(1)
        )

        if not shared_asset.scalar_one_or_none() and not shared_artifact.scalar_one_or_none():
            try:
                await get_storage(artifact.storage_backend).delete(artifact.storage_path)
            except Exception as exc:
                logger.warning(
                    "Failed to delete artifact payload %s:%s during cleanup: %s",
                    artifact.storage_backend,
                    artifact.storage_path,
                    exc,
                )

        freed_bytes += artifact.file_size or 0
        node_execution.output_artifact_id = None
        await db.delete(artifact)
        deleted_count += 1

    await db.commit()
    return {
        "deleted_count": deleted_count,
        "freed_bytes": freed_bytes,
    }


@router.get("/{artifact_id}/download")
async def download_artifact(artifact_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    artifact = await db.get(Artifact, artifact_id)
    if not artifact:
        raise HTTPException(status_code=404, detail="Artifact not found")

    storage = get_storage(artifact.storage_backend)
    return await build_download_response(
        storage=storage,
        storage_path=artifact.storage_path,
        filename=artifact.filename,
        media_type=artifact.mime_type,
    )
