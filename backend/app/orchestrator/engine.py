from __future__ import annotations
import json
import logging
import time
import uuid
from collections import Counter
from datetime import datetime

import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.db import async_session
from app.models.asset import Asset
from app.models.artifact import Artifact, ArtifactKind
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.node_registry.registry import NodeTypeRegistry
from app.orchestrator.artifact_cache import IntermediateArtifactCacheService
from app.schemas.pipeline import PipelineDefinition
from app.orchestrator.dag import topological_sort, build_dependency_map
from app.services.schedule_service import get_video_schedule_state, park_job_for_window, should_defer_job_start

logger = logging.getLogger(__name__)

TASK_STREAM = "vp:tasks:{worker_type}"
EVENT_STREAM = "vp:events"
CONSUMER_GROUP = "orchestrator"
CONSUMER_NAME = "orchestrator-1"


def _extract_worker_host(worker_id: str | None) -> str | None:
    if not worker_id:
        return None
    marker = "worker@"
    if marker not in worker_id:
        return None
    suffix = worker_id.split(marker, 1)[1]
    host, _, _rest = suffix.partition(":")
    host = host.strip()
    return host or None


def _redis() -> aioredis.Redis:
    return aioredis.from_url(settings.redis_url, decode_responses=True)


def _leaf_node_ids(definition: PipelineDefinition) -> set[str]:
    has_outgoing = {edge.source for edge in definition.edges}
    return {node.id for node in definition.nodes if node.id not in has_outgoing}


class JobEngine:
    """Orchestrates job execution by dispatching nodes to workers via Redis Streams."""

    def __init__(self, artifact_cache: IntermediateArtifactCacheService | None = None) -> None:
        self.artifact_cache = artifact_cache or IntermediateArtifactCacheService()

    async def _maybe_finalize_job(self, db: AsyncSession, job: Job) -> bool:
        """Mark the job terminal once all node executions have reached a terminal state."""
        statuses = [n.status for n in job.node_executions]
        active_statuses = {NodeStatus.PENDING, NodeStatus.QUEUED, NodeStatus.RUNNING}
        if any(status in active_statuses for status in statuses):
            return False

        has_success = any(status == NodeStatus.SUCCEEDED for status in statuses)
        failed_statuses = {NodeStatus.FAILED, NodeStatus.SKIPPED, NodeStatus.CANCELLED}
        has_fail = any(status in failed_statuses for status in statuses)
        definition = PipelineDefinition.model_validate(job.pipeline_snapshot)
        leaf_node_ids = _leaf_node_ids(definition)
        leaf_executions = [node for node in job.node_executions if node.node_id in leaf_node_ids]
        has_successful_leaf = any(node.status == NodeStatus.SUCCEEDED for node in leaf_executions)
        has_failed_leaf = any(node.status in failed_statuses for node in leaf_executions)

        if all(status == NodeStatus.SUCCEEDED for status in statuses):
            job.status = JobStatus.SUCCEEDED
            job.completed_at = datetime.utcnow()
            await db.commit()
            await self._mark_final_artifacts(db, job)
            logger.info(f"Job {job.id} SUCCEEDED")
            return True

        if has_fail:
            job.status = JobStatus.FAILED if has_failed_leaf or not has_successful_leaf else JobStatus.PARTIALLY_FAILED
            if not job.error_message:
                failed_nodes = [
                    n.node_label or n.node_id
                    for n in job.node_executions
                    if n.status in failed_statuses
                ]
                if failed_nodes:
                    job.error_message = f"Failed nodes: {', '.join(failed_nodes)}"
            job.completed_at = datetime.utcnow()
            await db.commit()
            if job.status != JobStatus.FAILED:
                await self._mark_final_artifacts(db, job)
            logger.info(f"Job {job.id} {job.status.value}")
            return True

        return False

    async def start_job(self, job_id: uuid.UUID) -> None:
        """Start executing a job: validate, plan, and dispatch root nodes."""
        async with async_session() as db:
            job = await db.get(Job, job_id, options=[selectinload(Job.node_executions)])
            if not job:
                logger.error(f"Job {job_id} not found")
                return
            if job.status in {
                JobStatus.SUCCEEDED,
                JobStatus.FAILED,
                JobStatus.CANCELLED,
                JobStatus.PARTIALLY_FAILED,
            }:
                return

            schedule_state = await get_video_schedule_state(db)
            if should_defer_job_start(job, schedule_state):
                await park_job_for_window(db, job)
                logger.info(
                    "Deferred job %s until next video window (state=%s)",
                    job_id,
                    schedule_state.value,
                )
                return

            try:
                job.status = JobStatus.PLANNING
                job.started_at = datetime.utcnow()

                definition = PipelineDefinition.model_validate(job.pipeline_snapshot)
                topo_order = topological_sort(definition)
                dep_map = build_dependency_map(definition)

                job.execution_plan = {
                    "topo_order": topo_order,
                    "dependencies": dep_map,
                }
                job.status = JobStatus.RUNNING
                await db.commit()

                # Resolve input artifacts for source nodes (asset -> artifact)
                await self._resolve_source_nodes(db, job)

                # Dispatch nodes that have no dependencies (root nodes)
                await self._dispatch_ready_nodes(db, job, dep_map)

            except Exception as e:
                logger.exception(f"Failed to start job {job_id}")
                job.status = JobStatus.FAILED
                job.error_message = str(e)
                job.completed_at = datetime.utcnow()
                await db.commit()

    async def _resolve_source_nodes(self, db: AsyncSession, job: Job) -> None:
        """For source nodes, create an artifact pointing to the asset file."""
        for ne in job.node_executions:
            if ne.node_type != "source":
                continue

            # Skip already resolved source nodes (idempotent for restart recovery)
            if ne.status == NodeStatus.SUCCEEDED and ne.output_artifact_id:
                continue

            asset_id_str = ne.node_config.get("asset_id")
            if not asset_id_str:
                ne.status = NodeStatus.FAILED
                ne.error_message = "No asset_id configured"
                ne.completed_at = datetime.utcnow()
                await db.commit()
                continue

            asset = await db.get(Asset, uuid.UUID(asset_id_str))
            if not asset:
                ne.status = NodeStatus.FAILED
                ne.error_message = f"Asset {asset_id_str} not found"
                ne.completed_at = datetime.utcnow()
                await db.commit()
                continue

            # Create an artifact that references the asset's storage path
            artifact = Artifact(
                job_id=job.id,
                node_execution_id=ne.id,
                kind=ArtifactKind.INTERMEDIATE,
                filename=asset.filename,
                mime_type=asset.mime_type,
                file_size=asset.file_size,
                storage_backend=asset.storage_backend,
                storage_path=asset.storage_path,
                media_info={
                    **(asset.media_info or {}),
                    "source_asset_id": str(asset.id),
                    "asset_id": str(asset.id),
                    "original_name": asset.original_name,
                },
            )
            db.add(artifact)
            await db.flush()

            ne.status = NodeStatus.SUCCEEDED
            ne.output_artifact_id = artifact.id
            ne.started_at = datetime.utcnow()
            ne.completed_at = datetime.utcnow()
            ne.progress = 100

        await db.commit()

    async def _dispatch_ready_nodes(
        self, db: AsyncSession, job: Job, dep_map: dict[str, list[str]]
    ) -> None:
        """Find nodes whose dependencies are all satisfied and dispatch them."""
        # Don't dispatch if job is cancelled
        if job.status == JobStatus.CANCELLED:
            return

        ne_by_node_id = {ne.node_id: ne for ne in job.node_executions}

        r = _redis()
        try:
            for node_id, deps in dep_map.items():
                ne = ne_by_node_id.get(node_id)
                if not ne or ne.status != NodeStatus.PENDING:
                    continue

                # Check all upstream nodes are SUCCEEDED
                all_deps_done = all(
                    ne_by_node_id.get(dep_id) and ne_by_node_id[dep_id].status == NodeStatus.SUCCEEDED
                    for dep_id in deps
                )
                if not all_deps_done:
                    continue

                # Resolve input artifacts from upstream nodes
                definition = PipelineDefinition.model_validate(job.pipeline_snapshot)
                input_artifacts = {}
                preferred_hosts = self._preferred_hosts_for_node(ne_by_node_id, deps)
                for edge in definition.edges:
                    if edge.target == node_id:
                        upstream_ne = ne_by_node_id.get(edge.source)
                        if upstream_ne and upstream_ne.output_artifact_id:
                            input_artifacts[edge.targetHandle] = str(upstream_ne.output_artifact_id)
                input_artifact_objects = await self._input_artifacts_by_handle(db, input_artifacts)
                if await self._apply_cached_artifact_if_available(db, job, ne, input_artifact_objects):
                    continue

                ne.status = NodeStatus.QUEUED
                ne.queued_at = datetime.utcnow()
                ne.input_artifact_ids = [
                    uuid.UUID(aid) for aid in input_artifacts.values()
                ]
                await db.commit()

                # Determine worker_type from node registry
                registry = NodeTypeRegistry.get()
                node_def = registry.get_type(ne.node_type)
                worker_type = node_def.worker_type if node_def else "ffmpeg"

                # Push task to Redis Stream
                task = {
                    "job_id": str(job.id),
                    "node_execution_id": str(ne.id),
                    "node_id": ne.node_id,
                    "node_type": ne.node_type,
                    "config": json.dumps(ne.node_config),
                    "input_artifacts": json.dumps(input_artifacts),
                    "preferred_hosts": json.dumps(preferred_hosts),
                    "affinity_enqueued_at": str(int(time.time())),
                    "affinity_bounces": "0",
                }
                stream_key = TASK_STREAM.format(worker_type=worker_type)
                await r.xadd(stream_key, task)
                logger.info(
                    "Dispatched node %s (type=%s) to %s for job %s with preferred_hosts=%s",
                    ne.node_id, ne.node_type, stream_key, job.id, preferred_hosts,
                )
        finally:
            await r.aclose()
        await self._maybe_finalize_job(db, job)

    async def _apply_cached_artifact_if_available(
        self,
        db: AsyncSession,
        job: Job,
        ne: NodeExecution,
        input_artifacts: dict[str, Artifact],
    ) -> bool:
        if not input_artifacts:
            return False
        try:
            entry = await self.artifact_cache.lookup(
                db,
                node_type=ne.node_type,
                node_config=ne.node_config or {},
                input_artifacts=input_artifacts,
            )
        except Exception:
            logger.exception("Artifact cache lookup failed for job=%s node=%s", job.id, ne.node_id)
            return False
        if entry is None:
            return False

        ne.status = NodeStatus.SUCCEEDED
        ne.started_at = ne.started_at or datetime.utcnow()
        ne.completed_at = datetime.utcnow()
        ne.progress = 100
        ne.output_artifact_id = entry.output_artifact_id
        ne.input_artifact_ids = [artifact.id for artifact in input_artifacts.values()]
        await self.artifact_cache.record_hit(db, entry)
        await db.commit()
        logger.info(
            "Reused cached artifact for job=%s node=%s artifact=%s",
            job.id,
            ne.node_id,
            entry.output_artifact_id,
        )
        return True

    async def _input_artifacts_by_handle(
        self,
        db: AsyncSession,
        input_artifact_ids: dict[str, str],
    ) -> dict[str, Artifact]:
        input_artifacts: dict[str, Artifact] = {}
        for handle, artifact_id in input_artifact_ids.items():
            artifact = await db.get(Artifact, uuid.UUID(str(artifact_id)))
            if artifact:
                input_artifacts[handle] = artifact
        return input_artifacts

    async def _input_artifacts_for_node(
        self,
        db: AsyncSession,
        job: Job,
        ne: NodeExecution,
    ) -> dict[str, Artifact]:
        ne_by_node_id = {node.node_id: node for node in job.node_executions}
        definition = PipelineDefinition.model_validate(job.pipeline_snapshot)
        input_artifact_ids: dict[str, str] = {}
        for edge in definition.edges:
            if edge.target != ne.node_id:
                continue
            upstream_ne = ne_by_node_id.get(edge.source)
            if upstream_ne and upstream_ne.output_artifact_id:
                input_artifact_ids[edge.targetHandle] = str(upstream_ne.output_artifact_id)
        if input_artifact_ids:
            return await self._input_artifacts_by_handle(db, input_artifact_ids)

        fallback_artifact_ids = list(ne.input_artifact_ids or [])
        if not fallback_artifact_ids:
            return {}
        fallback_handles = {
            ("input" if index == 0 else f"input_{index + 1}"): str(artifact_id)
            for index, artifact_id in enumerate(fallback_artifact_ids)
        }
        return await self._input_artifacts_by_handle(db, fallback_handles)

    async def _write_artifact_cache_for_node(
        self,
        db: AsyncSession,
        job: Job,
        ne: NodeExecution,
    ) -> None:
        if not ne.output_artifact_id:
            return
        output_artifact = await db.get(Artifact, ne.output_artifact_id)
        if not output_artifact:
            return
        input_artifacts = await self._input_artifacts_for_node(db, job, ne)
        await self.artifact_cache.store(
            db,
            node_type=ne.node_type,
            node_config=ne.node_config or {},
            input_artifacts=input_artifacts,
            output_artifact=output_artifact,
            node_id=ne.node_id,
            job_id=job.id,
        )

    @staticmethod
    def _preferred_hosts_for_node(
        ne_by_node_id: dict[str, NodeExecution],
        deps: list[str],
    ) -> list[str]:
        counts: Counter[str] = Counter()
        for dep_id in deps:
            upstream_ne = ne_by_node_id.get(dep_id)
            host = _extract_worker_host(upstream_ne.worker_id if upstream_ne else None)
            if host:
                counts[host] += 1
        if not counts:
            return []
        ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        top_count = ranked[0][1]
        return [host for host, count in ranked if count == top_count]

    async def on_node_completed(
        self, job_id: uuid.UUID, node_execution_id: uuid.UUID, output_artifact_id: uuid.UUID
    ) -> None:
        """Handle a node completion event: update status, dispatch downstream."""
        async with async_session() as db:
            job = await db.get(Job, job_id, options=[selectinload(Job.node_executions)])
            if not job:
                return

            # If job was cancelled, ignore the completion event
            if job.status == JobStatus.CANCELLED:
                logger.info(f"Job {job_id} cancelled, ignoring node completion")
                return

            ne = next((n for n in job.node_executions if n.id == node_execution_id), None)
            if not ne:
                return

            ne.status = NodeStatus.SUCCEEDED
            ne.output_artifact_id = output_artifact_id
            ne.completed_at = datetime.utcnow()
            ne.progress = 100
            await db.commit()
            try:
                await self._write_artifact_cache_for_node(db, job, ne)
                await db.commit()
            except Exception:
                await db.rollback()
                logger.exception(
                    "Failed to write artifact cache for job=%s node=%s",
                    job.id,
                    ne.node_id,
                )

            if await self._maybe_finalize_job(db, job):
                return

            # Dispatch newly ready downstream nodes
            dep_map = job.execution_plan.get("dependencies", {}) if job.execution_plan else {}
            await self._dispatch_ready_nodes(db, job, dep_map)

    async def on_node_failed(
        self, job_id: uuid.UUID, node_execution_id: uuid.UUID, error: str
    ) -> None:
        """Handle a node failure event."""
        async with async_session() as db:
            job = await db.get(Job, job_id, options=[selectinload(Job.node_executions)])
            if not job:
                return

            # If job was cancelled, ignore the failure event
            if job.status == JobStatus.CANCELLED:
                logger.info(f"Job {job_id} cancelled, ignoring node failure")
                return

            ne = next((n for n in job.node_executions if n.id == node_execution_id), None)
            if not ne:
                return

            # Basic retry: retry once
            if ne.retry_count < 1:
                ne.retry_count += 1
                ne.status = NodeStatus.QUEUED
                ne.error_message = None
                ne.queued_at = datetime.utcnow()
                await db.commit()

                # Re-dispatch
                r = _redis()
                try:
                    definition = PipelineDefinition.model_validate(job.pipeline_snapshot)
                    ne_by_node_id = {n.node_id: n for n in job.node_executions}
                    input_artifacts = {}
                    dep_map = job.execution_plan.get("dependencies", {}) if job.execution_plan else {}
                    deps = dep_map.get(ne.node_id, [])
                    preferred_hosts = self._preferred_hosts_for_node(ne_by_node_id, deps)
                    for edge in definition.edges:
                        if edge.target == ne.node_id:
                            upstream_ne = ne_by_node_id.get(edge.source)
                            if upstream_ne and upstream_ne.output_artifact_id:
                                input_artifacts[edge.targetHandle] = str(upstream_ne.output_artifact_id)

                    registry = NodeTypeRegistry.get()
                    node_def = registry.get_type(ne.node_type)
                    worker_type = node_def.worker_type if node_def else "ffmpeg"

                    task = {
                        "job_id": str(job.id),
                        "node_execution_id": str(ne.id),
                        "node_id": ne.node_id,
                        "node_type": ne.node_type,
                        "config": json.dumps(ne.node_config),
                        "input_artifacts": json.dumps(input_artifacts),
                        "preferred_hosts": json.dumps(preferred_hosts),
                        "affinity_enqueued_at": str(int(time.time())),
                        "affinity_bounces": "0",
                    }
                    stream_key = TASK_STREAM.format(worker_type=worker_type)
                    await r.xadd(stream_key, task)
                finally:
                    await r.aclose()
                logger.info(f"Retrying node {ne.node_id} for job {job_id} (attempt {ne.retry_count})")
                return

            # Max retries exhausted
            ne.status = NodeStatus.FAILED
            ne.error_message = error
            ne.completed_at = datetime.utcnow()
            await db.commit()

            # Skip downstream nodes
            dep_map = job.execution_plan.get("dependencies", {}) if job.execution_plan else {}
            await self._skip_downstream(db, job, ne.node_id, dep_map)

            job.error_message = f"Node '{ne.node_label}' failed: {error}"
            await db.commit()
            await self._maybe_finalize_job(db, job)

    async def _skip_downstream(
        self, db: AsyncSession, job: Job, failed_node_id: str, dep_map: dict
    ) -> None:
        """Skip all nodes that depend (directly or transitively) on a failed node."""
        # Build reverse map: node -> downstream nodes
        downstream: dict[str, list[str]] = {}
        for node_id, deps in dep_map.items():
            for dep in deps:
                downstream.setdefault(dep, []).append(node_id)

        # BFS to find all transitive downstream nodes
        to_skip = set()
        queue = list(downstream.get(failed_node_id, []))
        while queue:
            nid = queue.pop(0)
            if nid in to_skip:
                continue
            to_skip.add(nid)
            queue.extend(downstream.get(nid, []))

        ne_by_node_id = {n.node_id: n for n in job.node_executions}
        for nid in to_skip:
            ne = ne_by_node_id.get(nid)
            if ne and ne.status == NodeStatus.PENDING:
                ne.status = NodeStatus.SKIPPED
                ne.completed_at = datetime.utcnow()

        await db.commit()

    async def _mark_final_artifacts(self, db: AsyncSession, job: Job) -> None:
        """Mark output artifacts of terminal nodes as FINAL."""
        definition = PipelineDefinition.model_validate(job.pipeline_snapshot)
        terminal_node_ids = _leaf_node_ids(definition)

        for ne in job.node_executions:
            if ne.node_id in terminal_node_ids and ne.status == NodeStatus.SUCCEEDED and ne.output_artifact_id:
                artifact = await db.get(Artifact, ne.output_artifact_id)
                if artifact:
                    artifact.kind = ArtifactKind.FINAL
        await db.commit()


# Singleton
engine = JobEngine()
