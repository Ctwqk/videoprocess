#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlsplit

import httpx
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.channel_agent.constants import TERMINAL_TASK_STATES  # noqa: E402
from app.channel_agent.queue import ChannelOpsQueueService, utc_hour_bucket  # noqa: E402
from app.models.asset import Asset  # noqa: E402
from app.models.channel_agent import (  # noqa: E402
    ChannelOpsQueueItem,
    ChannelProfile,
    FeedbackSnapshot,
    LaneFormatMatrix,
    ManualSeed,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TopicLane,
)
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus  # noqa: E402
from app.models.youtube_upload_operation import YouTubeUploadOperation  # noqa: E402


ADVISORY_LOCK_KEY = 8_537_601_337_126
CANARY_APPROVAL_REASON = "operator_preapproved_live_unlisted_canary"
CANARY_FAILURE_REASON = "operator_canary_failure"
NO_DELETE_POLICY = "This runner never deletes the YouTube video automatically."
SCHEDULE_STATUS_PATH = "/internal/schedule/video/status"
SCHEDULE_OPEN_PATH = "/internal/schedule/video/open"
SCHEDULE_DRAIN_PATH = "/internal/schedule/video/drain"
SCHEDULE_CLOSE_PATH = "/internal/schedule/video/close"
RUNNABLE_JOB_STATUSES = {
    JobStatus.PENDING,
    JobStatus.WAITING_WINDOW,
    JobStatus.VALIDATING,
    JobStatus.PLANNING,
    JobStatus.RUNNING,
}
ACTIVE_NODE_STATUSES = {NodeStatus.PENDING, NodeStatus.QUEUED, NodeStatus.RUNNING}
RECOGNIZED_METRIC_KEYS = {
    "views",
    "likes",
    "comments",
    "shares",
    "avg_view_duration_sec",
    "retention_curve_json",
    "retention_curve",
    "ctr",
    "impressions",
    "virality_score",
}
SENSITIVE_KEY_PARTS = {
    "authorization",
    "cookie",
    "credential",
    "database_url",
    "password",
    "refresh_token",
    "secret",
    "token",
}
CONNECTION_URL_PATTERN = re.compile(
    r"(?i)(?:postgres(?:ql)?(?:\+asyncpg)?|rediss?)://[^\s'\"<>]+"
)
READINESS_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


class CanaryError(RuntimeError):
    pass


class CanaryInterrupted(CanaryError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run exactly one guarded live unlisted YouTube canary")
    parser.add_argument("--confirm-live-unlisted", action="store_true", default=False)
    parser.add_argument("--api-url", default="http://10.0.0.127:18080")
    parser.add_argument("--youtube-manager-url", default="http://10.0.0.150:18999")
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", ""))
    parser.add_argument("--runtime-host", default="10.0.0.127")
    parser.add_argument("--manager-host", default="10.0.0.150")
    parser.add_argument("--publisher-service", default="vp-youtube-publisher-swarm")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=1_200.0)
    return parser.parse_args()


def async_database_url(value: str) -> str:
    if value.startswith("postgresql+asyncpg://"):
        return value
    if value.startswith("postgresql://"):
        return value.replace("postgresql://", "postgresql+asyncpg://", 1)
    if value.startswith("postgres://"):
        return value.replace("postgres://", "postgresql+asyncpg://", 1)
    return value


def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            normalized = str(key).casefold()
            if any(part in normalized for part in SENSITIVE_KEY_PARTS):
                continue
            sanitized[str(key)] = sanitize(item)
        return sanitized
    if isinstance(value, (list, tuple, set)):
        return [sanitize(item) for item in value]
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str) and value.casefold().startswith(
        ("postgres://", "postgresql://", "postgresql+asyncpg://", "redis://", "rediss://")
    ):
        return "[redacted connection URL]"
    if isinstance(value, str):
        return CONNECTION_URL_PATTERN.sub("[redacted connection URL]", value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def safe_failure_message(exc: BaseException) -> str:
    if isinstance(exc, CanaryError):
        return str(sanitize(str(exc)))[:1_000]
    return "unexpected failure; inspect sanitized service logs by exception type"


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    created_parent = False
    if not path.parent.exists():
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=False)
            created_parent = True
        except FileExistsError:
            pass
    if created_parent:
        os.chmod(path.parent, 0o700)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(sanitize(payload), handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        os.chmod(path, 0o600)
    except BaseException:
        Path(temporary_name).unlink(missing_ok=True)
        raise


def run_readonly_command(command: list[str], *, timeout_seconds: float = 30.0) -> str:
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CanaryError(f"read-only readiness command failed: {command[0]} ({type(exc).__name__})") from exc
    return completed.stdout.strip()


async def request_json(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> dict[str, Any]:
    try:
        response = await client.request(method, url, **kwargs)
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        path = urlsplit(url).path
        raise CanaryError(f"{method} {path} failed ({type(exc).__name__})") from exc
    if not isinstance(payload, dict):
        raise CanaryError(f"{method} {urlsplit(url).path} returned non-object JSON")
    return payload


def api_root(value: str) -> str:
    base = value.rstrip("/")
    return base.removesuffix("/api/v1")


def manager_root(value: str) -> str:
    return value.rstrip("/").removesuffix("/api")


def evidence_path(args: argparse.Namespace, run_id: str) -> Path:
    return args.evidence or ROOT / ".runtime" / "youtube-canary" / f"unlisted-canary-{run_id}.json"


async def deployment_readiness(args: argparse.Namespace, client: httpx.AsyncClient) -> dict[str, Any]:
    source_commit = await asyncio.to_thread(
        run_readonly_command,
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
    )
    deployed_commit = await asyncio.to_thread(
        run_readonly_command,
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            args.runtime_host,
            "tr -d '\\n' < /Users/wenjieliu/VideoProcess-app/.deploy-sync-source-commit",
        ],
    )
    if not source_commit or source_commit != deployed_commit:
        raise CanaryError("source/deployed commit mismatch")

    service_row = await asyncio.to_thread(
        run_readonly_command,
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            args.manager_host,
            f"docker service ls --filter name={args.publisher_service} --format '{{{{.Name}}}}|{{{{.Replicas}}}}'",
        ],
    )
    if service_row != f"{args.publisher_service}|1/1":
        raise CanaryError("deployed publisher service is not exactly 1/1")
    publisher_image = await asyncio.to_thread(
        run_readonly_command,
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            args.manager_host,
            f"docker service inspect {args.publisher_service} --format '{{{{.Spec.TaskTemplate.ContainerSpec.Image}}}}'",
        ],
    )
    expected_image_tag = f":deploy-{source_commit[:12]}"
    if expected_image_tag not in publisher_image.split("@", 1)[0]:
        raise CanaryError("deployed publisher image does not match the source commit")
    constraints = await asyncio.to_thread(
        run_readonly_command,
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            args.manager_host,
            (
                f"docker service inspect {args.publisher_service} "
                "--format '{{range .Spec.TaskTemplate.Placement.Constraints}}{{println .}}{{end}}'"
            ),
        ],
    )
    constraint_rows = {row.strip() for row in constraints.splitlines() if row.strip()}
    if "node.labels.vp.publisher==true" not in constraint_rows or "node.hostname==ccttww-lap" not in constraint_rows:
        raise CanaryError("publisher placement constraints are not ready")
    await request_json(client, "GET", f"{api_root(args.api_url)}/health")
    return {
        "source_commit": source_commit,
        "deployed_commit": deployed_commit,
        "publisher_service": args.publisher_service,
        "publisher_replicas": "1/1",
        "publisher_image": publisher_image,
        "publisher_expected_commit_tag": expected_image_tag,
        "publisher_constraints": sorted(constraint_rows),
    }


def quota_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    quota = payload.get("quota_estimate")
    if payload.get("authenticated") is not True or not isinstance(quota, dict):
        raise CanaryError("YouTubeManager is not authenticated or has no quota estimate")
    fields = {
        key: quota.get(key)
        for key in (
            "daily_limit",
            "estimated_units_used",
            "estimated_units_remaining",
            "upload_cost_per_request",
        )
    }
    numeric: dict[str, float] = {}
    for key, value in fields.items():
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) < 0
        ):
            raise CanaryError("YouTubeManager quota estimate is malformed")
        numeric[key] = float(value)
    upload_cost = numeric["upload_cost_per_request"]
    if (
        upload_cost < 1_600
        or numeric["daily_limit"] < upload_cost
        or numeric["estimated_units_remaining"] < upload_cost
    ):
        raise CanaryError("YouTubeManager quota is below the 1600-unit canary minimum")
    return {"authenticated": True, "quota_estimate": numeric}


async def manager_readiness(args: argparse.Namespace, client: httpx.AsyncClient) -> dict[str, Any]:
    payload = await request_json(client, "GET", f"{manager_root(args.youtube_manager_url)}/api/auth/status")
    return quota_evidence(payload)


async def schedule_status(args: argparse.Namespace, client: httpx.AsyncClient) -> dict[str, Any]:
    return await request_json(client, "GET", f"{api_root(args.api_url)}{SCHEDULE_STATUS_PATH}")


def record_schedule(evidence: dict[str, Any], action: str, payload: dict[str, Any]) -> None:
    evidence.setdefault("schedule", {}).setdefault("transitions", []).append(
        {
            "action": action,
            "observed_at": utc_now().isoformat(),
            "state": payload.get("state"),
            "waiting_jobs": payload.get("waiting_jobs"),
            "active_jobs": payload.get("active_jobs"),
            "queued_nodes": payload.get("queued_nodes"),
            "running_nodes": payload.get("running_nodes"),
        }
    )


async def close_schedule(
    args: argparse.Namespace,
    client: httpx.AsyncClient,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    payload = await request_json(client, "POST", f"{api_root(args.api_url)}{SCHEDULE_CLOSE_PATH}")
    record_schedule(evidence, "close", payload)
    if payload.get("state") != "CLOSED":
        raise CanaryError("video schedule did not close")
    evidence.setdefault("schedule", {})["final_state"] = "CLOSED"
    return payload


async def mutate_schedule(
    args: argparse.Namespace,
    client: httpx.AsyncClient,
    evidence: dict[str, Any],
    action: str,
) -> dict[str, Any]:
    path = {"open": SCHEDULE_OPEN_PATH, "drain": SCHEDULE_DRAIN_PATH}.get(action)
    if path is None:
        raise CanaryError(f"unsupported schedule action: {action}")
    payload = await request_json(client, "POST", f"{api_root(args.api_url)}{path}")
    record_schedule(evidence, action, payload)
    return payload


async def active_backlog(
    db: AsyncSession,
    *,
    allowed_channel_id: uuid.UUID | None = None,
) -> dict[str, list[str]]:
    job_stmt = select(Job.id).where(Job.status.in_(RUNNABLE_JOB_STATUSES))
    queue_stmt = select(ChannelOpsQueueItem.id).where(ChannelOpsQueueItem.status.in_(("queued", "running")))
    publication_task_ids = select(PublicationRecord.production_task_id)
    task_stmt = (
        select(ProductionTask.id)
        .where(ProductionTask.state.not_in((*TERMINAL_TASK_STATES, "held")))
        .where(ProductionTask.id.not_in(publication_task_ids))
    )
    if allowed_channel_id is not None:
        queue_stmt = queue_stmt.where(
            (ChannelOpsQueueItem.channel_profile_id.is_(None))
            | (ChannelOpsQueueItem.channel_profile_id != allowed_channel_id)
        )
        task_stmt = task_stmt.where(ProductionTask.channel_profile_id != allowed_channel_id)
    jobs = sorted(str(value) for value in (await db.scalars(job_stmt)).all())
    queues = sorted(str(value) for value in (await db.scalars(queue_stmt)).all())
    tasks = sorted(str(value) for value in (await db.scalars(task_stmt)).all())
    await db.commit()
    return {"runnable_job_ids": jobs, "unsafe_queue_item_ids": queues, "unsafe_task_ids": tasks}


def assert_no_preexisting_backlog(report: dict[str, list[str]]) -> None:
    if report["runnable_job_ids"]:
        raise CanaryError("pre-existing runnable jobs remain after quarantine")
    if report["unsafe_queue_item_ids"] or report["unsafe_task_ids"]:
        raise CanaryError("unsafe ChannelOps backlog remains after quarantine")


def generate_owned_video(path: Path, *, duration_seconds: int) -> dict[str, Any]:
    if duration_seconds != 8:
        raise CanaryError("the owned canary must be exactly 8 seconds")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"color=c=0x12212b:s=1080x1920:r=30:d={duration_seconds}",
        "-vf",
        (
            "drawgrid=width=135:height=240:thickness=3:color=0x5bc0be@0.45,"
            "drawbox=x=120:y=240:w=840:h=1440:color=0xf4d35e@0.80:t=24,"
            "drawbox=x=240:y=720:w=600:h=480:color=0xee6c4d@0.90:t=fill"
        ),
        "-t",
        str(duration_seconds),
        "-an",
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-pix_fmt",
        "yuv420p",
        "-r",
        "30",
        "-g",
        "60",
        "-threads",
        "1",
        "-map_metadata",
        "-1",
        "-fflags",
        "+bitexact",
        "-flags:v",
        "+bitexact",
        "-movflags",
        "+faststart",
        str(path),
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, timeout=300)
        probe = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height:format=duration",
                "-of",
                "json",
                str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        probe_payload = json.loads(probe.stdout)
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        raise CanaryError(f"deterministic FFmpeg generation failed ({type(exc).__name__})") from exc
    stream = (probe_payload.get("streams") or [{}])[0]
    duration = float((probe_payload.get("format") or {}).get("duration") or 0)
    if stream.get("width") != 1080 or stream.get("height") != 1920 or abs(duration - 8.0) > 0.05:
        raise CanaryError("generated media failed the 8s 1080x1920 probe")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        "path": str(path),
        "duration_seconds": duration_seconds,
        "width": 1080,
        "height": 1920,
        "content_sha256": digest,
        "file_size": path.stat().st_size,
        "generator": "ffmpeg_lavfi_owned_v1",
    }


async def upload_and_attest_asset(
    args: argparse.Namespace,
    client: httpx.AsyncClient,
    db: AsyncSession,
    media_path: Path,
    media: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    with media_path.open("rb") as handle:
        uploaded = await request_json(
            client,
            "POST",
            f"{api_root(args.api_url)}/api/v1/assets/upload",
            files={"file": (media_path.name, handle, "video/mp4")},
            timeout=180,
        )
    asset_id = uuid.UUID(str(uploaded["id"]))
    asset = await db.get(Asset, asset_id)
    if asset is None:
        raise CanaryError("uploaded asset was not visible in PostgreSQL")
    asset.media_info = {
        **dict(asset.media_info or {}),
        "license": "owned",
        "provenance": "generated",
        "generated_at": generated_at,
        "content_sha256": media["content_sha256"],
    }
    await db.commit()
    db.expire_all()
    verified = await db.get(Asset, asset_id)
    expected = {
        "license": "owned",
        "provenance": "generated",
        "generated_at": generated_at,
        "content_sha256": media["content_sha256"],
    }
    if verified is None or any((verified.media_info or {}).get(key) != value for key, value in expected.items()):
        raise CanaryError("owned asset provenance did not survive a database re-read")
    await db.commit()
    return {"id": str(asset_id), **expected, "filename": uploaded.get("filename")}


async def create_canary_graph(
    db: AsyncSession,
    run_id: str,
    asset_id: str,
) -> dict[str, str]:
    try:
        owned_asset_id = uuid.UUID(asset_id)
    except ValueError as exc:
        raise CanaryError("owned input asset ID is invalid") from exc

    queue = ChannelOpsQueueService()
    async with db.begin():
        asset = await db.get(Asset, owned_asset_id)
        media_info = asset.media_info if asset is not None and isinstance(asset.media_info, dict) else {}
        if (
            asset is None
            or not isinstance(asset.mime_type, str)
            or not asset.mime_type.startswith("video/")
            or media_info.get("license") != "owned"
            or media_info.get("provenance") != "generated"
        ):
            raise CanaryError("input asset must be an owned generated video")

        channel = ChannelProfile(
            name=f"youtube-unlisted-canary-{run_id}",
            positioning="Owned generated VideoProcess unlisted canary",
            language="en",
            default_aspect_ratio="9:16",
            risk_policy_json={"publication_privacy": "unlisted", "external_sources": False},
            content_mix_policy_json={"manual_seed_only": True},
            cadence_policy_json={"max_posts_per_day": 1, "max_posts_per_tick": 1},
            alert_policy_json={},
            enabled=True,
            dry_run=False,
        )
        db.add(channel)
        await db.flush()

        lane = TopicLane(
            channel_profile_id=channel.id,
            name=f"owned-canary-{run_id}",
            description="One owned generated vertical canary",
            weight=1.0,
            keywords_json=["videoprocess", "canary"],
            negative_keywords_json=[],
            min_posts_per_week=0,
            max_posts_per_day=1,
            max_consecutive_streak=1,
            cooldown_after_post_minutes=1_440,
        )
        account = PublishingAccount(
            channel_profile_id=channel.id,
            account_label=f"youtube-unlisted-canary-{run_id}",
            platform="youtube",
            platform_account_id="",
            credential_ref="",
            platform_specific_config_json={"canary_run_id": run_id},
            default_privacy="unlisted",
            external_asset_auto_publish=False,
        )
        db.add_all((lane, account))
        await db.flush()

        lane_format = LaneFormatMatrix(
            topic_lane_id=lane.id,
            format_key=f"owned_unlisted_9x16_{run_id}",
            enabled=True,
            weight=1.0,
            target_duration_sec=8,
            template_pool_json=["material_library_remix"],
            source_platforms_json=[],
            default_publish_visibility="unlisted",
        )
        seed = ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="Create one deterministic eight-second owned vertical canary with no external media.",
            title_seed=f"VideoProcess Unlisted Canary {run_id[:8]}",
            source_policy="owned_only",
            source_platforms_json=[],
            material_library_ids_json=[],
            constraints_json={
                "input_asset_id": asset_id,
                "source_strategy": "input_video",
                "planning_mode": "template",
                "target_duration": 8,
            },
        )
        db.add_all((lane_format, seed))
        tick = await queue.enqueue(
            db,
            kind="agent_tick",
            idempotency_key=f"agent_tick:{channel.id}:{utc_hour_bucket(utc_now())}",
            payload={"channel_id": str(channel.id)},
            priority=20,
            channel_profile_id=channel.id,
            commit=False,
        )

    return {
        "channel_id": str(channel.id),
        "lane_id": str(lane.id),
        "account_id": str(account.id),
        "lane_format_id": str(lane_format.id),
        "manual_seed_id": str(seed.id),
        "agent_tick_id": str(tick.id),
    }


async def poll_until(
    check: Callable[[], Awaitable[Any]],
    *,
    timeout_seconds: float,
    description: str,
    interval_seconds: float = 0.2,
) -> Any:
    deadline = time.monotonic() + timeout_seconds
    while True:
        value = await check()
        if value is not None:
            return value
        if time.monotonic() >= deadline:
            raise CanaryError(f"timed out waiting for {description}")
        await asyncio.sleep(interval_seconds)


async def preapprove_exactly_one_task(
    db: AsyncSession,
    channel_id: uuid.UUID,
    run_id: str,
) -> tuple[ProductionTask, ChannelOpsQueueItem]:
    now = utc_now()
    async with db.begin():
        tasks = list(
            (
                await db.execute(
                    select(ProductionTask)
                    .where(ProductionTask.channel_profile_id == channel_id)
                    .with_for_update()
                )
            ).scalars()
        )
        if not tasks:
            raise CanaryError("canary task disappeared during preapproval")
        if len(tasks) != 1:
            raise CanaryError("more than one canary task exists; refusing approval")
        task = tasks[0]
        plan_items = list(
            (
                await db.execute(
                    select(ChannelOpsQueueItem)
                    .where(ChannelOpsQueueItem.channel_profile_id == channel_id)
                    .where(ChannelOpsQueueItem.kind == "plan_task")
                    .with_for_update()
                )
            ).scalars()
        )
        plan_items = [
            item
            for item in plan_items
            if str((item.payload_json or {}).get("production_task_id")) == str(task.id)
        ]
        if len(plan_items) != 1 or plan_items[0].status != "queued":
            raise CanaryError("plan processing won the preapproval race; canary is fail-closed")
        plan_item = plan_items[0]
        channel = (
            await db.execute(select(ChannelProfile).where(ChannelProfile.id == channel_id).with_for_update())
        ).scalar_one()
        channel.halted_at = now
        channel.halt_reason = CANARY_APPROVAL_REASON
        task.approval_mode = "agent"
        task.agent_approval_evidence_json = {
            "approved_by": "operator",
            "approval_type": "explicit_cli_confirmation",
            "confirmation": "--confirm-live-unlisted",
            "reason": CANARY_APPROVAL_REASON,
            "recorded_at": now.isoformat(),
            "canary_run_id": run_id,
        }
        plan_item.run_after = now
    return task, plan_item


async def wait_for_single_task_and_preapprove(
    db: AsyncSession,
    channel_id: uuid.UUID,
    run_id: str,
    timeout_seconds: float,
) -> tuple[ProductionTask, ChannelOpsQueueItem]:
    async def check() -> tuple[ProductionTask, ChannelOpsQueueItem] | None:
        task_ids = list(
            await db.scalars(select(ProductionTask.id).where(ProductionTask.channel_profile_id == channel_id))
        )
        await db.commit()
        if len(task_ids) > 1:
            raise CanaryError("more than one canary task exists; refusing approval")
        if not task_ids:
            return None
        return await preapprove_exactly_one_task(db, channel_id, run_id)

    return await poll_until(
        check,
        timeout_seconds=timeout_seconds,
        description="one canary task before the runner's 5s plan loop",
        interval_seconds=0.05,
    )


async def wait_for_waiting_job(
    args: argparse.Namespace,
    client: httpx.AsyncClient,
    db: AsyncSession,
    task_id: uuid.UUID,
    timeout_seconds: float,
) -> tuple[ProductionTask, Job]:
    async def check() -> tuple[ProductionTask, Job] | None:
        db.expire_all()
        task = await db.get(ProductionTask, task_id)
        if task is None:
            raise CanaryError("canary task disappeared")
        if task.state in {"failed", "cancelled", "rejected", "held"}:
            raise CanaryError(f"canary task became {task.state} before WAITING_WINDOW")
        job = await db.get(Job, task.job_id) if task.job_id else None
        await db.commit()
        status = await schedule_status(args, client)
        if status.get("state") != "CLOSED":
            raise CanaryError("global video schedule opened before the canary gate")
        if job is None:
            return None
        if job.status == JobStatus.WAITING_WINDOW:
            return task, job
        if job.status in {JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.PARTIALLY_FAILED, JobStatus.SUCCEEDED}:
            raise CanaryError(f"canary job reached {job.status.value} before WAITING_WINDOW")
        return None

    return await poll_until(
        check,
        timeout_seconds=timeout_seconds,
        description="canary plan/job WAITING_WINDOW",
    )


async def assert_open_gate(
    db: AsyncSession,
    *,
    channel_id: uuid.UUID,
    job_id: uuid.UUID,
) -> dict[str, Any]:
    report = await active_backlog(db, allowed_channel_id=channel_id)
    runnable_ids = list(await db.scalars(select(Job.id).where(Job.status.in_(RUNNABLE_JOB_STATUSES))))
    publish_nodes = list(
        await db.scalars(
            select(NodeExecution.id)
            .where(NodeExecution.node_type == "youtube_upload")
            .where(NodeExecution.status.in_((NodeStatus.QUEUED, NodeStatus.RUNNING)))
        )
    )
    publish_queue = list(
        await db.scalars(
            select(ChannelOpsQueueItem.id)
            .where(ChannelOpsQueueItem.kind.in_(("publish_task", "promote_publication")))
            .where(ChannelOpsQueueItem.status.in_(("queued", "running")))
        )
    )
    await db.commit()
    if set(runnable_ids) != {job_id}:
        raise CanaryError("exactly one runnable job is required and it must be the canary")
    if report["unsafe_queue_item_ids"] or report["unsafe_task_ids"]:
        raise CanaryError("unsafe ChannelOps backlog appeared before schedule open")
    if publish_nodes or publish_queue:
        raise CanaryError("queued youtube publish work exists before schedule open")
    return {
        **report,
        "runnable_job_ids": sorted(str(item) for item in runnable_ids),
        "queued_youtube_node_ids": sorted(str(item) for item in publish_nodes),
        "queued_youtube_queue_item_ids": sorted(str(item) for item in publish_queue),
    }


async def wait_for_running_job(db: AsyncSession, job_id: uuid.UUID, timeout_seconds: float) -> Job:
    async def check() -> Job | None:
        db.expire_all()
        job = await db.get(Job, job_id)
        await db.commit()
        if job is None:
            raise CanaryError("canary job disappeared")
        if job.status == JobStatus.RUNNING:
            return job
        if job.status in {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.PARTIALLY_FAILED}:
            raise CanaryError(f"canary job reached {job.status.value} before RUNNING was observed")
        return None

    return await poll_until(
        check,
        timeout_seconds=timeout_seconds,
        description="canary job RUNNING",
        interval_seconds=0.1,
    )


async def wait_for_upload_and_publication(
    db: AsyncSession,
    task_id: uuid.UUID,
    timeout_seconds: float,
) -> tuple[YouTubeUploadOperation, PublicationRecord]:
    async def check() -> tuple[YouTubeUploadOperation, PublicationRecord] | None:
        operations = list(
            await db.scalars(
                select(YouTubeUploadOperation).where(YouTubeUploadOperation.production_task_id == task_id)
            )
        )
        publications = list(
            await db.scalars(select(PublicationRecord).where(PublicationRecord.production_task_id == task_id))
        )
        await db.commit()
        if len(operations) > 1 or len(publications) > 1:
            raise CanaryError("canary produced duplicate operation or publication rows")
        if operations and operations[0].status in {"failed", "uncertain"}:
            raise CanaryError(f"YouTube upload operation became {operations[0].status}; no retry will be attempted")
        if not operations or operations[0].status != "succeeded" or not publications:
            return None
        operation = operations[0]
        publication = publications[0]
        if not operation.platform_video_id or operation.platform_video_id != publication.platform_content_id:
            raise CanaryError("operation/publication video IDs do not match")
        if operation.privacy != "unlisted" or publication.desired_privacy != "unlisted":
            raise CanaryError("canary upload/publication privacy is not unlisted")
        return operation, publication

    return await poll_until(
        check,
        timeout_seconds=timeout_seconds,
        description="one durable succeeded upload operation and one publication",
    )


def manager_task_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    candidate = payload.get("result")
    result: dict[str, Any] = candidate if isinstance(candidate, dict) else {}
    return {
        "status": payload.get("status"),
        "video_id": result.get("video_id"),
        "completed": payload.get("status") == "completed",
    }


def manager_video_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    processing = payload.get("upload_status") or payload.get("processing_state") or payload.get("status")
    privacy = payload.get("privacy") or payload.get("current_privacy") or payload.get("privacy_status")
    return {
        "video_id": payload.get("video_id"),
        "processing_status": processing,
        "privacy": str(privacy).lower() if privacy is not None else None,
    }


async def wait_for_manager_ready(
    args: argparse.Namespace,
    client: httpx.AsyncClient,
    manager_task_id: str,
    video_id: str,
    timeout_seconds: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    async def check() -> tuple[dict[str, Any], dict[str, Any]] | None:
        task_payload = await request_json(
            client,
            "GET",
            f"{manager_root(args.youtube_manager_url)}/api/status/{manager_task_id}",
        )
        task_status = manager_task_evidence(task_payload)
        if task_status["status"] == "failed":
            raise CanaryError("YouTubeManager reports the upload task failed")
        if task_status["status"] != "completed":
            return None
        video_payload = await request_json(
            client,
            "GET",
            f"{manager_root(args.youtube_manager_url)}/api/videos/{video_id}/status",
        )
        video_status = manager_video_evidence(video_payload)
        if video_status["processing_status"] not in {"processed", "completed"}:
            return None
        if video_status["privacy"] != "unlisted":
            return None
        return task_status, video_status

    return await poll_until(
        check,
        timeout_seconds=timeout_seconds,
        description="manager processed/completed unlisted status",
        interval_seconds=1.0,
    )


async def replace_auto_promotion_with_immediate(
    db: AsyncSession,
    channel_id: uuid.UUID,
    publication_id: uuid.UUID,
) -> tuple[list[str], ChannelOpsQueueItem]:
    now = utc_now()
    queue = ChannelOpsQueueService()
    immediate_key = f"promote_publication:{publication_id}:unlisted:manual"
    async with db.begin():
        publication = (
            await db.execute(
                select(PublicationRecord)
                .where(PublicationRecord.id == publication_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if publication is None:
            raise CanaryError("canary publication disappeared before promotion")
        task = (
            await db.execute(
                select(ProductionTask)
                .where(ProductionTask.id == publication.production_task_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if task is None or task.channel_profile_id != channel_id:
            raise CanaryError("canary publication task does not belong to its channel")
        if publication.publish_status != "uploaded" or task.state not in {"uploaded_private", "held"}:
            raise CanaryError("publication is not ready for immediate promotion")
        if publication.desired_privacy != "unlisted":
            raise CanaryError("immediate canary promotion must remain unlisted")

        rows = list(
            (
                await db.execute(
                    select(ChannelOpsQueueItem)
                    .where(ChannelOpsQueueItem.channel_profile_id == channel_id)
                    .where(ChannelOpsQueueItem.kind == "promote_publication")
                    .with_for_update()
                )
            ).scalars()
        )
        rows = [
            row
            for row in rows
            if str((row.payload_json or {}).get("publication_id")) == str(publication_id)
        ]
        immediate_rows = [row for row in rows if row.idempotency_key == immediate_key]
        if len(immediate_rows) > 1:
            raise CanaryError("duplicate immediate promotion rows already exist")
        running_automatic = [
            row for row in rows if row.status == "running" and row.idempotency_key != immediate_key
        ]
        if running_automatic:
            raise CanaryError("automatic promotion is already running; refusing duplicate promotion")
        queued_automatic = [
            row for row in rows if row.status == "queued" and row.idempotency_key != immediate_key
        ]
        if not immediate_rows and len(queued_automatic) != 1:
            raise CanaryError("expected exactly one queued automatic promotion to cancel")
        for row in queued_automatic:
            row.status = "cancelled"
            row.last_error = "replaced_by_immediate_unlisted_canary_promotion"
            row.locked_at = None
            row.locked_by = None
            row.dead_letter_at = now
        if immediate_rows:
            immediate = immediate_rows[0]
            if immediate.status not in {"queued", "running", "succeeded"}:
                raise CanaryError(f"existing immediate promotion is {immediate.status}")
        else:
            immediate = await queue.enqueue(
                db,
                kind="promote_publication",
                idempotency_key=immediate_key,
                payload={
                    "publication_id": str(publication.id),
                    "target_visibility": "unlisted",
                    "channel_profile_id": str(task.channel_profile_id),
                },
                priority=70,
                channel_profile_id=task.channel_profile_id,
                commit=False,
            )
    return sorted(str(row.id) for row in queued_automatic), immediate


async def enqueue_metrics_probe(
    db: AsyncSession,
    publication_id: uuid.UUID,
) -> ChannelOpsQueueItem:
    queue = ChannelOpsQueueService()
    async with db.begin():
        publication = await db.get(PublicationRecord, publication_id)
        if publication is None:
            raise CanaryError("canary publication disappeared before metrics enqueue")
        task = await db.get(ProductionTask, publication.production_task_id)
        if task is None:
            raise CanaryError("canary publication task disappeared before metrics enqueue")
        return await queue.enqueue(
            db,
            kind="collect_metrics",
            idempotency_key=f"collect_metrics:{publication.id}:{utc_hour_bucket(utc_now())}",
            payload={"publication_id": str(publication.id)},
            priority=90,
            channel_profile_id=task.channel_profile_id,
            commit=False,
        )


async def wait_for_queue_success(
    db: AsyncSession,
    queue_id: uuid.UUID,
    timeout_seconds: float,
    description: str,
) -> ChannelOpsQueueItem:
    async def check() -> ChannelOpsQueueItem | None:
        db.expire_all()
        row = await db.get(ChannelOpsQueueItem, queue_id)
        await db.commit()
        if row is None:
            raise CanaryError(f"{description} queue row disappeared")
        if row.status == "succeeded":
            return row
        if row.status in {"failed", "dead_lettered", "cancelled"}:
            raise CanaryError(f"{description} queue row became {row.status}")
        return None

    return await poll_until(check, timeout_seconds=timeout_seconds, description=description)


async def assert_promotion_succeeded(
    db: AsyncSession,
    publication_id: uuid.UUID,
    task_id: uuid.UUID,
) -> dict[str, Any]:
    db.expire_all()
    publication = await db.get(PublicationRecord, publication_id)
    task = await db.get(ProductionTask, task_id)
    await db.commit()
    if publication is None or task is None:
        raise CanaryError("publication/task disappeared after immediate promotion")
    if publication.publish_status != "scheduled" or publication.desired_privacy != "unlisted":
        raise CanaryError("immediate unlisted promotion did not reach scheduled state")
    if task.state not in {"scheduled", "measured"}:
        raise CanaryError("canary task did not accept the immediate unlisted promotion")
    return {
        "publish_status": publication.publish_status,
        "desired_privacy": publication.desired_privacy,
        "current_privacy": publication.current_privacy,
        "task_state": task.state,
    }


def recognized_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    candidate = payload.get("metrics")
    metrics: dict[str, Any] = candidate if isinstance(candidate, dict) else payload
    return {key: metrics[key] for key in sorted(RECOGNIZED_METRIC_KEYS) if key in metrics}


async def wait_for_feedback_snapshot(
    db: AsyncSession,
    publication_id: uuid.UUID,
    timeout_seconds: float,
) -> FeedbackSnapshot:
    async def check() -> FeedbackSnapshot | None:
        rows = list(
            await db.scalars(select(FeedbackSnapshot).where(FeedbackSnapshot.publication_id == publication_id))
        )
        await db.commit()
        if len(rows) > 1:
            raise CanaryError("more than one feedback snapshot exists for the canary publication")
        return rows[0] if rows else None

    return await poll_until(
        check,
        timeout_seconds=timeout_seconds,
        description="one FeedbackSnapshot for recognized immediate metrics",
    )


async def pending_metrics_rows(db: AsyncSession, publication_id: uuid.UUID) -> list[dict[str, Any]]:
    rows = list(
        await db.scalars(
            select(ChannelOpsQueueItem)
            .where(ChannelOpsQueueItem.kind == "collect_metrics")
            .where(ChannelOpsQueueItem.status == "queued")
            .order_by(ChannelOpsQueueItem.run_after.asc(), ChannelOpsQueueItem.id.asc())
        )
    )
    await db.commit()
    matching = [row for row in rows if str((row.payload_json or {}).get("publication_id")) == str(publication_id)]
    return [
        {
            "id": str(row.id),
            "status": row.status,
            "run_after": row.run_after.isoformat(),
            "metrics_poll_count": (row.payload_json or {}).get("metrics_poll_count"),
        }
        for row in matching
    ]


async def assert_never_public(
    db: AsyncSession,
    graph: dict[str, str],
    task_id: uuid.UUID,
) -> None:
    account = await db.get(PublishingAccount, uuid.UUID(graph["account_id"]))
    lane_format = await db.get(LaneFormatMatrix, uuid.UUID(graph["lane_format_id"]))
    task = await db.get(ProductionTask, task_id)
    publications = list(
        await db.scalars(select(PublicationRecord).where(PublicationRecord.production_task_id == task_id))
    )
    await db.commit()
    if account is None or account.default_privacy != "unlisted" or account.external_asset_auto_publish is not False:
        raise CanaryError("canary publishing account safety settings changed")
    if lane_format is None or lane_format.default_publish_visibility != "unlisted":
        raise CanaryError("canary lane format safety settings changed")
    if task is None or task.uses_external_assets or list(task.source_platforms_json or []):
        raise CanaryError("canary task contains external source evidence")
    for publication in publications:
        if "public" in {publication.desired_privacy, publication.current_privacy}:
            raise CanaryError("public publication state is forbidden for this canary")


async def canary_counts(
    db: AsyncSession,
    graph: dict[str, str],
    task_id: uuid.UUID,
) -> dict[str, int]:
    channel_id = uuid.UUID(graph["channel_id"])
    lane_id = uuid.UUID(graph["lane_id"])
    task_count = int(
        await db.scalar(select(func.count()).select_from(ProductionTask).where(ProductionTask.channel_profile_id == channel_id))
        or 0
    )
    channel_count = int(
        await db.scalar(select(func.count()).select_from(ChannelProfile).where(ChannelProfile.id == channel_id)) or 0
    )
    lane_count = int(
        await db.scalar(select(func.count()).select_from(TopicLane).where(TopicLane.channel_profile_id == channel_id))
        or 0
    )
    account_count = int(
        await db.scalar(
            select(func.count()).select_from(PublishingAccount).where(PublishingAccount.channel_profile_id == channel_id)
        )
        or 0
    )
    format_count = int(
        await db.scalar(select(func.count()).select_from(LaneFormatMatrix).where(LaneFormatMatrix.topic_lane_id == lane_id))
        or 0
    )
    seed_count = int(
        await db.scalar(select(func.count()).select_from(ManualSeed).where(ManualSeed.channel_profile_id == channel_id))
        or 0
    )
    operation_count = int(
        await db.scalar(
            select(func.count())
            .select_from(YouTubeUploadOperation)
            .where(YouTubeUploadOperation.production_task_id == task_id)
        )
        or 0
    )
    publication_count = int(
        await db.scalar(
            select(func.count())
            .select_from(PublicationRecord)
            .where(PublicationRecord.production_task_id == task_id)
        )
        or 0
    )
    feedback_count = int(
        await db.scalar(
            select(func.count())
            .select_from(FeedbackSnapshot)
            .join(PublicationRecord, FeedbackSnapshot.publication_id == PublicationRecord.id)
            .where(PublicationRecord.production_task_id == task_id)
        )
        or 0
    )
    await db.commit()
    return {
        "channels": channel_count,
        "lanes": lane_count,
        "publishing_accounts": account_count,
        "lane_formats": format_count,
        "manual_seeds": seed_count,
        "tasks": task_count,
        "upload_operations": operation_count,
        "publications": publication_count,
        "feedback_snapshots": feedback_count,
    }


async def redis_pending_audit(redis_url: str) -> dict[str, Any]:
    if not redis_url:
        return {"available": False, "reason": "REDIS_URL not provided"}
    try:
        import redis.asyncio as aioredis

        client = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=3, socket_timeout=3)
        try:
            result: dict[str, Any] = {"available": True, "streams": {}}
            for stream, group in (
                ("vp:tasks:youtube_publisher", "youtube_publisher-workers"),
                ("vp:events", "orchestrator"),
            ):
                try:
                    pending = await client.xpending(stream, group)
                    result["streams"][stream] = {
                        "group": group,
                        "pending": int(pending.get("pending", 0)),
                    }
                except Exception as exc:
                    result["streams"][stream] = {"group": group, "available": False, "reason": type(exc).__name__}
            return result
        finally:
            await client.aclose()
    except Exception as exc:
        return {"available": False, "reason": type(exc).__name__}


async def failure_cleanup(db: AsyncSession, channel_id: uuid.UUID) -> dict[str, Any]:
    now = utc_now()
    async with db.begin():
        channel = (
            await db.execute(select(ChannelProfile).where(ChannelProfile.id == channel_id).with_for_update())
        ).scalar_one_or_none()
        if channel is None:
            return {"channel_missing": True}
        channel.halted_at = channel.halted_at or now
        channel.halt_reason = CANARY_FAILURE_REASON
        tasks = list(
            (
                await db.execute(
                    select(ProductionTask)
                    .where(ProductionTask.channel_profile_id == channel_id)
                    .with_for_update()
                )
            ).scalars()
        )
        held_task_ids: list[str] = []
        for task in tasks:
            if task.state in TERMINAL_TASK_STATES or task.state == "held":
                continue
            previous = task.state
            task.state = "held"
            task.blocked_by_guard = CANARY_FAILURE_REASON
            task.failure_reason = CANARY_FAILURE_REASON
            task.state_updated_at = now
            task.transition_history_json = [
                *list(task.transition_history_json or []),
                {
                    "from": previous,
                    "to": "held",
                    "actor": CANARY_FAILURE_REASON,
                    "at": now.isoformat(),
                },
            ]
            held_task_ids.append(str(task.id))
        queue_rows = list(
            (
                await db.execute(
                    select(ChannelOpsQueueItem)
                    .where(ChannelOpsQueueItem.channel_profile_id == channel_id)
                    .where(ChannelOpsQueueItem.status.in_(("queued", "running")))
                    .with_for_update()
                )
            ).scalars()
        )
        for row in queue_rows:
            row.status = "dead_lettered"
            row.last_error = CANARY_FAILURE_REASON
            row.dead_letter_at = now
            row.locked_at = None
            row.locked_by = None
        job_ids = {task.job_id for task in tasks if task.job_id is not None}
        jobs = list(
            (
                await db.execute(select(Job).where(Job.id.in_(job_ids)).with_for_update())
            ).scalars()
        ) if job_ids else []
        cancelled_job_ids: list[str] = []
        for job in jobs:
            if job.status not in RUNNABLE_JOB_STATUSES:
                continue
            job.status = JobStatus.CANCELLED
            job.completed_at = now
            job.error_message = CANARY_FAILURE_REASON
            cancelled_job_ids.append(str(job.id))
        nodes = list(
            (
                await db.execute(select(NodeExecution).where(NodeExecution.job_id.in_(job_ids)).with_for_update())
            ).scalars()
        ) if job_ids else []
        cancelled_node_ids: list[str] = []
        for node in nodes:
            if node.status not in ACTIVE_NODE_STATUSES:
                continue
            node.status = NodeStatus.CANCELLED
            node.completed_at = now
            node.worker_id = None
            node.error_message = CANARY_FAILURE_REASON
            cancelled_node_ids.append(str(node.id))
    return {
        "halted_channel_id": str(channel_id),
        "held_task_ids": sorted(held_task_ids),
        "dead_lettered_queue_item_ids": sorted(str(row.id) for row in queue_rows),
        "cancelled_job_ids": sorted(cancelled_job_ids),
        "cancelled_node_execution_ids": sorted(cancelled_node_ids),
    }


async def acquire_advisory_lock(connection: AsyncConnection) -> None:
    if connection.dialect.name != "postgresql":
        raise CanaryError("the live canary requires PostgreSQL for its advisory lock")
    acquired = bool(
        await connection.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": ADVISORY_LOCK_KEY})
    )
    await connection.commit()
    if not acquired:
        raise CanaryError("another unlisted canary runner holds the PostgreSQL advisory lock")


async def release_advisory_lock(connection: AsyncConnection) -> None:
    try:
        await connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": ADVISORY_LOCK_KEY})
        await connection.commit()
    except Exception:
        await connection.rollback()


async def execute_canary(
    args: argparse.Namespace,
    db: AsyncSession,
    client: httpx.AsyncClient,
    evidence: dict[str, Any],
    path: Path,
) -> None:
    initial_schedule = await schedule_status(args, client)
    record_schedule(evidence, "initial", initial_schedule)
    if initial_schedule.get("state") != "CLOSED":
        raise CanaryError("global video schedule must be CLOSED before canary start")

    preexisting = await active_backlog(db)
    evidence["preflight_backlog"] = preexisting
    assert_no_preexisting_backlog(preexisting)
    evidence["deployment"] = await deployment_readiness(args, client)
    evidence["manager"] = {"auth": await manager_readiness(args, client)}
    atomic_write_json(path, evidence)

    generated_at = utc_now().isoformat()
    media_path = path.with_suffix(".mp4")
    media = await asyncio.to_thread(generate_owned_video, media_path, duration_seconds=8)
    evidence["asset"] = {"generated_media": media}
    atomic_write_json(path, evidence)

    asset = await upload_and_attest_asset(args, client, db, media_path, media, generated_at)
    evidence["asset"].update(asset)
    graph = await create_canary_graph(db, evidence["run_id"], asset["id"])
    evidence["graph"] = graph
    atomic_write_json(path, evidence)

    evidence["queue"] = {"agent_tick_id": graph["agent_tick_id"]}
    task, plan_item = await wait_for_single_task_and_preapprove(
        db,
        uuid.UUID(graph["channel_id"]),
        evidence["run_id"],
        min(args.timeout_seconds, 60.0),
    )
    evidence["task"] = {
        "id": str(task.id),
        "approval_mode": task.approval_mode,
        "agent_approval_evidence": task.agent_approval_evidence_json,
        "plan_queue_item_id": str(plan_item.id),
        "channel_halted_after_exactly_one_task": True,
    }
    atomic_write_json(path, evidence)

    task, job = await wait_for_waiting_job(args, client, db, task.id, args.timeout_seconds)
    evidence["task"].update(
        {
            "autoflow_plan_id": str(task.autoflow_plan_id),
            "autoflow_run_id": str(task.autoflow_run_id),
            "pipeline_id": str(task.pipeline_id),
            "job_id": str(job.id),
        }
    )
    evidence["job"] = {"id": str(job.id), "status_before_open": job.status.value}
    await assert_never_public(db, graph, task.id)

    current_schedule = await schedule_status(args, client)
    record_schedule(evidence, "pre_open_recheck", current_schedule)
    if current_schedule.get("state") != "CLOSED":
        raise CanaryError("video schedule changed before final open gate")
    evidence["open_gate"] = await assert_open_gate(
        db,
        channel_id=uuid.UUID(graph["channel_id"]),
        job_id=job.id,
    )
    atomic_write_json(path, evidence)

    opened = await mutate_schedule(args, client, evidence, "open")
    if opened.get("state") != "OPEN" or int(opened.get("released_jobs") or 0) != 1:
        raise CanaryError("schedule open did not release exactly one canary job")
    running_job = await wait_for_running_job(db, job.id, min(args.timeout_seconds, 120.0))
    evidence["job"]["running_observed_at"] = utc_now().isoformat()
    evidence["job"]["status_when_drained"] = running_job.status.value
    drained = await mutate_schedule(args, client, evidence, "drain")
    if drained.get("state") != "DRAINING":
        raise CanaryError("video schedule did not enter DRAINING")
    atomic_write_json(path, evidence)

    operation, publication = await wait_for_upload_and_publication(db, task.id, args.timeout_seconds)
    if operation.manager_task_id is None or operation.platform_video_id is None:
        raise CanaryError("succeeded upload operation lacks manager task or video ID")
    evidence["operation"] = {
        "id": str(operation.id),
        "status": operation.status,
        "manager_task_id": operation.manager_task_id,
        "job_id": str(operation.job_id),
        "node_execution_id": str(operation.node_execution_id),
        "content_sha256": operation.content_sha256,
        "privacy": operation.privacy,
    }
    evidence["publication"] = {
        "id": str(publication.id),
        "platform": publication.platform,
        "video_id": publication.platform_content_id,
        "desired_privacy": publication.desired_privacy,
        "current_privacy": publication.current_privacy,
        "publish_status": publication.publish_status,
    }
    evidence["video"] = {"id": operation.platform_video_id, "count": 1, "deletion_policy": NO_DELETE_POLICY}

    manager_task, manager_video = await wait_for_manager_ready(
        args,
        client,
        operation.manager_task_id,
        operation.platform_video_id,
        min(args.timeout_seconds, 600.0),
    )
    evidence["manager"].update({"upload_task": manager_task, "video_status": manager_video})

    cancelled, promotion = await replace_auto_promotion_with_immediate(
        db,
        uuid.UUID(graph["channel_id"]),
        publication.id,
    )
    metrics_payload = await request_json(
        client,
        "GET",
        f"{manager_root(args.youtube_manager_url)}/api/videos/{operation.platform_video_id}/metrics",
    )
    immediate_metrics = recognized_metrics(metrics_payload)
    metrics_item = await enqueue_metrics_probe(db, publication.id)
    evidence["queue"].update(
        {
            "cancelled_auto_promotion_ids": cancelled,
            "immediate_promotion_id": str(promotion.id),
            "immediate_metrics_id": str(metrics_item.id),
        }
    )
    await wait_for_queue_success(
        db,
        promotion.id,
        min(args.timeout_seconds, 180.0),
        "immediate unlisted promotion",
    )
    evidence["publication"]["after_immediate_promotion"] = await assert_promotion_succeeded(
        db,
        publication.id,
        task.id,
    )
    manager_task, manager_video = await wait_for_manager_ready(
        args,
        client,
        operation.manager_task_id,
        operation.platform_video_id,
        min(args.timeout_seconds, 300.0),
    )
    evidence["manager"].update(
        {"upload_task_after_promotion": manager_task, "video_status_after_promotion": manager_video}
    )

    await wait_for_queue_success(
        db,
        metrics_item.id,
        min(args.timeout_seconds, 180.0),
        "immediate metrics probe",
    )
    durable_pending = await pending_metrics_rows(db, publication.id)
    if immediate_metrics:
        snapshot = await wait_for_feedback_snapshot(db, publication.id, min(args.timeout_seconds, 180.0))
        evidence["feedback"] = {
            "immediate_platform_feedback": {
                "available": True,
                "observed_at": utc_now().isoformat(),
                "metrics": immediate_metrics,
            },
            "feedback_snapshot": {
                "id": str(snapshot.id),
                "snapshot_stage": snapshot.snapshot_stage,
                "classification": "immediate_platform_feedback",
                "age_appropriate": False,
            },
            "age_appropriate_durable_metrics_queue": durable_pending,
        }
    else:
        if not durable_pending:
            raise CanaryError("manager metrics were unavailable and no durable metrics queue remains pending")
        evidence["feedback"] = {
            "immediate_platform_feedback": {
                "available": False,
                "observed_at": utc_now().isoformat(),
                "metrics": {},
            },
            "feedback_snapshot": None,
            "age_appropriate_durable_metrics_queue": durable_pending,
        }

    await assert_never_public(db, graph, task.id)
    counts = await canary_counts(db, graph, task.id)
    exact_one_keys = {
        "channels",
        "lanes",
        "publishing_accounts",
        "lane_formats",
        "manual_seeds",
        "tasks",
        "upload_operations",
        "publications",
    }
    if any(counts[key] != 1 for key in exact_one_keys):
        raise CanaryError("final canary row counts are not exactly one")
    evidence["counts"] = counts
    evidence["final_external_backlog"] = await active_backlog(
        db,
        allowed_channel_id=uuid.UUID(graph["channel_id"]),
    )
    assert_no_preexisting_backlog(evidence["final_external_backlog"])
    evidence["status"] = "succeeded"
    atomic_write_json(path, evidence)


async def run(args: argparse.Namespace, database_url: str) -> Path:
    run_id = str(uuid.uuid4())
    path = evidence_path(args, run_id)
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "status": "running",
        "started_at": utc_now().isoformat(),
        "safety": {
            "confirmation": "--confirm-live-unlisted",
            "privacy": "unlisted",
            "external_sources": False,
            "automatic_video_deletion": False,
        },
        "schedule": {"transitions": [], "final_state": None},
    }
    atomic_write_json(path, evidence)
    engine = create_async_engine(async_database_url(database_url), pool_pre_ping=True)
    try:
        async with engine.connect() as connection:
            await acquire_advisory_lock(connection)
            db = AsyncSession(bind=connection, expire_on_commit=False)
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), follow_redirects=True) as client:
                    try:
                        await execute_canary(args, db, client, evidence, path)
                    except BaseException as exc:
                        evidence["status"] = "failed"
                        evidence["failure"] = {
                            "type": type(exc).__name__,
                            "message": safe_failure_message(exc),
                            "at": utc_now().isoformat(),
                        }
                        graph = evidence.get("graph")
                        if isinstance(graph, dict) and graph.get("channel_id"):
                            try:
                                await db.rollback()
                                evidence["failure_cleanup"] = await failure_cleanup(
                                    db,
                                    uuid.UUID(str(graph["channel_id"])),
                                )
                            except Exception as cleanup_exc:
                                evidence["failure_cleanup"] = {
                                    "failed": True,
                                    "type": type(cleanup_exc).__name__,
                                }
                        raise
                    finally:
                        close_error: BaseException | None = None
                        try:
                            await close_schedule(args, client, evidence)
                        except BaseException as exc:
                            close_error = exc
                            evidence.setdefault("schedule", {})["final_state"] = "UNKNOWN"
                            evidence["schedule"]["close_error"] = type(exc).__name__
                        evidence["redis_stream_pending_audit"] = await redis_pending_audit(args.redis_url)
                        evidence["completed_at"] = utc_now().isoformat()
                        atomic_write_json(path, evidence)
                        if close_error is not None:
                            raise CanaryError("final schedule close failed") from close_error
            finally:
                await db.close()
                await release_advisory_lock(connection)
    finally:
        await engine.dispose()
    return path


def install_signal_guards() -> None:
    def interrupt(signum: int, _frame: Any) -> None:
        raise CanaryInterrupted(f"received signal {signum}")

    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, interrupt)


def main() -> int:
    args = parse_args()
    if not args.confirm_live_unlisted:
        print("refusing live canary without --confirm-live-unlisted", file=sys.stderr)
        return 2
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 2
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        print("--timeout-seconds must be finite and positive", file=sys.stderr)
        return 2
    for label, value in (
        ("--runtime-host", args.runtime_host),
        ("--manager-host", args.manager_host),
        ("--publisher-service", args.publisher_service),
    ):
        if not READINESS_NAME_PATTERN.fullmatch(value):
            print(f"{label} contains unsupported characters", file=sys.stderr)
            return 2
    install_signal_guards()
    try:
        path = asyncio.run(run(args, database_url))
    except BaseException as exc:
        print(f"unlisted canary failed: {type(exc).__name__}: {safe_failure_message(exc)}", file=sys.stderr)
        return 1
    print(f"unlisted canary evidence={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
