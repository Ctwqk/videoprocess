"""Explicit PG16 scratch fixtures; never use application DB settings."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.models.artifact import Artifact
from app.models.asset import Asset
from app.models.channel_agent import ChannelProfile, ProductionTask
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.pipeline import Pipeline
from app.services.job_execution_authority import claim_registered_worker_node
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventReceiptService, stage_worker_task_dispatch,
)
from app.services.worker_registration import WorkerLease
from app.services.worker_control_role_cli import ROLE_FUNCTIONS
from app.services.worker_role_cli_common import (
    create_login_role, ensure_stable_role, grant_functions, reset_public_privileges,
)
from app.services.worker_runtime_role_cli import _set_runtime_privileges
from app.services.youtube_upload_operations import UploadOperationContext


BACKEND_ROOT = Path(__file__).resolve().parents[2]


def scratch_url():
    raw = os.environ.get("CHANNEL_OPS_POSTGRES_TEST_URL")
    if not raw:
        pytest.skip("Task1b requires the explicitly authorized disposable PostgreSQL16 URL")
    url = make_url(raw)
    repository_ci = (
        os.environ.get("GITHUB_ACTIONS") == "true"
        and os.environ.get("GITHUB_REPOSITORY") == "Ctwqk/videoprocess"
    )
    allowed_ports = {55449, 5432} if repository_ci else {55449}
    if (
        url.drivername != "postgresql+asyncpg" or url.host != "127.0.0.1"
        or url.port not in allowed_ports or url.database != "postgres" or url.username != "postgres"
        or url.query
    ):
        raise ValueError("Task1b requires the authorized scratch or exact repository CI endpoint")
    return url


def asyncpg_dsn(url):
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


@pytest.fixture(scope="module")
def ack_drill_database():
    admin_url = scratch_url()
    database = f"vp_ack_drill_{uuid.uuid4().hex}"
    role = f"vp_ack_runtime_{uuid.uuid4().hex}"
    operator_role = f"vp_ack_operator_{uuid.uuid4().hex}"
    password = secrets.token_hex(24)
    target_url = admin_url.set(database=database)
    runtime_url = target_url.set(username=role, password=password)
    operator_url = target_url.set(username=operator_role, password=secrets.token_hex(24))
    stable_role = "vp_worker_runtime"
    created = {"database": False, "role": False, "operator": False, "group": False}

    async def create_database():
        admin = await asyncpg.connect(asyncpg_dsn(admin_url), timeout=10)
        try:
            version = int(await admin.fetchval("SHOW server_version_num"))
            assert 160000 <= version < 170000, "Task1b requires PostgreSQL16"
            await admin.execute(f'CREATE DATABASE "{database}"')
            created["database"] = True
        finally:
            await admin.close()

    async def provision_role():
        owner = await asyncpg.connect(asyncpg_dsn(target_url), timeout=10)
        try:
            async with owner.transaction():
                assert not await owner.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", stable_role,
                ), "Task1b must create, not reuse, its canonical scratch group"
                await ensure_stable_role(
                    owner, stable_role, setting_prefix="ack_drill_test", authorized_members=(role,),
                )
                await _set_runtime_privileges(owner, stable_role)
                await create_login_role(
                    owner, role, password, setting_prefix="ack_drill_test", stable_role=stable_role,
                )
            created["group"] = True
            created["role"] = True
            await owner.execute(f'GRANT CONNECT ON DATABASE "{database}" TO "{role}"')
            async with owner.transaction():
                await create_login_role(owner, operator_role, operator_url.password, setting_prefix="ack_drill_operator")
            created["operator"] = True
            await owner.execute(f'GRANT CONNECT ON DATABASE "{database}" TO "{operator_role}"')
            await reset_public_privileges(owner, operator_role)
            await grant_functions(owner, operator_role, ROLE_FUNCTIONS["operator"])
            attributes = await owner.fetchrow(
                "SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls "
                "FROM pg_roles WHERE rolname = $1", role,
            )
            assert not any(attributes.values())
            assert not await owner.fetchval(
                "SELECT has_table_privilege($1, 'public.youtube_upload_operations', 'INSERT,UPDATE,DELETE')",
                role,
            )
        finally:
            await owner.close()

    async def cleanup():
        if not any(created.values()):
            return
        admin = await asyncpg.connect(asyncpg_dsn(admin_url), timeout=10)
        try:
            # All resources were created here, including the initially absent group.
            if created["database"]:
                await admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
            if created["role"]:
                await admin.execute(f'DROP ROLE "{role}"')
            if created["operator"]:
                await admin.execute(f'DROP ROLE "{operator_role}"')
            if created["group"]:
                await admin.execute(f'DROP ROLE "{stable_role}"')
            assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", database)
            assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role)
            assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", operator_role)
            if created["group"]:
                assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", stable_role)
        finally:
            await admin.close()

    try:
        asyncio.run(create_database())
        migrated = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"], cwd=BACKEND_ROOT,
            env={**os.environ, "DATABASE_URL": target_url.render_as_string(hide_password=False)},
            capture_output=True, text=True, timeout=120, check=False,
        )
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        asyncio.run(provision_role())
        yield SimpleNamespace(
            owner_url=target_url, runtime_url=runtime_url, operator_url=operator_url, role=role,
            service=f"ack-drill-{uuid.uuid4().hex}",
            token_hash=hashlib.sha256(secrets.token_bytes(32)).hexdigest(),
        )
    finally:
        asyncio.run(cleanup())


@pytest.fixture
async def ack_drill_runtime(ack_drill_database, media_paths, request):
    # Existing handler cases retain their preclaimed, minimal fixture unchanged.
    whole_worker = getattr(request, "param", None) == "whole_worker"
    database = ack_drill_database
    owner_engine = create_async_engine(database.owner_url, poolclass=NullPool)
    runtime_engine = create_async_engine(database.runtime_url, poolclass=NullPool)
    owner_sessions = async_sessionmaker(owner_engine, expire_on_commit=False)
    runtime_sessions = async_sessionmaker(runtime_engine, expire_on_commit=False)
    owner = await asyncpg.connect(asyncpg_dsn(database.owner_url), timeout=10)
    runtime = None
    operator = None
    try:
        runtime = await asyncpg.connect(asyncpg_dsn(database.runtime_url), timeout=10)
        operator = await asyncpg.connect(asyncpg_dsn(database.operator_url), timeout=10)
        service = database.service
        stream, group = "vp:tasks:youtube_publisher", "youtube_publisher-workers"
        worker = f"publisher:{uuid.uuid4().hex}"
        instance = uuid.uuid4()
        token_hash = database.token_hash
        lease_secret = secrets.token_hex(32)
        lease_hash = hashlib.sha256(lease_secret.encode()).hexdigest()
        bindings = {
            # Admission metadata forbids loopback; these fixture-only names are
            # never resolved. All actual connections use the guarded scratch URL.
            "database": {"driver": "postgresql", "host": "scratch-postgres.invalid", "port": database.owner_url.port,
                         "database": database.owner_url.database},
            "redis": {"scheme": "redis", "host": "mock-redis.invalid", "port": 6379, "database": 0},
            "storage": {"backend": "not_applicable"},
        }
        release, image = "da2b6a703335909ec521faab9f882ed53bc53d9f", "vp-python-worker:deploy-da2b6a703335"
        if whole_worker:
            release = "dc53c3768486a20aa1d2581b6b4b0bc1b050c739"
            image = "vp-python-worker:deploy-dc53c3768486"
        if not await owner.fetchval("SELECT EXISTS(SELECT 1 FROM public.worker_admission_grants WHERE service_name=$1)", service):
            await operator.fetchval(
                "SELECT public.vp_worker_grant_upsert($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11::jsonb,$12,$13)",
                service, 1, "youtube_publisher", "scratch-test", '["youtube_publisher"]', release, image,
                database.role, stream, group, json.dumps(bindings), token_hash, "task1b-test",
            )
            await operator.execute("SELECT public.vp_worker_grant_activate($1,$2)", service, 1)
        fingerprints = await owner.fetchrow(
            "SELECT * FROM public.vp_worker_endpoint_fingerprints($1::jsonb)", json.dumps(bindings),
        )
        registration = await runtime.fetchrow(
            "SELECT * FROM public.vp_worker_register($1,$2,$3,$4,$5,$6,$7,$8::jsonb,"
            "$9,$10,$11,$12,$13::jsonb,$14,$15,$16,$17,$18)",
            service, 1, "youtube_publisher", "scratch-test", instance, 1, worker,
            '["youtube_publisher"]', release, image, stream, group, json.dumps(bindings),
            fingerprints["database_fingerprint"], fingerprints["redis_fingerprint"],
            fingerprints["storage_fingerprint"], token_hash, lease_hash,
        )
        asset = source = None
        snapshot = {"nodes": [], "edges": []}
        upload_config = {"title": "Owned canary", "privacy": "unlisted"}
        async with owner_sessions() as db:
            if whole_worker:
                input_path = Path(media_paths[0]["input"])
                asset = Asset(
                    filename="input.mp4", original_name="input.mp4", mime_type="video/mp4",
                    storage_path=str(input_path), file_size=input_path.stat().st_size,
                    media_info={"license": "owned", "provenance": "generated"},
                )
                db.add(asset)
                await db.flush()
                snapshot = {
                    "nodes": [
                        {"id": "source", "type": "source", "position": {"x": 0, "y": 0},
                         "data": {"config": {"asset_id": str(asset.id), "media_type": "video"}}},
                        {"id": "publish", "type": "youtube_upload", "position": {"x": 200, "y": 0},
                         "data": {"config": upload_config}},
                    ],
                    "edges": [{"id": "source-publish", "source": "source", "target": "publish",
                               "sourceHandle": "output", "targetHandle": "input"}],
                }
            pipeline = Pipeline(name="Task1b scratch", definition=snapshot)
            channel = ChannelProfile(name=f"Task1b-{uuid.uuid4().hex}")
            db.add_all([pipeline, channel])
            await db.flush()
            job = Job(pipeline_id=pipeline.id, pipeline_snapshot=snapshot, status=JobStatus.RUNNING)
            db.add(job)
            await db.flush()
            node = NodeExecution(job_id=job.id, node_id="publish", node_type="youtube_upload", status=NodeStatus.QUEUED)
            if whole_worker:
                node.node_config = upload_config
                source = NodeExecution(
                    job_id=job.id, node_id="source", node_type="source", status=NodeStatus.SUCCEEDED,
                    node_config=snapshot["nodes"][0]["data"]["config"],
                )
                db.add(source)
            task = ProductionTask(
                channel_profile_id=channel.id, target_account_id=uuid.uuid4(), prompt="Task1b owned scratch bytes",
                job_id=job.id, pipeline_id=pipeline.id, state="producing",
            )
            db.add_all([node, task])
            await db.flush()
            artifact = Artifact(
                job_id=job.id, node_execution_id=source.id if whole_worker else node.id,
                filename="input.mp4", storage_path=str(input_path) if whole_worker else "test/input.mp4",
            )
            if whole_worker:
                artifact.file_size = input_path.stat().st_size
                artifact.media_info = dict(asset.media_info)
            db.add(artifact)
            await db.flush()
            node.input_artifact_ids = [artifact.id]
            if whole_worker:
                source.output_artifact_id = artifact.id
            await db.commit()

        claim = attestation = context = None
        if whole_worker:
            redis = request.getfixturevalue("worker_redis")
            async with owner_sessions() as db:
                staged = await stage_worker_task_dispatch(
                    db, origin_receipt_id=None, job_id=job.id, node_execution_id=node.id,
                    redis_stream=stream, consumer_group=group,
                    payload={
                        "job_id": str(job.id), "node_execution_id": str(node.id),
                        "node_id": node.node_id, "node_type": node.node_type,
                        "config": json.dumps({"title": "UNTRUSTED queue title", "privacy": "public"}),
                        "input_artifacts": json.dumps({"input": str(artifact.id)}),
                    },
                )
                await db.commit()
            await RegisteredWorkerEventReceiptService(owner_sessions).deliver_pending_dispatches(redis)
            dispatch, payload, payload_hash = staged.dispatch_key, staged.payload_json, staged.payload_sha256
            message = await owner.fetchval(
                "SELECT redis_message_id FROM public.worker_task_dispatches WHERE dispatch_key=$1", dispatch,
            )
        else:
            dispatch, message = uuid.uuid4(), f"1710000000000-{secrets.randbits(63)}"
            payload = {"dispatch_key": str(dispatch), "job_id": str(job.id), "node_execution_id": str(node.id)}
            payload_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
            await owner.execute(
                "INSERT INTO public.worker_task_dispatches (dispatch_key,job_id,node_execution_id,"
                "redis_stream,consumer_group,payload_sha256,payload_json,delivery_state,redis_message_id,"
                "delivery_attempted_at,delivered_at) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,'delivered',$8,"
                "clock_timestamp(),clock_timestamp())",
                dispatch, job.id, node.id, stream, group, payload_hash, json.dumps(payload), message,
            )
            async with runtime_sessions() as db:
                claim, attestation = await claim_registered_worker_node(
                    db, registration_id=registration["registration_id"], lease_epoch=registration["lease_epoch"],
                    worker_id=worker, job_id=job.id, node_execution_id=node.id, redis_stream=stream,
                    consumer_group=group, message_id=message, payload_sha256=payload_hash, dispatch_key=dispatch,
                )
                await db.commit()
            context = UploadOperationContext(
                job_id=job.id, node_execution_id=node.id, execution_claim=claim, input_artifact_id=artifact.id,
                content_sha256=hashlib.sha256(Path(media_paths[0]["input"]).read_bytes()).hexdigest(),
                title="Owned canary", privacy="unlisted",
            )

        async def refresh_worker_lease(*, minimum_margin_seconds):
            await runtime.fetchval("SELECT public.vp_worker_heartbeat($1,$2,$3,$4,$5)",
                                   registration["registration_id"], service, instance, registration["lease_epoch"], lease_hash)
            await runtime.execute("SELECT public.vp_require_worker_lease_margin($1,$2,$3)",
                                  registration["registration_id"], registration["lease_epoch"], int(minimum_margin_seconds))

        yield SimpleNamespace(
            context=context, task_id=task.id, sessions=runtime_sessions, owner_sessions=owner_sessions,
            owner=owner, runtime=runtime, role=database.role, refresh=refresh_worker_lease,
            dispatch_key=dispatch, attestation_id=attestation, message_id=message,
            job_id=job.id, node_id=node.id, artifact_id=artifact.id,
            asset_id=asset.id if asset is not None else None,
            channel_id=channel.id, account_id=task.target_account_id, snapshot=snapshot,
            payload=payload, payload_sha256=payload_hash, release=release,
            lease=WorkerLease(
                registration_id=registration["registration_id"], grant_id=registration["grant_id"],
                service_name=service, worker_instance_id=instance, worker_slot=1,
                redis_consumer_id=worker, lease_epoch=registration["lease_epoch"],
                lease_secret=lease_secret, lease_expires_at=registration["lease_expires_at"],
            ),
        )
    finally:
        if operator is not None:
            await operator.close()
        if runtime is not None:
            await runtime.close()
        await owner.close()
        await runtime_engine.dispose()
        await owner_engine.dispose()
