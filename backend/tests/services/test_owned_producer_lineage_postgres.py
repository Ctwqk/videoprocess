"""Parent-only restricted RPC regressions for the complete upload ancestry."""
import asyncpg
import pytest

from tests.services.test_owned_producer_postgres import (
    d_database as d_database, d_pg as d_pg, media_paths as media_paths, reserve, transition,
)


@pytest.mark.parametrize("boundary", ["reserve", "attempt", "fence"])
@pytest.mark.parametrize("fault", ["trim_input", "transcode_input", "artifact_owner"])
async def test_pg_d_restricted_upload_ancestor_rebinding_denied(d_pg, boundary, fault):
    env, operation_id = d_pg, None
    if boundary != "reserve":
        operation_id = await reserve(env)
    if boundary == "fence":
        await transition(env, operation_id, "attempting")
    source = await env.native.owner.fetchrow("SELECT id,output_artifact_id FROM node_executions WHERE job_id=$1 AND node_type='source'", env.native.job_id)
    if fault == "trim_input":
        await env.native.owner.execute("UPDATE node_executions SET input_artifact_ids=ARRAY[$2]::uuid[] WHERE job_id=$1 AND node_type='trim'", env.native.job_id, env.native.artifact_id)
    elif fault == "transcode_input":
        await env.native.owner.execute("UPDATE node_executions SET input_artifact_ids=ARRAY[$2]::uuid[] WHERE job_id=$1 AND node_type='transcode'", env.native.job_id, source["output_artifact_id"])
    else:
        parent = await env.native.owner.fetchrow("SELECT id,output_artifact_id FROM node_executions WHERE job_id=$1 AND node_type='trim'", env.native.job_id)
        await env.native.owner.execute("UPDATE artifacts SET node_execution_id=$2 WHERE id=$1", parent["output_artifact_id"], source["id"])
    with pytest.raises(asyncpg.RaiseError, match="owned_inventory_artifact_lineage"):
        if boundary == "reserve":
            await reserve(env)
        else:
            await transition(env, operation_id, "attempting" if boundary == "attempt" else "fence")
    rows = await env.native.owner.fetch("SELECT request_attempted_at,manager_task_id FROM youtube_upload_operations WHERE production_task_id=$1", env.task_id)
    assert len(rows) == (0 if boundary == "reserve" else 1)
    assert all(r["manager_task_id"] is None and (r["request_attempted_at"] is not None) == (boundary == "fence") for r in rows)


async def test_pg_d_pending_export_sibling_does_not_block_upload(d_pg):
    env = d_pg
    await env.native.owner.execute("UPDATE node_executions SET status='PENDING',started_at=NULL,completed_at=NULL,output_artifact_id=NULL,input_artifact_ids=ARRAY[]::uuid[] WHERE job_id=$1 AND node_type='export'", env.native.job_id)
    operation_id = await reserve(env)
    await transition(env, operation_id, "attempting")
    await transition(env, operation_id, "fence")
    row = await env.native.owner.fetchrow("SELECT request_attempted_at,manager_task_id FROM youtube_upload_operations WHERE id=$1", operation_id)
    assert row["request_attempted_at"] is not None and row["manager_task_id"] is None
