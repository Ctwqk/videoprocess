"""Snapshot capacity is independent of the unchanged proof/JSON limits."""
from copy import deepcopy
import json
from pathlib import Path
import runpy

import pytest
from sqlalchemy.dialects import postgresql

from app.services import owned_seed_inventory_history as history
from tests.services.test_owned_seed_inventory_history import NOW, UC, empty_rows, retired_rows, snap, uid


MIGRATION = Path(__file__).parents[2] / "alembic/versions/043_owned_history_snapshot_rows.py"


@pytest.mark.parametrize("count", [4097, 4930, 8192])
def test_snapshot_capacity_retains_every_row_and_sorted_digest(count):
    rows = empty_rows()
    rows["assets"] = [{"id": uid(i)} for i in reversed(range(count))]
    snapshot = snap(rows)
    assert len(snapshot.rows.as_dict()["assets"]) == count
    rows["assets"].reverse()
    assert snapshot.rows == history.FrozenJSON.from_value(rows)
    assert snap(rows).rows == snapshot.rows


def test_snapshot_8193_refuses_without_partial_result():
    rows = empty_rows()
    rows["assets"] = [{"id": uid(i)} for i in range(8193)]
    with pytest.raises(history.OwnedHistoryError, match="^owned_history_incomplete$"):
        snap(rows)


@pytest.mark.asyncio
async def test_loader_8193_sentinel_in_one_unfiltered_mvcc_statement():
    class DB:
        calls = []

        async def scalar(self, statement):
            self.calls.append(statement)
            return {"observed_at": NOW, "rows": empty_rows()}

    db = DB()
    await history.load_owned_history_evidence(db, platform_channel_id=UC)
    assert len(db.calls) == 1
    sql = str(db.calls[0].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert sql.count("LIMIT 8193") == 27
    assert "clock_timestamp()" in sql
    assert not any(word in sql.upper() for word in (" WHERE ", " JOIN ", "FOR UPDATE", "FOR SHARE"))


def test_snapshot_tail_bad_node_is_not_dropped_by_old_capacity():
    path = Path(__file__).parents[1] / "fixtures/owned_seed_inventory_history/direct.json"
    fixture = json.loads(path.read_text())
    rows = fixture["rows"]
    original = deepcopy(rows["node_executions"][0])
    for i in range(4096):
        rows["node_executions"].append({**original, "id": uid(10000 + i), "job_id": uid(99999)})
    before = history.OwnedHistorySnapshot.from_rows(rows, platform_channel_id=fixture["platform_channel_id"], observed_at=fixture["observed_at"])
    assert history.assess_owned_history(before, now=history._time(fixture["now"])).block_reason is None
    rows["node_executions"].append({**original, "id": "ffffffff-ffff-ffff-ffff-ffffffffffff", "status": "RUNNING"})
    after = history.OwnedHistorySnapshot.from_rows(rows, platform_channel_id=fixture["platform_channel_id"], observed_at=fixture["observed_at"])
    assert after.rows.as_dict()["node_executions"][-1]["status"] == "RUNNING"
    assert history.assess_owned_history(after, now=history._time(fixture["now"])).block_reason is not None


def test_graph_pel_and_json_bounds_are_not_snapshot_capacity():
    assert history.MAX_ROWS == 4096 and history.MAX_BYTES == 16 * 1024 * 1024
    rows, _ = retired_rows()
    graph = {name: sorted(deepcopy(rows[name]), key=lambda row: row["id"]) for name in history.TERMINAL_TABLES}
    history.TerminalGraph.parse(graph)
    original = graph["node_executions"][0]
    graph["node_executions"] = [{**deepcopy(original), "id": uid(10000 + i)} for i in range(4097)]
    with pytest.raises(history.OwnedHistoryError):
        history.TerminalGraph.parse(graph)
    pending = {"kind": "task", "redis_stream": "vp:tasks:ffmpeg_go", "consumer_group": "ffmpeg_go-workers",
        "message_id": None, "dispatch_key": uid(1), "payload_sha256": "a" * 64, "marker_message_id": None,
        "pending_message_ids": [f"{i}-0" for i in range(4097)], "observed_at": NOW.isoformat()}
    with pytest.raises(history.OwnedHistoryError):
        history.RedisTerminalObservation.parse(pending)
    history.FrozenJSON.from_value("x" * (history.MAX_BYTES - 2))
    with pytest.raises(history.OwnedHistoryError):
        history.FrozenJSON.from_value("x" * (history.MAX_BYTES - 1))


def test_capacity_migration_is_exact_additive_and_offline(monkeypatch):
    assert MIGRATION.exists(), "capacity migration missing"
    migration = runpy.run_path(str(MIGRATION))
    assert (migration["revision"], migration["down_revision"]) == ("043_owned_history_snapshot_rows", "042_owned_producer_fence")
    emitted = []
    monkeypatch.setattr(migration["op"], "execute", emitted.append)
    monkeypatch.setattr(migration["op"], "get_bind", lambda: pytest.fail("offline migration queried database"))
    migration["upgrade"]()
    assert len(emitted) == 3 and all(statement.lstrip().startswith("DO ") for statement in emitted)
    changes = migration["_bodies"]()
    assert set(changes) == {"public.vp_owned_producer_rows()", "public.vp_registered_consumer_uploads_quiescent()",
        "public.vp_registered_consumer_terminal_upload(uuid,timestamp with time zone)"}
    for name, (before, after) in changes.items():
        if "terminal_upload(" not in name:
            assert after == before.replace("LIMIT 4097", "LIMIT 8193").replace("> 4096", "> 8192")
        else:
            assert "cardinality(v_nodes) > 4096" in after
            assert after.index("cardinality(v_nodes) > 4096") < after.index("WITH RECURSIVE paths")
            for collection in ("v_keys", "v_atts", "v_receipts", "v_emissions", "v_queues"):
                assert f"cardinality({collection}) > 4096" in after
            assert "v_delivery_count > 4096" in after
        assert migration["_bodies"](downgrade=True)[name] == (after, before)
    sql = "\n".join(emitted)
    assert "pg_get_functiondef" in sql and "v_source IS DISTINCT FROM v_before" in sql
    assert "GRANT " not in sql and "DROP FUNCTION" not in sql and "ALTER TABLE" not in sql
