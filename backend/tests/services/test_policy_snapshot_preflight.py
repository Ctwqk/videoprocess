"""Offline SQL/projection tests; real PostgreSQL qualification belongs to integration."""

import importlib.util
import json
import sqlite3
import stat
from pathlib import Path
from uuid import UUID

import pytest


SCRIPT = Path(__file__).resolve().parents[3] / "scripts/channelops_policy_snapshot_preflight.py"
HEAD = "045_registered_consumer_history"
SECRET = "postgresql://operator:never-print-this@secret-host/private-db"
TICK, POLICY, FEATURE, DECISION, CHANNEL = (str(UUID(int=i)) for i in range(1, 6))
OTHER = str(UUID(int=99))
HASH = "a" * 64
AS_OF = "2026-07-26T12:00:00+00:00"


def test_preflight_entrypoint_exists():
    assert SCRIPT.is_file(), "passive snapshot preflight has not been implemented"


@pytest.fixture
def preflight():
    assert SCRIPT.is_file(), "passive snapshot preflight has not been implemented"
    spec = importlib.util.spec_from_file_location("policy_snapshot_preflight", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def database():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    # Projection-only schema, deliberately allowing corrupt links for negative tests.
    db.executescript("""
        CREATE TABLE alembic_version (version_num TEXT);
        CREATE TABLE decision_policy_versions (
            id TEXT, feature_schema_version TEXT, config_hash TEXT, version TEXT);
        CREATE TABLE agent_tick_audits (
            id TEXT, channel_profile_id TEXT, replay_status TEXT, policy_version_id TEXT,
            candidate_set_hash TEXT, feature_as_of TEXT, candidates_scored INTEGER);
        CREATE TABLE candidate_feature_snapshots (
            id TEXT, tick_audit_id TEXT, policy_version_id TEXT, candidate_id TEXT,
            candidate_source TEXT, source_kind TEXT, topic_lane_id TEXT, lane_format_id TEXT,
            target_account_id TEXT, feature_schema_version TEXT, feature_as_of TEXT,
            candidate_set_hash TEXT, feature_hash TEXT, raw_features_json TEXT);
        CREATE TABLE decision_audit_entries (
            id TEXT, tick_audit_id TEXT, channel_profile_id TEXT, policy_version_id TEXT,
            feature_snapshot_id TEXT, candidate_id TEXT, candidate_source TEXT,
            topic_lane_id TEXT, lane_format_id TEXT, target_account_id TEXT,
            candidate_set_hash TEXT, decision_hash TEXT, decision TEXT, selected INTEGER,
            score_json TEXT);
        CREATE TABLE policy_activation_history (mode TEXT);
    """)
    db.execute("INSERT INTO alembic_version VALUES (?)", (HEAD,))
    db.execute("INSERT INTO decision_policy_versions VALUES (?, ?, ?, ?)",
               (POLICY, "channelops-candidate-v1", HASH, "sha256:" + HASH))
    db.execute("INSERT INTO agent_tick_audits VALUES (?, ?, ?, ?, ?, ?, ?)",
               (TICK, CHANNEL, "snapshot_complete", POLICY, HASH, AS_OF, 1))
    db.execute("INSERT INTO candidate_feature_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
               (FEATURE, TICK, POLICY, "candidate-one", "idea", "idea", None, None, None,
                "channelops-candidate-v1", AS_OF, HASH, "b" * 64, SECRET))
    db.execute("INSERT INTO decision_audit_entries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
               (DECISION, TICK, CHANNEL, POLICY, FEATURE, "candidate-one", "idea",
                None, None, None, HASH, "c" * 64, "accepted", 1, SECRET))
    yield db
    db.close()


class ReadOnlyDatabase:
    def __init__(self, db):
        self.db = db
        self.queries = []
        self.transaction_options = None
        self.closed = False
        self.in_transaction = False
        db.set_authorizer(self.authorize)

    @staticmethod
    def authorize(action, arg1, arg2, _database, _source):
        if action == sqlite3.SQLITE_READ and arg2 in {"raw_features_json", "score_json"}:
            return sqlite3.SQLITE_DENY
        return (sqlite3.SQLITE_OK if action in {
            sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION,
        } else sqlite3.SQLITE_DENY)

    async def fetch(self, query):
        assert self.in_transaction
        assert query.lstrip().upper().startswith("SELECT ")
        self.queries.append(query)
        return [dict(row) for row in self.db.execute(query)]

    def transaction(self, **kwargs):
        self.transaction_options = kwargs
        return self

    async def __aenter__(self):
        self.in_transaction = True

    async def __aexit__(self, *_args):
        self.in_transaction = False

    async def close(self, **_kwargs):
        self.closed = True


async def audit(preflight, database):
    connection = ReadOnlyDatabase(database)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        return await preflight.collect_report(connection)


@pytest.mark.asyncio
async def test_complete_queries_are_read_only_and_do_not_read_raw_payloads(preflight, database):
    report = await audit(preflight, database)
    assert report["ok"] is True
    assert report["migration_head"] == HEAD
    assert report["ticks"] == {
        "total": 1, "legacy": 0, "new": 1, "pending": 0,
        "complete_labelled": 1, "actually_complete": 1, "partial": 0,
        "coverage": 1.0,
    }
    encoded = json.dumps(report)
    assert TICK in encoded and POLICY in encoded and HASH in encoded
    assert SECRET not in encoded and "candidate-one" not in encoded


@pytest.mark.parametrize("selected", [True, False])
@pytest.mark.asyncio
async def test_trend_policy_manual_seed_source_kind_is_complete(preflight, database, selected):
    database.execute("UPDATE candidate_feature_snapshots "
                     "SET candidate_source='manual_seed', source_kind='trend_youtube'")
    database.execute("UPDATE decision_audit_entries "
                     "SET candidate_source='trend_youtube', decision=?, selected=?",
                     ("accepted" if selected else "rejected", selected))
    changes_before = database.total_changes

    report = await audit(preflight, database)

    assert report["ok"] is True
    assert report["ticks"]["actually_complete"] == 1
    assert report["ticks"]["partial"] == 0
    assert report["ticks"]["coverage"] == 1.0
    assert report["tick_evidence"][0]["complete"] is True
    assert database.total_changes == changes_before
    assert SECRET not in json.dumps(report)


@pytest.mark.parametrize("decision_source", ["manual_seed", "lane_seed"])
@pytest.mark.asyncio
async def test_mismatched_source_kind_is_partial_even_when_origin_matches(
    preflight, database, decision_source,
):
    database.execute("UPDATE candidate_feature_snapshots "
                     "SET candidate_source='manual_seed', source_kind='trend_youtube'")
    database.execute("UPDATE decision_audit_entries SET candidate_source=?", (decision_source,))

    report = await audit(preflight, database)

    assert report["ok"] is False
    assert report["ticks"]["actually_complete"] == 0
    assert report["ticks"]["partial"] == 1
    assert report["tick_evidence"][0]["complete"] is False
    assert report["errors"] == ["partial_new_ticks"]


@pytest.mark.asyncio
async def test_counts_separate_legacy_pending_labels_and_actual_completeness(preflight, database):
    database.execute("INSERT INTO agent_tick_audits VALUES (?, ?, ?, NULL, NULL, NULL, 7)",
                     (OTHER, CHANNEL, "legacy_unreplayable"))
    database.execute("INSERT INTO agent_tick_audits VALUES (?, ?, ?, NULL, NULL, NULL, 0)",
                     (str(UUID(int=100)), CHANNEL, "snapshot_pending"))
    report = await audit(preflight, database)
    assert report["ok"] is False
    assert report["ticks"] == {
        "total": 3, "legacy": 1, "new": 2, "pending": 1,
        "complete_labelled": 1, "actually_complete": 1, "partial": 1,
        "coverage": 0.5,
    }


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.asyncio
async def test_no_new_ticks_has_null_coverage(preflight, database, legacy):
    database.execute("DELETE FROM candidate_feature_snapshots")
    database.execute("DELETE FROM decision_audit_entries")
    if legacy:
        database.execute("UPDATE agent_tick_audits SET replay_status='legacy_unreplayable'")
    else:
        database.execute("DELETE FROM agent_tick_audits")
    report = await audit(preflight, database)
    assert report["ok"] is True
    assert report["ticks"]["new"] == 0
    assert report["ticks"]["coverage"] is None
    assert report["production_run_proven"] is False


@pytest.mark.asyncio
async def test_empty_candidate_complete_tick_is_valid(preflight, database):
    database.execute("DELETE FROM candidate_feature_snapshots")
    database.execute("DELETE FROM decision_audit_entries")
    database.execute("UPDATE agent_tick_audits SET candidates_scored=0")
    report = await audit(preflight, database)
    assert report["ok"] is True
    assert report["ticks"]["actually_complete"] == 1


@pytest.mark.parametrize(("table", "field", "value"), [
    ("agent_tick_audits", "replay_status", "snapshot_pending"),
    ("agent_tick_audits", "replay_status", SECRET),
    ("agent_tick_audits", "policy_version_id", None),
    ("agent_tick_audits", "policy_version_id", OTHER),
    ("agent_tick_audits", "candidate_set_hash", None),
    ("agent_tick_audits", "candidate_set_hash", SECRET),
    ("agent_tick_audits", "feature_as_of", None),
    ("agent_tick_audits", "candidates_scored", 2),
    ("agent_tick_audits", "candidates_scored", -1),
    ("decision_policy_versions", "config_hash", None),
    ("decision_policy_versions", "config_hash", SECRET),
    ("decision_policy_versions", "feature_schema_version", "wrong-schema"),
    ("decision_policy_versions", "version", "sha256:" + "d" * 64),
    ("candidate_feature_snapshots", "tick_audit_id", OTHER),
    ("candidate_feature_snapshots", "policy_version_id", OTHER),
    ("candidate_feature_snapshots", "candidate_id", "wrong-candidate"),
    ("candidate_feature_snapshots", "candidate_id", " "),
    ("candidate_feature_snapshots", "source_kind", "wrong-source"),
    ("candidate_feature_snapshots", "topic_lane_id", OTHER),
    ("candidate_feature_snapshots", "lane_format_id", OTHER),
    ("candidate_feature_snapshots", "target_account_id", OTHER),
    ("candidate_feature_snapshots", "feature_schema_version", "wrong-schema"),
    ("candidate_feature_snapshots", "feature_as_of", None),
    ("candidate_feature_snapshots", "feature_as_of", "2026-07-25T12:00:00+00:00"),
    ("candidate_feature_snapshots", "candidate_set_hash", "d" * 64),
    ("candidate_feature_snapshots", "feature_hash", None),
    ("candidate_feature_snapshots", "feature_hash", SECRET),
    ("decision_audit_entries", "tick_audit_id", OTHER),
    ("decision_audit_entries", "channel_profile_id", OTHER),
    ("decision_audit_entries", "policy_version_id", OTHER),
    ("decision_audit_entries", "feature_snapshot_id", None),
    ("decision_audit_entries", "feature_snapshot_id", OTHER),
    ("decision_audit_entries", "candidate_id", "wrong-candidate"),
    ("decision_audit_entries", "candidate_set_hash", "d" * 64),
    ("decision_audit_entries", "decision_hash", None),
    ("decision_audit_entries", "decision_hash", ""),
    ("decision_audit_entries", "decision", None),
    ("decision_audit_entries", "decision", "rejected"),
])
@pytest.mark.asyncio
async def test_malformed_new_tick_is_partial(preflight, database, table, field, value):
    database.execute(f"UPDATE {table} SET {field}=?", (value,))
    report = await audit(preflight, database)
    assert report["ok"] is False
    assert report["ticks"]["partial"] == 1
    assert report["ticks"]["actually_complete"] == 0
    assert SECRET not in json.dumps(report)


@pytest.mark.parametrize("mutation", [
    "DELETE FROM decision_policy_versions",
    "DELETE FROM candidate_feature_snapshots",
    "DELETE FROM decision_audit_entries",
    "INSERT INTO decision_audit_entries SELECT * FROM decision_audit_entries",
    "INSERT INTO candidate_feature_snapshots SELECT * FROM candidate_feature_snapshots",
    "INSERT INTO candidate_feature_snapshots SELECT 'extra', tick_audit_id, policy_version_id, "
    "candidate_id, candidate_source, source_kind, topic_lane_id, lane_format_id, target_account_id, "
    "feature_schema_version, feature_as_of, candidate_set_hash, feature_hash, raw_features_json "
    "FROM candidate_feature_snapshots",
])
@pytest.mark.asyncio
async def test_missing_extra_or_duplicate_rows_fail(preflight, database, mutation):
    database.execute(mutation)
    report = await audit(preflight, database)
    assert report["ok"] is False
    assert report["ticks"]["partial"] == 1


@pytest.mark.asyncio
async def test_duplicate_linkage_cannot_hide_behind_equal_cardinality(preflight, database):
    database.execute("UPDATE agent_tick_audits SET candidates_scored=2")
    database.execute("INSERT INTO candidate_feature_snapshots SELECT ?, tick_audit_id, policy_version_id, "
                     "'candidate-two', candidate_source, source_kind, topic_lane_id, lane_format_id, target_account_id, "
                     "feature_schema_version, feature_as_of, candidate_set_hash, feature_hash, raw_features_json "
                     "FROM candidate_feature_snapshots", (OTHER,))
    database.execute("INSERT INTO decision_audit_entries SELECT ?, tick_audit_id, channel_profile_id, "
                     "policy_version_id, feature_snapshot_id, candidate_id, candidate_source, topic_lane_id, "
                     "lane_format_id, target_account_id, candidate_set_hash, decision_hash, decision, selected, "
                     "score_json FROM decision_audit_entries", (OTHER,))
    report = await audit(preflight, database)
    assert report["ticks"]["partial"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_snapshot_id", [FEATURE, None])
async def test_legacy_reverse_link_marks_only_affected_new_tick_partial(
    preflight, database, legacy_snapshot_id,
):
    unaffected_tick = str(UUID(int=100))
    database.execute("INSERT INTO agent_tick_audits VALUES (?, ?, ?, NULL, NULL, NULL, 7)",
                     (OTHER, CHANNEL, "legacy_unreplayable"))
    database.execute("INSERT INTO agent_tick_audits VALUES (?, ?, ?, ?, ?, ?, 0)",
                     (unaffected_tick, CHANNEL, "snapshot_complete", POLICY, HASH, AS_OF))
    database.execute("INSERT INTO decision_audit_entries SELECT ?, ?, channel_profile_id, "
                     "policy_version_id, ?, candidate_id, candidate_source, topic_lane_id, "
                     "lane_format_id, target_account_id, candidate_set_hash, decision_hash, "
                     "decision, selected, score_json FROM decision_audit_entries",
                     (OTHER, OTHER, legacy_snapshot_id))
    changes_before = database.total_changes
    report = await audit(preflight, database)
    linked = legacy_snapshot_id is not None
    assert report["ok"] is (not linked)
    assert report["ticks"] == {
        "total": 3, "legacy": 1, "new": 2, "pending": 0,
        "complete_labelled": 2, "actually_complete": 1 if linked else 2,
        "partial": 1 if linked else 0, "coverage": 0.5 if linked else 1.0,
    }
    assert report["errors"] == (["partial_new_ticks"] if linked else [])
    evidence = {row["tick_audit_id"]: row for row in report["tick_evidence"]}
    assert evidence[TICK]["complete"] is (not linked)
    assert evidence[unaffected_tick]["complete"] is True
    assert OTHER not in evidence
    assert database.total_changes == changes_before
    assert SECRET not in json.dumps(report)


@pytest.mark.asyncio
async def test_accepted_and_rejected_candidates_are_both_required(preflight, database):
    database.execute("UPDATE agent_tick_audits SET candidates_scored=2")
    database.execute("INSERT INTO candidate_feature_snapshots SELECT ?, tick_audit_id, policy_version_id, "
                     "'candidate-two', candidate_source, source_kind, topic_lane_id, lane_format_id, target_account_id, "
                     "feature_schema_version, feature_as_of, candidate_set_hash, feature_hash, raw_features_json "
                     "FROM candidate_feature_snapshots", (OTHER,))
    database.execute("INSERT INTO decision_audit_entries SELECT ?, tick_audit_id, channel_profile_id, "
                     "policy_version_id, ?, 'candidate-two', candidate_source, topic_lane_id, lane_format_id, "
                     "target_account_id, candidate_set_hash, decision_hash, 'rejected', 0, score_json "
                     "FROM decision_audit_entries", (OTHER, OTHER))
    report = await audit(preflight, database)
    assert report["ok"] is True
    assert report["ticks"]["actually_complete"] == 1
    database.set_authorizer(None)
    database.execute("DELETE FROM decision_audit_entries WHERE selected=0")
    report = await audit(preflight, database)
    assert report["ok"] is False
    assert report["ticks"]["partial"] == 1


@pytest.mark.parametrize(("mode", "ok"), [("off", True), ("shadow", True),
                                           ("canary", False), ("active", False), (SECRET, False)])
@pytest.mark.asyncio
async def test_all_activation_history_is_counted(preflight, database, mode, ok):
    database.execute("INSERT INTO policy_activation_history VALUES (?)", (mode,))
    database.execute("INSERT INTO policy_activation_history VALUES ('off')")
    report = await audit(preflight, database)
    assert report["ok"] is ok
    assert sum(report["activation_modes"].values()) == 2
    assert SECRET not in json.dumps(report)


@pytest.mark.parametrize("heads", [[], ["043_owned_history_snapshot_rows"], [HEAD, HEAD], [SECRET]])
@pytest.mark.asyncio
async def test_wrong_missing_or_multiple_heads_fail_sanitized(preflight, database, heads):
    database.execute("DELETE FROM alembic_version")
    database.executemany("INSERT INTO alembic_version VALUES (?)", [(head,) for head in heads])
    report = await audit(preflight, database)
    assert report["ok"] is False
    assert report["migration_head"] is None
    assert report["errors"] == ["migration_head_mismatch"]
    assert SECRET not in json.dumps(report)


def install_connection(monkeypatch, database):
    import asyncpg

    connection = ReadOnlyDatabase(database)
    options = {}

    async def connect(**kwargs):
        options.update(kwargs)
        return connection

    monkeypatch.setattr(asyncpg, "connect", connect)
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://operator:secret@localhost/fixture?sslmode=require")
    return connection, options


def test_cli_uses_explicit_read_only_transaction_and_bounded_timeouts(preflight, database, monkeypatch, capsys):
    connection, options = install_connection(monkeypatch, database)
    assert preflight.main([]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["ok"] is True
    assert connection.transaction_options == {"isolation": "repeatable_read", "readonly": True}
    assert connection.closed
    assert 0 < options["timeout"] <= 10
    assert 0 < options["command_timeout"] <= 30
    assert options["server_settings"]["default_transaction_read_only"] == "on"
    assert 0 < int(options["server_settings"]["statement_timeout"]) <= 30000
    assert options["server_settings"]["search_path"] == "pg_catalog,public"
    assert options["host"] == "localhost" and options["database"] == "fixture"
    assert options["ssl"] == "require"


@pytest.mark.parametrize("url", [None, "", "sqlite:///tmp/db", "postgresql:///ambient", SECRET + "?options=unsafe"])
def test_cli_has_no_ambient_target_or_unsafe_url_options(preflight, monkeypatch, capsys, url):
    import asyncpg

    def forbidden(**_kwargs):
        pytest.fail("invalid configuration must fail before connecting")

    monkeypatch.setattr(asyncpg, "connect", forbidden)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    if url is not None:
        monkeypatch.setenv("DATABASE_URL", url)
    assert preflight.main([]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["ok"] is False
    assert SECRET not in output.out + output.err


@pytest.mark.parametrize("failure", ["connect", "query", "close"])
def test_cli_sanitizes_database_failures(preflight, database, monkeypatch, capsys, failure):
    import asyncpg

    connection, _ = install_connection(monkeypatch, database)

    async def fail(*_args, **_kwargs):
        raise RuntimeError(SECRET)

    if failure == "connect":
        monkeypatch.setattr(asyncpg, "connect", fail)
    else:
        monkeypatch.setattr(connection, "fetch" if failure == "query" else "close", fail)
    assert preflight.main([]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["errors"] == ["database_read_failed"]
    assert SECRET not in output.out + output.err
    if failure == "query":
        assert connection.closed


def test_evidence_is_atomic_private_and_matches_stdout(preflight, database, monkeypatch, capsys, tmp_path):
    install_connection(monkeypatch, database)
    evidence = tmp_path / "evidence.json"
    assert preflight.main(["--evidence", str(evidence)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert json.loads(evidence.read_text()) == report
    assert stat.S_IMODE(evidence.stat().st_mode) == 0o600
    assert list(tmp_path.iterdir()) == [evidence]


@pytest.mark.parametrize("mutation", [
    "UPDATE agent_tick_audits SET replay_status='snapshot_pending'",
    "INSERT INTO policy_activation_history VALUES ('active')",
])
def test_failed_audit_returns_nonzero_and_preserves_private_evidence(
    preflight, database, monkeypatch, capsys, tmp_path, mutation,
):
    database.execute(mutation)
    install_connection(monkeypatch, database)
    evidence = tmp_path / "failure.json"
    assert preflight.main(["--evidence", str(evidence)]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["ok"] is False
    assert json.loads(evidence.read_text()) == report
    assert stat.S_IMODE(evidence.stat().st_mode) == 0o600


def test_evidence_race_cannot_replace_a_newly_created_user_file(
    preflight, database, monkeypatch, capsys, tmp_path,
):
    install_connection(monkeypatch, database)
    evidence = tmp_path / "evidence.json"
    original_link = preflight.os.link

    def racing_link(source, destination, **kwargs):
        assert stat.S_IMODE(Path(source).stat().st_mode) == 0o600
        assert json.loads(Path(source).read_text())["ok"] is True
        evidence.write_text("concurrent user file")
        original_link(source, destination, **kwargs)

    monkeypatch.setattr(preflight.os, "link", racing_link)
    assert preflight.main(["--evidence", str(evidence)]) == 1
    assert json.loads(capsys.readouterr().out)["errors"] == ["evidence_write_failed"]
    assert evidence.read_text() == "concurrent user file"
    assert list(tmp_path.iterdir()) == [evidence]


@pytest.mark.parametrize("kind", ["file", "symlink", "dangling", "directory"])
def test_evidence_refuses_existing_destinations(preflight, database, monkeypatch, capsys, tmp_path, kind):
    install_connection(monkeypatch, database)
    target = tmp_path / "target"
    target.write_text("keep me")
    evidence = tmp_path / "evidence.json"
    if kind == "file":
        evidence.write_text("original")
    elif kind in {"symlink", "dangling"}:
        evidence.symlink_to(target if kind == "symlink" else tmp_path / "missing")
    else:
        evidence.mkdir()
    before = sorted(tmp_path.iterdir())
    assert preflight.main(["--evidence", str(evidence)]) == 1
    assert "evidence_write_failed" in json.loads(capsys.readouterr().out)["errors"]
    assert target.read_text() == "keep me"
    assert sorted(tmp_path.iterdir()) == before
    if kind == "file":
        assert evidence.read_text() == "original"


@pytest.mark.parametrize("operation", ["fsync", "link"])
def test_atomic_evidence_failure_leaves_no_file_or_secret(preflight, database, monkeypatch, capsys, tmp_path, operation):
    install_connection(monkeypatch, database)

    def fail(*_args, **_kwargs):
        raise OSError(SECRET)

    monkeypatch.setattr(preflight.os, operation, fail)
    assert preflight.main(["--evidence", str(tmp_path / "evidence.json")]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["ok"] is False
    assert SECRET not in output.out + output.err
    assert list(tmp_path.iterdir()) == []


def test_argument_errors_do_not_echo_credentials(preflight, capsys):
    assert preflight.main(["--database-url", SECRET]) == 1
    output = capsys.readouterr()
    assert json.loads(output.out)["errors"] == ["invalid_arguments"]
    assert SECRET not in output.out + output.err
