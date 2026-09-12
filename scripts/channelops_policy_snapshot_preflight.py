#!/usr/bin/env python3
"""Read-only audit of passive policy snapshot metadata, without application startup."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit
from uuid import UUID


EXPECTED_HEAD = "044_policy_decision_snapshots"
FEATURE_SCHEMA = "channelops-candidate-v1"
HEAD_SQL = "SELECT version_num FROM alembic_version"
TICKS_SQL = """SELECT id, channel_profile_id, replay_status, policy_version_id,
    candidate_set_hash, feature_as_of, candidates_scored FROM agent_tick_audits"""
POLICIES_SQL = """SELECT id, feature_schema_version, config_hash, version
    FROM decision_policy_versions"""
FEATURES_SQL = """SELECT id, tick_audit_id, policy_version_id, candidate_id,
    candidate_source, topic_lane_id, lane_format_id, target_account_id,
    feature_schema_version, feature_as_of, candidate_set_hash, feature_hash
    FROM candidate_feature_snapshots"""
DECISIONS_SQL = """SELECT id, tick_audit_id, channel_profile_id, policy_version_id,
    feature_snapshot_id, candidate_id, candidate_source, topic_lane_id,
    lane_format_id, target_account_id, candidate_set_hash, decision_hash,
    decision, selected FROM decision_audit_entries"""
ACTIVATIONS_SQL = "SELECT mode, COUNT(*) AS count FROM policy_activation_history GROUP BY mode"


def stored_hash(value):
    return value if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) else None


def stored_uuid(value):
    try:
        return str(UUID(str(value))) if value is not None else None
    except (ValueError, TypeError, AttributeError):
        return None


def group_by(rows, key):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row[key]].append(row)
    return grouped


def tick_is_complete(tick, policies, features, decisions, decisions_by_snapshot):
    """Verify stored identities and one-to-one links, not raw-feature hash contents."""
    policy_rows = policies[tick["policy_version_id"]]
    if len(policy_rows) != 1:
        return False
    policy = policy_rows[0]
    count = tick["candidates_scored"]
    if not (
        tick["replay_status"] == "snapshot_complete"
        and stored_uuid(tick["id"])
        and stored_uuid(tick["policy_version_id"])
        and stored_hash(tick["candidate_set_hash"])
        and tick["feature_as_of"] is not None
        and policy["feature_schema_version"] == FEATURE_SCHEMA
        and stored_hash(policy["config_hash"])
        and policy["version"] == "sha256:" + policy["config_hash"]
        and isinstance(count, int) and count >= 0
        and len(features) == len(decisions) == count
    ):
        return False

    by_id = {feature["id"]: feature for feature in features}
    if (len(by_id) != count
            or len({feature["candidate_id"] for feature in features}) != count
            or len({decision["id"] for decision in decisions}) != count
            or len({decision["feature_snapshot_id"] for decision in decisions}) != count):
        return False
    for feature in features:
        references = decisions_by_snapshot[feature["id"]]
        if not (
            stored_uuid(feature["id"])
            and len(references) == 1
            and references[0]["tick_audit_id"] == tick["id"]
            and isinstance(feature["candidate_id"], str)
            and feature["candidate_id"].strip()
            and feature["policy_version_id"] == tick["policy_version_id"]
            and feature["candidate_set_hash"] == tick["candidate_set_hash"]
            and feature["feature_as_of"] == tick["feature_as_of"]
            and feature["feature_schema_version"] == policy["feature_schema_version"]
            and stored_hash(feature["feature_hash"])
        ):
            return False
    for decision in decisions:
        feature = by_id.get(decision["feature_snapshot_id"])
        if not (
            feature is not None
            and stored_uuid(decision["id"])
            and decision["channel_profile_id"] == tick["channel_profile_id"]
            and decision["policy_version_id"] == tick["policy_version_id"]
            and decision["candidate_set_hash"] == tick["candidate_set_hash"]
            and stored_hash(decision["decision_hash"])
            and decision["decision"] in {"accepted", "rejected"}
            and decision["selected"] == (decision["decision"] == "accepted")
            and all(decision[key] == feature[key] for key in (
                "candidate_id", "candidate_source", "topic_lane_id",
                "lane_format_id", "target_account_id",
            ))
        ):
            return False
    return True


def failure(code):
    return {"ok": False, "expected_migration_head": EXPECTED_HEAD,
            "migration_head": None, "errors": [code]}


async def collect_report(connection):
    """Caller must hold a read-only repeatable-read transaction across all queries."""
    heads = await connection.fetch(HEAD_SQL)
    if len(heads) != 1 or heads[0]["version_num"] != EXPECTED_HEAD:
        return failure("migration_head_mismatch")
    ticks = await connection.fetch(TICKS_SQL)
    policies = group_by(await connection.fetch(POLICIES_SQL), "id")
    features = group_by(await connection.fetch(FEATURES_SQL), "tick_audit_id")
    decision_rows = await connection.fetch(DECISIONS_SQL)
    decisions = group_by(decision_rows, "tick_audit_id")
    # Legacy decisions remain unreplayable but can still corrupt new reverse links.
    decisions_by_snapshot = group_by(decision_rows, "feature_snapshot_id")
    modes = dict.fromkeys(("off", "shadow", "canary", "active", "unknown"), 0)
    for activation in await connection.fetch(ACTIVATIONS_SQL):
        mode = activation["mode"] if activation["mode"] in modes else "unknown"
        modes[mode] += activation["count"]

    counts = dict.fromkeys(("total", "legacy", "new", "pending", "complete_labelled",
                           "actually_complete", "partial"), 0)
    evidence = []
    for tick in ticks:
        counts["total"] += 1
        if tick["replay_status"] == "legacy_unreplayable":
            counts["legacy"] += 1
            continue
        counts["new"] += 1
        counts["pending"] += tick["replay_status"] == "snapshot_pending"
        counts["complete_labelled"] += tick["replay_status"] == "snapshot_complete"
        complete = tick_is_complete(
            tick, policies, features[tick["id"]], decisions[tick["id"]], decisions_by_snapshot,
        )
        counts["actually_complete" if complete else "partial"] += 1
        policy_rows = policies[tick["policy_version_id"]]
        evidence.append({
            "tick_audit_id": stored_uuid(tick["id"]),
            "policy_version_id": stored_uuid(tick["policy_version_id"]),
            "candidate_set_hash": stored_hash(tick["candidate_set_hash"]),
            "policy_config_hash": stored_hash(policy_rows[0]["config_hash"]) if len(policy_rows) == 1 else None,
            "complete": complete,
        })
    counts["coverage"] = counts["actually_complete"] / counts["new"] if counts["new"] else None
    errors = []
    if counts["partial"]:
        errors.append("partial_new_ticks")
    if modes["canary"] or modes["active"] or modes["unknown"]:
        errors.append("non_passive_activation_history")
    return {
        "ok": not errors,
        "expected_migration_head": EXPECTED_HEAD,
        "migration_head": EXPECTED_HEAD,
        "ticks": counts,
        "activation_modes": modes,
        "tick_evidence": sorted(evidence, key=lambda row: row["tick_audit_id"] or ""),
        "production_run_proven": False,
        "errors": errors,
    }


def connection_options(database_url):
    """Parse an explicit target; prevent ambient PG variables or URL session overrides."""
    if not database_url:
        raise ValueError("invalid_database_configuration")
    url = urlsplit(database_url)
    options = parse_qsl(url.query, strict_parsing=True)
    if not (
        url.scheme in {"postgres", "postgresql", "postgresql+asyncpg"}
        and url.hostname and url.username and url.path.startswith("/")
        and len(url.path) > 1 and not url.fragment
        and len(options) <= 1
        and all(key == "sslmode" and value in {
            "disable", "allow", "prefer", "require", "verify-ca", "verify-full",
        } for key, value in options)
    ):
        raise ValueError("invalid_database_configuration")
    return {
        "host": url.hostname,
        "port": url.port or 5432,
        "user": unquote(url.username),
        "password": unquote(url.password or ""),
        "database": unquote(url.path[1:]),
        "ssl": dict(options).get("sslmode", "prefer"),
        "timeout": 5,
        "command_timeout": 15,
        "server_settings": {
            "default_transaction_read_only": "on",
            "statement_timeout": "15000",
            "idle_in_transaction_session_timeout": "15000",
            "search_path": "pg_catalog,public",
        },
    }


async def read_database(options):
    import asyncpg

    connection = await asyncpg.connect(**options)
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            return await collect_report(connection)
    finally:
        await connection.close(timeout=5)


def write_evidence(destination, payload):
    """Publish a fully written private inode atomically without replacing any path."""
    path = Path(destination)
    if path.exists() or path.is_symlink():
        raise FileExistsError("evidence_destination_exists")
    fd, temporary = tempfile.mkstemp(prefix=".policy-snapshot-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        # link() is an atomic no-clobber publication, unlike replace()/rename().
        os.link(temporary, path, follow_symlinks=False)
    finally:
        os.unlink(temporary)


class SanitizedParser(argparse.ArgumentParser):
    def error(self, _message):
        raise ValueError("invalid_arguments")


def main(argv=None):
    parser = SanitizedParser(description=__doc__)
    parser.add_argument("--evidence", metavar="PATH", help="new private JSON evidence file")
    try:
        args = parser.parse_args(argv)
    except ValueError:
        print(json.dumps(failure("invalid_arguments")))
        return 1
    try:
        options = connection_options(os.environ.get("DATABASE_URL"))
    except (ValueError, TypeError):
        report = failure("invalid_database_configuration")
    else:
        try:
            report = asyncio.run(read_database(options))
        except Exception:
            # Driver exceptions can contain credentials, SQL or source data.
            report = failure("database_read_failed")
    payload = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.evidence is not None:
        try:
            write_evidence(args.evidence, payload)
        except Exception:
            report["ok"] = False
            report["errors"].append("evidence_write_failed")
            payload = json.dumps(report, sort_keys=True, indent=2) + "\n"
    sys.stdout.write(payload)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
