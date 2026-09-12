"""Offline schema contracts; these do not qualify PostgreSQL enforcement."""
from __future__ import annotations

import io
import runpy
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import CheckConstraint, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateColumn

from app.models import channel_agent as models


PREVIOUS = "043_owned_history_snapshot_rows"
REVISION = "044_policy_decision_snapshots"
MIGRATION = Path(__file__).resolve().parents[2] / f"alembic/versions/{REVISION}.py"
POLICY_REQUIRED = {
    "id", "policy_key", "version", "status", "feature_schema_version", "reward_version",
    "formula_json", "hard_guard_config_json", "portfolio_config_json", "exploration_config_json",
    "code_commit_sha", "template_registry_version", "prompt_bundle_version", "config_hash",
    "created_by", "change_reason", "created_at",
}
ACTIVATION_REQUIRED = {
    "id", "channel_profile_id", "policy_version_id", "mode", "rollout_percentage",
    "deterministic_salt", "effective_from", "request_id", "actor", "reason",
    "feature_flag_snapshot_json", "created_at",
}
ACTIVATION_OPTIONAL = {
    "target_account_id", "effective_to", "previous_activation_id", "rollback_reason",
}
SNAPSHOT_REQUIRED = {
    "id", "tick_audit_id", "candidate_id", "candidate_source", "source_kind",
    "policy_version_id", "feature_schema_version", "feature_as_of", "raw_features_json",
    "missing_feature_mask_json", "source_record_refs_json", "candidate_set_hash", "feature_hash",
    "created_at",
}
SNAPSHOT_OPTIONAL = {
    "topic_lane_id", "lane_format_id", "target_account_id", "normalized_features_json",
    "cadence_snapshot_json", "content_mix_snapshot_json", "material_supply_json",
    "production_reliability_json", "learning_references_json", "cost_estimate_json",
    "risk_estimate_json",
}
TICK_OPTIONAL = {"policy_version_id", "candidate_set_hash", "feature_as_of"}
DECISION_OPTIONAL = {
    "policy_version_id", "feature_snapshot_id", "candidate_set_hash", "decision_hash", "decision",
    "baseline_score", "final_score", "rank", "shadow_score", "shadow_rank", "shadow_selected",
    "experiment_id",
}
CONTRACTS = (
    ("DecisionPolicyVersion", POLICY_REQUIRED, set()),
    ("PolicyActivationHistory", ACTIVATION_REQUIRED, ACTIVATION_OPTIONAL),
    ("CandidateFeatureSnapshot", SNAPSHOT_REQUIRED, SNAPSHOT_OPTIONAL),
)
FOREIGN_KEYS = {
    "decision_policy_versions": {},
    "policy_activation_history": {
        "channel_profile_id": "channel_profiles.id",
        "target_account_id": "publishing_accounts.id",
        "policy_version_id": "decision_policy_versions.id",
        "previous_activation_id": "policy_activation_history.id",
    },
    "candidate_feature_snapshots": {
        "tick_audit_id": "agent_tick_audits.id", "policy_version_id": "decision_policy_versions.id",
    },
    "agent_tick_audits": {"policy_version_id": "decision_policy_versions.id"},
    "decision_audit_entries": {
        "policy_version_id": "decision_policy_versions.id",
        "feature_snapshot_id": "candidate_feature_snapshots.id",
    },
}
UNIQUE_COLUMNS = {
    "DecisionPolicyVersion": ("policy_key", "version"),
    "PolicyActivationHistory": ("request_id",),
    "CandidateFeatureSnapshot": ("tick_audit_id", "candidate_id", "feature_schema_version"),
}


@pytest.mark.parametrize("name,required,optional", CONTRACTS)
def test_immutable_fact_model_contract(name, required, optional):
    model = getattr(models, name, None)
    assert model is not None, f"missing {name}"
    table = model.__table__
    assert set(table.c.keys()) == required | optional
    assert {c.name for c in table.c if not c.nullable} == required
    assert tuple(table.primary_key.columns.keys()) == ("id",)
    assert UNIQUE_COLUMNS[name] in {
        tuple(c.columns.keys()) for c in table.constraints if isinstance(c, UniqueConstraint)
    }
    for column in optional:
        assert table.c[column].default is None
        assert table.c[column].server_default is None
    for column in required - {"id", "created_at"}:
        assert table.c[column].default is None
        assert table.c[column].server_default is None
    assert table.c.created_at.type.timezone
    assert table.c.created_at.onupdate is None
    for column in required | optional:
        if column.endswith("_json"):
            assert table.c[column].type.__class__.__name__ == "JSON"
            assert table.c[column].type.none_as_null


def test_audit_fields_preserve_missingness_and_old_writer_default():
    for model, columns in (
        (models.AgentTickAudit, TICK_OPTIONAL), (models.DecisionAuditEntry, DECISION_OPTIONAL),
    ):
        assert columns <= set(model.__table__.c.keys())
        for name in columns:
            column = model.__table__.c[name]
            assert column.nullable and column.default is None and column.server_default is None
    replay = models.AgentTickAudit.__table__.c.replay_status
    assert not replay.nullable
    assert replay.default.arg == "legacy_unreplayable"
    assert str(replay.server_default.arg) == "legacy_unreplayable"
    assert "decision" not in models.CandidateFeatureSnapshot.__table__.c


def test_foreign_keys_protect_facts_without_changing_legacy_cascades():
    for table_name, expected in FOREIGN_KEYS.items():
        table = models.Base.metadata.tables.get(table_name)
        assert table is not None, f"missing {table_name}"
        for name, target in expected.items():
            fk, = table.c[name].foreign_keys
            assert fk.target_fullname == target
            assert fk.ondelete == "RESTRICT"
            assert fk.onupdate in (None, "NO ACTION", "RESTRICT")
    tick_fk, = models.AgentTickAudit.__table__.c.channel_profile_id.foreign_keys
    decision_fk, = models.DecisionAuditEntry.__table__.c.tick_audit_id.foreign_keys
    assert tick_fk.ondelete == decision_fk.ondelete == "CASCADE"


@pytest.mark.parametrize("model_name,expressions", [
    ("DecisionPolicyVersion", ["status IN ('draft','validated','retired')"]),
    ("PolicyActivationHistory", [
        "mode IN ('off','shadow','canary','active')",
        "rollout_percentage >= 0 AND rollout_percentage <= 100",
        "effective_to IS NULL OR effective_to > effective_from",
    ]),
    ("AgentTickAudit", ["replay_status IN ('legacy_unreplayable','snapshot_pending','snapshot_complete')"]),
    ("DecisionAuditEntry", ["decision IN ('accepted','rejected')"]),
])
def test_model_checks(model_name, expressions):
    model = getattr(models, model_name, None)
    assert model is not None, f"missing {model_name}"
    checks = {str(c.sqltext) for c in model.__table__.constraints if isinstance(c, CheckConstraint)}
    assert set(expressions) <= checks


def render_migration(direction):
    assert MIGRATION.is_file(), f"missing {REVISION}"
    migration = runpy.run_path(str(MIGRATION))
    assert migration["revision"] == REVISION
    assert migration["down_revision"] == PREVIOUS
    output = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output},
    )
    with Operations.context(context):
        migration[direction]()
    return output.getvalue()


def test_offline_migration_guards_and_downgrade_order():
    upgrade = render_migration("upgrade")
    downgrade = render_migration("downgrade")
    for table in ("decision_policy_versions", "policy_activation_history", "candidate_feature_snapshots"):
        assert f"CREATE TABLE {table}" in upgrade
        assert f"BEFORE UPDATE OR DELETE ON {table}" in upgrade
        assert f"DROP TRIGGER trg_{table}_immutable ON {table}" in downgrade
        assert downgrade.index(f"DROP TRIGGER trg_{table}_immutable") < downgrade.index("DROP FUNCTION")
    assert "immutable_policy_fact" in upgrade
    assert "WHEN (OLD.feature_snapshot_id IS NOT NULL)" in upgrade
    assert "WHEN (OLD.replay_status = 'snapshot_complete')" in upgrade
    assert "DEFAULT 'legacy_unreplayable' NOT NULL" in upgrade
    assert "UPDATE decision_audit_entries" not in upgrade


def test_offline_migration_column_types_and_checks_match_models():
    sql = render_migration("upgrade")
    dialect = postgresql.dialect()
    for name, required, optional in CONTRACTS:
        table = getattr(models, name).__table__
        statement = sql.split(f"CREATE TABLE {table.name} (", 1)[1].split(";", 1)[0]
        for column in required | optional:
            declaration = str(CreateColumn(table.c[column]).compile(dialect=dialect))
            assert declaration in statement, (table.name, declaration)
        for constraint in table.constraints:
            if isinstance(constraint, CheckConstraint):
                assert f"CONSTRAINT {constraint.name} CHECK ({constraint.sqltext})" in statement
            elif isinstance(constraint, UniqueConstraint):
                assert f"CONSTRAINT {constraint.name} UNIQUE ({', '.join(constraint.columns.keys())})" in statement
    for table, columns in (
        (models.AgentTickAudit.__table__, TICK_OPTIONAL | {"replay_status"}),
        (models.DecisionAuditEntry.__table__, DECISION_OPTIONAL),
    ):
        for name in columns:
            declaration = str(CreateColumn(table.c[name]).compile(dialect=dialect))
            assert f"ALTER TABLE {table.name} ADD COLUMN {declaration};" in sql
    for name in SNAPSHOT_OPTIONAL:
        column = models.CandidateFeatureSnapshot.__table__.c[name]
        if name.endswith("_json"):
            assert column.type.bind_processor(dialect)(None) is None
