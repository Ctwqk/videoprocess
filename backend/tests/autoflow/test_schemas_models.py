from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy.dialects import postgresql

from app.schemas.pipeline import PipelineDefinition


def test_autoflow_request_defaults_are_conservative():
    from app.schemas.autoflow import AutoFlowRequest

    request = AutoFlowRequest(prompt="我要一个 30 秒小猫视频集锦")

    assert request.source_policy == "owned_only"
    assert request.publish_mode == "preview_only"
    assert request.aspect_ratio == "auto"
    assert request.material_library_ids == []
    assert request.source_platforms == ["youtube", "bilibili", "x", "xiaohongshu"]
    assert request.user_constraints == {}


def test_autoflow_plan_uses_pipeline_definition_shape():
    from app.schemas.autoflow import (
        AutoFlowIntent,
        AutoFlowMetadata,
        AutoFlowPlan,
        AutoFlowRequest,
    )

    definition = PipelineDefinition(nodes=[], edges=[])
    plan = AutoFlowPlan(
        plan_id="plan_1",
        request=AutoFlowRequest(prompt="预览一个短视频"),
        intent=AutoFlowIntent(intent_type="generic_video", subject="短视频"),
        template_id="material_library_remix",
        pipeline_definition=definition,
        metadata=AutoFlowMetadata(title_candidates=["标题"]),
    )

    assert isinstance(plan.pipeline_definition, PipelineDefinition)
    assert plan.pipeline_definition.viewport == {"x": 0, "y": 0, "zoom": 1}
    assert plan.needs_review is True


def test_invalid_policy_values_are_rejected():
    from pydantic import ValidationError

    from app.schemas.autoflow import AutoFlowRequest

    with pytest.raises(ValidationError):
        AutoFlowRequest(prompt="test", source_policy="external_everywhere")

    with pytest.raises(ValidationError):
        AutoFlowRequest(prompt="test", publish_mode="public_now")


def test_autoflow_orm_models_import_and_define_expected_tables():
    from app.models import AutoFlowPlan as ImportedPlan
    from app.models.autoflow import AutoFlowPlan, AutoFlowRun, ContentMetric, TrendSignal

    assert ImportedPlan is AutoFlowPlan
    assert AutoFlowPlan.__tablename__ == "autoflow_plans"
    assert AutoFlowRun.__tablename__ == "autoflow_runs"
    assert ContentMetric.__tablename__ == "content_metrics"
    assert TrendSignal.__tablename__ == "trend_signals"

    assert isinstance(AutoFlowPlan.__table__.c.intent_json.type, postgresql.JSON)
    assert isinstance(AutoFlowPlan.__table__.c.pipeline_definition.type, postgresql.JSON)
    assert isinstance(AutoFlowRun.__table__.c.artifacts_json.type, postgresql.JSON)
    assert isinstance(ContentMetric.__table__.c.retention_json.type, postgresql.JSON)
    assert isinstance(TrendSignal.__table__.c.metadata_json.type, postgresql.JSON)


def test_autoflow_migration_declares_required_tables():
    migration = Path("alembic/versions/004_autoflow.py")
    review_state_migration = Path("alembic/versions/005_autoflow_review_state.py")

    assert migration.exists()
    assert review_state_migration.exists()
    text = migration.read_text()
    review_state_text = review_state_migration.read_text()

    assert 'revision: str = "004"' in text
    assert 'down_revision: Union[str, None] = "003"' in text
    assert 'revision: str = "005"' in review_state_text
    assert 'down_revision: Union[str, None] = "004"' in review_state_text
    for table_name in (
        "autoflow_plans",
        "autoflow_runs",
        "content_metrics",
        "trend_signals",
    ):
        assert f'"{table_name}"' in text
    for column_name in (
        "request_json",
        "review_approved_at",
        "public_approved_at",
        "review_notes",
        "rejected_reason",
        "error_message",
    ):
        assert column_name in review_state_text
