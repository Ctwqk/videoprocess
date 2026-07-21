from __future__ import annotations

from pathlib import Path

from app.models.publication_promotion_operation import PublicationPromotionOperation


def test_promotion_operation_model_has_stable_identity_and_safe_target_constraint():
    table = PublicationPromotionOperation.__table__

    assert {column.name for column in table.primary_key.columns} == {"id"}
    assert {constraint.name for constraint in table.constraints} >= {
        "uq_publication_promotion_operations_publication",
        "uq_publication_promotion_operations_queue_item",
        "uq_publication_promotion_operations_attempt_key",
        "ck_publication_promotion_operations_target_privacy",
        "ck_publication_promotion_operations_status",
    }
    assert table.c.decision_json.nullable is False
    assert table.c.queue_item_id.nullable is False
    assert table.c.attempt_key.nullable is False


def test_promotion_operation_migration_follows_current_head():
    migration = Path("alembic/versions/027_publication_promotion_operations.py").read_text()

    assert 'revision: str = "027_publication_promotion_operations"' in migration
    assert 'down_revision: Union[str, None] = "026_autoflow_authority_fence"' in migration
    assert "publication_promotion_operations" in migration
    assert "target_privacy IN ('private', 'unlisted')" in migration
    assert "reserved', 'submitting', 'confirmed', 'finalized', 'uncertain" in migration
