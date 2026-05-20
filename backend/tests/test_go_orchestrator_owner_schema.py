from __future__ import annotations

from app.models.job import Job
from app.schemas.job import JobResponse


def test_job_model_has_orchestrator_owner_column() -> None:
    assert "orchestrator_owner" in Job.__table__.columns
    column = Job.__table__.columns["orchestrator_owner"]
    assert column.default is not None


def test_job_response_exposes_orchestrator_owner() -> None:
    fields = JobResponse.model_fields
    assert "orchestrator_owner" in fields
    assert fields["orchestrator_owner"].default == "python"
