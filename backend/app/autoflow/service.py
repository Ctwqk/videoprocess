from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.autoflow.clip_ranker import ClipRanker
from app.autoflow.intent_parser import RuleBasedIntentParser
from app.autoflow.material_selector import MaterialSelector
from app.autoflow.metadata_generator import MetadataGenerator
from app.autoflow.pipeline_builder import PipelineBuilder
from app.autoflow.rights_policy import RightsPolicy
from app.autoflow.template_library import TemplateLibrary
from app.autoflow.validation_repair import AutoFlowRepairService
from app.models.autoflow import AutoFlowPlan as AutoFlowPlanModel
from app.models.autoflow import AutoFlowRun as AutoFlowRunModel
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import (
    AutoFlowClipCandidate,
    AutoFlowExecuteRequest,
    AutoFlowIntent,
    AutoFlowMetadata,
    AutoFlowPlan,
    AutoFlowPlanPatch,
    AutoFlowRequest,
    AutoFlowRun,
)
from app.schemas.pipeline import PipelineCreate, PipelineDefinition
from app.services.job_runtime import start_or_defer_jobs
from app.services.job_service import create_job
from app.services.pipeline_service import create_pipeline


class CandidateSelector(Protocol):
    async def find_candidates(
        self,
        intent,
        request: AutoFlowRequest,
        db: AsyncSession | None = None,
    ) -> list[AutoFlowClipCandidate]:
        ...


class AutoFlowService:
    def __init__(
        self,
        material_selector: CandidateSelector | None = None,
        clip_ranker: ClipRanker | None = None,
    ) -> None:
        self.intent_parser = RuleBasedIntentParser()
        self.template_library = TemplateLibrary()
        self.metadata_generator = MetadataGenerator()
        self.pipeline_builder = PipelineBuilder()
        self.validation_repair = AutoFlowRepairService()
        self.rights_policy = RightsPolicy()
        self.material_selector = material_selector or MaterialSelector()
        self.clip_ranker = clip_ranker or ClipRanker()
        self._plans: dict[str, AutoFlowPlan] = {}
        self._runs: dict[str, AutoFlowRun] = {}

    async def plan(self, request: AutoFlowRequest, db: AsyncSession | None = None) -> AutoFlowPlan:
        intent = self.intent_parser.parse(request)
        template = self.template_library.select_template(intent)
        warnings: list[str] = []
        candidates = await self.material_selector.find_candidates(intent, request, db=db)
        if not candidates:
            candidates = self._fixture_candidates(intent, request)
            warnings.append("Material selector returned no candidates; using AutoFlow fixture candidates.")
        ranked_candidates = self.clip_ranker.rank(intent, candidates)
        if len(ranked_candidates) < 5:
            warnings.append("AutoFlow found fewer than 5 candidate clips; review material coverage before publishing.")
        metadata = self.metadata_generator.generate(intent, ranked_candidates)
        definition = self.pipeline_builder.build(template, intent, ranked_candidates, metadata)
        validation = validate_pipeline(definition)
        repair_result = None
        if not validation.valid:
            repair_result = self.validation_repair.repair(definition, validation.errors, ranked_candidates)
            definition = repair_result.definition
            validation = validate_pipeline(definition)

        rights = self.rights_policy.evaluate(request, ranked_candidates)
        rights_payload = rights.model_dump()
        plan = AutoFlowPlan(
            plan_id=str(uuid.uuid4()),
            request=request,
            intent=intent,
            template_id=template.id,
            pipeline_definition=definition,
            candidates=ranked_candidates,
            metadata=metadata,
            validation={
                "valid": validation.valid,
                "errors": [error.model_dump(mode="json") for error in validation.errors],
                "warnings": [warning.model_dump(mode="json") for warning in validation.warnings],
                "repairs": repair_result.applied_repairs if repair_result else [],
                "plan_warnings": warnings if validation.valid else [*warnings, "Generated workflow still needs manual repair."],
            },
            rights=rights_payload,
            warnings=warnings if validation.valid else [*warnings, "Generated workflow still needs manual repair."],
            needs_review=_needs_review(rights_payload, review_approved_at=None),
            status=_status_for(rights_payload, request.publish_mode),
        )
        if db is not None:
            return await self._save_plan(db, plan)

        self._plans[plan.plan_id] = plan
        return plan

    async def list_plans(self, db: AsyncSession | None = None) -> list[AutoFlowPlan]:
        if db is None:
            return list(self._plans.values())

        result = await db.execute(select(AutoFlowPlanModel).order_by(AutoFlowPlanModel.created_at.desc()))
        return [_plan_from_model(row) for row in result.scalars().all()]

    async def get_plan(self, plan_id: str, db: AsyncSession | None = None) -> AutoFlowPlan | None:
        if db is None:
            return self._plans.get(plan_id)

        plan_uuid = _uuid_or_none(plan_id)
        if plan_uuid is None:
            return None
        row = await db.get(AutoFlowPlanModel, plan_uuid)
        return _plan_from_model(row) if row else None

    async def patch_plan(
        self,
        plan_id: str,
        patch: AutoFlowPlanPatch,
        db: AsyncSession | None = None,
    ) -> AutoFlowPlan | None:
        plan = await self.get_plan(plan_id, db)
        if not plan:
            return None

        request = _patched_request(plan.request, patch)
        intent = plan.intent.model_copy(
            update={
                "target_platforms": request.target_platforms,
                "source_policy": request.source_policy,
                "publish_mode": request.publish_mode,
            }
        )
        candidates = _patched_candidates(plan.candidates, patch)
        metadata = _patched_metadata(plan.metadata, patch.metadata)
        definition = plan.pipeline_definition
        validation_payload = dict(plan.validation)
        warnings = list(plan.warnings)

        if patch.rebuild_definition:
            template = self.template_library.get_template(plan.template_id)
            definition = self.pipeline_builder.build(template, intent, candidates, metadata)

        if patch.run_validation:
            validation = validate_pipeline(definition)
            validation_payload = {
                "valid": validation.valid,
                "errors": [error.model_dump(mode="json") for error in validation.errors],
                "warnings": [warning.model_dump(mode="json") for warning in validation.warnings],
                "repairs": [],
                "plan_warnings": warnings,
            }

        rights_payload = dict(plan.rights)
        if patch.evaluate_rights:
            rights_payload = self.rights_policy.evaluate(request, candidates).model_dump(mode="json")

        approvals_reset = _patch_resets_approval(patch)
        review_approved_at = None if approvals_reset else plan.review_approved_at
        public_approved_at = None if approvals_reset else plan.public_approved_at
        updated = plan.model_copy(
            update={
                "request": request,
                "intent": intent,
                "pipeline_definition": definition,
                "candidates": candidates,
                "metadata": metadata,
                "validation": validation_payload,
                "rights": rights_payload,
                "warnings": warnings,
                "needs_review": _needs_review(rights_payload, review_approved_at=review_approved_at),
                "status": _status_for(
                    rights_payload,
                    request.publish_mode,
                    review_approved_at=review_approved_at,
                    public_approved_at=public_approved_at,
                ),
                "review_approved_at": review_approved_at,
                "public_approved_at": public_approved_at,
                "rejected_reason": None,
            }
        )

        if db is not None:
            return await self._save_plan(db, updated)

        self._plans[plan_id] = updated
        return updated

    async def approve(
        self,
        plan_id: str,
        db: AsyncSession | None = None,
        review_notes: str | None = None,
    ) -> AutoFlowPlan | None:
        plan = await self.get_plan(plan_id, db)
        if not plan:
            return None
        _assert_not_blocked_or_rejected(plan, action="approve")

        now = _utcnow()
        rights = {**plan.rights, "review_approved": True}
        updated = plan.model_copy(
            update={
                "needs_review": False,
                "rights": rights,
                "status": _status_for(
                    rights,
                    plan.request.publish_mode,
                    review_approved_at=now,
                    public_approved_at=plan.public_approved_at,
                ),
                "review_approved_at": now,
                "review_notes": review_notes,
                "rejected_reason": None,
            }
        )
        if db is not None:
            return await self._save_plan(db, updated)

        self._plans[plan_id] = updated
        return updated

    async def approve_public(
        self,
        plan_id: str,
        db: AsyncSession | None = None,
        review_notes: str | None = None,
    ) -> AutoFlowPlan | None:
        plan = await self.get_plan(plan_id, db)
        if not plan:
            return None
        _assert_not_blocked_or_rejected(plan, action="approve public publication")
        if _requires_review_approval(plan) and not plan.review_approved_at:
            raise PermissionError("AutoFlow plan requires review approval before public approval")

        now = _utcnow()
        rights = {**plan.rights, "review_approved": True, "public_approved": True, "publish_allowed": True}
        updated = plan.model_copy(
            update={
                "needs_review": False,
                "rights": rights,
                "status": "public_approved",
                "review_approved_at": plan.review_approved_at or now,
                "public_approved_at": now,
                "review_notes": review_notes if review_notes is not None else plan.review_notes,
                "rejected_reason": None,
            }
        )
        if db is not None:
            return await self._save_plan(db, updated)

        self._plans[plan_id] = updated
        return updated

    async def reject(
        self,
        plan_id: str,
        db: AsyncSession | None = None,
        rejected_reason: str | None = None,
    ) -> AutoFlowPlan | None:
        plan = await self.get_plan(plan_id, db)
        if not plan:
            return None
        updated = plan.model_copy(
            update={
                "status": "rejected",
                "needs_review": True,
                "review_approved_at": None,
                "public_approved_at": None,
                "rejected_reason": rejected_reason or "Rejected by reviewer",
            }
        )
        if db is not None:
            return await self._save_plan(db, updated)

        self._plans[plan_id] = updated
        return updated

    async def execute(
        self,
        request: AutoFlowExecuteRequest,
        db: AsyncSession | None = None,
    ) -> AutoFlowRun:
        plan = await self._resolve_execute_plan(request, db)
        if not plan:
            raise ValueError("AutoFlow plan not found")

        _assert_execute_allowed(plan, request)

        pipeline_id: str | None = None
        job_id: str | None = None
        run_status = "pending"
        error_message: str | None = None
        artifacts: dict[str, Any] = {}
        if db is not None and request.execute:
            try:
                pipeline = await create_pipeline(
                    db,
                    PipelineCreate(
                        name=f"AutoFlow {plan.intent.subject}",
                        description=f"Generated from prompt: {plan.request.prompt}",
                        definition=plan.pipeline_definition,
                        is_template=request.save_as_template,
                        template_tags=["autoflow", plan.template_id],
                    ),
                )
                pipeline_id = str(pipeline.id)
                artifacts["pipeline_id"] = pipeline_id
                job = await create_job(db, pipeline.id)
                await start_or_defer_jobs(db, [job])
                job_id = str(job.id)
                artifacts["job_id"] = job_id
                run_status = str(job.status.value)
            except Exception as exc:
                await db.rollback()
                run_status = "failed"
                error_message = str(exc)
                artifacts["error"] = error_message

        run = AutoFlowRun(
            run_id=str(uuid.uuid4()),
            plan_id=plan.plan_id,
            pipeline_id=pipeline_id,
            job_id=job_id,
            status=run_status,
            artifacts=artifacts,
            publish={
                "mode": plan.request.publish_mode,
                "review_approved": bool(plan.review_approved_at or request.review_approved),
                "public_approved": bool(plan.public_approved_at or request.public_approved),
            },
            error_message=error_message,
        )
        if db is not None:
            return await self._save_run(db, run, mark_plan_executed=request.execute and error_message is None)

        self._runs[run.run_id] = run
        return run

    async def list_runs(self, db: AsyncSession | None = None) -> list[AutoFlowRun]:
        if db is None:
            return list(self._runs.values())

        result = await db.execute(select(AutoFlowRunModel).order_by(AutoFlowRunModel.created_at.desc()))
        return [_run_from_model(row) for row in result.scalars().all()]

    async def get_run(self, run_id: str, db: AsyncSession | None = None) -> AutoFlowRun | None:
        if db is None:
            return self._runs.get(run_id)

        run_uuid = _uuid_or_none(run_id)
        if run_uuid is None:
            return None
        row = await db.get(AutoFlowRunModel, run_uuid)
        return _run_from_model(row) if row else None

    async def _resolve_execute_plan(
        self,
        request: AutoFlowExecuteRequest,
        db: AsyncSession | None,
    ) -> AutoFlowPlan | None:
        if request.plan_id:
            return await self.get_plan(request.plan_id, db)
        if request.plan and db is not None:
            return await self._save_plan(db, request.plan)
        return request.plan

    async def _save_plan(self, db: AsyncSession, plan: AutoFlowPlan) -> AutoFlowPlan:
        plan_uuid = uuid.UUID(plan.plan_id)
        row = await db.get(AutoFlowPlanModel, plan_uuid)
        if row is None:
            row = AutoFlowPlanModel(id=plan_uuid)
            db.add(row)

        row.prompt = plan.request.prompt
        row.request_json = plan.request.model_dump(mode="json")
        row.intent_json = plan.intent.model_dump(mode="json")
        row.template_id = plan.template_id
        row.pipeline_definition = plan.pipeline_definition.model_dump(mode="json")
        row.candidates_json = [candidate.model_dump(mode="json") for candidate in plan.candidates]
        row.metadata_json = plan.metadata.model_dump(mode="json")
        row.rights_json = dict(plan.rights)
        row.validation_json = dict(plan.validation)
        row.status = plan.status
        row.review_approved_at = plan.review_approved_at
        row.public_approved_at = plan.public_approved_at
        row.review_notes = plan.review_notes
        row.rejected_reason = plan.rejected_reason

        await db.commit()
        await db.refresh(row)
        return _plan_from_model(row)

    async def _save_run(
        self,
        db: AsyncSession,
        run: AutoFlowRun,
        *,
        mark_plan_executed: bool,
    ) -> AutoFlowRun:
        run_uuid = uuid.UUID(run.run_id)
        plan_uuid = uuid.UUID(run.plan_id) if run.plan_id else None
        row = AutoFlowRunModel(
            id=run_uuid,
            plan_id=plan_uuid,
            pipeline_id=uuid.UUID(run.pipeline_id) if run.pipeline_id else None,
            job_id=uuid.UUID(run.job_id) if run.job_id else None,
            status=run.status,
            artifacts_json=run.artifacts,
            publish_json=run.publish,
            error_message=run.error_message,
        )
        db.add(row)

        if mark_plan_executed and plan_uuid is not None:
            plan_row = await db.get(AutoFlowPlanModel, plan_uuid)
            if plan_row is not None:
                plan_row.status = "executed"

        await db.commit()
        await db.refresh(row)
        return _run_from_model(row)

    def _fixture_candidates(
        self,
        intent,
        request: AutoFlowRequest,
    ) -> list[AutoFlowClipCandidate]:
        subject = intent.subject or "video"
        if request.source_policy in {"research_only", "remix_with_review"}:
            return [
                AutoFlowClipCandidate(
                    id="external-1",
                    title=f"{subject} external clip 1",
                    source_type="youtube",
                    url="https://example.test/autoflow-clip-1.mp4",
                    start_sec=0,
                    end_sec=5,
                    rights_status="review_required",
                ),
                AutoFlowClipCandidate(
                    id="external-2",
                    title=f"{subject} external clip 2",
                    source_type="youtube",
                    url="https://example.test/autoflow-clip-2.mp4",
                    start_sec=0,
                    end_sec=5,
                    rights_status="review_required",
                ),
            ]
        return [
            AutoFlowClipCandidate(
                id="owned-1",
                title=f"{subject} owned clip 1",
                source_type="asset",
                asset_id="autoflow-demo-asset-1",
                start_sec=0,
                end_sec=5,
                rights_status="allowed",
            ),
            AutoFlowClipCandidate(
                id="owned-2",
                title=f"{subject} owned clip 2",
                source_type="asset",
                asset_id="autoflow-demo-asset-2",
                start_sec=0,
                end_sec=5,
                rights_status="allowed",
            ),
        ]


def _plan_from_model(row: AutoFlowPlanModel) -> AutoFlowPlan:
    rights = dict(row.rights_json or {})
    validation = dict(row.validation_json or {})
    return AutoFlowPlan(
        plan_id=str(row.id),
        request=AutoFlowRequest.model_validate(_request_json(row)),
        intent=AutoFlowIntent.model_validate(row.intent_json),
        template_id=row.template_id,
        pipeline_definition=PipelineDefinition.model_validate(row.pipeline_definition),
        candidates=[AutoFlowClipCandidate.model_validate(candidate) for candidate in (row.candidates_json or [])],
        metadata=AutoFlowMetadata.model_validate(row.metadata_json or {}),
        validation=validation,
        rights=rights,
        warnings=list(validation.get("plan_warnings") or []),
        needs_review=_needs_review(rights, row.review_approved_at),
        status=row.status,
        review_approved_at=row.review_approved_at,
        public_approved_at=row.public_approved_at,
        review_notes=row.review_notes,
        rejected_reason=row.rejected_reason,
    )


def _run_from_model(row: AutoFlowRunModel) -> AutoFlowRun:
    return AutoFlowRun(
        run_id=str(row.id),
        plan_id=str(row.plan_id) if row.plan_id else None,
        pipeline_id=str(row.pipeline_id) if row.pipeline_id else None,
        job_id=str(row.job_id) if row.job_id else None,
        status=row.status,
        artifacts=dict(row.artifacts_json or {}),
        publish=dict(row.publish_json or {}),
        error_message=row.error_message,
    )


def _request_json(row: AutoFlowPlanModel) -> dict[str, Any]:
    request_json = getattr(row, "request_json", None)
    if request_json:
        return dict(request_json)

    intent = row.intent_json or {}
    return {
        "prompt": row.prompt,
        "target_platforms": list(intent.get("target_platforms") or []),
        "duration_sec": intent.get("duration_sec"),
        "aspect_ratio": intent.get("aspect_ratio") or "auto",
        "source_policy": intent.get("source_policy") or "owned_only",
        "publish_mode": intent.get("publish_mode") or "preview_only",
        "material_library_ids": [],
        "user_constraints": {},
    }


def _patched_request(request: AutoFlowRequest, patch: AutoFlowPlanPatch) -> AutoFlowRequest:
    updates: dict[str, Any] = {}
    publish_mode = patch.publish_mode or patch.publish_settings.get("publish_mode") or patch.publish_settings.get("mode")
    if publish_mode:
        updates["publish_mode"] = publish_mode
    if patch.target_platforms is not None:
        updates["target_platforms"] = patch.target_platforms
    if patch.user_constraints is not None:
        updates["user_constraints"] = {**request.user_constraints, **patch.user_constraints}
    return request.model_copy(update=updates)


def _patched_candidates(
    candidates: list[AutoFlowClipCandidate],
    patch: AutoFlowPlanPatch,
) -> list[AutoFlowClipCandidate]:
    by_id = {candidate.id: candidate for candidate in candidates}
    for replacement in patch.replacement_candidates or []:
        by_id[replacement.id] = replacement

    if patch.selected_candidate_ids is not None:
        selected = [by_id[candidate_id] for candidate_id in patch.selected_candidate_ids if candidate_id in by_id]
    else:
        selected = list(by_id.values())

    locked_ids = set(patch.locked_candidate_ids or [])
    if locked_ids:
        selected = [
            candidate.model_copy(update={"metadata": {**candidate.metadata, "locked": candidate.id in locked_ids}})
            for candidate in selected
        ]
    return selected


def _patched_metadata(metadata: AutoFlowMetadata, patch_metadata: dict[str, Any] | None) -> AutoFlowMetadata:
    if patch_metadata is None:
        return metadata
    return AutoFlowMetadata.model_validate({**metadata.model_dump(mode="json"), **patch_metadata})


def _patch_resets_approval(patch: AutoFlowPlanPatch) -> bool:
    return any(
        (
            patch.selected_candidate_ids is not None,
            patch.locked_candidate_ids is not None,
            patch.replacement_candidates is not None,
            patch.publish_mode is not None,
            bool(patch.publish_settings),
        )
    )


def _status_for(
    rights: dict[str, Any],
    publish_mode: str,
    *,
    review_approved_at: datetime | None = None,
    public_approved_at: datetime | None = None,
) -> str:
    if rights.get("status") == "blocked":
        return "blocked"
    if public_approved_at is not None:
        return "public_approved"
    if review_approved_at is not None:
        return "review_approved"
    if rights.get("status") == "review_required" or publish_mode == "public_after_review":
        return "review_required"
    return "drafted"


def _needs_review(rights: dict[str, Any], review_approved_at: datetime | None) -> bool:
    return rights.get("status") != "allowed" and review_approved_at is None


def _requires_review_approval(plan: AutoFlowPlan) -> bool:
    return plan.rights.get("status") == "review_required" or plan.request.publish_mode in {
        "private_upload",
        "unlisted_upload",
        "public_after_review",
    }


def _assert_not_blocked_or_rejected(plan: AutoFlowPlan, *, action: str) -> None:
    if plan.status == "blocked" or plan.rights.get("status") == "blocked":
        raise PermissionError(f"Blocked AutoFlow plan cannot {action}")
    if plan.status == "rejected":
        raise PermissionError(f"Rejected AutoFlow plan cannot {action}")


def _assert_execute_allowed(plan: AutoFlowPlan, request: AutoFlowExecuteRequest) -> None:
    if plan.status == "blocked" or plan.rights.get("status") == "blocked":
        raise PermissionError("AutoFlow plan is blocked by rights policy")
    if plan.status == "rejected":
        raise PermissionError("AutoFlow plan was rejected and cannot be executed")

    publish_mode = plan.request.publish_mode
    upload_requested = publish_mode in {"private_upload", "unlisted_upload", "public_after_review"}
    review_approved = bool(plan.review_approved_at or request.review_approved)
    if plan.rights.get("status") == "review_required" and upload_requested and not review_approved:
        raise PermissionError("AutoFlow plan requires review approval before upload execution")

    public_approved = bool(plan.public_approved_at or request.public_approved)
    if publish_mode == "public_after_review" and not public_approved:
        raise PermissionError("AutoFlow plan requires public approval before public execution")


def _uuid_or_none(value: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


autoflow_service = AutoFlowService()
