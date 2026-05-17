from __future__ import annotations

import uuid
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from app.autoflow.clip_ranker import ClipRanker
from app.autoflow.intent_parser import RuleBasedIntentParser
from app.autoflow.material_selector import MaterialSelector
from app.autoflow.metadata_generator import MetadataGenerator
from app.autoflow.pipeline_builder import PipelineBuilder
from app.autoflow.rights_policy import RightsPolicy
from app.autoflow.template_library import TemplateLibrary
from app.autoflow.validation_repair import AutoFlowRepairService
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import (
    AutoFlowClipCandidate,
    AutoFlowExecuteRequest,
    AutoFlowPlan,
    AutoFlowRequest,
    AutoFlowRun,
)
from app.schemas.pipeline import PipelineCreate
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
                "errors": [error.model_dump() for error in validation.errors],
                "warnings": [warning.model_dump() for warning in validation.warnings],
                "repairs": repair_result.applied_repairs if repair_result else [],
            },
            rights=rights.model_dump(),
            warnings=warnings if validation.valid else [*warnings, "Generated workflow still needs manual repair."],
            needs_review=rights.status != "allowed",
        )
        self._plans[plan.plan_id] = plan
        return plan

    async def list_plans(self) -> list[AutoFlowPlan]:
        return list(self._plans.values())

    async def get_plan(self, plan_id: str) -> AutoFlowPlan | None:
        return self._plans.get(plan_id)

    async def approve(self, plan_id: str) -> AutoFlowPlan | None:
        plan = self._plans.get(plan_id)
        if not plan:
            return None
        plan.needs_review = False
        plan.rights = {**plan.rights, "review_approved": True}
        self._plans[plan_id] = plan
        return plan

    async def execute(
        self,
        request: AutoFlowExecuteRequest,
        db: AsyncSession | None = None,
    ) -> AutoFlowRun:
        plan = request.plan
        if request.plan_id:
            plan = self._plans.get(request.plan_id)
        if not plan:
            raise ValueError("AutoFlow plan not found")

        status = str(plan.rights.get("status") or "")
        if status == "blocked":
            raise PermissionError("AutoFlow plan is blocked by rights policy")
        if plan.needs_review and not request.review_approved:
            raise PermissionError("AutoFlow plan requires review approval before execution")

        pipeline_id: str | None = None
        job_id: str | None = None
        run_status = "pending"
        error_message: str | None = None
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
                job = await create_job(db, pipeline.id)
                await start_or_defer_jobs(db, [job])
                job_id = str(job.id)
                run_status = str(job.status.value)
            except Exception as exc:
                run_status = "failed"
                error_message = str(exc)

        run = AutoFlowRun(
            run_id=str(uuid.uuid4()),
            plan_id=plan.plan_id,
            pipeline_id=pipeline_id,
            job_id=job_id,
            status=run_status,
            publish={"mode": plan.request.publish_mode},
            error_message=error_message,
        )
        self._runs[run.run_id] = run
        return run

    async def list_runs(self) -> list[AutoFlowRun]:
        return list(self._runs.values())

    async def get_run(self, run_id: str) -> AutoFlowRun | None:
        return self._runs.get(run_id)

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


autoflow_service = AutoFlowService()
