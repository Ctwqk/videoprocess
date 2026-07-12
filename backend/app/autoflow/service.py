from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.autoflow.clip_ranker import ClipRanker
from app.autoflow.embedding_relevance import EmbeddingRelevanceService
from app.autoflow.graph_planner import AutoFlowGraphPlanner, GraphPlanningFailed, GraphPlanningUnavailable
from app.autoflow.intent_parser import RuleBasedIntentParser
from app.autoflow.material_selector import MaterialSelector
from app.autoflow.metadata_generator import LLMGatewayMetadataClient, MetadataGenerator
from app.autoflow.pipeline_builder import PipelineBuilder
from app.autoflow.platform_profiles import PlatformProfileService
from app.autoflow.recent_usage import RecentClipUsageStore
from app.autoflow.rights_policy import RightsPolicy
from app.autoflow.storyboard_generator import StoryboardGenerator
from app.autoflow.template_library import TemplateLibrary
from app.autoflow.validation_repair import AutoFlowRepairService, AutoFlowUnrepairableError
from app.models.autoflow import AutoFlowPlan as AutoFlowPlanModel
from app.models.autoflow import AutoFlowRun as AutoFlowRunModel
from app.models.asset import Asset
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
    AutoFlowStoryboardRequest,
    StoryboardPlan,
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


class OwnedInputAssetError(ValueError):
    pass


class AutoFlowService:
    def __init__(
        self,
        material_selector: CandidateSelector | None = None,
        clip_ranker: ClipRanker | None = None,
    ) -> None:
        self.intent_parser = RuleBasedIntentParser()
        self.template_library = TemplateLibrary()
        self.platform_profiles = PlatformProfileService()
        self.metadata_generator = MetadataGenerator(
            llm_client=_llm_metadata_client() if settings.autoflow_ai_enabled else None,
            platform_profiles=self.platform_profiles,
        )
        self.storyboard_generator = StoryboardGenerator()
        self.graph_planner = AutoFlowGraphPlanner()
        self.pipeline_builder = PipelineBuilder()
        self.validation_repair = AutoFlowRepairService()
        self.rights_policy = RightsPolicy()
        self.material_selector = material_selector or MaterialSelector()
        self.clip_ranker = clip_ranker or ClipRanker()
        self.embedding_relevance = EmbeddingRelevanceService(
            embedding_url=settings.autoflow_embedding_url if settings.autoflow_ai_enabled else "",
            timeout_seconds=settings.autoflow_ai_timeout_seconds,
        )
        self.recent_usage_store = RecentClipUsageStore()
        self._plans: dict[str, AutoFlowPlan] = {}
        self._runs: dict[str, AutoFlowRun] = {}

    async def plan(self, request: AutoFlowRequest, db: AsyncSession | None = None) -> AutoFlowPlan:
        await self._validate_owned_input_asset(request, db)
        fallback_warnings: list[str] = []
        if request.planning_mode == "ai_graph":
            try:
                return await self._plan_graph(request, db)
            except GraphPlanningUnavailable as exc:
                fallback_warnings.append(f"AI graph planner unavailable: {exc}")
            except GraphPlanningFailed as exc:
                fallback_warnings.append(f"AI graph planner failed validation: {exc}")

        if _uses_storyboard_planner(request):
            return await self._plan_storyboard(request, db)

        intent = self.intent_parser.parse(request)
        template = self.template_library.select_template(intent)
        warnings: list[str] = list(fallback_warnings)
        select_with_warnings = getattr(self.material_selector, "find_candidates_with_warnings", None)
        if callable(select_with_warnings):
            selection = await select_with_warnings(intent, request, db=db)
            candidates = selection.candidates
            warnings.extend(selection.warnings)
        else:
            candidates = await self.material_selector.find_candidates(intent, request, db=db)
            warnings.extend(getattr(self.material_selector, "last_warnings", []))
        if not candidates:
            candidates = self._fixture_candidates(intent, request)
            warnings.append("Material selector returned no candidates; using AutoFlow fixture candidates.")
        recent_used_asset_ids: set[str] = set()
        if db is not None:
            try:
                recent_used_asset_ids = await self.recent_usage_store.load_recent_asset_ids(db)
            except Exception:
                warnings.append("recent_clip_usage_unavailable")
        relevance = await self.embedding_relevance.score(intent, candidates)
        warnings.extend(relevance.warnings)
        platform_profile = self.platform_profiles.for_platforms(intent.target_platforms)
        ranked_candidates = self.clip_ranker.rank(
            intent,
            candidates,
            semantic_relevance_scores=relevance.scores,
            recent_used_asset_ids=recent_used_asset_ids,
            platform_profile=platform_profile,
        )
        if len(ranked_candidates) < 5:
            warnings.append("AutoFlow found fewer than 5 candidate clips; review material coverage before publishing.")
        metadata = self.metadata_generator.generate(intent, ranked_candidates)
        definition = self.pipeline_builder.build(template, intent, ranked_candidates, metadata)
        validation = validate_pipeline(definition)
        repair_result = None
        if not validation.valid:
            try:
                repair_result = self.validation_repair.repair(definition, validation.errors, ranked_candidates)
            except AutoFlowUnrepairableError as exc:
                warnings.append(
                    "Generated workflow was unrepairable; rebuilt with material_library_remix fallback."
                )
                warnings.extend(f"Unrepairable workflow error: {error}" for error in exc.unrepairable_errors)
                template = self.template_library.get_template("material_library_remix")
                definition = self.pipeline_builder.build(template, intent, ranked_candidates, metadata)
                validation = validate_pipeline(definition)
                if not validation.valid:
                    try:
                        repair_result = self.validation_repair.repair(definition, validation.errors, ranked_candidates)
                    except AutoFlowUnrepairableError as fallback_exc:
                        warnings.extend(
                            f"Fallback workflow unrepairable error: {error}"
                            for error in fallback_exc.unrepairable_errors
                        )
                        repair_result = None
                    else:
                        definition = repair_result.definition
                        validation = validate_pipeline(definition)
            else:
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

    async def plan_graph(self, request: AutoFlowRequest, db: AsyncSession | None = None) -> AutoFlowPlan:
        graph_request = request.model_copy(update={"planning_mode": "ai_graph"})
        await self._validate_owned_input_asset(graph_request, db)
        return await self._plan_graph(graph_request, db)

    async def storyboard(
        self,
        request: AutoFlowStoryboardRequest,
    ):
        return self.storyboard_generator.generate(request)

    async def _plan_graph(self, request: AutoFlowRequest, db: AsyncSession | None = None) -> AutoFlowPlan:
        outcome = await self.graph_planner.plan(request)
        intent = self.intent_parser.parse(request)
        rights_payload = self.rights_policy.evaluate(request, outcome.candidates).model_dump(mode="json")
        if outcome.policy.requires_review and rights_payload.get("status") == "allowed":
            rights_payload = {
                **rights_payload,
                "status": "review_required",
                "reasons": [
                    *list(rights_payload.get("reasons") or []),
                    "AI graph policy requires human review before upload or public publishing",
                ],
                "publish_allowed": request.publish_mode in {"preview_only", "private_upload", "unlisted_upload"},
            }

        validation_payload = {
            "valid": outcome.validation.valid and outcome.policy.valid,
            "errors": [error.model_dump(mode="json") for error in outcome.validation.errors],
            "warnings": [warning.model_dump(mode="json") for warning in outcome.validation.warnings],
            "repairs": outcome.graph_result.attempts[-1].repairs if outcome.graph_result.attempts else [],
            "graph_planning": outcome.graph_result.model_dump(mode="json"),
            "policy": outcome.policy.model_dump(mode="json", exclude={"definition"}),
            "plan_warnings": outcome.warnings,
        }
        plan = AutoFlowPlan(
            plan_id=str(uuid.uuid4()),
            request=request,
            intent=intent,
            template_id="ai_graph",
            pipeline_definition=outcome.definition,
            candidates=outcome.candidates,
            metadata=outcome.metadata,
            validation=validation_payload,
            rights=rights_payload,
            warnings=outcome.warnings,
            needs_review=_needs_review(rights_payload, review_approved_at=None),
            status=_status_for(rights_payload, request.publish_mode),
        )
        if db is not None:
            return await self._save_plan(db, plan)

        self._plans[plan.plan_id] = plan
        return plan


    async def _plan_storyboard(self, request: AutoFlowRequest, db: AsyncSession | None = None) -> AutoFlowPlan:
        storyboard_request = _storyboard_request_from_autoflow(request)
        storyboard_response = self.storyboard_generator.generate(storyboard_request)
        storyboard = storyboard_response.storyboard
        warnings = list(storyboard_response.warnings)
        warnings.extend(storyboard.warnings)

        intent = self.intent_parser.parse(request)
        metadata = _metadata_from_storyboard(storyboard)
        candidates: list[AutoFlowClipCandidate] = []

        if storyboard.source_strategy in {"material_library", "hybrid"} and db is not None:
            material_warnings = await self._materialize_storyboard_shots(storyboard, request, db)
            warnings.extend(material_warnings)
            candidates = _candidates_from_storyboard_matches(storyboard)

        if storyboard.source_strategy == "input_video" or (
            storyboard.source_strategy == "hybrid" and request.input_asset_id
        ):
            if not request.input_asset_id:
                definition = PipelineDefinition(nodes=[], edges=[])
                warnings.append("Storyboard input_video strategy requires input_asset_id.")
            else:
                candidates = [
                    AutoFlowClipCandidate(
                        id=f"storyboard-input-{request.input_asset_id}",
                        title=storyboard.title or "Storyboard input video",
                        source_type="asset",
                        asset_id=request.input_asset_id,
                        start_sec=0,
                        end_sec=storyboard.total_duration,
                        rights_status="allowed",
                        metadata={"storyboard_source": "input_video"},
                    )
                ]
                definition = self.pipeline_builder.build_storyboard_input_video(
                    storyboard,
                    input_asset_id=request.input_asset_id,
                    metadata=metadata,
                    publish_mode=request.publish_mode,
                )
        else:
            definition = self.pipeline_builder.build_storyboard_material_library(
                storyboard,
                metadata=metadata,
                publish_mode=request.publish_mode,
            )
            if not definition.nodes:
                warnings.append("Storyboard found no matched material clips; no executable media pipeline was generated.")

        if storyboard.allow_video_generation:
            for shot in storyboard.shots:
                if shot.match_status in {"pending", "missing"}:
                    shot.generation.enabled = True
            warnings.append("Video generation is represented as storyboard metadata only; no video_generate node is available yet.")
        elif storyboard.source_strategy in {"material_library", "hybrid"}:
            for shot in storyboard.shots:
                if shot.match_status == "pending":
                    shot.match_status = "missing"

        validation = validate_pipeline(definition)
        rights_payload = self.rights_policy.evaluate(request, candidates).model_dump()
        plan = AutoFlowPlan(
            plan_id=str(uuid.uuid4()),
            request=request,
            intent=intent,
            template_id=f"storyboard_{storyboard.source_strategy}",
            pipeline_definition=definition,
            storyboard=storyboard,
            candidates=candidates,
            metadata=metadata,
            validation={
                "valid": validation.valid,
                "errors": [error.model_dump(mode="json") for error in validation.errors],
                "warnings": [warning.model_dump(mode="json") for warning in validation.warnings],
                "repairs": [],
                "plan_warnings": warnings,
            },
            rights=rights_payload,
            warnings=warnings,
            needs_review=_needs_review(rights_payload, review_approved_at=None),
            status=_status_for(rights_payload, request.publish_mode),
        )
        if db is not None:
            return await self._save_plan(db, plan)

        self._plans[plan.plan_id] = plan
        return plan

    async def _materialize_storyboard_shots(
        self,
        storyboard: StoryboardPlan,
        request: AutoFlowRequest,
        db: AsyncSession,
    ) -> list[str]:
        warnings: list[str] = []
        if not request.material_library_ids:
            for shot in storyboard.shots:
                shot.match_status = "missing"
            return ["Storyboard material_library strategy requires material_library_ids."]

        from app.schemas.material import MaterialSearchRequest
        from app.services.material_service import materialize_material_search

        for shot in storyboard.shots:
            payload = MaterialSearchRequest(
                query=shot.search_query,
                source_library_ids=request.material_library_ids,
                result_library_ids=list(request.constraints.get("result_material_library_ids") or request.material_library_ids),
                top_k=int(request.constraints.get("top_k") or 8),
                rerank_top_m=int(request.constraints.get("rerank_top_m") or 4),
                min_duration=shot.min_duration,
                max_duration=shot.max_duration,
            )
            try:
                _query, results = await materialize_material_search(db, payload)
            except Exception as exc:
                shot.match_status = "missing"
                warnings.append(f"{shot.id} material search failed: {exc}")
                continue

            if not results:
                shot.match_status = "missing"
                continue

            best = results[0]
            shot.match_status = "matched"
            shot.matched_asset_id = _string_or_none(best.get("asset_id"))
            shot.matched_source_asset_id = _string_or_none(best.get("source_asset_id"))
            shot.matched_start_sec = _float_or_none(best.get("start_sec"))
            shot.matched_end_sec = _float_or_none(best.get("end_sec"))
            shot.match_score = _float_or_none(best.get("confidence") or best.get("lighthouse_score") or best.get("coarse_score"))
            if not shot.matched_asset_id:
                shot.match_status = "missing"
                warnings.append(f"{shot.id} material search returned no refined asset_id.")
        return warnings

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
            if plan.storyboard:
                if plan.storyboard.source_strategy == "input_video" or (
                    plan.storyboard.source_strategy == "hybrid" and request.input_asset_id
                ):
                    if request.input_asset_id:
                        definition = self.pipeline_builder.build_storyboard_input_video(
                            plan.storyboard,
                            input_asset_id=request.input_asset_id,
                            metadata=metadata,
                            publish_mode=request.publish_mode,
                        )
                else:
                    definition = self.pipeline_builder.build_storyboard_material_library(
                        plan.storyboard,
                        metadata=metadata,
                        publish_mode=request.publish_mode,
                    )
            else:
                template = self.template_library.get_template(plan.template_id)
                definition = self.pipeline_builder.build(template, intent, candidates, metadata)

        if patch.run_validation:
            validation = validate_pipeline(definition)
            repair_result = None
            if not validation.valid:
                try:
                    repair_result = self.validation_repair.repair(definition, validation.errors, candidates)
                except AutoFlowUnrepairableError as exc:
                    warnings.append(
                        "Generated workflow was unrepairable; rebuilt with material_library_remix fallback."
                    )
                    warnings.extend(f"Unrepairable workflow error: {error}" for error in exc.unrepairable_errors)
                    fallback_template = self.template_library.get_template("material_library_remix")
                    definition = self.pipeline_builder.build(fallback_template, intent, candidates, metadata)
                    validation = validate_pipeline(definition)
                    if not validation.valid:
                        try:
                            repair_result = self.validation_repair.repair(definition, validation.errors, candidates)
                        except AutoFlowUnrepairableError as fallback_exc:
                            warnings.extend(
                                f"Fallback workflow unrepairable error: {error}"
                                for error in fallback_exc.unrepairable_errors
                            )
                            repair_result = None
                        else:
                            definition = repair_result.definition
                            validation = validate_pipeline(definition)
                else:
                    definition = repair_result.definition
                    validation = validate_pipeline(definition)
            validation_payload = {
                "valid": validation.valid,
                "errors": [error.model_dump(mode="json") for error in validation.errors],
                "warnings": [warning.model_dump(mode="json") for warning in validation.warnings],
                "repairs": repair_result.applied_repairs if repair_result else [],
                "plan_warnings": warnings if validation.valid else [*warnings, "Generated workflow still needs manual repair."],
            }

        rights_payload = dict(plan.rights)
        if patch.evaluate_rights:
            rights_payload = self.rights_policy.evaluate(request, candidates).model_dump(mode="json")

        approvals_reset = _patch_resets_approval(patch)
        review_approved_at = None if approvals_reset else plan.review_approved_at
        public_approved_at = None if approvals_reset else plan.public_approved_at
        agent_approved_by = None if approvals_reset else plan.agent_approved_by
        if approvals_reset:
            rights_payload = _rights_without_approval_state(rights_payload, request.publish_mode)
        updated = plan.model_copy(
            update={
                "request": request,
                "intent": intent,
                "pipeline_definition": definition,
                "storyboard": plan.storyboard,
                "candidates": candidates,
                "metadata": metadata,
                "validation": validation_payload,
                "rights": rights_payload,
                "warnings": warnings,
                "needs_review": _needs_review(
                    rights_payload,
                    review_approved_at=review_approved_at,
                    agent_approved_by=agent_approved_by,
                ),
                "status": _status_for(
                    rights_payload,
                    request.publish_mode,
                    review_approved_at=review_approved_at,
                    public_approved_at=public_approved_at,
                    agent_approved_by=agent_approved_by,
                ),
                "review_approved_at": review_approved_at,
                "public_approved_at": public_approved_at,
                "agent_approved_by": agent_approved_by,
                "rejected_reason": None,
            }
        )

        if db is not None:
            return await self._save_plan(db, updated)

        self._plans[plan_id] = updated
        return updated

    async def approve_internal(
        self,
        plan_id: str,
        db: AsyncSession | None = None,
        *,
        approved_by: str,
        evidence: dict[str, Any],
    ) -> AutoFlowPlan | None:
        plan = await self.get_plan(plan_id, db)
        if not plan:
            return None
        _assert_not_blocked_or_rejected(plan, action="approve internally")

        rights = {
            **plan.rights,
            "review_approved": True,
            "agent_approval": {
                "approved_by": approved_by,
                "evidence": dict(evidence),
            },
        }
        updated = plan.model_copy(
            update={
                "needs_review": False,
                "rights": rights,
                "status": _status_for(
                    rights,
                    plan.request.publish_mode,
                    review_approved_at=plan.review_approved_at,
                    public_approved_at=plan.public_approved_at,
                    agent_approved_by=approved_by,
                ),
                "agent_approved_by": approved_by,
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
                "agent_approved_by": None,
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
        if _requires_review_approval(plan) and not (plan.review_approved_at or plan.agent_approved_by):
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
                "agent_approved_by": plan.agent_approved_by,
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
                "agent_approved_by": None,
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
            await self._validate_owned_input_asset(plan.request, db)
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
                "review_approved": bool(plan.review_approved_at or plan.agent_approved_by),
                "agent_approved_by": plan.agent_approved_by,
                "public_approved": bool(plan.public_approved_at),
            },
            error_message=error_message,
        )
        if db is not None:
            return await self._save_run(
                db,
                run,
                mark_plan_executed=request.execute and error_message is None,
                selected_candidates=plan.candidates,
            )

        self._runs[run.run_id] = run
        return run

    async def _validate_owned_input_asset(
        self,
        request: AutoFlowRequest,
        db: AsyncSession | None,
    ) -> None:
        if db is None or not request.input_asset_id:
            return
        asset_id = _uuid_or_none(request.input_asset_id)
        if asset_id is None or str(asset_id) != request.input_asset_id:
            raise OwnedInputAssetError("input_asset_id must be a canonical owned generated video asset UUID")
        asset = await db.get(Asset, asset_id)
        if asset is None:
            raise OwnedInputAssetError("Owned input asset was not found")
        media_info = asset.media_info if isinstance(asset.media_info, dict) else {}
        if (
            not isinstance(asset.mime_type, str)
            or not asset.mime_type.startswith("video/")
            or media_info.get("license") != "owned"
            or media_info.get("provenance") != "generated"
        ):
            raise OwnedInputAssetError("Input asset must be an owned generated video")

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
            raise PermissionError("AutoFlow execute requires plan_id for persisted execution")
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
        if hasattr(row, "storyboard_json"):
            row.storyboard_json = plan.storyboard.model_dump(mode="json") if plan.storyboard else None
        row.candidates_json = [candidate.model_dump(mode="json") for candidate in plan.candidates]
        row.metadata_json = plan.metadata.model_dump(mode="json")
        row.rights_json = dict(plan.rights)
        row.validation_json = dict(plan.validation)
        row.status = plan.status
        row.review_approved_at = plan.review_approved_at
        row.public_approved_at = plan.public_approved_at
        row.agent_approved_by = plan.agent_approved_by
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
        selected_candidates: list[AutoFlowClipCandidate] | None = None,
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
        if selected_candidates:
            try:
                await self.recent_usage_store.record_selected_clips(
                    db,
                    run_id=run.run_id,
                    candidates=selected_candidates,
                )
                await db.commit()
            except Exception:
                await db.rollback()
                artifacts = dict(row.artifacts_json or {})
                warnings = list(artifacts.get("warnings") or [])
                warnings.append("recent_clip_usage_write_failed")
                artifacts["warnings"] = warnings
                row.artifacts_json = artifacts
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
                    title=f"{subject} review-required placeholder 1",
                    source_type="external_url",
                    asset_id="autoflow-review-placeholder-1",
                    start_sec=0,
                    end_sec=5,
                    rights_status="review_required",
                    metadata={"placeholder": True, "source_policy": request.source_policy},
                ),
                AutoFlowClipCandidate(
                    id="external-2",
                    title=f"{subject} review-required placeholder 2",
                    source_type="external_url",
                    asset_id="autoflow-review-placeholder-2",
                    start_sec=0,
                    end_sec=5,
                    rights_status="review_required",
                    metadata={"placeholder": True, "source_policy": request.source_policy},
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


def _llm_metadata_client() -> LLMGatewayMetadataClient:
    return LLMGatewayMetadataClient(
        base_url=settings.autoflow_llm_gateway_url,
        timeout_seconds=settings.autoflow_ai_timeout_seconds,
        source=settings.autoflow_llm_source,
        profile=settings.autoflow_llm_profile,
    )


def _plan_from_model(row: AutoFlowPlanModel) -> AutoFlowPlan:
    rights = dict(row.rights_json or {})
    validation = dict(row.validation_json or {})
    return AutoFlowPlan(
        plan_id=str(row.id),
        request=AutoFlowRequest.model_validate(_request_json(row)),
        intent=AutoFlowIntent.model_validate(row.intent_json),
        template_id=row.template_id,
        pipeline_definition=PipelineDefinition.model_validate(row.pipeline_definition),
        storyboard=StoryboardPlan.model_validate(row.storyboard_json) if getattr(row, "storyboard_json", None) else None,
        candidates=[AutoFlowClipCandidate.model_validate(candidate) for candidate in (row.candidates_json or [])],
        metadata=AutoFlowMetadata.model_validate(row.metadata_json or {}),
        validation=validation,
        rights=rights,
        warnings=list(validation.get("plan_warnings") or []),
        needs_review=_needs_review(rights, row.review_approved_at, agent_approved_by=row.agent_approved_by),
        status=row.status,
        review_approved_at=row.review_approved_at,
        public_approved_at=row.public_approved_at,
        agent_approved_by=row.agent_approved_by,
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
        "source_strategy": "auto",
        "input_asset_id": None,
        "allow_video_generation": False,
        "min_shots": 3,
        "max_shots": 8,
        "provider_config_id": None,
        "model": None,
        "constraints": {},
        "user_constraints": {},
        "planning_mode": "auto",
        "max_repair_attempts": 3,
        "allow_experimental_graph_planning": False,
    }


def _uses_storyboard_planner(request: AutoFlowRequest) -> bool:
    return bool(
        request.input_asset_id
        or request.source_strategy != "auto"
        or request.allow_video_generation
        or request.constraints.get("use_storyboard")
    )


def _storyboard_request_from_autoflow(request: AutoFlowRequest) -> AutoFlowStoryboardRequest:
    target_duration = float(request.duration_sec or request.constraints.get("target_duration") or 30)
    strategy = request.source_strategy
    if strategy == "auto" and request.input_asset_id:
        strategy = "input_video"
    elif strategy == "auto" and request.material_library_ids:
        strategy = "material_library"
    return AutoFlowStoryboardRequest(
        prompt=request.prompt,
        input_asset_id=request.input_asset_id,
        material_library_ids=request.material_library_ids,
        target_duration=target_duration,
        aspect_ratio=request.aspect_ratio,
        target_platforms=request.target_platforms,
        source_strategy=strategy,
        allow_video_generation=request.allow_video_generation,
        max_shots=request.max_shots,
        min_shots=request.min_shots,
        style=str(request.constraints.get("style") or "auto"),
        provider_config_id=request.provider_config_id,
        model=request.model,
        constraints={**request.user_constraints, **request.constraints},
    )


def _metadata_from_storyboard(storyboard: StoryboardPlan) -> AutoFlowMetadata:
    return AutoFlowMetadata(
        title_candidates=storyboard.title_candidates or ([storyboard.title] if storyboard.title else []),
        selected_title=storyboard.title or (storyboard.title_candidates[0] if storyboard.title_candidates else None),
        description=storyboard.description or storyboard.logline,
        tags=storyboard.tags,
        hashtags=storyboard.hashtags,
        thumbnail_text_candidates=storyboard.title_candidates[:3],
        platform_payloads={},
    )


def _candidates_from_storyboard_matches(storyboard: StoryboardPlan) -> list[AutoFlowClipCandidate]:
    candidates: list[AutoFlowClipCandidate] = []
    for shot in storyboard.shots:
        if shot.match_status != "matched" or not shot.matched_asset_id:
            continue
        candidates.append(
            AutoFlowClipCandidate(
                id=f"storyboard-{shot.id}",
                title=shot.search_query,
                source_type="material",
                material_id=str(shot.extra.get("material_id") or shot.matched_asset_id or ""),
                asset_id=shot.matched_asset_id,
                start_sec=shot.matched_start_sec,
                end_sec=shot.matched_end_sec,
                score=shot.match_score or 0,
                rights_status="allowed",
                metadata={
                    "storyboard_shot_id": shot.id,
                    "source_asset_id": shot.matched_source_asset_id,
                },
            )
        )
    return candidates


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
    if patch.locked_candidate_ids is not None:
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
            patch.metadata is not None,
            patch.publish_mode is not None,
            bool(patch.publish_settings),
        )
    )


def _rights_without_approval_state(rights: dict[str, Any], publish_mode: str) -> dict[str, Any]:
    cleaned = dict(rights)
    cleaned.pop("review_approved", None)
    cleaned.pop("public_approved", None)
    cleaned.pop("agent_approval", None)
    if publish_mode == "public_after_review":
        cleaned["publish_allowed"] = False
    return cleaned


def _status_for(
    rights: dict[str, Any],
    publish_mode: str,
    *,
    review_approved_at: datetime | None = None,
    public_approved_at: datetime | None = None,
    agent_approved_by: str | None = None,
) -> str:
    if rights.get("status") == "blocked":
        return "blocked"
    if public_approved_at is not None:
        return "public_approved"
    if review_approved_at is not None or agent_approved_by:
        return "review_approved"
    if rights.get("status") == "review_required" or publish_mode == "public_after_review":
        return "review_required"
    return "drafted"


def _needs_review(
    rights: dict[str, Any],
    review_approved_at: datetime | None,
    *,
    agent_approved_by: str | None = None,
) -> bool:
    return rights.get("status") != "allowed" and review_approved_at is None and not agent_approved_by


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
    if plan.validation and plan.validation.get("valid") is not True:
        raise PermissionError("AutoFlow plan must have a valid workflow before execution")
    if plan.status == "blocked" or plan.rights.get("status") == "blocked":
        raise PermissionError("AutoFlow plan is blocked by rights policy")
    if plan.status == "rejected":
        raise PermissionError("AutoFlow plan was rejected and cannot be executed")

    publish_mode = plan.request.publish_mode
    upload_requested = publish_mode in {"private_upload", "unlisted_upload", "public_after_review"}
    review_approved = bool(plan.review_approved_at) or bool(plan.agent_approved_by)
    if plan.rights.get("status") == "review_required" and upload_requested and not review_approved:
        raise PermissionError("AutoFlow plan requires review approval before upload execution")

    public_approved = bool(plan.public_approved_at)
    if publish_mode == "public_after_review" and not public_approved:
        raise PermissionError("AutoFlow plan requires public approval before public execution")


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _uuid_or_none(value: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


autoflow_service = AutoFlowService()
