from __future__ import annotations

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from app.models.asset import Asset
from app.models.autoflow import AutoFlowPlan as PlanRow
from app.models.channel_agent import ProductionTask
from app.models.schedule import RuntimeSchedule
from app.models.owned_seed_inventory import OwnedSeedInventory

from app.autoflow.service import AutoFlowService, _candidates_from_storyboard_matches
from app.schemas.autoflow import AutoFlowRequest, ShotSpec, StoryboardPlan


@pytest.fixture
async def review_db():
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as conn:
        await conn.run_sync(Asset.__table__.create)
        for table in (ProductionTask.__table__, RuntimeSchedule.__table__, OwnedSeedInventory.__table__):
            await conn.run_sync(table.create)
        await conn.run_sync(PlanRow.__table__.create)
    async with async_sessionmaker(engine, expire_on_commit=False)() as db:
        yield db
    await engine.dispose()


@pytest.mark.asyncio
async def test_no_material_is_persisted_as_blocked(review_db):
    service = AutoFlowService()
    plan = await service.plan(AutoFlowRequest(prompt='cup'), review_db)
    loaded = await service.get_plan(plan.plan_id, review_db)
    assert loaded.status == 'blocked'
    assert loaded.candidates == []
    assert loaded.validation['material_status'] == 'no_material'


@pytest.mark.asyncio
async def test_generic_owned_original_footage_is_supported(review_db):
    import uuid
    asset = Asset(id=uuid.uuid4(), filename='cup.mp4', original_name='cup.mp4', mime_type='video/mp4',
                  storage_path='assets/cup.mp4', media_info={'license': 'owned', 'provenance': 'original', 'evidence_ref': 'capture'})
    review_db.add(asset)
    await review_db.commit()
    plan = await AutoFlowService().plan(AutoFlowRequest(prompt='cup', input_asset_id=str(asset.id)), review_db)
    assert plan.rights['status'] == 'allowed'
    assert plan.candidates[0].metadata['provenance'] == 'original'
    assert plan.candidates[0].metadata['evidence_ref'] == 'capture'


@pytest.mark.asyncio
async def test_graph_cannot_assert_blocked_asset_is_allowed(review_db):
    import uuid
    from app.schemas.autoflow import AutoFlowClipCandidate
    asset = Asset(id=uuid.uuid4(), filename='cup.mp4', original_name='cup.mp4', mime_type='video/mp4',
                  storage_path='assets/cup.mp4', media_info={'license': 'owned', 'rights_status': 'blocked'})
    review_db.add(asset)
    await review_db.commit()
    candidate = AutoFlowClipCandidate(id='graph-source', title='cup', asset_id=str(asset.id), source_type='asset', rights_status='allowed')
    resolved = await AutoFlowService()._asset_candidate_rights(candidate, review_db)
    assert resolved.rights_status == 'blocked'


@pytest.mark.asyncio
async def test_mixed_graph_external_source_cannot_receive_internal_approval(review_db):
    import uuid
    from app.schemas.autoflow import DraftNode
    from tests.autoflow.test_graph_planner import _dog_cat_draft

    asset = Asset(id=uuid.uuid4(), filename='cup.mp4', original_name='cup.mp4', mime_type='video/mp4',
                  storage_path='assets/cup.mp4', media_info={'license': 'owned'})
    review_db.add(asset)
    await review_db.commit()
    draft = _dog_cat_draft()
    draft.nodes[0].asset_id = str(asset.id)
    draft.nodes[0].config['asset_id'] = str(asset.id)
    url = 'https://example.com/external.mp4'
    draft.nodes[2] = DraftNode(id='source_cat', type='url_download', config={'url': url})
    service = AutoFlowService()
    plan = await service.plan(AutoFlowRequest(prompt='mixed material preview', planning_mode='ai_graph',
        source_policy='remix_with_review', constraints={'pipeline_draft': draft.model_dump()}), review_db)
    assert plan.validation['planning']['effective_mode'] == 'bounded_graph'
    with pytest.raises(PermissionError, match='human review'):
        await service.approve_internal(plan.plan_id, review_db, approved_by='agent', evidence={})
    assert any(candidate.url == url and candidate.rights_status == 'unknown' for candidate in plan.candidates)


@pytest.mark.asyncio
async def test_storyboard_search_enforces_hard_requirements_score_and_preserves_rights(monkeypatch):
    from app.services import material_service
    async def search(_db, _request):
        return None, [
            {'asset_id': 'wrong', 'confidence': .99, 'subtitle_text': 'plate', 'metadata': {'license': 'owned'}},
            {'asset_id': 'bad', 'confidence': .95, 'subtitle_text': 'cup with watermark', 'metadata': {'license': 'owned'}},
            {'asset_id': 'weak', 'confidence': .1, 'subtitle_text': 'cup', 'metadata': {'license': 'owned'}},
            {'asset_id': 'good', 'confidence': .8, 'subtitle_text': 'cup', 'metadata': {'license': 'owned', 'evidence_ref': 'capture'}},
        ]
    monkeypatch.setattr(material_service, 'materialize_material_search', search)
    storyboard = StoryboardPlan(subject='cup', source_strategy='material_library', shots=[
        ShotSpec(id='hook', search_query='cup', must_have=['cup'], must_not_have=['watermark'])])
    await AutoFlowService()._materialize_storyboard_shots(storyboard,
        AutoFlowRequest(prompt='cup', material_library_ids=['library']), object())
    assert storyboard.shots[0].matched_asset_id == 'good'
    assert storyboard.shots[0].extra['rights_metadata']['evidence_ref'] == 'capture'
    assert _candidates_from_storyboard_matches(storyboard)[0].rights_status == 'allowed'


@pytest.mark.asyncio
async def test_storyboard_search_keeps_missing_when_nothing_meets_minimum(monkeypatch):
    from app.services import material_service
    async def search(_db, _request):
        return None, [{'asset_id': 'weak', 'confidence': 0, 'lighthouse_score': .99, 'subtitle_text': 'cup'}]
    monkeypatch.setattr(material_service, 'materialize_material_search', search)
    storyboard = StoryboardPlan(subject='cup', source_strategy='material_library', shots=[ShotSpec(id='hook', search_query='cup')])
    await AutoFlowService()._materialize_storyboard_shots(storyboard,
        AutoFlowRequest(prompt='cup', material_library_ids=['library']), object())
    assert storyboard.shots[0].match_status == 'missing'


@pytest.mark.asyncio
@pytest.mark.parametrize('policy', ['owned_only', 'licensed_only', 'public_domain_or_cc'])
async def test_empty_material_search_is_blocked_and_cannot_be_approved(policy):
    service = AutoFlowService()
    plan = await service.plan(AutoFlowRequest(prompt='Ceramic cup', source_policy=policy, publish_mode='private_upload'))
    assert plan.status == 'blocked'
    assert plan.candidates == []
    assert plan.pipeline_definition.nodes == []
    assert plan.validation['material_status'] == 'no_material'
    with pytest.raises(PermissionError):
        await service.approve(plan.plan_id)


@pytest.mark.asyncio
async def test_explicit_storyboard_with_defaults_reports_actual_mode():
    plan = await AutoFlowService().plan(AutoFlowRequest(prompt='Ceramic cup', planning_mode='storyboard'))
    assert plan.storyboard is not None
    assert plan.validation['planning']['requested_mode'] == 'storyboard'
    assert plan.validation['planning']['effective_mode'] == 'storyboard'
    assert plan.status == 'blocked'


@pytest.mark.asyncio
async def test_explicit_template_does_not_switch_to_storyboard_for_library():
    plan = await AutoFlowService().plan(AutoFlowRequest(prompt='Ceramic cup', planning_mode='template', source_strategy='material_library'))
    assert plan.storyboard is None
    assert plan.validation['planning']['effective_mode'] == 'template'


def test_versioned_planning_options_are_resolved_without_enabling_experiments():
    request = AutoFlowRequest.model_validate({'prompt': 'cup', 'planning_options': {
        'version': 1, 'planning_mode': 'ai_graph', 'provider_config_id': 'provider',
        'model': 'model', 'allow_experimental_graph_planning': False, 'max_repair_attempts': 0,
    }})
    assert request.planning_mode == 'ai_graph'
    assert request.provider_config_id == 'provider'
    assert request.model == 'model'
    assert request.allow_experimental_graph_planning is False
    assert request.max_repair_attempts == 0


@pytest.mark.parametrize('extra', [
    {'planning_mode': 'template', 'planning_options': {'version': 1, 'planning_mode': 'storyboard'}},
    {'planning_options': {'version': 2}},
    {'min_shots': 8, 'max_shots': 3},
])
def test_conflicting_or_unsupported_planning_contract_is_rejected(extra):
    with pytest.raises(ValidationError):
        AutoFlowRequest.model_validate({'prompt': 'cup', **extra})


def test_shots_are_required_by_default():
    assert ShotSpec(id='hook', search_query='cup').required is True


@pytest.mark.parametrize('status', ['unknown', 'blocked'])
def test_storyboard_conversion_preserves_rights_instead_of_asserting_owned(status):
    shot = ShotSpec(id='hook', search_query='cup', match_status='matched', matched_asset_id='asset',
                    extra={'rights_status': status, 'rights_metadata': {'license': 'owned', 'evidence_ref': 'receipt'}})
    candidate = _candidates_from_storyboard_matches(StoryboardPlan(subject='cup', shots=[shot]))[0]
    assert candidate.rights_status == status
    assert candidate.metadata['evidence_ref'] == 'receipt'


@pytest.mark.asyncio
async def test_disabled_graph_provider_reports_fallback_without_model_call():
    class ForbiddenProvider:
        async def draft_for_request(self, *_args):
            raise AssertionError('disabled provider called')
    service = AutoFlowService()
    service.graph_planner.provider = ForbiddenProvider()
    plan = await service.plan(AutoFlowRequest(prompt='Ceramic cup', planning_mode='ai_graph',
                                             allow_experimental_graph_planning=False))
    planning = plan.validation['planning']
    assert planning['requested_mode'] == 'ai_graph'
    assert planning['effective_mode'] == 'template'
    assert planning['provider_used'] is None
    assert 'disabled' in planning['fallback_reason']


@pytest.mark.asyncio
async def test_internal_approval_cannot_substitute_for_unknown_rights_review():
    from tests.autoflow.factories import FixtureMaterialSelector
    service = AutoFlowService(material_selector=FixtureMaterialSelector())
    plan = await service.plan(AutoFlowRequest(prompt='cup', publish_mode='private_upload'))
    from app.schemas.autoflow import AutoFlowPlanPatch
    replacement = plan.candidates[0].model_copy(update={'rights_status': 'unknown', 'metadata': {}})
    plan = await service.patch_plan(plan.plan_id, AutoFlowPlanPatch(replacement_candidates=[replacement]))
    assert plan.rights['status'] == 'review_required'
    with pytest.raises(PermissionError, match='human'):
        await service.approve_internal(plan.plan_id, approved_by='channelops', evidence={'pds': 'allow'})


@pytest.mark.asyncio
async def test_patch_cannot_disable_rights_check_for_blocked_replacement():
    from tests.autoflow.factories import FixtureMaterialSelector
    from app.schemas.autoflow import AutoFlowPlanPatch
    service = AutoFlowService(material_selector=FixtureMaterialSelector())
    plan = await service.plan(AutoFlowRequest(prompt='cup', publish_mode='private_upload'))
    await service.approve(plan.plan_id)
    blocked = plan.candidates[0].model_copy(update={'rights_status': 'blocked'})
    updated = await service.patch_plan(plan.plan_id, AutoFlowPlanPatch(
        replacement_candidates=[blocked], evaluate_rights=False))
    assert updated.status == 'blocked'
    assert updated.review_approved_at is None


@pytest.mark.asyncio
async def test_global_ai_disable_prevents_graph_provider_call(monkeypatch):
    from app.autoflow import graph_planner
    monkeypatch.setattr(graph_planner.settings, 'autoflow_ai_enabled', False)
    class ForbiddenProvider:
        async def draft_for_request(self, *_args):
            raise AssertionError('globally disabled AI called')
    service = AutoFlowService()
    service.graph_planner.provider = ForbiddenProvider()
    plan = await service.plan(AutoFlowRequest(prompt='cup', planning_mode='ai_graph',
        allow_experimental_graph_planning=True, provider_config_id='provider', model='model'))
    assert 'disabled' in plan.validation['planning']['fallback_reason']


@pytest.mark.parametrize('options', [
    {'version': True}, {'version': 1, 'allow_experimental_graph_planning': 'true'},
    {'version': 1, 'max_repair_attempts': True},
])
def test_versioned_planning_options_require_exact_types(options):
    with pytest.raises(ValidationError):
        AutoFlowRequest.model_validate({'prompt': 'cup', 'planning_options': options})


@pytest.mark.asyncio
async def test_template_with_explicit_input_uses_that_asset_not_library_results():
    from tests.autoflow.factories import FixtureMaterialSelector
    plan = await AutoFlowService(material_selector=FixtureMaterialSelector()).plan(AutoFlowRequest(
        prompt='cup', input_asset_id='pinned-cup', planning_mode='template', source_strategy='input_video'))
    assert [candidate.asset_id for candidate in plan.candidates] == ['pinned-cup']
    assert plan.storyboard is None
    assert plan.validation['planning']['effective_mode'] == 'template'


@pytest.mark.asyncio
async def test_service_records_actual_graph_provider_and_preserves_requested_options(monkeypatch):
    from app.autoflow.graph_planner import settings
    from tests.autoflow.test_graph_planner import _dog_cat_draft
    monkeypatch.setattr(settings, 'autoflow_ai_enabled', True)
    received = []
    class Provider:
        async def draft_for_request(self, request, manifest):
            received.append((request.provider_config_id, request.model, request.allow_experimental_graph_planning))
            return _dog_cat_draft(), 'llm.test_provider'
    service = AutoFlowService()
    service.graph_planner.provider = Provider()
    plan = await service.plan(AutoFlowRequest.model_validate({'prompt': 'cup', 'planning_options': {
        'version': 1, 'planning_mode': 'ai_graph', 'provider_config_id': 'configured-provider',
        'model': 'configured-model', 'allow_experimental_graph_planning': True}}))
    assert received == [('configured-provider', 'configured-model', True)]
    assert plan.validation['planning']['effective_mode'] == 'ai_graph'
    assert plan.validation['planning']['provider_used'] == 'llm.test_provider'
    assert plan.validation['planning']['fallback_reason'] is None


def test_partial_planning_envelope_survives_persistence_roundtrip():
    request = AutoFlowRequest.model_validate({'prompt': 'cup', 'planning_mode': 'template',
                                             'planning_options': {'version': 1}})
    assert AutoFlowRequest.model_validate(request.model_dump(mode='json')) == request


@pytest.mark.asyncio
async def test_patch_preserves_planning_provenance():
    from tests.autoflow.factories import FixtureMaterialSelector
    from app.schemas.autoflow import AutoFlowPlanPatch
    service = AutoFlowService(material_selector=FixtureMaterialSelector())
    plan = await service.plan(AutoFlowRequest(prompt='cup'))
    updated = await service.patch_plan(plan.plan_id, AutoFlowPlanPatch(metadata={'selected_title': 'New title'}))
    assert updated.validation['planning'] == plan.validation['planning']
    assert updated.validation['material_status'] == 'complete'


@pytest.mark.asyncio
@pytest.mark.parametrize('rebuild', [True, False])
async def test_storyboard_candidate_replacement_cannot_change_rights_for_different_source(rebuild):
    from app.schemas.autoflow import AutoFlowPlanPatch
    service = AutoFlowService()
    plan = await service.plan(AutoFlowRequest(prompt='cup', input_asset_id='unknown-original',
        source_policy='remix_with_review', publish_mode='private_upload'))
    replacement = plan.candidates[0].model_copy(update={'asset_id': 'different-owned',
        'rights_status': 'allowed', 'metadata': {'license': 'owned'}})
    with pytest.raises(ValueError, match='storyboard'):
        await service.patch_plan(plan.plan_id, AutoFlowPlanPatch(replacement_candidates=[replacement], rebuild_definition=rebuild))


@pytest.mark.asyncio
async def test_storyboard_patch_preserves_explicit_blocked_status():
    from app.schemas.autoflow import AutoFlowPlanPatch
    service = AutoFlowService()
    plan = await service.plan(AutoFlowRequest(prompt='cup', input_asset_id='original', source_policy='remix_with_review'))
    blocked = plan.candidates[0].model_copy(update={'rights_status': 'blocked'})
    updated = await service.patch_plan(plan.plan_id, AutoFlowPlanPatch(replacement_candidates=[blocked]))
    assert updated.rights['status'] == 'blocked'


@pytest.mark.asyncio
async def test_storyboard_candidate_can_be_unlocked_after_rights_preservation():
    from app.schemas.autoflow import AutoFlowPlanPatch
    service = AutoFlowService()
    plan = await service.plan(AutoFlowRequest(prompt='cup', input_asset_id='original', source_policy='remix_with_review'))
    await service.patch_plan(plan.plan_id, AutoFlowPlanPatch(locked_candidate_ids=[plan.candidates[0].id]))
    unlocked = await service.patch_plan(plan.plan_id, AutoFlowPlanPatch(locked_candidate_ids=[]))
    assert unlocked.candidates[0].metadata['locked'] is False


@pytest.mark.asyncio
async def test_graph_endpoint_reports_disabled_provider_with_versioned_request(monkeypatch):
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient
    from app.api.autoflow import router
    from app.db import get_db
    from app.autoflow.graph_planner import settings
    monkeypatch.setattr(settings, 'autoflow_ai_enabled', False)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: None
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as client:
        response = await client.post('/api/v1/autoflow/plan/graph', json={
            'prompt': 'cup', 'planning_options': {'version': 1, 'planning_mode': 'storyboard'}})
    assert response.status_code == 400
    assert 'disabled' in response.json()['detail']


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['auto', 'template', 'storyboard', 'ai_graph'])
@pytest.mark.parametrize('strategy', ['auto', 'input_video', 'material_library', 'external_research', 'generate_missing', 'hybrid'])
async def test_requested_mode_and_source_strategy_matrix_has_explicit_effective_mode(mode, strategy):
    plan = await AutoFlowService().plan(AutoFlowRequest(prompt='cup', planning_mode=mode, source_strategy=strategy))
    expected = 'template' if mode == 'template' or (mode in {'auto', 'ai_graph'} and strategy == 'auto') else 'storyboard'
    assert plan.validation['planning']['requested_mode'] == mode
    assert plan.validation['planning']['effective_mode'] == expected
    assert plan.status == 'blocked'
    assert not plan.pipeline_definition.nodes
