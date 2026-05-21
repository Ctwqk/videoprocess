from __future__ import annotations

from app.autoflow.service import _candidates_from_storyboard_matches
from app.autoflow.search_service import _candidate_from_material_result
from app.schemas.autoflow import AutoFlowClipCandidate, ShotSpec, StoryboardPlan


def test_autoflow_clip_candidate_accepts_material_id():
    candidate = AutoFlowClipCandidate(
        id="clip-1",
        title="Clip",
        source_type="material",
        material_id="mat-1",
    )

    assert candidate.material_id == "mat-1"


def test_material_search_candidate_sets_material_id_and_metadata():
    candidate = _candidate_from_material_result(
        {
            "id": "clip-1",
            "material_id": "mat-1",
            "asset_id": "asset-materialized",
            "source_asset_id": "asset-source",
            "title": "Clip",
        },
        1,
    )

    assert candidate.material_id == "mat-1"
    assert candidate.metadata["material_id"] == "mat-1"
    assert candidate.metadata["asset_id"] == "asset-materialized"


def test_storyboard_material_candidates_fall_back_to_matched_asset_id():
    storyboard = StoryboardPlan(
        subject="clips",
        source_strategy="material_library",
        shots=[
            ShotSpec(
                id="shot-1",
                search_query="Clip",
                matched_asset_id="asset-materialized",
                matched_source_asset_id="asset-source",
                matched_start_sec=1,
                matched_end_sec=3,
                match_status="matched",
            )
        ],
    )

    [candidate] = _candidates_from_storyboard_matches(storyboard)

    assert candidate.material_id == "asset-materialized"
