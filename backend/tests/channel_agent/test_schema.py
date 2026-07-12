import pytest
from pydantic import ValidationError

from app.schemas.channel_agent import ManualSeedCreate


def test_manual_seed_create_accepts_owned_input_canary():
    seed = ManualSeedCreate(
        prompt="Create a canary",
        source_policy="owned_only",
        constraints_json={
            "input_asset_id": "00000000-0000-0000-0000-000000000123",
            "source_strategy": "input_video",
            "planning_mode": "template",
        },
    )

    assert seed.constraints_json["input_asset_id"] == "00000000-0000-0000-0000-000000000123"


@pytest.mark.parametrize(
    "payload",
    [
        {
            "source_platforms_json": ["youtube"],
        },
        {
            "source_policy": "remix_with_review",
        },
        {
            "constraints_json": {"source_strategy": "external_research"},
        },
        {
            "constraints_json": {"planning_mode": "storyboard"},
        },
    ],
)
def test_manual_seed_create_rejects_owned_input_canary_conflicts(payload):
    constraints = {
        "input_asset_id": "00000000-0000-0000-0000-000000000123",
        "source_strategy": "input_video",
        "planning_mode": "template",
    }
    constraints.update(payload.get("constraints_json", {}))
    values = {key: value for key, value in payload.items() if key != "constraints_json"}

    with pytest.raises(ValidationError):
        ManualSeedCreate(prompt="Create a canary", constraints_json=constraints, **values)


def test_manual_seed_create_accepts_external_sources_without_owned_input_asset():
    seed = ManualSeedCreate(
        prompt="Create a remix",
        source_policy="remix_with_review",
        source_platforms_json=["youtube"],
        constraints_json={"source_strategy": "external_research"},
    )

    assert seed.source_platforms_json == ["youtube"]


@pytest.mark.parametrize(
    "input_asset_id",
    [
        "00000000-0000-0000-0000-00000000012",
        "00000000-0000-0000-0000-000000000123 ",
        "00000000-0000-0000-0000-00000000012G",
        "abcdefab-1234-4abc-8def-abcdefabcdef".upper(),
    ],
)
def test_manual_seed_create_rejects_noncanonical_owned_input_asset_id(input_asset_id):
    with pytest.raises(ValidationError):
        ManualSeedCreate(
            prompt="Create a canary",
            source_policy="owned_only",
            constraints_json={
                "input_asset_id": input_asset_id,
                "source_strategy": "input_video",
                "planning_mode": "template",
            },
        )
