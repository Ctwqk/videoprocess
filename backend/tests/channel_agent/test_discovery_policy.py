from __future__ import annotations

from dataclasses import asdict
from dataclasses import FrozenInstanceError
import json
from pathlib import Path

import pytest

from app.channel_agent.discovery_policy import DiscoveryPolicy, DiscoveryPolicyError


SERIALIZED_POLICY_CASES = json.loads(
    (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "discovery_policy_serialized.json"
    ).read_text()
)


@pytest.mark.parametrize(
    "case",
    SERIALIZED_POLICY_CASES,
    ids=[case["name"] for case in SERIALIZED_POLICY_CASES],
)
def test_discovery_policy_matches_shared_serialized_corpus(case: dict[str, object]) -> None:
    content_mix = json.loads(str(case["policy_json"]))

    if not case["valid"]:
        with pytest.raises(DiscoveryPolicyError):
            DiscoveryPolicy.from_content_mix(content_mix)
        return

    assert asdict(DiscoveryPolicy.from_content_mix(content_mix)) == case["expected"]


def test_discovery_policy_defaults_to_disabled() -> None:
    policy = DiscoveryPolicy.from_content_mix({})

    assert policy == DiscoveryPolicy(
        enabled=False,
        interval_minutes=360,
        max_queries_per_run=3,
        max_results_per_query=10,
        min_view_count=1000,
        region_code="US",
    )


def test_discovery_policy_parses_valid_nested_settings_exactly() -> None:
    policy = DiscoveryPolicy.from_content_mix(
        {
            "youtube_discovery": {
                "enabled": True,
                "interval_minutes": 720,
                "max_queries_per_run": 5,
                "max_results_per_query": 25,
                "min_view_count": 1_000_000_000,
                "region_code": "GB",
            }
        }
    )

    assert policy == DiscoveryPolicy(
        enabled=True,
        interval_minutes=720,
        max_queries_per_run=5,
        max_results_per_query=25,
        min_view_count=1_000_000_000,
        region_code="GB",
    )


def test_discovery_policy_uses_legacy_region_only_when_nested_region_is_absent() -> None:
    legacy = DiscoveryPolicy.from_content_mix(
        {"region_code": "CA", "youtube_discovery": {"enabled": True}}
    )
    nested = DiscoveryPolicy.from_content_mix(
        {
            "region_code": "CA",
            "youtube_discovery": {"enabled": True, "region_code": "JP"},
        }
    )

    assert legacy.region_code == "CA"
    assert nested.region_code == "JP"


@pytest.mark.parametrize(
    ("field", "lower", "upper"),
    [
        ("interval_minutes", 60, 1440),
        ("max_queries_per_run", 1, 5),
        ("max_results_per_query", 1, 25),
        ("min_view_count", 0, 1_000_000_000),
    ],
)
def test_discovery_policy_accepts_inclusive_integer_bounds(
    field: str,
    lower: int,
    upper: int,
) -> None:
    lower_policy = DiscoveryPolicy.from_content_mix(
        {"youtube_discovery": {field: lower}}
    )
    upper_policy = DiscoveryPolicy.from_content_mix(
        {"youtube_discovery": {field: upper}}
    )

    assert getattr(lower_policy, field) == lower
    assert getattr(upper_policy, field) == upper


@pytest.mark.parametrize("value", [None, [], "settings", 1, True])
def test_discovery_policy_rejects_non_mapping_content_mix(value: object) -> None:
    with pytest.raises(DiscoveryPolicyError):
        DiscoveryPolicy.from_content_mix(value)


@pytest.mark.parametrize("value", [None, [], "settings", 1, True])
def test_discovery_policy_rejects_non_mapping_nested_policy(value: object) -> None:
    with pytest.raises(DiscoveryPolicyError):
        DiscoveryPolicy.from_content_mix({"youtube_discovery": value})


@pytest.mark.parametrize("value", [0, 1, "true", None, 1.0])
def test_discovery_policy_requires_an_exact_boolean(value: object) -> None:
    with pytest.raises(DiscoveryPolicyError):
        DiscoveryPolicy.from_content_mix({"youtube_discovery": {"enabled": value}})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("interval_minutes", True),
        ("interval_minutes", 60.0),
        ("interval_minutes", "60"),
        ("max_queries_per_run", False),
        ("max_queries_per_run", 3.0),
        ("max_results_per_query", True),
        ("max_results_per_query", "10"),
        ("min_view_count", False),
        ("min_view_count", 1000.0),
    ],
)
def test_discovery_policy_requires_exact_integers(field: str, value: object) -> None:
    with pytest.raises(DiscoveryPolicyError):
        DiscoveryPolicy.from_content_mix({"youtube_discovery": {field: value}})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("interval_minutes", 59),
        ("interval_minutes", 1441),
        ("max_queries_per_run", 0),
        ("max_queries_per_run", 6),
        ("max_results_per_query", 0),
        ("max_results_per_query", 26),
        ("min_view_count", -1),
        ("min_view_count", 1_000_000_001),
    ],
)
def test_discovery_policy_rejects_out_of_range_integers(field: str, value: int) -> None:
    with pytest.raises(DiscoveryPolicyError):
        DiscoveryPolicy.from_content_mix({"youtube_discovery": {field: value}})


@pytest.mark.parametrize("value", [None, 1, "us", "USA", "U1", "\u00c9U"])
def test_discovery_policy_requires_two_uppercase_ascii_region_letters(value: object) -> None:
    with pytest.raises(DiscoveryPolicyError):
        DiscoveryPolicy.from_content_mix(
            {"youtube_discovery": {"region_code": value}}
        )


def test_discovery_policy_validates_legacy_region_fallback() -> None:
    with pytest.raises(DiscoveryPolicyError):
        DiscoveryPolicy.from_content_mix(
            {"region_code": "usa", "youtube_discovery": {"enabled": True}}
        )


def test_discovery_policy_is_frozen() -> None:
    policy = DiscoveryPolicy.from_content_mix({})

    with pytest.raises(FrozenInstanceError):
        policy.enabled = True  # type: ignore[misc]
