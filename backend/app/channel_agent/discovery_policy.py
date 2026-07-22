from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


class DiscoveryPolicyError(ValueError):
    pass


@dataclass(frozen=True)
class DiscoveryPolicy:
    enabled: bool = False
    interval_minutes: int = 360
    max_queries_per_run: int = 3
    max_results_per_query: int = 10
    min_view_count: int = 1000
    region_code: str = "US"

    @classmethod
    def from_content_mix(cls, value: object) -> DiscoveryPolicy:
        if not isinstance(value, dict):
            raise DiscoveryPolicyError("content mix policy must be an object")

        nested_value = value.get("youtube_discovery", {})
        if not isinstance(nested_value, dict):
            raise DiscoveryPolicyError("youtube discovery policy must be an object")

        enabled = _exact_bool(nested_value, "enabled", cls.enabled)
        interval_minutes = _bounded_int(
            nested_value,
            "interval_minutes",
            cls.interval_minutes,
            minimum=60,
            maximum=1440,
        )
        max_queries_per_run = _bounded_int(
            nested_value,
            "max_queries_per_run",
            cls.max_queries_per_run,
            minimum=1,
            maximum=5,
        )
        max_results_per_query = _bounded_int(
            nested_value,
            "max_results_per_query",
            cls.max_results_per_query,
            minimum=1,
            maximum=25,
        )
        min_view_count = _bounded_int(
            nested_value,
            "min_view_count",
            cls.min_view_count,
            minimum=0,
            maximum=1_000_000_000,
        )
        region_value = (
            nested_value["region_code"]
            if "region_code" in nested_value
            else value.get("region_code", cls.region_code)
        )
        region_code = _region_code(region_value)

        return cls(
            enabled=enabled,
            interval_minutes=interval_minutes,
            max_queries_per_run=max_queries_per_run,
            max_results_per_query=max_results_per_query,
            min_view_count=min_view_count,
            region_code=region_code,
        )


def _exact_bool(value: dict[str, Any], field: str, default: bool) -> bool:
    parsed = value.get(field, default)
    if type(parsed) is not bool:
        raise DiscoveryPolicyError(f"{field} must be a boolean")
    return parsed


def _bounded_int(
    value: dict[str, Any],
    field: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    parsed = value.get(field, default)
    if type(parsed) is not int:
        raise DiscoveryPolicyError(f"{field} must be an integer")
    if not minimum <= parsed <= maximum:
        raise DiscoveryPolicyError(
            f"{field} must be between {minimum} and {maximum}"
        )
    return parsed


def _region_code(value: object) -> str:
    if type(value) is not str or re.fullmatch(r"[A-Z]{2}", value) is None:
        raise DiscoveryPolicyError("region_code must be two uppercase ASCII letters")
    return value
