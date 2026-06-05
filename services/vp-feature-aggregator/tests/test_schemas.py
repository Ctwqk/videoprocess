from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError
from pydantic import ValidationError as PydanticValidationError

from feature_aggregator.schemas import PDSDecisionEvent, VPActorActionEvent


SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas"


def _schema(name: str) -> dict:
    return json.loads((SCHEMA_DIR / name).read_text())


def _vp_actor_action_event() -> dict:
    return {
        "event_id": "event-1",
        "topic_version": "vp.actor.actions.v1",
        "actor_id": "account-1",
        "action_type": "candidate_accepted",
        "platform": "youtube",
        "occurred_at": "2026-05-19T00:00:00Z",
        "source": "videoprocess.channel_ops",
        "metadata": {"candidate_id": "candidate-1"},
    }


def _pds_decision_event() -> dict:
    return {
        "event_id": "decision-1",
        "topic_version": "pds.decisions.v1",
        "actor_id": "account-1",
        "action_type": "publish",
        "platform": "youtube",
        "verdict": "block",
        "score": 0.9,
        "reasons": [{"code": "burst", "rule": "r1"}],
        "decision_id": "decision-1",
        "occurred_at": "2026-05-19T00:00:00Z",
    }


def test_vp_actor_action_schema_accepts_candidate_event():
    schema = _schema("vp.actor.actions.v1.json")
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(_vp_actor_action_event())


def test_vp_actor_action_schema_and_model_accept_post_comment_event():
    payload = _vp_actor_action_event()
    payload["action_type"] = "post_comment"

    Draft202012Validator(_schema("vp.actor.actions.v1.json")).validate(payload)
    event = VPActorActionEvent.model_validate(payload)

    assert event.action_type == "post_comment"


def test_pds_decision_schema_accepts_block_event():
    schema = _schema("pds.decisions.v1.json")
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(_pds_decision_event())


@pytest.mark.parametrize(
    ("schema_name", "payload"),
    [
        ("vp.actor.actions.v1.json", _vp_actor_action_event()),
        ("pds.decisions.v1.json", _pds_decision_event()),
    ],
)
def test_json_schemas_reject_extra_properties(schema_name: str, payload: dict):
    payload["unexpected"] = "nope"

    with pytest.raises(ValidationError):
        Draft202012Validator(_schema(schema_name)).validate(payload)


def test_pds_decision_schema_rejects_extra_reason_properties():
    payload = _pds_decision_event()
    payload["reasons"] = [{"code": "burst", "unexpected": "nope"}]

    with pytest.raises(ValidationError):
        Draft202012Validator(_schema("pds.decisions.v1.json")).validate(payload)


def test_vp_actor_action_schema_rejects_invalid_action_enum():
    payload = _vp_actor_action_event()
    payload["action_type"] = "not_allowed"

    with pytest.raises(ValidationError):
        Draft202012Validator(_schema("vp.actor.actions.v1.json")).validate(payload)


@pytest.mark.parametrize(
    ("schema_name", "payload", "field_name"),
    [
        ("vp.actor.actions.v1.json", _vp_actor_action_event(), "platform"),
        ("pds.decisions.v1.json", _pds_decision_event(), "client"),
    ],
)
def test_json_schemas_reject_null_optional_strings(
    schema_name: str, payload: dict, field_name: str
):
    payload[field_name] = None

    with pytest.raises(ValidationError):
        Draft202012Validator(_schema(schema_name)).validate(payload)


@pytest.mark.parametrize(
    ("schema_name", "payload", "required_field"),
    [
        ("vp.actor.actions.v1.json", _vp_actor_action_event(), "source"),
        ("pds.decisions.v1.json", _pds_decision_event(), "decision_id"),
    ],
)
def test_json_schemas_reject_missing_required_fields(
    schema_name: str, payload: dict, required_field: str
):
    payload.pop(required_field)

    with pytest.raises(ValidationError):
        Draft202012Validator(_schema(schema_name)).validate(payload)


def test_vp_actor_action_model_rejects_extra_fields():
    payload = _vp_actor_action_event()
    payload["unexpected"] = "nope"

    with pytest.raises(PydanticValidationError):
        VPActorActionEvent.model_validate(payload)


def test_pds_decision_model_rejects_extra_fields():
    payload = _pds_decision_event()
    payload["unexpected"] = "nope"

    with pytest.raises(PydanticValidationError):
        PDSDecisionEvent.model_validate(payload)


def test_pds_decision_reason_model_rejects_extra_fields():
    payload = _pds_decision_event()
    payload["reasons"] = [{"code": "burst", "unexpected": "nope"}]

    with pytest.raises(PydanticValidationError):
        PDSDecisionEvent.model_validate(payload)


@pytest.mark.parametrize(
    ("event_model", "payload", "field_name"),
    [
        (VPActorActionEvent, _vp_actor_action_event(), "platform"),
        (PDSDecisionEvent, _pds_decision_event(), "client"),
    ],
)
def test_event_models_reject_null_optional_strings(event_model, payload: dict, field_name: str):
    payload[field_name] = None

    with pytest.raises(PydanticValidationError):
        event_model.model_validate(payload)


def test_vp_actor_action_model_dump_validates_against_json_schema():
    payload = _vp_actor_action_event()
    payload.pop("platform")
    event = VPActorActionEvent.model_validate(payload)

    Draft202012Validator(_schema("vp.actor.actions.v1.json")).validate(
        event.model_dump(mode="json")
    )


def test_pds_decision_model_dump_validates_against_json_schema():
    payload = _pds_decision_event()
    payload.pop("platform")
    event = PDSDecisionEvent.model_validate(payload)

    Draft202012Validator(_schema("pds.decisions.v1.json")).validate(event.model_dump(mode="json"))
