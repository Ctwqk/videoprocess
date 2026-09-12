from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from app.channel_agent import soak_guard_cli as cli
from app.services.channelops_soak_guard import SoakGuardAssessment
from tests.channel_agent.test_soak_guard_cli import CHANNEL_ID, _SessionFactory, _arguments, _payload


@pytest.mark.asyncio
@pytest.mark.parametrize("critical,apply", [(False, False), (True, False), (False, True), (True, True)])
async def test_inventory_dispatch_never_uses_blanket_quarantine(monkeypatch, capsys, critical, apply):
    inventory_id = uuid.uuid4()
    result = SoakGuardAssessment(
        ("owned_inventory_stop",) if critical else (),
        {"inventory_intake_status": "closed", "inventory_feedback_status": "pending"},
        inventory_id=inventory_id,
    )
    factory = _SessionFactory(object(), object())
    hold = AsyncMock(return_value=result)
    quarantine = AsyncMock(side_effect=AssertionError("inventory must not quarantine"))
    monkeypatch.setattr(cli, "get_session_factory", lambda: factory)
    monkeypatch.setattr(cli, "assess_channelops_soak", AsyncMock(return_value=result))
    monkeypatch.setattr(cli, "apply_inventory_soak_guard", hold, raising=False)
    monkeypatch.setattr(cli, "quarantine_channelops_backlog", quarantine)
    assert await cli.run(_arguments(*(["--apply"] if apply else []))) == (20 if critical else 0)
    if apply:
        hold.assert_awaited_once()
        assert hold.await_args.args[1:3] == (CHANNEL_ID, inventory_id)
    else:
        hold.assert_not_awaited()
    quarantine.assert_not_awaited()
    assert _payload(capsys)["status"] == ("inventory_held" if critical and apply else "critical" if critical else "healthy")


@pytest.mark.asyncio
async def test_fresh_inventory_apply_failure_is_not_healthy(monkeypatch, capsys):
    monkeypatch.setattr(cli, "get_session_factory", lambda: _SessionFactory(object(), object()))
    monkeypatch.setattr(cli, "assess_channelops_soak", AsyncMock(return_value=SoakGuardAssessment((), {}, uuid.uuid4())))
    monkeypatch.setattr(cli, "apply_inventory_soak_guard", AsyncMock(side_effect=ValueError("stale scope")), raising=False)
    assert await cli.run(_arguments("--apply")) == 3
    assert _payload(capsys)["status"] == "inventory_hold_error"
