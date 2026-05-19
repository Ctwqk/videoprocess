from __future__ import annotations

import json

import httpx
import pytest

from app.pds_client import NoopPDSClient, PDSClient, PDSDecisionRequest


@pytest.mark.asyncio
async def test_noop_pds_client_returns_disabled_allow_warning() -> None:
    decision = await NoopPDSClient().decide(PDSDecisionRequest(actor_id="actor-1", action_type="publish"))

    assert decision.verdict == "allow"
    assert decision.metadata["warning"] == "pds_disabled"


@pytest.mark.asyncio
async def test_pds_client_returns_block_decision_and_forwards_client_id() -> None:
    requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(
            {
                "url": str(request.url),
                "client_id": request.headers["X-Client-Id"],
                "payload": json.loads(request.content.decode()),
            }
        )
        return httpx.Response(
            200,
            json={
                "decision_id": "decision-1",
                "verdict": "block",
                "score": 0.8,
                "reasons": [{"code": "burst", "rule": "r1"}],
                "evaluated_rules": ["r1"],
                "rules_version": "sha256:test",
                "latency_ms": 3,
                "metadata": {"source": "test"},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = PDSClient(
            base_url="http://pds",
            client_id="videoprocess-channel-agent",
            timeout_seconds=0.5,
            http_client=http_client,
        )

        decision = await client.decide(
            PDSDecisionRequest(
                actor_id="actor-1",
                action_type="publish",
                platform="youtube",
                content={"title": "demo"},
                context={"channel_id": "channel-1"},
            )
        )

    assert decision.verdict == "block"
    assert decision.decision_id == "decision-1"
    assert decision.score == 0.8
    assert decision.reasons == [{"code": "burst", "rule": "r1"}]
    assert decision.evaluated_rules == ["r1"]
    assert decision.rules_version == "sha256:test"
    assert decision.latency_ms == 3
    assert decision.metadata == {"source": "test"}
    assert requests == [
        {
            "url": "http://pds/v1/decide",
            "client_id": "videoprocess-channel-agent",
            "payload": {
                "actor_id": "actor-1",
                "action": {"type": "publish", "platform": "youtube"},
                "content": {"title": "demo"},
                "context": {"channel_id": "channel-1"},
            },
        }
    ]


@pytest.mark.asyncio
async def test_pds_client_fails_open_on_500() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "down"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = PDSClient(
            base_url="http://pds",
            client_id="videoprocess-channel-agent",
            timeout_seconds=0.5,
            http_client=http_client,
        )

        decision = await client.decide(PDSDecisionRequest(actor_id="actor-1", action_type="publish"))

    assert decision.verdict == "allow"
    assert decision.metadata["warning"] == "pds_unavailable"


@pytest.mark.asyncio
async def test_pds_client_fails_open_on_network_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = PDSClient(
            base_url="http://pds",
            client_id="videoprocess-channel-agent",
            timeout_seconds=0.5,
            http_client=http_client,
        )

        decision = await client.decide(PDSDecisionRequest(actor_id="actor-1", action_type="publish"))

    assert decision.verdict == "allow"
    assert decision.metadata["warning"] == "pds_unavailable"


@pytest.mark.asyncio
async def test_pds_client_fails_open_on_timeout() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = PDSClient(
            base_url="http://pds",
            client_id="videoprocess-channel-agent",
            timeout_seconds=0.5,
            http_client=http_client,
        )

        decision = await client.decide(PDSDecisionRequest(actor_id="actor-1", action_type="publish"))

    assert decision.verdict == "allow"
    assert decision.metadata["warning"] == "pds_unavailable"


@pytest.mark.asyncio
async def test_pds_client_fails_open_on_invalid_json() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"{not-json")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = PDSClient(
            base_url="http://pds",
            client_id="videoprocess-channel-agent",
            timeout_seconds=0.5,
            http_client=http_client,
        )

        decision = await client.decide(PDSDecisionRequest(actor_id="actor-1", action_type="publish"))

    assert decision.verdict == "allow"
    assert decision.metadata["warning"] == "pds_parse_failed"


@pytest.mark.asyncio
async def test_pds_client_fails_open_on_non_object_200_response() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=["not", "an", "object"])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = PDSClient(
            base_url="http://pds",
            client_id="videoprocess-channel-agent",
            timeout_seconds=0.5,
            http_client=http_client,
        )

        decision = await client.decide(PDSDecisionRequest(actor_id="actor-1", action_type="publish"))

    assert decision.verdict == "allow"
    assert decision.metadata["warning"] == "pds_parse_failed"


@pytest.mark.asyncio
async def test_pds_client_fails_open_on_missing_verdict() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"decision_id": "decision-1"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = PDSClient(
            base_url="http://pds",
            client_id="videoprocess-channel-agent",
            timeout_seconds=0.5,
            http_client=http_client,
        )

        decision = await client.decide(PDSDecisionRequest(actor_id="actor-1", action_type="publish"))

    assert decision.verdict == "allow"
    assert decision.metadata["warning"] == "pds_parse_failed"


@pytest.mark.asyncio
async def test_pds_client_fails_open_on_invalid_verdict() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"decision_id": "decision-1", "verdict": "deny"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = PDSClient(
            base_url="http://pds",
            client_id="videoprocess-channel-agent",
            timeout_seconds=0.5,
            http_client=http_client,
        )

        decision = await client.decide(PDSDecisionRequest(actor_id="actor-1", action_type="publish"))

    assert decision.verdict == "allow"
    assert decision.metadata["warning"] == "pds_parse_failed"


@pytest.mark.asyncio
async def test_pds_client_raises_on_4xx_without_failing_open() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "bad request"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = PDSClient(
            base_url="http://pds",
            client_id="videoprocess-channel-agent",
            timeout_seconds=0.5,
            http_client=http_client,
        )

        with pytest.raises(httpx.HTTPStatusError):
            await client.decide(PDSDecisionRequest(actor_id="actor-1", action_type="publish"))
