from __future__ import annotations

import pytest

from app.autoflow.embedding_relevance import EmbeddingRelevanceService
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent


@pytest.mark.asyncio
async def test_embedding_relevance_falls_back_without_endpoint():
    service = EmbeddingRelevanceService(embedding_url="")
    intent = AutoFlowIntent(intent_type="animal_compilation", subject="小猫", keywords=["kitten"])
    candidates = [
        AutoFlowClipCandidate(id="a", title="office", source_type="asset", asset_id="asset-a"),
        AutoFlowClipCandidate(id="b", title="小猫 kitten jumps", source_type="asset", asset_id="asset-b"),
    ]

    result = await service.score(intent, candidates)

    assert result.scores["asset-b"] > result.scores["asset-a"]
    assert result.warnings == []


@pytest.mark.asyncio
async def test_embedding_relevance_records_warning_on_client_failure():
    async def failing_embedder(_texts: list[str]) -> list[list[float]]:
        raise RuntimeError("embedding down")

    service = EmbeddingRelevanceService(embedding_url="http://embedding.test", embedder=failing_embedder)
    intent = AutoFlowIntent(intent_type="animal_compilation", subject="小猫", keywords=["kitten"])
    candidates = [AutoFlowClipCandidate(id="a", title="小猫", source_type="asset", asset_id="asset-a")]

    result = await service.score(intent, candidates)

    assert result.scores["asset-a"] > 0
    assert "embedding_relevance_unavailable" in result.warnings
