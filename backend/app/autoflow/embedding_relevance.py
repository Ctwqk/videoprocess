from __future__ import annotations

import math
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx

from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent


Embedder = Callable[[list[str]], Awaitable[list[list[float]]]]


@dataclass(frozen=True)
class RelevanceResult:
    scores: dict[str, float]
    warnings: list[str]


class EmbeddingRelevanceService:
    def __init__(
        self,
        *,
        embedding_url: str = "",
        timeout_seconds: float = 8.0,
        embedder: Embedder | None = None,
    ) -> None:
        self.embedding_url = embedding_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.embedder = embedder

    async def score(
        self,
        intent: AutoFlowIntent,
        candidates: list[AutoFlowClipCandidate],
    ) -> RelevanceResult:
        fallback = _fallback_scores(intent, candidates)
        if not candidates or not self.embedding_url:
            return RelevanceResult(scores=fallback, warnings=[])

        texts = [_intent_text(intent), *[_candidate_text(candidate) for candidate in candidates]]
        try:
            vectors = await self._embed(texts)
            if len(vectors) != len(texts):
                raise ValueError(f"embedding service returned {len(vectors)} vectors for {len(texts)} texts")
            intent_vector = vectors[0]
            scores = {
                _candidate_key(candidate): _clamp_float((_cosine_similarity(intent_vector, vectors[index + 1]) + 1) / 2)
                for index, candidate in enumerate(candidates)
            }
            return RelevanceResult(scores=scores, warnings=[])
        except Exception:
            return RelevanceResult(scores=fallback, warnings=["embedding_relevance_unavailable"])

    async def _embed(self, texts: list[str]) -> list[list[float]]:
        if self.embedder is not None:
            return await self.embedder(texts)

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(f"{self.embedding_url}/embed", json={"texts": texts})
            response.raise_for_status()
            payload = response.json()

        raw_vectors = payload.get("embeddings") or payload.get("vectors") or []
        if isinstance(raw_vectors, dict):
            raw_vectors = raw_vectors.get("embeddings") or raw_vectors.get("vectors") or []
        return [list(map(float, vector)) for vector in raw_vectors if vector]


def _fallback_scores(intent: AutoFlowIntent, candidates: list[AutoFlowClipCandidate]) -> dict[str, float]:
    return {_candidate_key(candidate): _token_relevance(_intent_text(intent), _candidate_text(candidate)) for candidate in candidates}


def _intent_text(intent: AutoFlowIntent) -> str:
    return " ".join([intent.subject, intent.intent_type, intent.style, *intent.keywords])


def _candidate_text(candidate: AutoFlowClipCandidate) -> str:
    metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
    visual = metadata.get("visual") if isinstance(metadata.get("visual"), dict) else {}
    parts: list[str] = [
        candidate.title,
        candidate.source_type,
        str(metadata.get("description") or ""),
        str(metadata.get("platform") or metadata.get("source_platform") or ""),
    ]
    for key in ("tags", "keywords", "object_labels"):
        value = metadata.get(key, visual.get(key))
        if isinstance(value, list):
            parts.extend(str(item) for item in value)
        elif value:
            parts.append(str(value))
    dominant_action = visual.get("dominant_action") or metadata.get("dominant_action")
    if dominant_action:
        parts.append(str(dominant_action))
    return " ".join(part for part in parts if part)


def _candidate_key(candidate: AutoFlowClipCandidate) -> str:
    return candidate.asset_id or candidate.id


def _token_relevance(query: str, text: str) -> float:
    query_tokens = _tokens(query)
    text_tokens = _tokens(text)
    if not query_tokens:
        return 0.5
    if not text_tokens:
        return 0.0
    matches = len(query_tokens & text_tokens)
    partial_matches = sum(
        1
        for query_token in query_tokens
        if query_token not in text_tokens and any(query_token in text_token or text_token in query_token for text_token in text_tokens)
    )
    return _clamp_float((matches + 0.5 * partial_matches) / max(1, min(len(query_tokens), 6)))


def _tokens(text: str) -> set[str]:
    return {token for token in re.findall(r"[\w\u4e00-\u9fff]+", text.lower()) if token}


def _cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm <= 0 or right_norm <= 0:
        return 0.0
    return dot / (left_norm * right_norm)


def _clamp_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, number))
