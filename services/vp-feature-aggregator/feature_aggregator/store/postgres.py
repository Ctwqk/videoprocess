from __future__ import annotations

import asyncpg


class PostgresSummaryStore:
    """Task 5 placeholder; durable summary persistence is not implemented yet."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def ensure_schema(self) -> None:
        raise NotImplementedError(
            "PostgresSummaryStore is a placeholder until durable Task 5+ persistence is scoped."
        )
