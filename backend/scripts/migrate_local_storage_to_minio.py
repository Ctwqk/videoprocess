from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.config import settings
from app.models.artifact import Artifact
from app.models.asset import Asset
from app.storage.manager import get_storage


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("migrate_local_storage_to_minio")


def _object_key_for(storage_path: str) -> str:
    path = Path(storage_path)
    if not path.is_absolute():
        return storage_path.lstrip("/")

    local_root = Path(settings.storage_local_root).resolve()
    try:
        return str(path.resolve().relative_to(local_root))
    except ValueError as exc:
        raise RuntimeError(f"{storage_path} is outside STORAGE_LOCAL_ROOT={local_root}") from exc


async def _migrate_rows(rows: list[Asset | Artifact], label: str) -> tuple[int, int]:
    local_storage = get_storage("local")
    minio_storage = get_storage("minio")

    migrated = 0
    skipped = 0
    for row in rows:
        local_path = local_storage.get_local_path(row.storage_path)
        if not local_path or not Path(local_path).is_file():
            logger.warning("Skipping %s %s: missing local file %s", label, row.id, local_path)
            skipped += 1
            continue

        object_key = _object_key_for(row.storage_path)
        with open(local_path, "rb") as fh:
            await minio_storage.save(object_key, fh)

        row.storage_backend = "minio"
        row.storage_path = object_key
        migrated += 1
        logger.info("Migrated %s %s -> %s", label, row.id, object_key)

    return migrated, skipped


async def main() -> None:
    engine = create_async_engine(settings.database_url, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as db:
        assets = list((await db.execute(select(Asset).where(Asset.storage_backend == "local"))).scalars())
        artifacts = list(
            (await db.execute(select(Artifact).where(Artifact.storage_backend == "local"))).scalars()
        )

        asset_migrated, asset_skipped = await _migrate_rows(assets, "asset")
        artifact_migrated, artifact_skipped = await _migrate_rows(artifacts, "artifact")

        await db.commit()
        logger.info(
            "Done. assets migrated=%s skipped=%s, artifacts migrated=%s skipped=%s",
            asset_migrated,
            asset_skipped,
            artifact_migrated,
            artifact_skipped,
        )

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
