import io
from types import SimpleNamespace
from datetime import datetime, timezone

import pytest

from app.storage.local import LocalStorageBackend
from app.storage.minio_backend import MinioStorageBackend
from app.services.owned_seed_inventory import _now


@pytest.mark.parametrize("backend", ["local", "minio"])
async def test_inventory_storage_read_is_size_bounded_and_closes_response(tmp_path, backend):
    assert callable(getattr(LocalStorageBackend, "read_bounded", None))
    content = b"abcdefgh"
    if backend == "local":
        storage = LocalStorageBackend(str(tmp_path))
        await storage.save("assets/test.mp4", io.BytesIO(content))
    else:
        storage = MinioStorageBackend.__new__(MinioStorageBackend)
        storage.bucket = "test"
        responses = []

        class Response(io.BytesIO):
            released = False

            def release_conn(self):
                self.released = True

        def get_object(*args):
            response = Response(content)
            responses.append(response)
            return response

        storage.client = SimpleNamespace(get_object=get_object)
    assert await storage.read_bounded("assets/test.mp4", len(content)) == content
    with pytest.raises(ValueError):
        await storage.read_bounded("assets/test.mp4", len(content) - 1)
    if backend == "minio":
        assert all(response.closed and response.released for response in responses)


async def test_approval_expiry_uses_post_lock_database_wall_clock():
    statements = []
    observed = datetime(2026, 9, 10, tzinfo=timezone.utc)

    async def execute(statement):
        statements.append(str(statement))
        return SimpleNamespace(scalar_one=lambda: observed)

    db = SimpleNamespace(get_bind=lambda: SimpleNamespace(dialect=SimpleNamespace(name="postgresql")), execute=execute)
    assert await _now(db) == observed
    assert "clock_timestamp()" in statements[0]
