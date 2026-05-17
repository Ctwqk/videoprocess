import pytest


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def test_asset_id() -> str:
    return "00000000-0000-0000-0000-000000000001"
