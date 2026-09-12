"""Owned-history file selection only; no Redis connection or credential provisioning."""
from datetime import datetime, timezone

import pytest

from app.channel_agent import owned_inventory as admission
from app.config import Settings, settings
from app.services import owned_seed_inventory as inventory
from worker import secret_config


URL = "redis://vp-control:synthetic-secret@localhost:6399/3"


@pytest.fixture
def reader(monkeypatch):
    clients = []
    class Client:
        closed = False
        async def acl_whoami(self):
            return "vp-control"
        async def aclose(self):
            self.closed = True
    def create(url, **_kwargs):
        client = Client()
        clients.append((url, client))
        return client
    monkeypatch.setattr(inventory.aioredis, "from_url", create)
    monkeypatch.setattr(settings, "redis_url", "redis://ordinary:ordinary-secret@localhost:6398/0")
    monkeypatch.setitem(settings.__dict__, "owned_history_redis_url_file", None)
    return clients


def test_optional_history_file_setting_defaults_unset_and_inventory_stays_disabled(monkeypatch):
    monkeypatch.setenv("OWNED_HISTORY_REDIS_URL_FILE", "/run/secrets/owned-history-redis-url")
    configured = Settings(_env_file=None)
    assert configured.owned_history_redis_url_file == "/run/secrets/owned-history-redis-url"
    monkeypatch.delenv("OWNED_HISTORY_REDIS_URL_FILE")
    defaults = Settings(_env_file=None)
    assert defaults.owned_history_redis_url_file is None
    assert defaults.owned_seed_inventory_enabled is False


async def test_history_file_is_read_once_and_same_url_validates_and_connects(tmp_path, monkeypatch, reader):
    path = tmp_path / "reader"
    path.write_text(URL + "\n")
    path.chmod(0o400)
    monkeypatch.setitem(settings.__dict__, "owned_history_redis_url_file", str(path))
    calls = []
    original = secret_config.read_mode_0400_secret
    def read(*args, **kwargs):
        calls.append(args[0])
        value = original(*args, **kwargs)
        path.chmod(0o600)
        path.write_text("redis://default:wrong@localhost:6399/0")
        return value
    monkeypatch.setattr(secret_config, "read_mode_0400_secret", read)
    result = await admission.observe_redis(admission.RedisRequest("fixture", datetime.now(timezone.utc), ()))
    assert result == () and calls == [str(path)]
    assert reader[0][0] == URL and reader[0][1].closed
    assert settings.redis_url == "redis://ordinary:ordinary-secret@localhost:6398/0"


async def test_approval_factory_also_selects_file_and_unset_keeps_legacy_url(tmp_path, monkeypatch, reader):
    legacy = inventory._history_redis()
    assert reader[-1][0] == settings.redis_url
    await legacy.aclose()
    path = tmp_path / "reader"
    path.write_text(URL)
    path.chmod(0o400)
    monkeypatch.setitem(settings.__dict__, "owned_history_redis_url_file", str(path))
    selected = inventory._history_redis()
    assert reader[-1][0] == URL
    await selected.aclose()
    from app.orchestrator.engine import _redis
    ordinary = _redis()
    assert reader[-1][0] == settings.redis_url
    await ordinary.aclose()


@pytest.mark.parametrize("bad", ["missing", "empty_path", "empty", "mode", "symlink", "oversize", "utf8",
                                 "invalid_url", "default_user", "missing_password", "query"])
async def test_explicit_bad_history_file_never_falls_back_or_leaks(tmp_path, monkeypatch, reader, bad):
    path = tmp_path / "private-sentinel"
    path.write_bytes(b"\xff" if bad == "utf8" else URL.encode())
    path.chmod(0o400)
    if bad == "missing":
        path.unlink()
    elif bad == "mode":
        path.chmod(0o600)
    elif bad == "symlink":
        link = tmp_path / "link"
        link.symlink_to(path)
        path = link
    elif bad not in {"empty_path", "utf8"}:
        contents = {"empty": "", "oversize": "x" * 4097, "invalid_url": "https://private-sentinel/secret",
            "default_user": "redis://default:secret@localhost:6399/0", "missing_password": "redis://vp-control@localhost:6399/0",
            "query": URL + "?private-sentinel=secret"}[bad]
        path.chmod(0o600)
        path.write_text(contents)
        path.chmod(0o400)
    monkeypatch.setitem(settings.__dict__, "owned_history_redis_url_file", "" if bad == "empty_path" else str(path))
    with pytest.raises(inventory.OwnedInventoryError, match="^owned_history_redis_configuration$") as caught:
        inventory._history_redis()
    assert "secret" not in str(caught.value) and "private-sentinel" not in str(caught.value)
    assert reader == []
