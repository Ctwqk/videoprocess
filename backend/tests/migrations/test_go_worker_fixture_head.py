"""Keep current-head Go worker fixtures aligned without connecting to PostgreSQL."""

from pathlib import Path
import re

from alembic.config import Config
from alembic.script import ScriptDirectory
import pytest


BACKEND = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("path", "function"),
    (
        ("internal/store/worker_registration_test.go", "newWorkerPostgresFixture"),
        ("internal/worker/consumer_test.go", "newWorkerIntakePostgresFixture"),
    ),
)
def test_go_worker_functional_fixture_requires_current_migration_head(path, function):
    config = Config(str(BACKEND / "alembic.ini"))
    config.set_main_option("script_location", str(BACKEND / "alembic"))
    heads = ScriptDirectory.from_config(config).get_heads()
    assert len(heads) == 1
    source = (BACKEND.parent / path).read_text()
    fixture = re.search(
        rf"^func {function}\(.*?(?=^func |\Z)", source, re.MULTILINE | re.DOTALL
    )
    assert fixture is not None, f"missing functional fixture {function}"
    body = fixture.group()
    assert "SELECT version_num FROM public.alembic_version" in body
    guard = re.search(
        r'if migration != "([^"]+)" \{\s*admin.Close\(\)\s*'
        r't.Fatalf\((.*?)\)\s*\}',
        body,
        re.DOTALL,
    )
    assert guard is not None, "functional fixture must retain its exact revision refusal"
    assert guard.group(1) == heads[0], (
        f"{function} expects {guard.group(1)} but CI migrates to head {heads[0]}"
    )
    assert f"want {heads[0]}" in guard.group(2)
