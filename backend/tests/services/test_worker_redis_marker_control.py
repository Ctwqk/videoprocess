from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from app.services import worker_redis_marker_control as control


def _payload_hash(fields: Mapping[str, str]) -> str:
    return hashlib.sha256(
        json.dumps(fields, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _expectation(
    *,
    marker_key: str = "vp:worker-event-emission:00000000-0000-0000-0000-000000000001",
    expected_message_id: str | None = "1710000000000-0",
    source_state: str = "emitted",
    absence_allowed: bool = False,
) -> control.MarkerExpectation:
    return control.MarkerExpectation(
        marker_kind="event_emission",
        source_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        marker_key=marker_key,
        redis_stream="vp:events",
        expected_message_id=expected_message_id,
        payload_sha256=_payload_hash({"event": "node_completed"}),
        source_state=source_state,
        absence_allowed=absence_allowed,
    )


def _expectation_row(
    expectation: control.MarkerExpectation,
) -> dict[str, object]:
    return {
        "marker_kind": expectation.marker_kind,
        "source_id": expectation.source_id,
        "marker_key": expectation.marker_key,
        "redis_stream": expectation.redis_stream,
        "expected_message_id": expectation.expected_message_id,
        "payload_sha256": expectation.payload_sha256,
        "source_state": expectation.source_state,
        "absence_allowed": expectation.absence_allowed,
    }


def _authorization_row(
    authorization: control.MarkerCleanupAuthorization,
) -> dict[str, object]:
    return {
        "id": authorization.id,
        "marker_kind": authorization.marker_kind,
        "source_id": authorization.source_id,
        "marker_key": authorization.marker_key,
        "redis_stream": authorization.redis_stream,
        "expected_message_id": authorization.expected_message_id,
        "payload_sha256": authorization.payload_sha256,
    }


def _evidence_row(
    evidence: control.MarkerRepairEvidence,
) -> dict[str, object]:
    return {
        "marker_kind": evidence.marker_kind,
        "source_id": evidence.source_id,
        "marker_key": evidence.marker_key,
        "redis_stream": evidence.redis_stream,
        "expected_message_id": evidence.expected_message_id,
        "payload_sha256": evidence.payload_sha256,
        "source_state": evidence.source_state,
    }


class _Result:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def mappings(self) -> _Result:
        return self

    def all(self) -> list[dict[str, object]]:
        return self._rows


class _Database:
    def __init__(
        self,
        *,
        pages: list[list[dict[str, object]]] | None = None,
        claims: list[dict[str, object]] | None = None,
        evidence: dict[str, object] | None = None,
    ) -> None:
        self.pages = list(pages or [])
        self.claims = list(claims or [])
        self.evidence = evidence
        self.calls: list[tuple[str, dict[str, object]]] = []

    @asynccontextmanager
    async def begin(self):
        yield self

    async def scalar(self, statement: object, params: dict[str, object]):
        query = str(statement)
        self.calls.append((query, params))
        if "vp_begin_worker_redis_continuity_check" in query:
            return "begun"
        if "vp_finish_worker_redis_continuity_check" in query:
            return True
        if "vp_record_worker_redis_marker_observation" in query:
            return True
        if "vp_finish_worker_redis_marker_cleanup" in query:
            return True
        if "vp_promote_observed_worker_event_emission" in query:
            return True
        raise AssertionError(query)

    async def execute(self, statement: object, params: dict[str, object]):
        query = str(statement)
        self.calls.append((query, params))
        if "vp_list_worker_redis_marker_expectations" in query:
            return _Result(self.pages.pop(0) if self.pages else [])
        if "vp_claim_worker_redis_marker_cleanup" in query:
            return _Result(self.claims)
        if "vp_load_worker_redis_marker_repair" in query:
            return _Result([self.evidence] if self.evidence is not None else [])
        raise AssertionError(query)


class _Redis:
    def __init__(self, *, condition: str = "ok") -> None:
        self.condition = condition
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.marker = "1710000000000-0"
        self.fields = {"event": "node_completed"}

    async def acl_whoami(self):
        self.calls.append(("acl_whoami", ()))
        return "vp-marker-readiness"

    async def info(self, section: str):
        self.calls.append(("info", (section,)))
        if section == "server":
            return {"run_id": "redis-run", "loading": "1" if self.condition == "loading" else "0"}
        if self.condition == "aof_disabled":
            return {"aof_enabled": "0", "aof_last_write_status": "ok", "aof_last_bgrewrite_status": "ok"}
        if self.condition == "aof_error":
            return {"aof_enabled": "1", "aof_last_write_status": "err", "aof_last_bgrewrite_status": "ok"}
        return {"aof_enabled": "1", "aof_last_write_status": "ok", "aof_last_bgrewrite_status": "ok"}

    async def config_get(self, name: str):
        self.calls.append(("config_get", (name,)))
        return {name: "allkeys-lru" if self.condition == "wrong_eviction" else "noeviction"}

    async def get(self, _key: str):
        self.calls.append(("get", (_key,)))
        if self.condition == "marker_missing":
            return None
        if self.condition == "marker_mismatch":
            return "1710000000001-0"
        return self.marker

    async def xrange(self, _stream: str, minimum: str, maximum: str):
        self.calls.append(("xrange", (_stream, minimum, maximum)))
        if self.condition == "stream_missing":
            return []
        if self.condition == "payload_mismatch":
            return [(self.marker, {"event": "node_failed"})]
        return [(self.marker, self.fields)]

    async def eval(self, script: str, key_count: int, *args: object):
        self.calls.append(("eval", (script, key_count, *args)))
        return 1


def test_canonical_payload_hash_is_sorted_compact_and_string_only() -> None:
    assert control.canonical_payload_sha256({"z": "last", "a": "first"}) == _payload_hash({"a": "first", "z": "last"})

    with pytest.raises(control.MarkerControlError, match="payload_not_canonical"):
        control.canonical_payload_sha256({"event": 1})  # type: ignore[dict-item]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("condition", "reason"),
    [
        ("loading", "redis_loading"),
        ("aof_disabled", "redis_aof_disabled"),
        ("aof_error", "redis_aof_unhealthy"),
        ("wrong_eviction", "redis_eviction_policy_invalid"),
        ("marker_missing", "active_marker_missing"),
        ("marker_mismatch", "active_marker_mismatch"),
        ("stream_missing", "event_stream_entry_missing"),
        ("payload_mismatch", "event_payload_mismatch"),
    ],
)
async def test_continuity_fails_closed_with_stable_reasons(
    condition: str,
    reason: str,
) -> None:
    expectation = _expectation()
    database = _Database(pages=[[_expectation_row(expectation)], []])
    result = await control.check_worker_redis_continuity(
        database,
        _Redis(condition=condition),
        "vp-marker-readiness",
    )

    assert result.state == "error"
    assert result.reason_code == reason
    assert result.expected_count == 1
    assert all("vp_finish_worker_redis_continuity_check" not in query or params["reason_code"] == reason for query, params in database.calls)


@pytest.mark.asyncio
async def test_continuity_pages_stably_and_records_exact_observations() -> None:
    first = _expectation(marker_key="vp:worker-event-emission:00000000-0000-0000-0000-000000000001")
    second = _expectation(marker_key="vp:worker-event-emission:00000000-0000-0000-0000-000000000002")
    database = _Database(
        pages=[
            [_expectation_row(first), _expectation_row(second)],
            [],
        ]
    )
    redis = _Redis()
    redis.marker = first.expected_message_id or ""
    result = await control.check_worker_redis_continuity(
        database,
        redis,
        "vp-marker-readiness",
        page_size=2,
    )

    assert result.state == "ready"
    assert result.expected_count == 2
    assert result.checked_count == 2
    observations = [query for query, _params in database.calls if "vp_record_worker_redis_marker_observation" in query]
    assert len(observations) == 2


@pytest.mark.asyncio
async def test_continuity_records_the_exact_marker_value_it_hashed() -> None:
    expectation = _expectation()

    class RacingRedis(_Redis):
        def __init__(self) -> None:
            super().__init__()
            self.get_calls = 0

        async def get(self, key: str):
            self.get_calls += 1
            if self.get_calls == 1:
                return await super().get(key)
            return "1710000000001-0"

    database = _Database(pages=[[_expectation_row(expectation)], []])
    result = await control.check_worker_redis_continuity(
        database,
        RacingRedis(),
        "vp-marker-readiness",
    )

    observations = [params for query, params in database.calls if "vp_record_worker_redis_marker_observation" in query]
    assert result.state == "ready"
    assert observations == [
        {
            "run_id": result.run_id,
            "marker_kind": expectation.marker_kind,
            "source_id": expectation.source_id,
            "message_id": expectation.expected_message_id,
            "payload_sha256": expectation.payload_sha256,
        }
    ]


@pytest.mark.asyncio
async def test_continuity_rejects_non_increasing_expectation_page() -> None:
    expectation = _expectation()
    database = _Database(
        pages=[
            [_expectation_row(expectation)],
            [_expectation_row(expectation)],
        ]
    )
    result = await control.check_worker_redis_continuity(
        database,
        _Redis(),
        "vp-marker-readiness",
        page_size=1,
    )

    assert result.state == "error"
    assert result.reason_code == "expectation_page_incomplete"


@pytest.mark.asyncio
async def test_prepared_without_message_and_marker_is_consistent() -> None:
    expectation = _expectation(
        expected_message_id=None,
        source_state="prepared",
        absence_allowed=True,
    )
    database = _Database(pages=[[_expectation_row(expectation)], []])
    redis = _Redis(condition="marker_missing")

    result = await control.check_worker_redis_continuity(
        database,
        redis,
        "vp-marker-readiness",
    )

    assert result.state == "ready"
    assert not any("vp_record_worker_redis_marker_observation" in query for query, _params in database.calls)


def test_marker_config_reads_only_bounded_mode_0400_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database_secret = tmp_path / "database-url"
    redis_secret = tmp_path / "redis-url"
    database_secret.write_text("postgresql+asyncpg://marker:secret@db/vp\n", encoding="utf-8")
    redis_secret.write_text("redis://:secret@redis/0\n", encoding="utf-8")
    database_secret.chmod(0o400)
    redis_secret.chmod(0o400)
    monkeypatch.setenv(control.DATABASE_URL_FILE_ENV, str(database_secret))
    monkeypatch.setenv(control.REDIS_URL_FILE_ENV, str(redis_secret))
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)

    config = control.load_marker_control_config(production=True)

    assert config.database_url == "postgresql+asyncpg://marker:secret@db/vp"
    assert config.redis_url == "redis://:secret@redis/0"
    database_secret.chmod(0o600)
    with pytest.raises(control.MarkerControlConfigError):
        control.load_marker_control_config(production=True)


def test_lua_scripts_are_exact_key_scoped_and_cannot_mutate_streams() -> None:
    assert "KEYS[1]" in control.COMPARE_DELETE
    assert "KEYS[1]" in control.RESTORE_IF_ABSENT
    assert "'NX'" in control.RESTORE_IF_ABSENT
    for script in (control.COMPARE_DELETE, control.RESTORE_IF_ABSENT):
        lowered = script.lower()
        assert "xadd" not in lowered
        assert "xack" not in lowered
        assert "expire" not in lowered
        assert "pexpire" not in lowered
        assert "xtrim" not in lowered
    assert "set" not in control.COMPARE_DELETE.lower()


@pytest.mark.asyncio
async def test_janitor_uses_atomic_compare_delete_and_finishes_each_claim() -> None:
    authorization = control.MarkerCleanupAuthorization(
        id=uuid.uuid4(),
        marker_kind="event_emission",
        source_id=uuid.uuid4(),
        marker_key="vp:worker-event-emission:cleanup",
        redis_stream="vp:events",
        expected_message_id="1710000000000-0",
        payload_sha256="a" * 64,
    )
    database = _Database(claims=[_authorization_row(authorization)])
    redis = _Redis()

    result = await control.run_worker_redis_marker_janitor(
        database,
        redis,
        uuid.uuid4(),
    )

    assert result == {"claimed": 1, "deleted": 1, "absent": 0, "conflict": 0}
    eval_call = next(call for call in redis.calls if call[0] == "eval")
    assert eval_call[1][1] == 1
    assert eval_call[1][2:] == (authorization.marker_key, authorization.expected_message_id)
    assert sum("vp_finish_worker_redis_marker_cleanup" in query for query, _params in database.calls) == 1


@pytest.mark.asyncio
async def test_restore_requires_exact_stream_proof_then_authorizes_before_atomic_restore() -> None:
    evidence = control.MarkerRepairEvidence(
        marker_kind="event_emission",
        source_id=uuid.uuid4(),
        marker_key="vp:worker-event-emission:repair",
        redis_stream="vp:events",
        expected_message_id="1710000000000-0",
        payload_sha256=_payload_hash({"event": "node_completed"}),
        source_state="emitted",
    )
    database = _Database(evidence=_evidence_row(evidence))
    redis = _Redis(condition="marker_missing")

    result = await control.restore_worker_redis_marker(
        database,
        redis,
        evidence.source_id,
        apply=True,
    )

    assert result == "restore_applied"
    load_actions = [params["action"] for query, params in database.calls if "vp_load_worker_redis_marker_repair" in query]
    assert load_actions == ["restore_marker", "authorize_restore_marker"]
    assert [name for name, _args in redis.calls].index("xrange") < [name for name, _args in redis.calls].index("eval")
    assert not any("restored" in str(params) for _query, params in database.calls)


@pytest.mark.asyncio
async def test_promote_prepared_requires_marker_and_exact_stream_entry() -> None:
    evidence = control.MarkerRepairEvidence(
        marker_kind="event_emission",
        source_id=uuid.uuid4(),
        marker_key="vp:worker-event-emission:prepared",
        redis_stream="vp:events",
        expected_message_id=None,
        payload_sha256=_payload_hash({"event": "node_completed"}),
        source_state="prepared",
    )
    database = _Database(evidence=_evidence_row(evidence))
    redis = _Redis()

    result = await control.promote_prepared_worker_event(
        database,
        redis,
        evidence.source_id,
        apply=True,
    )

    assert result == "promotion_applied"
    promoted = [params for query, params in database.calls if "vp_promote_observed_worker_event_emission" in query]
    assert promoted == [{"emission_id": evidence.source_id, "message_id": redis.marker, "payload_sha256": evidence.payload_sha256}]
