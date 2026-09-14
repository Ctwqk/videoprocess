"""Read-only, bounded provenance capture; never an authority to retire consumers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields
from datetime import datetime
import json
from typing import Any
from uuid import UUID

from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
from app.services.registered_consumer_reconcile import (
    IdentityPin,
    MAX_RETIRING_PER_SERVICE,
    PinDocument,
    WorkerPin,
    _CONTRACTS,
    _decode_identity,
    _identity_matches,
    collapse_grant_facts,
    validate_database,
)
from app.services.registered_consumer_reconcile_job import (
    ProtocolError,
    STREAMS,
    _pairs,
    exact,
    read_snapshot,
    require,
)
from app.services.registered_consumer_reconcile_runtime import _io


REGISTRATION_FIELDS = (
    "id", "grant_id", "service_name", "worker_type", "worker_host",
    "capabilities_json", "image_identity", "database_principal", "worker_instance_id",
    "worker_slot", "redis_consumer_id", "lease_epoch", "registered_at",
    "database_fingerprint", "redis_fingerprint", "storage_fingerprint", "status",
    "lease_expires_at", "revoked_at", "revoke_reason", "superseded_by",
)
GRANT_FIELDS = (
    "id", "service_name", "generation", "worker_type", "worker_host", "capabilities_json",
    "release_commit", "image_identity", "database_principal", "redis_stream", "redis_group",
    "endpoint_bindings_json", "state", "activated_at", "revoked_at", "revoke_reason",
)

# One row per depth, with two incoming links as a fork sentinel. Stop once every
# required name is covered, or at depth 65 (overflow), never walk unrelated history.
HISTORY_QUERY = """
WITH RECURSIVE chain AS (
    SELECT r.id, 0 AS depth, array_remove($2::text[], r.redis_consumer_id::text) AS remaining,
           false AS fork
      FROM public.worker_registrations r WHERE r.id = $1::uuid
    UNION ALL
    SELECT r.id, c.depth + 1, array_remove(c.remaining, r.redis_consumer_id::text),
           cardinality(children.ids) > 1
      FROM chain c
      JOIN public.worker_registrations newer ON newer.id = c.id
      CROSS JOIN LATERAL (
          SELECT ARRAY(
              SELECT child.id FROM public.worker_registrations child
               WHERE child.superseded_by = c.id OR (
                   child.superseded_by IS NULL
                   AND child.status = 'revoked'
                   AND child.revoke_reason = 'worker_redis_continuity_unready'
                   AND child.grant_id = newer.grant_id
                   AND child.service_name = newer.service_name
                   AND child.lease_epoch + 1 = newer.lease_epoch
                   AND child.registered_at < newer.registered_at
                   AND child.registered_at <= child.revoked_at
                   AND child.revoked_at <= newer.registered_at
               )
               ORDER BY child.id LIMIT 2
          ) AS ids
      ) children
      JOIN public.worker_registrations r ON r.id = children.ids[1]
     WHERE cardinality(c.remaining) > 0 AND c.depth < $3::integer AND NOT c.fork
)
SELECT c.depth, c.fork, """ + ", ".join(
    [f"r.{name} AS r_{name}" for name in REGISTRATION_FIELDS]
    + [f"g.{name} AS g_{name}" for name in GRANT_FIELDS]
) + """
  FROM chain c JOIN public.worker_registrations r ON r.id = c.id
  LEFT JOIN public.worker_admission_grants g ON g.id = r.grant_id
 ORDER BY c.depth
"""


async def _inventories(client: Any) -> dict[str, set[str]]:
    result = {}
    for service, stream in STREAMS.items():
        worker_type = _CONTRACTS[service][0]
        rows = await _io(client.xinfo_consumers(stream, f"{worker_type}-workers"))
        require(type(rows) is list and 1 <= len(rows) <= 1 + MAX_RETIRING_PER_SERVICE)
        names = []
        for row in rows:
            require(isinstance(row, Mapping))
            name = row.get("name")
            require(type(name) is str and 0 < len(name) <= 255 and name.isascii())
            names.append(name)
        require(len(set(names)) == len(names))
        result[service] = set(names)
    return result


def _model(row: Mapping, prefix: str, names: tuple[str, ...], model: Any) -> Any:
    values = {name: row[prefix + name] for name in names}
    for name in ("capabilities_json", "endpoint_bindings_json"):
        if name in values and type(values[name]) is str:
            values[name] = json.loads(values[name], object_pairs_hook=_pairs)
    return model(**values)


def _pin(row: WorkerRegistration, grant: WorkerAdmissionGrant) -> IdentityPin:
    values = {
        field.name: getattr(row, field.name)
        for field in fields(IdentityPin)
        if field.name not in {"registration_id", "generation", "release_commit", "capabilities"}
    }
    values.update(
        registration_id=UUID(str(row.id)), grant_id=UUID(str(row.grant_id)),
        worker_instance_id=UUID(str(row.worker_instance_id)), generation=grant.generation,
        release_commit=grant.release_commit, capabilities=tuple(row.capabilities_json),
    )
    pin = IdentityPin(**values)
    require(grant.id == pin.grant_id)
    _identity_matches(pin, row, grant)
    return pin


async def capture_history(
    connection: Any, client: Any, snapshot: dict, baseline: dict
) -> dict[str, tuple[IdentityPin, ...]]:
    """Capture complete chains to Redis-present names plus exact baseline pins.

    Caller owns the 15-second overall deadline, credentials and resource cleanup.
    The connection must be the verified read-only capture principal, not operator.
    """
    try:
        for value in (snapshot, baseline):
            exact(value, {"observed_at", "workers"})
            require(type(value["workers"]) is list and len(value["workers"]) == 4)
        current = tuple(_decode_identity(pin) for pin in snapshot["workers"])
        predecessors = tuple(
            None if pin is None else _decode_identity(pin) for pin in baseline["workers"]
        )
        require(tuple(pin.service_name for pin in current) == tuple(_CONTRACTS))
        inventory = await _inventories(client)
        workers, registrations, grants = [], [], []
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            fresh = await read_snapshot(connection, _in_transaction=True)
            require(fresh["workers"] == snapshot["workers"])
            now = datetime.fromisoformat(fresh["observed_at"])
            for pin, predecessor in zip(current, predecessors, strict=True):
                names = inventory.get(pin.service_name, set())
                require(pin.service_name not in STREAMS or pin.redis_consumer_id in names)
                required = names | ({predecessor.redis_consumer_id} if predecessor else set())
                rows = await _io(connection.fetch(
                    HISTORY_QUERY, pin.registration_id, sorted(required),
                    MAX_RETIRING_PER_SERVICE + 1,
                ))
                require(type(rows) is list and 1 <= len(rows) <= 1 + MAX_RETIRING_PER_SERVICE)
                chain = []
                for depth, row in enumerate(rows):
                    require(type(row["depth"]) is int and row["depth"] == depth)
                    require(row["fork"] is False)
                    registration = _model(row, "r_", REGISTRATION_FIELDS, WorkerRegistration)
                    grant = _model(row, "g_", GRANT_FIELDS, WorkerAdmissionGrant)
                    chain.append(_pin(registration, grant))
                    registrations.append(registration)
                    grants.append(grant)
                require(chain[0] == pin)
                retiring = tuple(chain[1:])
                require((retiring[0] if retiring else None) == predecessor)
                require(required <= {item.redis_consumer_id for item in chain})
                # No unsolicited older row is accepted from the query boundary.
                require(not retiring or retiring[-1].redis_consumer_id in required)
                workers.append(WorkerPin(pin, predecessor, retiring[1:]))
            # Metadata here is inert; the caller binds these identities to its
            # real transaction/revision when constructing the immutable document.
            pins = PinDocument(2, "tx-" + "0" * 32, 0, current[0].release_commit, tuple(workers))
            validate_database(pins, registrations, collapse_grant_facts(pins, grants), now=now)
        require(await _inventories(client) == inventory)
        return {worker.current.service_name: worker.retiring for worker in workers}
    except Exception:
        raise ProtocolError() from None
