"""Offline v2 cases; counts include predecessor and exclude current.

history_document keeps the legacy direct predecessor for ordinary cases, adds
explicit newest-first ancestors, and leaves vision ancestry empty. Larger counts
raise epochs/generations to keep boundary fixtures valid. history_facts links
every old row to its exact next-newer registration, including absent consumers.
"""

from __future__ import annotations

import copy
from datetime import timedelta
from uuid import UUID

from app.services import registered_consumer_reconcile as reconcile
from tests.services.test_registered_consumer_reconcile import (
    NOW,
    RELEASE,
    decode,
    document,
    facts,
    identity,
    inventories,
)


def history_document(retiring_count=3):
    """Build four workers, with retiring_count old pins on each mutable stream."""
    payload = document()
    payload["version"] = 2
    for index, worker in enumerate(payload["workers"]):
        worker["ancestors"] = []
        if index == 2:
            continue
        if retiring_count == 0:
            worker["predecessor"] = None
            continue
        current = worker["current"]
        current["generation"] = max(current["generation"], retiring_count + 1)
        current["lease_epoch"] = max(current["lease_epoch"], retiring_count + 1)
        worker["predecessor"]["generation"] = current["generation"] - 1
        worker["predecessor"]["lease_epoch"] = current["lease_epoch"] - 1
        for depth in range(2, retiring_count + 1):
            ancestor = identity(index, old=True)
            number = 1000 + index * 1000 + depth * 3
            ancestor.update(
                registration_id=str(UUID(int=number)),
                grant_id=str(UUID(int=number + 1)),
                worker_instance_id=str(UUID(int=number + 2)),
                generation=current["generation"] - depth,
                lease_epoch=current["lease_epoch"] - depth,
                registered_at=(NOW - timedelta(hours=depth + 1)).isoformat(),
            )
            ancestor["redis_consumer_id"] = (
                f"{ancestor['worker_type']}-worker@{ancestor['worker_host']}:1:"
                f"{ancestor['worker_instance_id']}"
            )
            # Rollback may revisit a release; provenance is not release ordering.
            if depth % 2 == 0:
                ancestor["release_commit"] = RELEASE
                ancestor["image_identity"] = (
                    f"vp-{ancestor['worker_type']}:deploy-{RELEASE[:12]}"
                )
            worker["ancestors"].append(ancestor)
    return payload


def history_facts(payload):
    """Return mutable ORM row/grant lists for every explicitly pinned identity."""
    registrations, grants = facts(payload)
    for worker in payload["workers"]:
        successor = worker["predecessor"]
        for ancestor in worker["ancestors"]:
            old_rows, old_grants = facts(
                {"workers": [{"current": successor, "predecessor": ancestor}]}
            )
            registrations.append(old_rows[1])
            grants.append(old_grants[1])
            successor = ancestor
    return registrations, grants


def history_inventories(payload):
    """Return the three fixed inventories with all requested old names present."""
    result = inventories(payload)
    for worker in payload["workers"]:
        service = worker["current"]["service_name"]
        if service in result:
            result[service]["consumers"].extend(
                {"name": pin["redis_consumer_id"], "pending": 0, "idle": 120001}
                for pin in worker["ancestors"]
            )
    return result


def history_assess(payload=None, *, rows=None, inventory=None, replay_only=False):
    """Exercise the pure assessor with optional mutated facts/inventories."""
    payload = history_document() if payload is None else copy.deepcopy(payload)
    return reconcile.assess(
        decode(payload),
        *(history_facts(payload) if rows is None else rows),
        history_inventories(payload) if inventory is None else inventory,
        now=NOW,
        replay_only=replay_only,
    )
