# Scoped Worker Session Retirement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow a non-superuser deploy owner to retire only revoked VP worker sessions during replacement.

**Architecture:** A bootstrap-owned private SQL helper derives a principal from a managed service and generation, and reads only built-in catalogs. Existing guarded activation/revocation functions retain business-table validation; deployment receives no general signaling privilege.

**Tech Stack:** PostgreSQL 16, Alembic, Python asyncpg, pytest, Bash deployment contracts.

**Spec:** `docs/superpowers/specs/2026-09-06-worker-session-signal-design.md`

## Global Constraints

- No superuser or usable parent-role privilege for the deploy principal.
- Only exact revoked, NOLOGIN, canonical VP worker principals in the current database may be signaled.
- Preserve existing worker API signatures, function ownership, and EXECUTE ACLs.
- No uploads, no changes to review rules, no VP operations on 126.

## Task 1: Reproduce and Repair the SQL Boundary

**Files:** Existing PG16 operator integration tests; new private SQL helper bootstrap; new Alembic migration following `035_worker_creator_edges`.

**Interfaces:** Existing `vp_worker_grant_activate(text,bigint)` and `vp_worker_grant_revoke(text,bigint,text)` retain their contracts. The new private function accepts a managed service name and generation and returns the count of signaled sessions. Grant checks run without bootstrap authority. The operator CLI performs a separately committed scoped drain after activation/revocation.

- [x] Add a real connection to the existing non-superuser lifecycle fixture. Execute `await fixture.activate(2)` while generation 1 remains connected; require activation success and the old connection to close. Run this against 035 and observe the permission error before changing implementation. Reproduced September 6 on isolated local PG16, matching production SQLSTATE 42501.
- [x] Implement the helper's ownership, caller, principal, grant-state, role-graph, database, and PID scope checks. Signal through catalog functions only after all target validation succeeds. Canonical cleanup also uses the helper after quarantine commits; helper lock waits are bounded at two seconds.
- [x] Replace only the two operator bodies in the new migration. Require the verified helper to exist before upgrading.
- [x] Test direct-call denial and rejected target categories, including other-database preservation. Verify failed activation retains the original grant state and authority. Final focused PG16/service selection: 240 passed, no skips.

## Task 2: Verify and Deploy

**Files:** Migration-head assertions, CI lifecycle selection, deployment closeout record.

- [x] Update only assertions that intentionally track the new migration head. Run the isolated PG16 lifecycle and backend suite; run required advisory Ruff/mypy checks. Full backend: 1825 passed, 15 optional skips; focused checks pass, broad advisory findings unchanged.
- [x] Review the privilege boundary independently and address concrete findings before integration. Final scoped review found no introduced issues after the bounded lock-wait and existing-worker rollback-intent repairs.
- [ ] Rehearse the administrator bootstrap in isolation, then apply it only to `videoprocess` under the deployment writer lock after a schema backup.
- [ ] Push, wait for exact-commit CI, and use the normal project-scoped 127/150 deployment path. Require a completed durable transaction and matching service/commit evidence before claiming convergence.
- [ ] Run read-only canary preflight. Record the fifth canary's failed result accurately; do not launch another upload without its separate authorization.
