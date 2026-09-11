# Registered Consumer Reconcile Unit 2 Checkpoint

## Scope And Integration

- Base: Unit 1 `b7188c91c7cc5e88893fccfa2786825d0783a7b6` on `codex/registered-consumer-reconcile`.
- Inert library plus restricted SQL guard only. No CLI, shell, admission transaction, managed-job transport, daemon, deployment activation, or legacy vision changes.
- Unit 1 immutable pins, assessment, and fixed Lua are byte-unchanged.
- Frozen8ffdaff used local037 after036. Parent integration now uses final `039_registered_consumer_guard`, after integrated `038_owned_seed_inventory`, preserving ACK037 and native038 role/pointer restrictions. Runtime head, fixture constants and migration-chain tests moved together. A2's future040 is outside this checkpoint.
- Agent did not run PG, Redis, network, SSH, Docker, live recovery, push, or another agent. All actual role/lock/Lua qualification below remains parent-owned.

## Delivered Files

1. `backend/alembic/versions/039_registered_consumer_guard.py`: fixed SECURITY DEFINER guard, `pg_catalog` search path, exact generation-bound operator login, stable operator membership, safe-principal/assumable-role checks, no PUBLIC execution. Locks CLOSED/no-guard schedule plus all pinned registration/grant rows and rejects active work. Returns at most eight nonsecret typed rows and fresh DB time; no DML.
2. `backend/app/services/registered_consumer_reconcile_runtime.py`: fixed mounted credentials, endpoint/generation/principal validation, direct no-retry connections, bounded fully awaited controller, fresh guards/polls, one EVAL per stream, final read-only proof, owned rollback/resource cleanup.
3. `backend/app/services/worker_control_role_cli.py`: one operator function allowlist addition; no table privileges added.
4. `backend/app/services/worker_deployment_cli.py`: expected local migration head only.
5. `backend/tests/services/test_worker_control_role_cli.py`: exact function allowlist expectation.
6. `backend/tests/services/test_registered_consumer_reconcile_runtime.py`: offline controller/credentials/native-record/schema/fixture-guard regressions.
7. `backend/tests/migrations/test_registered_consumer_reconcile_postgres.py`: 16 opt-in real PG cases.
8. `backend/tests/services/test_registered_consumer_reconcile_redis.py`: two opt-in real Redis families, each with natural 120.1-second aging.
9. This report.

## Runtime Entry Contract

The only entry is the explicitly awaited library call:

```python
result = await reconcile_registered_consumers(invocation, attempt_authority)
```

`Invocation` binds a Unit 1 `PinDocument`, nonzero UUID attempt ID, strict boolean replay mode, control/Redis generations, expected non-default Redis username, exact 25-character managed DB/Redis secret IDs, and SHA256 digests of their complete mounted bytes. DB secret name is `vp-wc-operator-{control_generation}`; Redis name is `vp-control-redis-{redis_generation}`. No secrets appear in the result.

Only these regular, stable, no-follow mounts are read, owned by `10001:10001` with mode `0400`:

- `/run/secrets/registered-reconcile-database-url`
- `/run/secrets/registered-reconcile-redis-url`
- `/run/secrets/registered-reconcile-pins`

Raw URL variables, alternate credential-file variables and PG connection/option fallbacks are refused. URLs require explicit ports, passwords, exact principal/endpoints, no query options. Actual PG `session_user`, `current_user`, database and Redis `ACL WHOAMI` are checked. Redis >=7.2 is required. Managed secret **descriptor/ID provenance cannot be established by reading file bytes alone**; Unit 3 must validate it using its owned journal/job descriptor before this call and on each authority callback.

`AttemptAuthority` is mandatory, with no production default:

- `revalidate(request)`: prove exact owned FORWARD_APPLYING transaction/revision, selected worker plans, pin hash, exact managed input/credential descriptors and retained deployment/admission ownership. Reject consumed/foreign/ambiguous authority.
- `before_eval(request, command)`: atomically persist/reserve this immutable attempt+stream before returning; never permit duplicate or ambiguous attempts. It receives the complete Unit 1 immutable command/pins.
- `after_eval(request, command, outcome)`: persist `retired`, `already_absent`, or `unknown`; tolerate an uncertain previous result write only according to the durable journal's fail-closed rules. No retry authorization is created here.

Callbacks must be cancellation-cooperative, bounded and retain caller ownership. The local attempted set is only duplicate defense, not a cross-process fence. Callback/transport failure stops forward work; even failure to persist `unknown` closes resources and fails with a static error. Callers must not print chained driver exceptions.

The caller must await the coroutine and its cleanup, cancel it at most once for a signal, retain its locks until settled, and never shield the entire forward sequence. Runtime shields only its bounded cleanup. Total budget is200s: normal work stops by192s, with rollback2s, uncertain-result callback2s and two resource closes2s each reserved. Each locked section has6s work plus at most2s rollback; natural-aging sleep happens only after rollback. All five PELs must be zero; affected three groups require strict integer zero lag. Vision/events membership is preserved; ordinary idle advancement is allowed. Final DB clock/readiness is refreshed after the final Redis observation. No retry/reconnect/replay of an EVAL is enabled.

An already-absent/replay success is read-only and consumes no mutation intent. Partial or unknown outcomes do not authorize another job or another EVAL. Unit 3 must enforce job terminal/removal receipts and durable recovery before FORWARD_VERIFIED. **This checkpoint is not the native deployment gate.**

## Offline Evidence

Commands run from this worktree's `backend` with root venv and this worktree's PYTHONPATH; opt-in URLs were absent via `env -i`.

- Focused Unit1/Unit2 plus role/deployment CLI: **292 passed, 19 explicit external skips, 0.65s**.
- Final full-backend run on checkpoint source: **2593 passed, 328 skipped, 29 warnings, 77.67s** (`pytest -q --disable-warnings`, exit0).
- Changed-file Ruff: PASS. Full backend Ruff:14 existing errors (9 F401,5 E402), all outside changed files.
- Runtime module mypy: PASS. Full `mypy app`:61 existing errors in21 unchanged files; no new runtime error.
- Local Alembic graph/head and offline PostgreSQL SQL rendering are included in offline/full backend tests. No actual migration execution is claimed.
- Offline fixture check inserts complete registration/grant rows into in-memory SQLite with model constraints, verifies required fields and unique principals/tokens/leases; this is not PG constraint qualification.

Observed RED/GREEN regressions included missing runtime/guard contract, stale ready decision after a newer wait, unknown journaling under row locks, unsupported Redis versions, native asyncpg Record acceptance (not a Mapping), failure to preserve unknown after a result-journal error, secret-free cleanup errors, and final DB-clock ordering. Repeated cancellation and rollback/close timeouts prove no late fake I/O child remains.

## Parent-Only Qualification

Do not use operational credentials. Provision a fresh disposable PG16 and Redis>=7.2 separately; neither fixture provisions/migrates a server/database, uses production DATABASE_URL, or falls back to other URLs. Do not run these fixed-key/fixed-service fixtures concurrently.

PG requires an owner/admin **fixture-only** URL on `127.0.0.1` or `::1`, explicit port1024..65535 other than5432, no URL query, DB matching `vp_registered_reconcile_test_[a-z0-9_]+`, and confirmation equal to that exact DB name. The DB must already be migrated to the checkpoint's actual head (local037, parent-renumbered039 only after updating the test constant). Registration/grant/job tables must be empty; an existing schedule must already be CLOSED/no-guard. Fixtures create bounded canonical authenticated operator/runtime/watcher test logins, only the guard EXECUTE grant for operator, full-provenance rows and test-owned negative variants; cleanup removes only fixture-owned rows/roles. They never grant table SELECT/UPDATE or use SET ROLE to qualify a login.

```bash
# Parent supplies a fresh local owner URL, for example port55465/database
# vp_registered_reconcile_test_20260911, in the first variable; no URL is logged.
REGISTERED_RECONCILE_DISPOSABLE_POSTGRES_URL="$PARENT_DISPOSABLE_PG_URL" \
REGISTERED_RECONCILE_DISPOSABLE_POSTGRES_CONFIRM=vp_registered_reconcile_test_20260911 \
PYTHONPATH="$PWD" /Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest -q \
  tests/migrations/test_registered_consumer_reconcile_postgres.py
```

PG covers actual intended login, native UUID/complete facts, direct SELECT/FOR SHARE and DML denial before/after guard, PUBLIC denial/search path, owner/runtime/watcher refusal, schedule/registration/grant lock contention and rollback release, changed generation/pins/schedule/current lease/grant binding/supersession/active work, and assumable administrator-role refusal.

Redis requires `redis://127.0.0.1:<explicit-nondefault-port>/15` (IPv6 loopback also accepted), no query/fragment, and the fixed confirmation below. All five fixed keys must initially be absent. The disposable admin fixture creates/removes exact test keys and random test ACL users; control gets read-only five-key selectors plus EVAL/DELCONSUMER only on the three affected keys, watcher stays read-only. Fixture-only seed/restore XADD/read/ACK/group metadata writes are not runtime capabilities.

```bash
REGISTERED_RECONCILE_DISPOSABLE_REDIS_URL="$PARENT_DISPOSABLE_REDIS_DB15_URL" \
REGISTERED_RECONCILE_DISPOSABLE_REDIS_CONFIRM=registered-consumer-reconcile-disposable-db15 \
PYTHONPATH="$PWD" /Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest -q \
  tests/services/test_registered_consumer_reconcile_redis.py
```

Allow over240s total for both tests' real aging; production thresholds are unchanged. Families cover minimal real control/watcher ACLs, stale current, lag-only XADD, PEL race, native unknown lag, unknown/replaced/missing current names, all three successful retirements, native already-absent result, fresh-old refusal, partial runtime failure and no replay. The partial runtime test uses **real Redis but fake DB/Unit3 authority**, not a combined PG+Redis+managed-job qualification. No real case was executed by the implementing agent.

## Remaining Acceptance

Parent actual migration, intended-role/lock and Redis qualification; integration renumber/rebase qualification; independent review; Unit3 managed job/secret/attempt/terminal/removal and signal/lock transport; normal and recovery promotion gating. No claim of production readiness, solved latency, or a completed deployment gate.
