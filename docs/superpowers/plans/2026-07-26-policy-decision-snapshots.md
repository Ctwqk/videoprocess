# Policy Decision Snapshots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist an immutable baseline policy version and complete candidate feature snapshots for every new ChannelOps tick without changing live selection or publication behavior.

**Architecture:** Add normalized append-only policy and snapshot schema, deterministic Go builders, transactional tick persistence, and read-only API views. Existing candidate building, guards, PDS decisions, task creation, scheduling, upload, and publication remain behaviorally unchanged.

**Tech Stack:** PostgreSQL 16, Alembic, SQLAlchemy 2, Go 1.25, pgx, FastAPI, Pydantic, pytest.

## Global Constraints

- This is a passive audit increment; it must not change selected candidates, task count, scheduling, uploads, or publication.
- New and existing publication privacy remains `private` or `unlisted`; `public` remains blocked.
- External platform assets still require explicit human review.
- Policy versions and candidate snapshots are immutable after insertion.
- Historical decisions are `legacy_unreplayable`; they are never reconstructed from current mutable configuration.
- New ticks fail closed and roll back if policy or snapshot facts cannot be persisted completely.
- No policy activation mutation API is introduced.
- No sixth canary, schedule mutation, queue mutation, or soak activation is part of this plan.

---

### Task 1: Add Immutable Policy And Snapshot Schema

**Files:**
- Create: `backend/alembic/versions/034_policy_decision_snapshots.py`
- Modify: `backend/app/models/channel_agent.py`
- Create: `backend/tests/migrations/test_policy_decision_snapshots_postgres.py`
- Create: `backend/tests/channel_agent/test_policy_snapshot_models.py`

**Interfaces:**
- Produces tables `decision_policy_versions`, `policy_activation_history`, and `candidate_feature_snapshots`.
- Produces audit columns named in the design.
- Produces replay statuses `legacy_unreplayable`, `snapshot_pending`, and `snapshot_complete`.

- [ ] **Step 1: Write failing schema and model tests**

Require all columns, foreign keys, checks, and unique indexes from the design.
Create a legacy tick fixture before applying revision 034 and assert the
forward migration sets only that row to `legacy_unreplayable`. Assert policy
status, activation mode, rollout range, replay status, and snapshot decision
checks reject unsupported values.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/migrations/test_policy_decision_snapshots_postgres.py \
  tests/channel_agent/test_policy_snapshot_models.py -q
```

Expected: fail because revision 034 and model classes do not exist.

- [ ] **Step 3: Implement revision 034 and ORM models**

Use additive tables/columns. Make `decision_policy_versions` and
`candidate_feature_snapshots` append-only by application contract. Add:

```text
decision_policy_versions:
  unique(policy_key, version)
  check status in draft/validated/retired
  required JSON policy components and config_hash

policy_activation_history:
  unique(request_id)
  check mode in off/shadow/canary/active
  check rollout_percentage between 0 and 100
  effective_to must be null or after effective_from

candidate_feature_snapshots:
  unique(tick_audit_id, candidate_id, feature_schema_version)
  foreign keys to tick and policy
  required feature_as_of, candidate_set_hash, feature_hash, and JSON facts
```

Extend audits with nullable foreign keys and hash/score/rank/decision fields.
Set existing ticks to `legacy_unreplayable`; leave existing decisions'
new foreign keys null.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run the command from Step 2. Expected: pass.

- [ ] **Step 5: Run a fresh PostgreSQL upgrade**

Run the repository's disposable PostgreSQL migration test and:

```bash
cd backend
DATABASE_URL="$CHANNEL_OPS_POSTGRES_TEST_URL" \
  /Users/wenjieliu/videoprocess/backend/.venv/bin/alembic upgrade head
```

Expected: revision `034_policy_decision_snapshots`. The environment variable is
the same PostgreSQL 16 URL used by `.github/workflows/ci.yml`.

- [ ] **Step 6: Commit**

```bash
git add backend/alembic/versions/034_policy_decision_snapshots.py \
  backend/app/models/channel_agent.py \
  backend/tests/migrations/test_policy_decision_snapshots_postgres.py \
  backend/tests/channel_agent/test_policy_snapshot_models.py
git commit -m "feat(channelops): add policy snapshot schema"
```

### Task 2: Build Deterministic Policy And Feature Facts

**Files:**
- Create: `internal/channelops/policy_snapshots.go`
- Create: `internal/channelops/policy_snapshots_test.go`
- Modify: `internal/channelops/types.go`

**Interfaces:**
- Produces `const CandidateFeatureSchemaVersion = "channelops-candidate-v1"`.
- Produces `BuildBaselinePolicy(channel ChannelProfileRow) PolicyVersion`.
- Produces `BuildCandidateSnapshots(policy PolicyVersion, candidates []TickCandidate, asOf time.Time) SnapshotSet`.
- Produces stable `ConfigHash`, `CandidateSetHash`, `FeatureHash`, and `DecisionHash` values.

- [ ] **Step 1: Write failing deterministic builder tests**

Cover:

- map insertion order does not change canonical hashes;
- candidate input order does not change `CandidateSetHash`;
- accepted and rejected candidates are both present;
- one `asOf` value is used across all snapshots;
- missing lane/format/account/source IDs are represented in a missing mask;
- timestamps and later PDS/metrics values do not affect feature hashes;
- semantic config changes change `ConfigHash`;
- exact replay fixtures produce fixed expected hashes.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
go test ./internal/channelops -run 'Test(BuildBaselinePolicy|BuildCandidateSnapshots|CanonicalPolicyHash)' -count=1
```

Expected: compile failure because the builder is absent.

- [ ] **Step 3: Implement canonical builders**

Use `encoding/json` over structs and recursively normalized maps. Sort
candidates by candidate ID before calculating the set hash. Keep the builder
pure: no database, environment, clock, or network reads. Represent unknown
code/template/prompt identities with explicit constants.

- [ ] **Step 4: Run focused and package tests**

```bash
go test ./internal/channelops -run 'Test(BuildBaselinePolicy|BuildCandidateSnapshots|CanonicalPolicyHash)' -count=1
go test ./internal/channelops -count=1
go test -race ./internal/channelops -count=1
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add internal/channelops/policy_snapshots.go \
  internal/channelops/policy_snapshots_test.go \
  internal/channelops/types.go
git commit -m "feat(channelops): build deterministic policy snapshots"
```

### Task 3: Persist Complete Snapshots In The Fenced Tick Transaction

**Files:**
- Create: `internal/channelops/store_policy_snapshots.go`
- Create: `internal/channelops/store_policy_snapshots_test.go`
- Modify: `internal/channelops/store_tasks.go`
- Modify: `internal/channelops/store_tick.go`
- Modify: `internal/channelops/store_tick_test.go`
- Modify: `internal/channelops/integration_test.go`

**Interfaces:**
- Consumes `PolicyVersion` and `SnapshotSet` from Task 2.
- Produces `ResolvePolicyVersion(ctx, db, policy) (string, error)`.
- Produces `InsertCandidateSnapshots(ctx, db, tickAuditID, policyID, snapshots) (map[string]string, error)`.
- Extends decision-audit insertion with policy and feature-snapshot IDs.

- [ ] **Step 1: Write failing store and integration tests**

Require:

- same policy version and content reuses one row;
- same key/version with different content fails closed;
- every candidate gets exactly one snapshot and decision link;
- duplicate candidate IDs roll back;
- missing snapshot link rolls back;
- tick remains `snapshot_pending` until all facts and tasks are written;
- successful commit marks `snapshot_complete`;
- selected IDs, rejected IDs, task count, dry-run behavior, and task payloads
  match the pre-change fixture;
- transaction failure leaves no policy, snapshot, decision, or task partials.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
go test ./internal/channelops -run 'Test(ResolvePolicyVersion|InsertCandidateSnapshots|RunTickPersistsPolicySnapshots)' -count=1
```

Expected: fail because store methods and transaction wiring are absent.

- [ ] **Step 3: Implement transactional persistence**

Resolve the policy with insert-on-conflict followed by exact content/hash
verification. Insert the tick audit as pending, persist all candidate
snapshots, insert linked decision rows, create the unchanged tasks, and mark
the tick complete in the same fenced transaction. Enforce snapshot cardinality
before commit.

- [ ] **Step 4: Run focused, full, and race tests**

```bash
go test ./internal/channelops -run 'Test(ResolvePolicyVersion|InsertCandidateSnapshots|RunTickPersistsPolicySnapshots)' -count=1
go test ./...
go test -race ./internal/channelops
```

Expected: pass with no change to baseline selection fixtures.

- [ ] **Step 5: Commit**

```bash
git add internal/channelops/store_policy_snapshots.go \
  internal/channelops/store_policy_snapshots_test.go \
  internal/channelops/store_tasks.go \
  internal/channelops/store_tick.go \
  internal/channelops/store_tick_test.go \
  internal/channelops/integration_test.go
git commit -m "feat(channelops): persist complete decision snapshots"
```

### Task 4: Expose Read-Only Policy And Decision Evidence

**Files:**
- Modify: `backend/app/schemas/channel_agent.py`
- Modify: `backend/app/api/channel_agent.py`
- Create: `backend/app/services/policy_evidence.py`
- Create: `backend/tests/channel_agent/test_policy_evidence_api.py`

**Interfaces:**
- Produces `GET /channel-agent/channels/{channel_id}/policy-status`.
- Produces `GET /channel-agent/channels/{channel_id}/policy-versions`.
- Produces `GET /channel-agent/channels/{channel_id}/policy-activations`.
- Produces `GET /channel-agent/ticks/{tick_audit_id}/decision-explanation`.
- Does not produce any policy mutation endpoint.

- [ ] **Step 1: Write failing service and API tests**

Cover empty status (`mode=off`), latest validated policy, immutable version
detail, ordered activation history, complete tick explanation, explicit legacy
unreplayable explanation, cross-channel isolation, invalid UUID, and not found.
Assert router source exposes no POST/PATCH/DELETE policy route.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/channel_agent/test_policy_evidence_api.py -q
```

Expected: fail with 404 or missing service.

- [ ] **Step 3: Implement read-only queries and schemas**

Use SQLAlchemy structured queries and existing UUID/error conventions. Return
stored facts without recomputing historical configuration or hashes. Keep
activation mode `off` when history is empty.

- [ ] **Step 4: Run focused and full backend checks**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/channel_agent/test_policy_evidence_api.py -q
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m ruff check . || true
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m mypy app || true
```

Expected: focused and full pytest pass; any pre-existing Ruff/mypy baseline is
recorded without adding owned-file findings.

- [ ] **Step 5: Commit**

```bash
git add backend/app/schemas/channel_agent.py \
  backend/app/api/channel_agent.py \
  backend/app/services/policy_evidence.py \
  backend/tests/channel_agent/test_policy_evidence_api.py
git commit -m "feat(channelops): expose policy decision evidence"
```

### Task 5: Verify Passive Rollout Contract

**Files:**
- Create: `scripts/channelops_policy_snapshot_preflight.py`
- Create: `backend/tests/services/test_policy_snapshot_preflight.py`
- Modify: `docs/channelops-go-live-runner.md`

**Interfaces:**
- Produces a read-only preflight report with migration head, tick snapshot
  coverage, legacy count, partial snapshot count, and activation modes.
- The preflight performs no insert, update, delete, queue, schedule, or network
  side effect.

- [ ] **Step 1: Write failing read-only preflight tests**

Require zero-write SQL/source behavior, restrictive evidence file permissions,
no credentials in output, correct complete/legacy/partial counts, and failure
when any new tick is partial or any activation is canary/active.

- [ ] **Step 2: Run tests and verify RED**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/services/test_policy_snapshot_preflight.py -q
```

Expected: fail because the preflight is absent.

- [ ] **Step 3: Implement the preflight and runbook**

Default to read-only stdout JSON. Permit an optional evidence path written
atomically with mode `0600`. Report hashes and UUIDs, but never database URLs,
tokens, prompts, raw source payloads, or media metadata.

- [ ] **Step 4: Run all required checks**

```bash
go test ./...
go test -race ./internal/channelops
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m ruff check . || true
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m mypy app || true
cd ../frontend
npm install
npm run build
npm run lint || true
cd ..
bash tests/test_vp_deploy_sync_extension.sh
git diff --check
```

Expected: tests and builds pass; lint/type baseline does not regress.

- [ ] **Step 5: Commit**

```bash
git add scripts/channelops_policy_snapshot_preflight.py \
  backend/tests/services/test_policy_snapshot_preflight.py \
  docs/channelops-go-live-runner.md
git commit -m "ops(channelops): audit passive policy snapshots"
```
