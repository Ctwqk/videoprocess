# ChannelOps Leader Epoch Fencing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give production ChannelOps one PostgreSQL-fenced Go leader epoch and make the legacy Python runner reject shared/production startup before opening runtime dependencies.

**Architecture:** A dedicated pgx connection owns a session advisory lock. Each acquisition increments a singleton epoch row; scheduler, queue claim, and queue execution transactions validate and share-lock that epoch. The runner exposes leadership in health, while the managed deployment uses one explicit identity, a `/readyz` container healthcheck, and stop-first replacement.

**Tech Stack:** Go 1.25, pgx v5, PostgreSQL advisory locks, Alembic/SQLAlchemy, Python 3.12/pytest, Bash deployment contract tests, Docker Swarm.

## Global Constraints

- Do not open the VideoProcess schedule, activate or resume a channel, create soak activation state, or call YouTube.
- Do not weaken private/unlisted defaults, `PUBLIC_PUBLISH_ENABLED=false`, or external-asset human-review gates.
- The leadership service key is exactly `channelops-go`.
- The managed holder identity is exactly `channelops-go@colima-127:1`.
- A standby process performs no scheduler, queue claim, or handler work.
- Epochs are positive and monotonically increasing.
- Scheduler, queue claim, and queue execution domain transactions must validate and share-lock the active epoch row.
- The legacy Python runner must reject `DEPLOY_MODE=shared` and `DEPLOY_MODE=production` before importing runtime runner modules.
- The managed Go singleton must update with `stop-first`.
- Existing APIs and queue rows remain compatible.

---

### Task 1: Expand The Database With A Durable Leader Epoch

**Files:**
- Create: `backend/alembic/versions/032_channelops_leader_epoch.py`
- Create: `backend/tests/migrations/test_channelops_leader_epoch_postgres.py`
- Modify: `backend/tests/migrations/test_final_review10_postgres.py`

**Interfaces:**
- Produces table `channelops_leader_epochs(service_name, epoch, holder_id, acquired_at, heartbeat_at, released_at)`.
- `service_name` is the primary key.
- `epoch > 0`; `holder_id` is non-empty after trimming; heartbeat is not before acquisition; release is null or not before acquisition.

- [ ] **Step 1: Write the failing migration tests**

Add a PostgreSQL migration test that upgrades through revision `031_guarded_schedule_job_authority`, asserts the leader table is absent, upgrades to head, and then verifies:

```python
columns = {
    "service_name",
    "epoch",
    "holder_id",
    "acquired_at",
    "heartbeat_at",
    "released_at",
}
assert primary_key_columns == {"service_name"}
assert insert_epoch_zero_fails
assert insert_blank_holder_fails
assert insert_heartbeat_before_acquired_fails
```

Extend the migration-head assertion:

```python
assert current_heads == {"032_channelops_leader_epoch"}
```

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
cd backend
.venv/bin/python -m pytest \
  tests/migrations/test_channelops_leader_epoch_postgres.py \
  tests/migrations/test_final_review10_postgres.py -q
```

Expected: failure because revision `032_channelops_leader_epoch` and its table do not exist.

- [ ] **Step 3: Add the expand-only migration**

Create revision metadata:

```python
revision = "032_channelops_leader_epoch"
down_revision = "031_guarded_schedule_job_authority"
```

Create the table with:

```python
op.create_table(
    "channelops_leader_epochs",
    sa.Column("service_name", sa.String(length=64), primary_key=True),
    sa.Column("epoch", sa.BigInteger(), nullable=False),
    sa.Column("holder_id", sa.String(length=255), nullable=False),
    sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("epoch > 0", name="ck_channelops_leader_epoch_positive"),
    sa.CheckConstraint("length(btrim(holder_id)) > 0", name="ck_channelops_leader_holder_nonempty"),
    sa.CheckConstraint(
        "heartbeat_at >= acquired_at",
        name="ck_channelops_leader_heartbeat_order",
    ),
    sa.CheckConstraint(
        "released_at IS NULL OR released_at >= acquired_at",
        name="ck_channelops_leader_release_order",
    ),
)
```

Downgrade drops only this table.

- [ ] **Step 4: Run migration tests and verify GREEN**

Run the Step 2 command.

Expected: all selected migration tests pass, with only environment-defined PostgreSQL skips permitted.

- [ ] **Step 5: Commit Task 1**

```bash
git add \
  backend/alembic/versions/032_channelops_leader_epoch.py \
  backend/tests/migrations/test_channelops_leader_epoch_postgres.py \
  backend/tests/migrations/test_final_review10_postgres.py
git commit -m "feat: add channelops leader epoch schema"
```

### Task 2: Implement PostgreSQL Leadership And Transaction Fences

**Files:**
- Create: `internal/channelops/leader.go`
- Create: `internal/channelops/leader_test.go`
- Modify: `internal/channelops/store.go`
- Modify: `internal/channelops/execution_fence.go`
- Modify: `internal/channelops/integration_test.go`

**Interfaces:**
- Produces:

```go
type LeaderAuthority struct {
    ServiceName string
    HolderID    string
    Epoch       int64
    AcquiredAt  time.Time
    HeartbeatAt time.Time
}

type LeaderLease struct { /* dedicated pgxpool connection and authority */ }

func (s *Store) TryAcquireLeader(
    ctx context.Context,
    holderID string,
    now time.Time,
) (*LeaderLease, bool, error)

func (l *LeaderLease) Heartbeat(ctx context.Context, now time.Time) error
func (l *LeaderLease) Release(ctx context.Context, now time.Time) error
func (s *Store) WithLeaderExecutionFence(
    ctx context.Context,
    dispatch func(*Store) error,
) error
```

- Existing `WithQueueExecutionFence` validates the configured authority before resolving queue/channel authority.
- A store clone shares leadership state and uses the caller transaction.

- [ ] **Step 1: Write failing leadership integration tests**

Using two stores against `NewChannelOpsFixture`, add tests that prove:

```go
first, acquired, err := store1.TryAcquireLeader(ctx, "runner-a", now)
// acquired, epoch == 1

second, acquired, err := store2.TryAcquireLeader(ctx, "runner-b", now)
// !acquired and second == nil

require.NoError(t, first.Release(ctx, now.Add(time.Second)))
second, acquired, err = store2.TryAcquireLeader(ctx, "runner-b", now.Add(2*time.Second))
// acquired, epoch == 2
```

Also add tests for:

- blank holder rejection;
- heartbeat updates only the matching holder/epoch;
- manually incrementing the epoch causes old heartbeat and
  `WithLeaderExecutionFence` to return `ErrLeaderAuthorityLost`;
- `WithQueueExecutionFence` rejects old authority before invoking dispatch.

- [ ] **Step 2: Run focused Go tests and verify RED**

Run:

```bash
go test ./internal/channelops -run 'TestLeader|TestExecutionFenceRejectsOldLeader' -count=1
```

Expected: compile failure because leadership types and methods do not exist.

- [ ] **Step 3: Implement leadership state and advisory-lock lifecycle**

Use a dedicated `*pgxpool.Conn` and fixed `int64` advisory key. Acquisition:

```sql
SELECT pg_try_advisory_lock($1)
```

Then, on the same connection and inside a transaction:

```sql
INSERT INTO channelops_leader_epochs (
  service_name, epoch, holder_id, acquired_at, heartbeat_at, released_at
) VALUES ('channelops-go', 1, $1, $2, $2, NULL)
ON CONFLICT (service_name) DO UPDATE
SET epoch = channelops_leader_epochs.epoch + 1,
    holder_id = EXCLUDED.holder_id,
    acquired_at = EXCLUDED.acquired_at,
    heartbeat_at = EXCLUDED.heartbeat_at,
    released_at = NULL
RETURNING epoch
```

Heartbeat must use:

```sql
UPDATE channelops_leader_epochs
SET heartbeat_at = $3
WHERE service_name = $1 AND holder_id = $2 AND epoch = $4
RETURNING acquired_at
```

Zero rows returns sentinel `ErrLeaderAuthorityLost`, clears published local
authority, unlocks if possible, and releases the dedicated connection.

Release updates `released_at` only for the matching epoch, calls
`pg_advisory_unlock`, clears local authority, and releases the connection
exactly once.

- [ ] **Step 4: Implement transaction fencing**

`WithLeaderExecutionFence` begins or reuses a transaction, then executes:

```sql
SELECT acquired_at, heartbeat_at
FROM channelops_leader_epochs
WHERE service_name = $1 AND holder_id = $2 AND epoch = $3
  AND released_at IS NULL
FOR SHARE
```

No configured authority or no matching row returns
`ErrLeaderAuthorityUnavailable` or `ErrLeaderAuthorityLost` before dispatch.

Call the same assertion at the start of `WithQueueExecutionFence` and
`withChannelExecutionFence` when the store has leadership fencing configured.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run the Step 2 command.

Expected: all selected tests pass or PostgreSQL-dependent tests skip under the existing fixture contract.

- [ ] **Step 6: Run the package tests**

```bash
go test ./internal/channelops -count=1
```

Expected: package passes.

- [ ] **Step 7: Commit Task 2**

```bash
git add \
  internal/channelops/leader.go \
  internal/channelops/leader_test.go \
  internal/channelops/store.go \
  internal/channelops/execution_fence.go \
  internal/channelops/integration_test.go
git commit -m "feat: fence channelops leadership in postgres"
```

### Task 3: Require Leadership In The Runner And Health Surface

**Files:**
- Modify: `internal/channelops/config.go`
- Modify: `internal/channelops/config_test.go`
- Modify: `internal/channelops/runner.go`
- Modify: `internal/channelops/runner_test.go`
- Modify: `internal/channelops/health.go`
- Modify: `internal/channelops/health_test.go`
- Modify: `cmd/channelops-runner/main.go`

**Interfaces:**
- Adds `Config.RunnerID string`, loaded from `CHANNELOPS_RUNNER_ID`.
- Produces a runner-owned leadership controller:

```go
type LeadershipController interface {
    EnsureActive(context.Context, time.Time) (*LeaderAuthority, error)
    Status() LeaderStatus
    Close(context.Context, time.Time) error
}
```

- `Runner.runOnce` calls `EnsureActive` before scheduler or readiness checks.
- `LeaderStatus.Role` is exactly `active`, `standby`, or `unavailable`.

- [ ] **Step 1: Write failing config, runner, and health tests**

Add tests for:

```go
cfg := LoadConfig()
assert.Equal(t, "channelops-go@colima-127:1", cfg.RunnerID)
```

with the env set explicitly, and validation failures for blank or unsupported
characters.

Runner tests use a fake leadership controller to prove:

- standby returns no error and never calls scheduler, readiness, or claim;
- active scheduler is wrapped in `WithLeaderExecutionFence`;
- active claim uses `locked_by` value
  `channelops-go@colima-127:1:epoch:<positive integer>`;
- authority loss returns to standby instead of processing another item;
- `Close` releases leadership before closing the store.

Health tests assert active JSON fields and 503 readiness for standby or
unavailable leadership.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
go test ./internal/channelops ./cmd/channelops-runner \
  -run 'Test.*(RunnerID|Leadership|LeaderRole|Standby|Epoch)' -count=1
```

Expected: failures because config, controller, runner gating, and health fields are absent.

- [ ] **Step 3: Add config and leadership controller**

Load and validate:

```go
RunnerID: env("CHANNELOPS_RUNNER_ID", ""),
```

`RunnerID` must match:

```text
^[A-Za-z0-9_.@:-]+$
```

and must be non-empty. `NewRunner` wires a PostgreSQL leadership controller
using the store and configured identity.

- [ ] **Step 4: Fence runner scheduling and claiming**

`runOnce`:

1. calls `EnsureActive`;
2. returns without side effects when authority is nil;
3. runs `Scheduler.RunOnce` inside `Store.WithLeaderExecutionFence`;
4. claims inside another `WithLeaderExecutionFence`;
5. uses `fmt.Sprintf("%s:epoch:%d", holder, epoch)` as `locked_by`;
6. keeps existing queue handler and compare-and-set completion behavior.

Do not hold the scheduler transaction while sleeping.

- [ ] **Step 5: Extend health and shutdown**

Add JSON fields:

```go
LeaderRole        string     `json:"leader_role"`
LeaderEpoch       *int64     `json:"leader_epoch,omitempty"`
LeaderHolderID    string     `json:"leader_holder_id,omitempty"`
LeaderHeartbeatAt *time.Time `json:"leader_heartbeat_at,omitempty"`
```

`ReadyCheck` reports a leadership error unless role is active. `HealthCheck`
keeps DB and scheduler checks and reports standby separately.

`Runner.Close` releases leadership with a bounded background context before
closing the pool.

- [ ] **Step 6: Run focused and full Go tests**

Run the Step 2 command, then:

```bash
go test ./... -count=1
```

Expected: all Go tests pass.

- [ ] **Step 7: Commit Task 3**

```bash
git add \
  internal/channelops/config.go \
  internal/channelops/config_test.go \
  internal/channelops/runner.go \
  internal/channelops/runner_test.go \
  internal/channelops/health.go \
  internal/channelops/health_test.go \
  cmd/channelops-runner/main.go
git commit -m "feat: require active channelops leader epoch"
```

### Task 4: Fail Closed In Python And Deploy The Managed Identity

**Files:**
- Create: `backend/tests/channel_agent/test_runner_admission.py`
- Modify: `backend/channel_agent_runner.py`
- Modify: `backend/Dockerfile.channelops-runner-go`
- Modify: `deploy/swarm/deploy-sync-extension.sh`
- Modify: `tests/test_vp_deploy_sync_extension.sh`
- Modify: `docker-compose.yml`
- Modify: `deploy/four-machine-topology.md`

**Interfaces:**
- Produces:

```python
PRODUCTION_DEPLOY_MODES = frozenset({"shared", "production"})

def assert_python_channelops_runner_admission(
    env: Mapping[str, str] | None = None,
) -> None
```

- Raises `RuntimeError` with a stable message before importing
  `app.channel_agent.runner`.
- Managed Go env includes
  `CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1`.

- [ ] **Step 1: Write failing Python admission tests**

Load `backend/channel_agent_runner.py` without importing the runtime runner and
assert:

```python
with pytest.raises(RuntimeError, match="Go ChannelOps runner is the production owner"):
    module.assert_python_channelops_runner_admission({"DEPLOY_MODE": "shared"})

with pytest.raises(RuntimeError):
    module.assert_python_channelops_runner_admission({"DEPLOY_MODE": "production"})

module.assert_python_channelops_runner_admission({"DEPLOY_MODE": "local"})
```

Use a subprocess or import sentinel to prove the rejected path never imports
`app.channel_agent.runner`.

- [ ] **Step 2: Write failing deploy contract assertions**

Require:

```bash
CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1
```

in the managed service update, `stop-first` for
`vp-channel-agent-runner-swarm`, and a Dockerfile healthcheck for
`http://127.0.0.1:8080/readyz`.

- [ ] **Step 3: Run focused tests and verify RED**

```bash
cd backend
.venv/bin/python -m pytest tests/channel_agent/test_runner_admission.py -q
cd ..
bash tests/test_vp_deploy_sync_extension.sh
```

Expected: Python API/behavior is absent and deploy assertions fail.

- [ ] **Step 4: Implement Python admission before runtime imports**

Keep only standard-library imports at module scope. Admission reads a supplied
mapping or `os.environ`, normalizes `DEPLOY_MODE`, and raises for shared or
production. `main()` calls admission before importing:

```python
from app.channel_agent.runner import ChannelAgentRunner
from app.config import settings
```

The Compose profile keeps `DEPLOY_MODE: ${DEPLOY_MODE:-shared}` so accidental
startup fails; developers must explicitly set local.

- [ ] **Step 5: Add managed identity, healthcheck, and stop-first rollout**

The deploy extension removes any prior `CHANNELOPS_RUNNER_ID` env entry before
adding the exact managed value. Change only the ChannelOps runner update order
to stop-first.

Add:

```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
  CMD wget -qO- http://127.0.0.1:8080/readyz >/dev/null || exit 1
```

Update the topology runbook with leader epoch health fields and takeover
behavior.

- [ ] **Step 6: Run focused tests and verify GREEN**

Run the Step 3 commands.

Expected: both pass.

- [ ] **Step 7: Commit Task 4**

```bash
git add \
  backend/channel_agent_runner.py \
  backend/tests/channel_agent/test_runner_admission.py \
  backend/Dockerfile.channelops-runner-go \
  deploy/swarm/deploy-sync-extension.sh \
  tests/test_vp_deploy_sync_extension.sh \
  docker-compose.yml \
  deploy/four-machine-topology.md
git commit -m "fix: enforce channelops production owner"
```

### Task 5: Full Verification, Review, Push, And Production Readiness

**Files:**
- Modify only files required by review findings.

**Interfaces:**
- Consumes all prior tasks.
- Produces a reviewed commit range ready for the existing exact-SHA CI-gated
  auto-deploy controller.

- [ ] **Step 1: Run complete local verification**

```bash
go test ./... -count=1
cd backend
.venv/bin/python -m pytest
.venv/bin/python -m ruff check app tests/channel_agent/test_runner_admission.py
.venv/bin/python -m mypy app || true
cd ..
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_channelops_soak_watch.sh
bash tests/test_vp_unlisted_canary_scripts.sh
git diff --check
```

Expected:

- Go passes;
- backend reports no failures;
- changed-file Ruff passes;
- mypy baseline is reported honestly;
- shell contracts pass;
- diff check passes.

- [ ] **Step 2: Obtain task and whole-change reviews**

Review specifically for:

- advisory lock lifecycle leaks;
- epoch monotonicity and stale-owner commit races;
- standby side effects;
- start/stop deployment deadlocks;
- healthcheck behavior;
- Python import ordering;
- migration upgrade/downgrade safety.

Fix all Critical and Important findings and re-run covering tests.

- [ ] **Step 3: Push only after verification**

```bash
git status --short --branch
git push origin main
```

Do not add the untracked
`vp_autonomous_production_feedback_loop_plan.md`.

- [ ] **Step 4: Verify CI-gated auto-deployment**

After the exact-SHA CI run succeeds and the 150 cron deploys it, verify:

- local `main`, `origin/main`, 127 source marker, and 150 deploy state match;
- migration head is `032_channelops_leader_epoch`;
- `vp-channel-agent-runner-swarm` is `1/1` on `colima-127`;
- `/readyz` reports `leader_role=active` and a positive epoch;
- the singleton DB row holder is
  `channelops-go@colima-127:1`;
- a second read-only test process cannot acquire the advisory lock;
- no VP service is placed on 126;
- schedule and channel state were not changed by deployment;
- soak state remains disabled.

- [ ] **Step 5: Refresh canary preflight only when schedule is CLOSED**

Run the existing `--preflight-only` command after the scheduled 07:00 PDT
close. It must make no application or external mutation.

Do not run another live canary without a fresh, attempt-specific user
authorization.
