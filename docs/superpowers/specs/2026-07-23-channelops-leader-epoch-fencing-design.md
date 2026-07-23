# ChannelOps Leader Epoch Fencing Design

## Context

The production deployment currently runs one Go ChannelOps Swarm replica, but
replica count is an operational convention rather than a distributed ownership
fence. A second Go runner can schedule and claim the same queue immediately,
and the legacy Python ChannelOps runner can still start in a production deploy
mode when invoked outside the managed Swarm service.

This violates the Phase 0 requirements in
`vp_autonomous_production_feedback_loop_plan.md`:

- exactly one production Go leader epoch;
- no production Python ChannelOps owner;
- stale owners cannot commit scheduler or queue work after takeover.

This update must not open the VideoProcess schedule, activate a channel, create
soak state, call YouTube, or weaken private/unlisted and human-review gates.

## Approaches Considered

### PostgreSQL advisory lock only

A session advisory lock gives strong single-owner behavior and automatic
release on connection loss. It does not provide a durable, monotonic epoch for
health, audit, or transaction-level stale-owner checks.

### Timestamp lease row only

A renewable row lease is observable and supports an epoch, but expiry depends
on clock and timeout choices. A paused owner can overlap a replacement unless
every write carries and validates a fencing token.

### Session lock plus durable epoch (selected)

A dedicated PostgreSQL connection holds one advisory lock for the active
runner. Lock acquisition increments a singleton durable epoch. Scheduler,
queue claim, and queue execution transactions validate the holder and epoch
while locking the epoch row. This combines automatic lock release with a
monotonic fencing token and durable ownership evidence.

## Data Model

Migration `032_channelops_leader_epoch` adds
`channelops_leader_epochs`:

| Column | Meaning |
| --- | --- |
| `service_name` | Singleton key, fixed to `channelops-go` |
| `epoch` | Positive, monotonically increasing fencing token |
| `holder_id` | Non-empty runner identity |
| `acquired_at` | Time the current epoch was acquired |
| `heartbeat_at` | Last successful active-owner heartbeat |
| `released_at` | Graceful release time, otherwise null |

The migration is expand-only. Existing queue and task rows are unchanged.

## Leadership Lifecycle

1. The runner obtains a dedicated connection from the existing pgx pool.
2. It calls `pg_try_advisory_lock` with a fixed ChannelOps-specific key.
3. A failed attempt leaves the process in `standby` and performs no scheduler,
   claim, or handler work.
4. A successful attempt increments the singleton epoch row in a transaction,
   records the holder and timestamps, and publishes the authority to the
   runner store.
5. Before every runner cycle, the dedicated connection updates the matching
   holder/epoch heartbeat. Connection failure or a zero-row update revokes
   local authority and returns the runner to standby.
6. Graceful shutdown marks the matching epoch released, unlocks the advisory
   lock, clears local authority, and releases the connection.

The process remains alive while in standby and retries acquisition. It never
schedules or claims work without an active authority.

## Transaction Fencing

The active authority is represented by `(holder_id, epoch)`.

- Scheduler work runs inside one store transaction that validates the matching
  epoch row with `FOR SHARE` before reading channels or enqueuing work.
- Queue claim runs inside the same type of fenced transaction. `locked_by`
  includes the holder and epoch, so queue completion retains the existing
  compare-and-set lease protection.
- Every queue handler's existing execution-fence transaction validates and
  share-locks the matching epoch row before channel or domain mutation.
- A replacement leader must update the epoch row. That update waits for any
  in-flight transaction holding the prior epoch row. Once the new epoch is
  committed, a stale owner cannot pass the epoch check.
- Queue completion and failure updates remain guarded by the claimed
  `locked_by` and `locked_at` values. They may finalize already committed work,
  but cannot claim or perform new domain work after authority loss.

No external HTTP call is added to leadership acquisition or fencing.

## Health And Deployment

`/healthz` and `/readyz` include:

- `leader_role`: `active`, `standby`, or `unavailable`;
- `leader_epoch` when active;
- `leader_holder_id` when active;
- `leader_heartbeat_at` when known.

Readiness is unhealthy unless the process owns an active epoch. Health also
keeps the existing database and scheduler-staleness checks.

The managed runner receives an explicit
`CHANNELOPS_RUNNER_ID=channelops-go@colima-127:1` identity. The runner image
has a container healthcheck against `/readyz`. Deployment uses `stop-first`
for this singleton service so a replacement can acquire the lock and become
healthy without a start-first ownership deadlock.

## Python Runner Production Rejection

`backend/channel_agent_runner.py` validates admission before constructing
`ChannelAgentRunner`:

- `DEPLOY_MODE=shared` and `DEPLOY_MODE=production` are rejected;
- local/test modes remain available for explicit development profiles;
- the error identifies the Go runner as the production owner;
- no database, Redis, scheduler, or queue object is opened before rejection.

The legacy Python profile keeps its fail-closed `DEPLOY_MODE=shared` default.
Intentional local use must explicitly set `DEPLOY_MODE=local`.

## Tests

Test-first coverage must prove:

1. one PostgreSQL store acquires epoch 1 and a second remains standby;
2. graceful release permits takeover at epoch 2;
3. connection loss or mismatched row revokes authority;
4. scheduler and queue claim are no-ops while standby;
5. old-epoch scheduler, claim, and queue execution transactions are rejected
   after takeover;
6. health/readiness report the correct role and epoch;
7. queue `locked_by` includes the active epoch;
8. Python shared/production startup rejects before runner construction;
9. local Python runner mode remains available;
10. deploy tests require explicit identity and stop-first update order.

PostgreSQL integration tests may skip only when the repository's existing test
database fixture is unavailable. Unit, Python, deploy-contract, and full Go
tests remain mandatory.

## Rollout And Rollback

The migration lands before the new image starts. Deployment then replaces the
singleton Go runner stop-first and verifies active readiness. Existing queue
rows remain compatible.

Rollback restores the prior image. The epoch table is retained and ignored by
the old binary; no downgrade is required during an application rollback.

## Non-Goals

- worker registration and signed admission;
- general stale queue recovery;
- uncertain upload reconciliation;
- learned ranking or policy activation;
- channel or soak activation;
- GPU runtime maintenance;
- any YouTube upload, promotion, or public publication.
