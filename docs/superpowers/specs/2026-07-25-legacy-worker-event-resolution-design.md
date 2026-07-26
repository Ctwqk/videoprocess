# Legacy Worker Event Resolution Design

## Context

The fifth live unlisted canary was safely cancelled without an upload or
publication. Its unmanaged legacy vision worker nevertheless left three
`node_failed` entries pending in `vp:events` / `orchestrator`. The entries have
valid job and node identifiers but omit both execution-claim fields required by
the current worker protocol: `worker_id` and `started_at`.

Production inspection established that all three entries belong to job
`871ec6e9-1ac0-458c-8870-15e7684cf49f`. The job and nodes are `CANCELLED`, the
production task is `held`, the channel is halted, and the global schedule is
`CLOSED` without a guarded job. The current listener intentionally leaves such
unverifiable events pending, so the live canary preflight correctly remains
closed.

## Decision

Add an explicit operator maintenance command that durably archives and
acknowledges an exact, pre-reviewed set of legacy terminal events. Normal
orchestrator, canary, and soak behavior remains fail-closed: no general pending
exception or allowlist is added.

The command is dry-run by default. Applying a resolution requires:

- `--apply`
- a nonempty operator identity
- one `message-id=payload-sha256` expectation for every pending event
- the schedule to be `CLOSED` with no guarded job
- no queued or running node executions
- an exact match between the expected set and the complete current PEL
- each Redis entry to still exist and match its expected SHA-256
- `event=node_failed`
- valid job and node UUIDs
- both `worker_id` and `started_at` to be absent
- a matching `CANCELLED` job and `CANCELLED` node
- a matching `held` production task whose channel is halted

Any missing, additional, changed, malformed, nonterminal, or unverifiable fact
fails the entire operation before acknowledgement.

## Durable Audit

Migration `033` adds `legacy_worker_event_resolutions`. Each row records:

- Redis stream, consumer group, and message ID
- canonical payload SHA-256 and the complete payload JSON
- event type, job ID, and node execution ID
- resolution reason
- operator identity
- database state observed at resolution time
- record and acknowledgement timestamps

`(redis_stream, consumer_group, message_id)` is unique. Existing rows are
idempotently reusable only when every immutable event field and hash matches.
A mismatch fails closed.

## Apply And Recovery

The apply path uses this order:

1. Acquire a PostgreSQL advisory lock dedicated to legacy event resolution.
2. Lock and validate the global schedule, job, node, task, and channel facts.
3. Re-read the complete Redis PEL and exact stream payloads.
4. Insert the durable audit rows and commit.
5. Re-read each exact Redis payload and hash.
6. `XACK` only the expected message IDs.
7. Record acknowledgement time in Postgres.
8. Re-read the PEL and require it to be empty.

The database record deliberately precedes the external Redis mutation. If the
process stops after step 4, a rerun observes the exact existing records and
continues. If it stops after step 6, a rerun may mark acknowledgement as
observed only when the stream entry still matches the archived hash and the
complete PEL is empty. Stream entries are not deleted.

## Boundaries

- The command does not replay event business logic.
- It does not delete Redis stream entries or consumer groups.
- It does not alter job, node, task, channel, schedule, upload, publication, or
  YouTube state.
- It cannot resolve current-protocol events containing an execution claim.
- It cannot resolve `node_completed` events.
- It cannot run while production work is active.
- Host 126 is not involved.
- No live canary authorization is consumed.

## Evidence

The command writes sanitized JSON evidence with mode `0600`. Dry-run evidence
contains the verified candidates and no database mutation. Apply evidence also
contains inserted/reused audit row IDs, the exact `XACK` count, final pending
count, and completion status. Connection URLs and credentials are never
serialized.

## Tests

Tests cover:

- exact expectation parsing and canonical payload hashing
- dry-run verification without database writes or `XACK`
- rejection of expectation set drift, hash drift, malformed payloads,
  execution claims, non-failure events, active schedule/work, and nonterminal
  database state
- durable audit insertion before acknowledgement
- idempotent recovery before and after `XACK`
- fail-closed handling of partial acknowledgement
- evidence sanitization and `0600` permissions
- the migration schema and database constraints

The full backend suite, Ruff, Mypy, deployment contract tests, and existing
canary tests remain required before deployment.

## Success Criteria

- The three reviewed fifth-canary events can be durably archived and ACKed
  without replaying them.
- A read-only canary preflight then observes raw pending count zero.
- Any new or changed pending event still blocks preflight and soak monitoring.
- No upload, publication, public visibility, or live canary occurs as part of
  this maintenance operation.
