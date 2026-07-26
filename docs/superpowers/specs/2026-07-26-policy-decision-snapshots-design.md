# Policy Decision Snapshots Design

Status: pre-approved for implementation on 2026-07-26.

## Context

The production reliability work in T01-T09 is now present and deployed, while
the fresh-smoke gate in T10 still requires successful, separately approved
real canaries. T11 discovery ingestion is implemented but disabled by default.
The next safe non-live increment is T12: immutable policy versions and
candidate feature snapshots.

Current decision audits retain scores, guard results, PDS output, and learning
context, but they do not identify an immutable policy, feature schema, complete
candidate set, or feature as-of time. Mutable channel configuration therefore
prevents deterministic historical explanation.

This increment must be passive. It may add facts to a tick transaction, but it
must not change candidate construction, guards, ordering, task count,
scheduling, uploads, publication privacy, or policy activation.

## Goal

Make every new ChannelOps tick explainable against an immutable baseline policy
version and a complete, deterministic candidate feature snapshot, while
marking historical decisions as explicitly unreplayable.

## Approaches Considered

### Passive normalized snapshots (selected)

Add normalized policy, activation-history, and feature-snapshot tables. Resolve
or create the immutable baseline policy inside the existing tick transaction,
snapshot every accepted and rejected candidate, and link the existing tick and
decision audit rows.

This creates the durable facts needed by later shadow scoring and replay while
preserving current live behavior.

### Expand existing audit JSON only

Appending more JSON to `decision_audit_entries` would be smaller, but it would
not enforce immutable policy identity, candidate uniqueness, activation
history, or durable foreign-key relationships. It would also make later
replay and retention controls harder to verify.

### Implement policy activation and scoring now

Building shadow/canary/active behavior in the same increment would close more
roadmap items, but it would cross T13-T15 and weaken the T10 gate. Activation
and learned influence remain separate future increments.

## Data Model

### `decision_policy_versions`

Each row is immutable after creation.

- `id`: UUID primary key.
- `policy_key`: stable key, initially `channelops-baseline`.
- `version`: deterministic version string.
- `status`: `draft`, `validated`, or `retired`.
- `feature_schema_version`: initially `channelops-candidate-v1`.
- `reward_version`: initially the deployed reward identifier.
- `formula_json`: baseline scoring formula and constants.
- `hard_guard_config_json`: normalized safety and cadence settings.
- `portfolio_config_json`: normalized lane/content-mix settings.
- `exploration_config_json`: disabled configuration for this increment.
- `code_commit_sha`: exact 40-character deployed commit SHA. Local test and
  development builders may use `development`, but a live tick must fail closed
  if its binary was not built with an exact SHA.
- `template_registry_version` and `prompt_bundle_version`: explicit immutable
  identifiers, initially `legacy-unversioned`.
- `config_hash`: SHA-256 of canonical semantic policy content.
- `created_by` and `change_reason`: audit text.
- `created_at`: immutable creation time.

`(policy_key, version)` is unique. Reusing a version with different semantic
content fails closed. PostgreSQL rejects updates and deletes after insertion;
retirement is represented by a newer policy/activation fact rather than
rewriting the old row.

### `policy_activation_history`

The table records future activation and rollback facts without enabling them in
this increment.

- scope: channel and optional account;
- policy version;
- mode: `off`, `shadow`, `canary`, or `active`;
- rollout percentage and deterministic salt;
- effective interval;
- previous activation;
- request ID, actor, reason, rollback reason, and feature-flag snapshot.

Rows are append-only and PostgreSQL rejects updates and deletes. No mutation API
is introduced now. Read APIs may return an empty history and effective mode
`off`.

### `candidate_feature_snapshots`

One immutable row exists for every accepted or rejected candidate in a new
tick.

- tick audit, candidate ID/source, lane, format, account, and policy version;
- feature schema version and feature as-of timestamp;
- raw features, normalized features, and missing-feature mask;
- recent cadence/content-mix snapshot;
- material supply and production reliability summaries;
- learning references;
- source record references;
- cost and risk estimates;
- candidate-set hash and per-feature hash.

`(tick_audit_id, candidate_id, feature_schema_version)` is unique.
PostgreSQL rejects updates and deletes after insertion.

### Existing audit extensions

`agent_tick_audits` gains policy version, candidate-set hash, feature as-of,
and replay status.

`decision_audit_entries` gains policy and feature-snapshot foreign keys,
candidate-set and decision hashes, explicit decision, baseline/final score
fields, rank placeholders, and future shadow/experiment fields. Values not yet
computed remain nullable or neutral; they must not be fabricated.

Existing rows are backfilled with `replay_status=legacy_unreplayable`. New rows
are `snapshot_complete` only after all candidate snapshots and decision links
are persisted in the same transaction.

The existing audit cleanup keeps its current retention behavior for legacy
unreplayable ticks, but it must not delete `snapshot_complete` ticks or their
candidate facts. Policy, activation, and replayable decision evidence is
retained permanently in this phase.

## Deterministic Encoding

Canonical JSON uses stable key ordering, ordinary JSON number formatting, and
no insignificant whitespace. Candidate-set hashes are computed from candidates
sorted by candidate ID and include stable source/lane/format/account/source
record identities. They exclude timestamps, PDS responses obtained after
feature capture, and later metrics.

The feature as-of time is captured once per tick and reused for every candidate.
The snapshot builder consumes only the already loaded tick inputs and candidate
state. It performs no network or database reads.

The ChannelOps image receives the exact commit as a Docker build argument and
embeds it in the Go binary through `-ldflags -X`. The normal deploy controller
already validates a full commit against exact-SHA CI before building; it passes
that same value to the ChannelOps image build. Tests and local builds retain an
explicit `development` sentinel.

## Tick Transaction

The existing fenced tick transaction remains the sole writer:

1. load tick inputs and build candidates with current behavior;
2. capture one feature as-of timestamp;
3. resolve the immutable baseline policy version;
4. calculate the complete candidate-set hash;
5. insert/update the tick audit as `snapshot_pending`;
6. insert every immutable candidate snapshot;
7. insert decision audits linked to those snapshots;
8. create the same tasks as before;
9. mark the tick audit `snapshot_complete` before commit.

Any policy-version conflict, snapshot cardinality mismatch, duplicate candidate,
or missing link rolls back the whole tick. It must never fall back to an
unversioned live decision.

## Read APIs

Add read-only endpoints under the existing ChannelAgent API:

- channel policy status: latest immutable policy, effective mode, and current
  activation if one exists;
- policy version list and detail;
- activation history list;
- tick decision explanation with policy identity, replay status,
  candidate-set hash, and candidate snapshot links.

This increment has no create/update/activate/rollback endpoint. Existing API
authentication and deployment exposure rules remain unchanged.

## Safety

- Baseline candidate acceptance and task creation must be byte-for-byte
  behaviorally equivalent before and after the change.
- `public` remains blocked, and external assets still require human review.
- No scheduler, queue, canary, upload, publication, or soak activation is
  performed by migration or deployment.
- Missing code/template/prompt identity is represented explicitly, never
  inferred from current mutable state.
- Live ticks reject the local `development` commit sentinel.
- Historical audit rows are not rewritten as replayable.

## Testing

- migration contract, fresh upgrade, production-shaped forward upgrade, and
  legacy backfill;
- policy immutability and same-version conflict;
- canonical policy, candidate-set, feature, and decision hashes;
- accepted and rejected candidate snapshot cardinality;
- one as-of timestamp for the whole tick;
- transaction rollback on missing/duplicate snapshot;
- no change to selected IDs, task count, or dry-run behavior;
- read API status, history, detail, explanation, and not-found cases;
- Go race tests, full Go tests, backend pytest, migration upgrade, Ruff, mypy,
  frontend build, and deployment contract tests.

## Rollout

The schema and passive writer deploy through the normal exact-SHA CI path.
After deployment, a read-only production audit verifies migration head and
that new ticks are either absent or fully snapshotted. No channel is enabled,
resumed, or activated as part of rollout.

Rollback prefers a forward fix after any new snapshot row exists. Application
rollback is safe because old code ignores the additive tables and columns.
