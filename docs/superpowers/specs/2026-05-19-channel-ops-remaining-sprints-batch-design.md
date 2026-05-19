# ChannelOps Remaining Sprints Batch Design

Date: 2026-05-19
Status: Approved for implementation
Parent spec: `docs/superpowers/specs/2026-05-19-channel-ops-live-cutover-design.md`
Parent plan: `docs/superpowers/plans/2026-05-19-channel-ops-live-cutover.md`

## 1. Decision

Sprint 0 and Sprint 1 have been completed and live-smoked. The remaining
ChannelOps work will be implemented as one batch while preserving the sprint
order and verification gates:

1. Sprint 2: approval bridge, PDS fail-closed policy, and publication
   reconciliation.
2. Sprint 3: material usage ledger writes, repetition guards, and funnel
   rejection slices.
3. Sprint 4: internal scheduler, YouTube trend ingestion, candidate scoring,
   and retention cleanup.

The batch may modify the external runtime repository at
`/home/taiwei/Constructure-repos/constructure-platform-upload/YouTubeManager`
when a ChannelOps feature needs a YouTube Data API surface. OAuth and Google
client ownership stay inside YouTubeManager; ChannelOps continues to call it
over HTTP.

## 2. Why This Order

The implementation must close safety boundaries before enabling autonomous
work creation. Sprint 2 comes first because self-driving publication is not
acceptable while PDS can fail open, agent approvals are not represented in
AutoFlow, and platform status cannot be reconciled after promotion.

Sprint 3 follows because the scheduler should not repeatedly choose the same
material segment or sibling-account material once autonomous ticks begin.

Sprint 4 comes last because scheduler, trend ingestion, and retention are
operational accelerators. They should run on top of stable publish gates and
material guards, not compensate for missing ones.

## 3. Sprint 2 Design

### Approval Bridge

`ProductionTask` gains `approval_mode` and
`agent_approval_evidence_json`. Lane-seed tasks default to
`approval_mode="agent"`. Manual-seed tasks default to
`approval_mode="human"` so operator-provided prompts still require explicit
review when AutoFlow marks rights as `review_required`.

`AutoFlowPlan` gains `agent_approved_by`. AutoFlow execution treats
`agent_approved_by` as equivalent to `review_approved_at` only for the
review-required execution gate. It does not satisfy public publication
approval. `public_approved_at` remains the only public visibility gate.

`ChannelAgentService.handle_plan_task` calls PDS with
`action_type="plan_approval"`. It sets `agent_approved_by="channel_agent"`
only when the task approval mode is `agent` and PDS returns `allow`.
Otherwise the task is held with an explicit review/compliance guard reason.

### PDS Fail Policy

`PDSClient` applies per-action fail policy:

- `candidate_accept`: `allow`
- `plan_approval`: `flag`
- `publish`: `block`
- `promote_publication`: `block`

Request errors, 5xx responses, and invalid response payloads must preserve a
machine-readable warning such as `pds_unavailable` or `pds_parse_failed` in
decision metadata.

### PDS Health

Runner-side PDS health tracking records the most recent non-failure PDS
decision. During business hours, if PDS has had no healthy decision for more
than five minutes, the runner enqueues one hourly `send_alert` queue item with
resource `service:pds`.

### Publication Reconciliation

After successful `promote_publication`, ChannelOps enqueues
`reconcile_publication` for `scheduled_publish_at + 30 minutes`. The handler
calls `YouTubeClient.fetch_status`, updates publication privacy/status fields,
records explicit failure reasons, and writes a takedown event for severe
platform states such as rejection, removal, or claim.

## 4. Sprint 3 Design

### Material Usage Helper

Create `backend/app/channel_agent/material_usage.py` as the only service-level
module that extracts selected material references, computes deterministic
segment signatures, and queries recent usage windows. `service.py` should call
small helper functions instead of traversing AutoFlow JSON inline.

The helper extracts references from persisted AutoFlow plan/run artifacts and
upload metadata. When AutoFlow does not provide a segment signature, compute
one from `material_id:start_ms:end_ms`.

### Ledger Writes

`handle_publish_task` writes `MaterialUsageLedger` rows immediately after a
`PublicationRecord` exists. This matches the current service ownership:
publication creation happens in publish handling, so ledger rows should be
created there instead of in observe-job handling.

### Repetition Guards

Candidate selection rejects lane-generated work when:

- the same segment was used in the same lane within seven days;
- the same material was used by the same account within fourteen days;
- the same material was used by a sibling account within thirty days.

Manual seeds remain overrideable. When a manual seed would have been rejected
by the guard, the service allows it but annotates `rationale_json` with the
guard outcome.

### Funnel Reporting

The funnel API returns `repetition_rejected` and
`cross_account_rejected`. The status page renders these as normal funnel
slices, with no hard-coded one-off UI path beyond labels or ordering.

## 5. Sprint 4 Design

### Internal Scheduler

`ChannelProfile` gains `tick_interval_minutes` with a floor of fifteen
minutes. The runner starts a scheduler loop alongside queue consumption. The
scheduler enqueues at most one `agent_tick` per enabled, non-halted channel
per UTC bucket using an idempotency key derived from channel id and bucket.

Manual `POST /enqueue-tick` remains available and is not coupled to scheduler
state.

### YouTube Trend Ingester

ChannelOps adds a YouTube trend ingester that calls YouTubeManager over HTTP.
If YouTubeManager does not expose search, add a search endpoint there rather
than adding Google OAuth or google-api clients to ChannelOps.

Accepted trend results become `ManualSeed` rows with
`source_policy="trend_youtube"`, TTL/status metadata, lane linkage, and a
prompt derived from the source title/description. Stale trend seeds are
expired by the ingester or retention path.

### Candidate Scoring

Candidate scoring is observe-only in this batch. It writes
`ProductionTask.score_breakdown_json` and rationale fields for selected tasks
but does not replace current greedy selection.

The score includes lane weight, material fit, freshness, account fit, timing,
novelty, repetition risk, compliance risk, and total score. Later learning or
bandit selection is out of scope.

### Retention

Add a daily cleanup path for old queue, audit, and feedback records. Retention
thresholds live in settings so operators can tune them without changing code.
Recent records must be preserved.

## 6. Verification Gates

Each sprint segment must pass its focused tests before the next segment is
considered done:

- Sprint 2: review-required agent plans execute after PDS allow; forced PDS
  outage blocks publish; reconcile records either confirmed status or an
  explicit failure reason.
- Sprint 3: repeated ticks against a tiny material set do not select the same
  segment; manual override is allowed and annotated; funnel includes rejection
  slices.
- Sprint 4: scheduler enqueues idempotent ticks; trend search materializes
  seeds through YouTubeManager; every newly selected task has a non-empty
  score breakdown; retention deletes old rows and preserves recent rows.

Full verification remains:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
cd ../frontend
npm install
npm run build
npm run lint || true
```

Live deployment verification must also rebuild and smoke the affected Docker
services: API, channel-agent-runner, frontend when UI changes, and
YouTubeManager when its endpoints change.

## 7. Non-Goals

This batch does not add public auto-publication, multi-platform publication,
bandit learning, competitor/RSS ingesters, or a second OAuth credential path.
It also does not remove the manual review path; it adds an agent approval path
only for explicitly agent-owned work.
