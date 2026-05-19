# ChannelOps Live Cutover Design

Date: 2026-05-19
Status: Approved roadmap, ready for sprint-level execution
Related specs:

- `docs/superpowers/specs/2026-05-18-channel-ops-agent-design-v2.md`
- `docs/superpowers/specs/2026-05-18-channel-ops-agent-implementation-design.md`
- `docs/superpowers/specs/2026-05-18-channel-ops-alpha-hardening-design.md`

## 1. Problem Statement

The ChannelOps alpha now has all of the control-plane scaffolding in place:
configuration tables, dry-run tick, queue runner, basic guards, the
ProductionTask state machine, PublicationRecord, the FeedbackSnapshot schema,
and an operator-visible status panel. End-to-end tests pass under fake
clients. None of that proves the system can actually run live.

Several specific gaps block real-account operation:

- `ChannelAgentRunner` does not inject a real YouTube client. The service
  defaults to `FakeYouTubeClient`, so `promote_publication`,
  `quota_remaining_fraction`, and `refresh_token` are no-ops in production.
- `handle_collect_metrics` reads `payload["metrics"]` and re-queues when the
  field is empty. It never calls a YouTube Data API. `YouTubeClient` Protocol
  does not even declare a `fetch_metrics` method.
- AutoFlow `_assert_execute_allowed` still requires `review_approved_at` when
  rights status is `review_required` and any upload mode is requested.
  Agent-driven plans hit this and stall.
- `PDSClient` and `NoopPDSClient` are uniformly fail-open. A PDS outage looks
  identical to "allow" in publish decisions.
- `MaterialUsageLedger` exists in the schema and migrations but no service
  code writes to it, so duplicate-material control is structurally
  unenforceable today.
- `AgentTickAudit` has no `(channel_profile_id, tick_id)` UniqueConstraint,
  so a buggy double-enqueue can produce phantom rows.

GPT-Pro's external review and this team's internal review reached the same
conclusion: the control plane is built, the data plane is not. Trend
ingestion, automatic metrics collection, real publication promotion, and
material-reuse tracking all still need to land before the system can run
unattended.

## 2. Scope And Non-Goals

In scope for this design:

- Sprint 0: a manual smoke that proves end-to-end private upload works once.
- Sprint 1: real YouTube client wiring + live metrics collection + audit
  uniqueness.
- Sprint 2: bridge AutoFlow approval with agent-side compliance so
  review_required material no longer stalls plans.
- Sprint 3: write the MaterialUsageLedger and enforce repetition guards.
- Sprint 4: self-driving tick scheduler, minimum trend ingester, explainable
  candidate scoring, retention cleanup.

Out of scope. These are intentionally deferred until the live loop is stable:

- Thompson sampling / contextual bandit. Re-evaluate after ~500 publications
  of real `FeedbackSnapshot` data.
- Competitor / RSS ingesters beyond a single YouTube search source.
- Multi-platform publication (X, 小红书, Bilibili). YouTube must reach 50
  consecutive zero-incident runs first.
- Automatic public publication. Unlisted is the alpha ceiling; public
  promotion remains a manual API call until retention metrics justify it.
- New ML/learning surfaces. Stage A rule-based scoring is enough until real
  metrics arrive.

## 3. Reuse Decisions That Differ From The Outside Review

Two notes that diverge from the external review's framing:

- The external review proposed building a new `YouTubeManagerClient` against
  google-api-python-client. The repo already runs an independent
  `youtube-manager` service in `docker-compose.yml` with `YOUTUBE_MANAGER_URL`
  env. `youtube_upload.py` already uses its OAuth `token.json + credential_ref`
  path. The right work is to call the existing service over HTTP, not to add
  a second OAuth code path inside the channel agent worker. This keeps OAuth
  ownership in one process.
- The external review proposed four trend ingestion sources at once. The
  cheapest-to-validate single source is YouTube Data API
  `search.list?order=viewCount&publishedAfter=24h&regionCode=...` at roughly
  100 quota units per channel per day. Ship that source first, run it for
  two weeks, then decide whether RSS, competitor channels, or news feeds
  earn their cost.

## 4. Sprint 0 — Manual End-To-End Smoke

Priority: highest. This must happen before any new code lands.

The system currently has zero recorded evidence of a real private upload
completing under non-fake clients. Test green under `FakeYouTubeClient` does
not imply production parity. Expect to find 3–5 bugs of the form "OAuth
path mismatch", "payload field name divergence", "idempotency key collision",
or "MinIO bucket not present" the first time a real upload runs.

Procedure:

1. Configure one production-shaped channel via the existing API
   (`POST /channels`, `POST /lanes`, `POST /accounts`, `POST /lanes/{id}/formats`).
2. Verify the linked YouTubeManager has a valid OAuth token for that account.
3. Submit one `ManualSeed` via `POST /channels/{id}/manual-seeds` whose
   source platforms and material library make AutoFlow plan a real
   `youtube_upload` node.
4. Flip the channel to `dry_run=false` via `PATCH /channels/{id}/dry-run`.
5. Enqueue a tick via `POST /enqueue-tick`.
6. Observe the chain in the dashboard: tick → plan → execute → observe →
   publish → promote → metrics. Each transition must produce a real
   `transition_history_json` entry, and the final publication must appear in
   the YouTube account at `unlisted` or `private`.
7. File bugs against whichever stages drifted. Fix before starting Sprint 1.

Exit criteria: one `PublicationRecord` row with non-empty
`platform_content_id`, a permalink that returns 200 from YouTube, and a
`FeedbackSnapshot` (even synthetic) tied to it. No code change required if
the existing system actually works.

## 5. Sprint 1 — Live Loop (P0, 1 week)

Goal: replace every fake client in the runtime path with a real one so the
six health KPIs on the status panel reflect reality.

### 5.1 YouTube Client Surface

Extend the `YouTubeClient` Protocol in `backend/app/channel_agent/clients.py`:

```python
class YouTubeClient(Protocol):
    async def quota_remaining_fraction(self, account) -> float: ...
    async def schedule_publish(
        self,
        *,
        video_id: str,
        scheduled_at: datetime,
        privacy: str,
    ) -> dict[str, Any]: ...
    async def refresh_token(self, account) -> bool: ...
    async def fetch_metrics(self, account, video_id: str) -> dict[str, Any]: ...
    async def fetch_status(self, account, video_id: str) -> dict[str, Any]: ...
```

`fetch_metrics` returns the same shape `_dict_value(payload["metrics"])`
already expects (`views`, `likes`, `comments`, `shares`,
`avg_view_duration_sec`, optional `retention_curve_json`, `ctr`,
`impressions`). `fetch_status` returns `{privacy, processing_state,
permalink, error_message?}` so the reconciler can detect rejections.

### 5.2 YouTubeManagerClient Implementation

New `YouTubeManagerClient` in `clients.py` that calls the existing
`youtube-manager` service over HTTP. Endpoints, all under
`{YOUTUBE_MANAGER_URL}`:

- `GET /accounts/{account_id}/quota` → `{remaining_fraction: float}`
- `POST /accounts/{account_id}/videos/{video_id}/schedule` body
  `{scheduled_at, privacy}` → `{success, platform_status}`
- `POST /accounts/{account_id}/token/refresh` → `{ok: bool, status: str}`
- `GET /accounts/{account_id}/videos/{video_id}/metrics` → metrics payload
- `GET /accounts/{account_id}/videos/{video_id}/status` → status payload

If those endpoints do not yet exist on youtube-manager, add them in the same
sprint. Keep OAuth credentials owned by youtube-manager. Do not have
channel-agent-runner read `token.json` directly.

### 5.3 Runner Injection

`ChannelAgentRunner.__init__` must explicitly construct and pass both
clients:

```python
self.service = ChannelAgentService(
    queue=self.queue,
    autoflow_client=LocalAutoFlowClient(),
    youtube_client=YouTubeManagerClient(base_url=settings.youtube_manager_url),
    minimax_client=MiniMaxImageClient(),
    pds_client=_build_pds_client(),
)
```

No `FakeYouTubeClient` fallback in production. The runner must fail to start
if `YOUTUBE_MANAGER_URL` is unset, with an actionable error message.

### 5.4 Real Metrics Collection

`handle_collect_metrics` should call the client when payload metrics are
absent:

```python
metrics = _dict_value(payload.get("metrics"))
if not _has_real_metrics(metrics):
    account = await db.get(PublishingAccount, publication.account_id)
    try:
        metrics = await self.youtube_client.fetch_metrics(
            account=account,
            video_id=publication.platform_content_id,
        )
    except Exception as exc:
        # Fall through to the existing requeue path; record the cause.
        publication.warnings_json = [*publication.warnings_json, f"metrics_fetch_failed: {exc}"]
        metrics = {}
```

The existing `_MAX_METRICS_POLLS` ceiling and `metrics_unavailable` hold
remain. The change is to actually try the API on every poll instead of only
when payload had the data.

### 5.5 AgentTickAudit Uniqueness

Add `UniqueConstraint("channel_profile_id", "tick_id",
name="uq_agent_tick_audit_channel_tick")` to `AgentTickAudit.__table_args__`,
plus a matching Alembic migration. Tick generation already uses an hour
bucket; the constraint enforces that no double-enqueue produces shadow rows.

### 5.6 Frontend End-To-End Indicator

Add a `last_successful_measured_at` column to the channel summary on the
status panel. Pulls the most recent ProductionTask in state `measured` for
that channel. If older than 24h, render an amber pill. If older than 72h,
red. This makes "is the live loop actually moving" visible at a glance.

### 5.7 Acceptance Criteria

After flipping `dry_run=false`, one real channel produces, within 24h, at
least one `PublicationRecord` whose `platform_content_id` resolves on
YouTube, and at least one `FeedbackSnapshot` was written **without** any
operator manually injecting metrics into a queue payload.

## 6. Sprint 2 — Approval Bridge And Compliance Closure (P0/P1, 1 week)

Goal: stop AutoFlow's human-review gate from blocking agent-driven plans
when the agent itself has already validated compliance, and close the
PDS fail-open hole on publish.

### 6.1 ProductionTask Approval Mode

New columns on `ProductionTask`:

- `approval_mode: Literal["human", "agent", "none"]`, default `"agent"` for
  lane_seed and `"human"` for manual_seed.
- `agent_approval_evidence_json: dict`, captures the PDS verdict and
  rationale that justified an agent-approved plan.

New column on `AutoFlowPlan`:

- `agent_approved_by: str | None`. When non-null, equivalent to
  `review_approved_at` for `_assert_execute_allowed` purposes.

### 6.2 Auto-Approval Path

In `handle_plan_task`, after AutoFlow plan persists:

1. Fetch PDS decision for `action_type="plan_approval"`.
2. If verdict allows and `task.approval_mode == "agent"`:
   - Call `autoflow_service.approve_internal(plan_id,
     approved_by="channel_agent", evidence=pds_decision.payload)` (new
     internal API, no HTTP).
   - Persist `agent_approval_evidence_json`.
3. Otherwise, leave the plan unapproved; downstream `execute_task` will
   surface `review approval required` as a `held` state with
   `blocked_by_guard="human_review_required"`.

`_assert_execute_allowed` becomes:

```python
review_approved = bool(plan.review_approved_at) or bool(plan.agent_approved_by)
```

`public_approved_at` continues to gate `public_after_review` separately. The
agent never auto-approves public visibility in this design.

### 6.3 PDS Fail-Closed For Publish

`PDSClient.decide` already takes a `PDSDecisionRequest` with `action_type`.
Apply a per-action fail policy:

```python
FAIL_POLICY = {
    "candidate_accept": "allow",   # current behavior
    "plan_approval":    "flag",    # warn but proceed
    "publish":          "block",   # never fail-open
    "promote_publication": "block",
}
```

On request error / 5xx / parse failure, return verdict according to the
action's policy. The `pds_disabled` / `pds_unavailable` warnings stay so
operators can distinguish "PDS down" from "PDS said no".

### 6.4 PDS Health Monitor

`ChannelAgentRunner` maintains `pds_last_success_at` in process. Every time
`PDSClient.decide` returns a non-fail-open verdict, update the timestamp.
If the gap exceeds 5 minutes during business hours, enqueue
`send_alert` with type `pds_outage` and resource `service:pds`. Idempotency
key follows the standard pattern with an hour bucket.

### 6.5 PublicationStatusReconciler

New queue kind `reconcile_publication`. Created automatically when
`promote_publication` succeeds, with `run_after = scheduled_publish_at +
30 minutes`. Handler:

1. Calls `youtube_client.fetch_status(account, platform_content_id)`.
2. If platform privacy matches `desired_privacy`, write
   `publication.current_privacy = ...` and mark task `measured` if metrics
   already exist (otherwise let `collect_metrics` finish first).
3. If platform reports rejection/removal/claim, transition publication to
   `removed` / `held` with `failure_reason` and emit a `takedown_event`
   record for severe cases.

### 6.6 Acceptance Criteria

External-asset lane tasks no longer stall at `planning`. PDS forced into
"unavailable" mode causes publish tasks to be held with `pds_unavailable`
within one tick. Within 31 minutes of a scheduled publication, a
reconciliation row exists with either confirmed current_privacy or an
explicit failure_reason.

## 7. Sprint 3 — Material Ledger And Repetition Guards (P1, 1 week)

Goal: actually use the `MaterialUsageLedger` table so repeated reuse of the
same segment or asset is detected and prevented across runs and accounts.

### 7.1 Ledger Write Path

In `handle_observe_job`, after extracting the YouTube video id and creating
the PublicationRecord pre-image, walk `autoflow_run.artifacts` and
`autoflow_run.candidates` to extract:

- `material_id` (per-clip stable identifier)
- `asset_id` (UUID into `assets` table when sourced locally)
- `segment_signature` (`sha256(material_id + ":" + start_ms + ":" +
  end_ms)`; alternatively the autoflow-selected `segment_signature` if
  AutoFlow already produced one)

Insert one `MaterialUsageLedger` row per material per publication. Index on
`(channel_profile_id, topic_lane_id, segment_signature, used_at)`.

### 7.2 RepetitionGuard

Evaluated during tick candidate selection. Reject a candidate if:

- Same `segment_signature` already used in the same `topic_lane_id` within
  7 days, OR
- Same `material_id` used by the same account within 14 days.

Both windows configurable per channel via existing
`risk_policy_json.repetition`.

### 7.3 CrossAccountDuplicationGuard

Reject if `material_id` was used by a sibling account (different
`publishing_account_id` under the same `channel_profile_id`) within 30 days.
This is a flag-not-block guard for `manual_seed` to preserve operator
override; lane_seed candidates are hard-rejected.

### 7.4 Funnel API Update

`/channels/{id}/metrics/funnel` adds `repetition_rejected` and
`cross_account_rejected` counts. Frontend funnel chart shows these as
distinct rejection slices so operators can see why candidates dropped out.

### 7.5 Acceptance Criteria

Five consecutive ticks against a small material library never re-select the
same segment. Funnel UI shows the rejection slices. Manual override (operator
creates a ManualSeed for a material that the guard would flag) is still
allowed and is annotated in `rationale_json`.

## 8. Sprint 4 — Self-Driving (P1/P2, 1.5 weeks)

Goal: the system creates and consumes its own work without an external cron
or operator nudges, within configured rate limits.

### 8.1 Internal Tick Scheduler

`ChannelProfile` gains `tick_interval_minutes: int = 60`. The runner gains a
ticker goroutine (separate from the queue consumer loop) that, every
`tick_interval_minutes`:

1. Loads all enabled, non-halted channels.
2. For each, enqueues `agent_tick` with idempotency key
   `agent_tick:{channel_id}:{utc_hour_bucket}`.
3. Records its own audit row in a new `internal_scheduler_runs` table.

No external cron required. Existing `POST /enqueue-tick` continues to work
for one-off operator runs.

### 8.2 Trend YouTube Search Ingester

New module `backend/app/channel_agent/trend_ingesters/youtube_search.py`.
Daily per channel:

1. For each enabled `TopicLane`, call
   `youtube.search.list(q=lane.primary_keyword, order="viewCount",
   publishedAfter=now-24h, regionCode=channel.region, maxResults=25)`.
2. Filter to videos with `viewCount` above lane-specific floor.
3. Materialize each candidate as `ManualSeed(source="trend_youtube",
   topic_lane_id=lane.id, prompt=<auto-generated from title/desc>)`.
4. Mark seeds with TTL via existing `status` field; ingester re-runs
   refresh active seeds and expire stale ones.

Quota budget: ~100 units per channel per day. Documented in the runbook.

### 8.3 Explainable Candidate Scoring

Replace the current first-fit selection with a rule-based score writeback
(not used for selection yet — Stage A is observe-only):

```python
score = (
    0.25 * lane_weight
    + 0.20 * material_fit
    + 0.15 * freshness
    + 0.15 * account_fit
    + 0.10 * timing_score
    + 0.10 * novelty
    - 0.20 * repetition_risk
    - 0.30 * compliance_risk
)
```

Each factor sourced from existing data: `lane_weight` from
`TopicLane.weight`; `material_fit` from material availability in lane;
`freshness` from `MaterialUsageLedger.used_at`; `account_fit` from
`AccountConcurrencyGuard.headroom`; `timing_score` from publish-window
configuration; `novelty` from segment_signature uniqueness in window;
`repetition_risk` and `compliance_risk` from the corresponding guards.

Persist to `ProductionTask.score_breakdown_json` and `rationale_json`.
The tick still selects greedy-first as today; scoring is logged for later
Stage B promotion (epsilon-greedy) once `FeedbackSnapshot` has enough data.

### 8.4 Retention Cleanup

New queue kind `cleanup_expired`, scheduled by the internal scheduler once
daily:

- `ChannelOpsQueueItem` in succeeded/dead_lettered/cancelled state older
  than 30 days: hard-delete.
- `AgentTickAudit` older than 90 days: hard-delete.
- `FeedbackSnapshot` older than 365 days: hard-delete.

All retention thresholds live in `settings.channel_agent_retention_*` env
overrides so operators can tune without redeploying.

### 8.5 Acceptance Criteria

A channel configured at start of week 1 day 1 produces and publishes
content on every subsequent day for 7 days without any human action besides
inspecting the dashboard. All ProductionTask rows have non-empty
`score_breakdown_json`. Storage growth from queue/audit tables stabilizes
after retention runs.

## 9. What This Design Does Not Solve

Calling out explicitly so the next planning pass starts from the right
question:

- **Learning.** Stage B (epsilon-greedy) and Stage C (Thompson sampling) are
  not part of this design. They unlock only after Sprint 4 has accumulated
  real `FeedbackSnapshot` data for at least 30 days across at least
  100 publications.
- **Public auto-publication.** `_safe_privacy` still degrades `public` to
  `unlisted`. Lifting that gate requires retention thresholds and
  takedown-rate ceilings that should be data-driven, not designed.
- **Bilibili, X, 小红书.** Multi-platform publication is not in scope.
  YouTube must reach 50 consecutive zero-incident days first.
- **A/B variants per task_group_id.** Schema supports it; selection and
  promotion logic do not. Schedule for after Sprint 4 if needed.

## 10. Risks And Mitigations

- **Risk.** YouTubeManager endpoints for schedule / metrics / refresh / status
  may not exist yet.
  - **Mitigation.** Sprint 1 explicitly includes adding them. The cost of
    one extra service-side endpoint is lower than splitting OAuth ownership.
- **Risk.** Auto-approval skips legitimate compliance review.
  - **Mitigation.** PDS verdict for `plan_approval` is required to be
    `allow` (not just `flag`) before `agent_approved_by` is set. Manual_seed
    tasks default to `approval_mode="human"`.
- **Risk.** Repetition guard windows are too aggressive and starve lanes.
  - **Mitigation.** Windows are config (`risk_policy_json.repetition`).
    Funnel UI shows the `repetition_rejected` slice so operators can detect
    starvation and tune.
- **Risk.** Internal tick scheduler runs unbounded across channels and
  spikes resource use.
  - **Mitigation.** Each channel's `tick_interval_minutes` floor is 15.
    Scheduler enqueues at most one tick per channel per bucket. Queue
    runner concurrency cap remains the resource-side throttle.

## 11. Verification Strategy

Each sprint ships with the existing checks:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Plus sprint-specific live checks:

- Sprint 1: Manual smoke (Sprint 0 procedure rerun against the new clients).
- Sprint 2: Trigger `review_required` rights on a test ManualSeed; verify
  the plan moves to `producing` without human approval. Force PDS down via
  env override; verify publish enters `held`.
- Sprint 3: Insert a duplicate-segment fixture; verify the second tick
  rejects with `repetition_rejected` and funnel reflects it.
- Sprint 4: Disable external cron; verify the runner produces at least one
  publication on day +1 and day +7 with no API calls beyond initial config.

## 12. Out-Of-Scope Decisions Recorded For Later

These were considered and deferred. Document them so they are not
reinvented:

- `failure_category` enum on ProductionTask (`network` / `upload` / `auth` /
  `quota` / `validation` / `other`). Useful for guard dashboards. Add in a
  follow-up alongside the bandit work.
- Held-task TTL. A task held >24h with no operator action should auto-fail
  or auto-cancel. Trivial migration; deferred to avoid scope creep here.
- AccountFingerprint table. Required only when running multiple YouTube
  accounts under the same project; not relevant for the single-account
  alpha.
- LLM-driven idea elaboration. Today's prompts are template-filled. LLM
  rewriting is a Stage-C concern.

## 13. Implementation Order Summary

| Sprint | Length | Output | Gate To Next |
|---|---|---|---|
| Sprint 0 | 0.5 day | One real private upload completes end-to-end | Any code change merges to main |
| Sprint 1 | ~1 week | Real YouTubeManager wiring + live metrics + audit uniqueness | One unattended publish + measured snapshot in 24h |
| Sprint 2 | ~1 week | Agent approval bridge + PDS fail-closed + reconciler | review_required tasks no longer stall; PDS outage holds publish |
| Sprint 3 | ~1 week | MaterialUsageLedger writes + repetition guards | 5 ticks never reselect the same segment |
| Sprint 4 | ~1.5 weeks | Internal scheduler + YouTube trend ingester + scoring + retention | 7 days self-driving |

Total: roughly 4–5 weeks of focused work for one engineer to reach the
"truly unattended" state. Sprint 0 happens before everything else and is
non-negotiable.
