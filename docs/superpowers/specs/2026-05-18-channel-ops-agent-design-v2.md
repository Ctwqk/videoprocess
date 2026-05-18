# ChannelOps Agent — Refined Design (v2)

Date: 2026-05-18
Status: Draft for review
Supersedes: User's v1 outline (in-thread, 2026-05-18)

## 0. Document Status

This is a refinement of the v1 ChannelOps Agent outline after audit-driven review. Changes vs v1:

- **Added** chapters on Compliance Stance, Observability, Cold Start, Learning Mechanism, Test Harness, Thumbnail Generation, SLAs.
- **Corrected** baseline ("what we already have") — v1 overstated reusable capability for several services where the schema exists but the data flow is empty.
- **Merged** `ContentIdea` and `WorkOrder` into a single `ProductionTask` with an explicit state machine.
- **Added** `MaterialUsageLedger`, `AgentTickAudit`, `AccountFingerprint`, `LaneFormatMatrix` tables.
- **Restructured** rollout into smaller phases with an explicit dry-run gate.
- **Made explicit** the V1 (expert-weight) → V2 (bandit) learning path.

## 1. System Positioning

Three explicit layers. The agent is **only** the top layer:

| Layer | Responsibility | Code namespace | Decision authority |
|---|---|---|---|
| **ChannelOps Agent** | What to make, when, where, for whom; learn from outcomes | `backend/app/channel_agent/` | High — chooses topics, accounts, timing |
| **AutoFlow** | Turn one selected production task into an executable pipeline plan | `backend/app/autoflow/` | Medium — chooses template, materials, storyboard |
| **Pipeline / Worker** | Execute ffmpeg / upload / probe | `backend/worker/` | None — deterministic |

AutoFlow does **not** lose any capability. It loses **entrypoint authority**: prompts can still come from humans, but the agent becomes the dominant caller in production.

```text
ChannelProfile + TopicLane + PublishingAccount + history
            │
            ▼
   ┌────────────────────┐
   │  ChannelAgent.tick │  ◄── cron / on-demand
   └────────┬───────────┘
            │ ProductionTask draft
            ▼
   ┌────────────────────┐
   │  AutoFlow.plan     │  ◄── existing
   └────────┬───────────┘
            │ AutoFlowPlan
            ▼
   ┌────────────────────┐
   │  Pipeline executor │  ◄── existing
   └────────┬───────────┘
            │ Artifact + private upload
            ▼
   ┌────────────────────┐
   │ PublicationManager │  ◄── promotes to public per policy
   └────────┬───────────┘
            │ PublicationRecord
            ▼
   ┌────────────────────┐
   │ MetricsCollector   │  ◄── poll YouTube Data API
   └────────┬───────────┘
            │ FeedbackSnapshot
            ▼
   ┌────────────────────┐
   │  LearningEngine    │  ◄── update lane / template / time weights
   └────────────────────┘
```

## 2. Compliance Stance

**Decision (per operator):** Operator has explicitly accepted the risk that some content (e.g. Tom & Jerry derivative shorts) may trigger Content ID claims, takedowns, or revenue redirection. The system **does not block** publication on IP grounds.

This stance has design consequences that must still be honored:

- **`PublicationRecord.compliance_disposition`** tags each upload as one of: `original`, `licensed`, `cc0`, `assumed_fair_use`, `known_risk_accepted`. This is for audit, not for blocking.
- **`takedown_events` table** captures Content ID claims, copyright strikes, monetization restrictions, account-level penalties — agent reads these to throttle the offending lane / account automatically (separate from IP "rightness").
- **`account_strike_guard`** is mandatory: if any account accumulates ≥2 strikes in 90 days, the agent must auto-pause that account's lane. This is operational survival, not compliance enforcement.
- **Account fingerprint isolation**: an account that's been claimed should not propagate the strike-causing template to siblings. `AccountFingerprint` table records per-account upload posture (timing jitter, template pool, IP) — the guard ensures cross-account contamination is detected.

The Compliance section is therefore **kept in scope** but reframed from "preventive gate" to "post-hoc operational containment". No takedown handler ⇒ unbounded cascading failure when first strike lands.

## 3. Baseline Reality Check (Corrected)

What v1 listed as "existing capability" vs the actual state:

| v1 Claim | Reality | Implication for plan |
|---|---|---|
| `TrendService` has signals, opportunity, competition, rights risk | Schema ✅, **all signals manual-injected** via `add_signal` | Build `TrendIngester` from scratch (Phase 4) |
| `MetricsService` saves views/likes/.../virality | Schema ✅, **all manual via `save_manual_metrics`** | Build `MetricsCollector` from scratch (Phase 6a) |
| `ContentStrategyService` generates ideas from trend + template performance | ✅ function exists, but both inputs are empty | Output is meaningless until Phase 4 + Phase 6a land |
| AutoFlow run records publications | `AutoFlowRun.publish_json` exists, but is a single dict, not queryable history | Keep as snapshot; add separate `PublicationRecord` table |
| AutoFlow is "single-shot prompt" | True, AND has **two-stage approval** (`approve` + `approve-public`) | Agent must introduce `auto_approved_by_agent` semantics; see §6.4 |
| `YouTubeUploadHandler` handles quota | ✅ `_enforce_quota_estimate` is real | Reusable as-is; expose remaining quota as observable signal |
| `MaterialSelector` does dedupe and ranking | ✅ + recent_used_asset_ids wired | Extend to lane-scoped usage ledger (§5) |
| Thumbnail generation | ❌ **only text candidates exist** (`thumbnail_text_candidates`); no image node | Must build (§9) — biggest single CTR lever for YouTube |
| OAuth token refresh | ✅ on-demand at upload time | ❌ no proactive health check — add `account_health_check` task |

**Net:** the executable layer is mature; the **decision data plane** (trend ingestion, metrics collection, thumbnail generation) is essentially missing. Estimate this as 50% larger than v1 implied.

## 4. Data Model

### 4.1 Configuration Layer

```python
class ChannelProfile(Base):
    id: UUID
    owner_user_id: UUID                       # multi-tenant; missing in v1
    name: str
    positioning: str                          # human-readable elevator pitch
    language: str
    default_aspect_ratio: str
    risk_policy_json: dict                    # source_policy, allow_external, etc.
    content_mix_policy_json: dict             # exploit/explore/wildcard ratios
    cadence_policy_json: dict                 # global caps across accounts
    enabled: bool
    config_version: int                       # snapshotted into ProductionTask
    created_at: datetime
    updated_at: datetime


class TopicLane(Base):
    id: UUID
    channel_profile_id: UUID
    name: str
    description: str
    weight: float                             # initial expert weight (V1)
    learned_weight: float | None              # bandit posterior mean (V2)
    keywords_json: list[str]
    negative_keywords_json: list[str]
    min_posts_per_week: int
    max_posts_per_day: int
    max_consecutive_streak: int               # avoid same-lane back-to-back
    cooldown_after_post_minutes: int
    enabled: bool
    paused_until: datetime | None             # auto-pause from guards


class PublishingAccount(Base):
    id: UUID
    channel_profile_id: UUID
    platform: str                             # youtube | bilibili | x | xiaohongshu
    account_label: str
    platform_account_id: str
    credential_ref: str                       # path or vault key
    platform_specific_config_json: dict       # NOT a generic blob — see §4.6
    default_privacy: str
    enabled: bool
    paused_until: datetime | None
    last_token_check_at: datetime | None
    last_token_check_status: str | None       # ok | expiring_soon | invalid


class LaneFormatMatrix(Base):
    """A TopicLane × Format intersection. One lane can produce multiple formats."""
    id: UUID
    topic_lane_id: UUID
    format_key: str                           # shorts_9x16 | longform_16x9 | square_1x1
    enabled: bool
    weight: float
    target_duration_sec: int
    template_pool_json: list[str]             # template_ids eligible for this combo
```

### 4.2 Production Layer

`ContentIdea` and `WorkOrder` from v1 are merged. Same row, explicit states.

```python
class ProductionTask(Base):
    """A unit of 'we want to make and ship one piece of content'."""
    id: UUID
    task_group_id: UUID | None                # for A/B variants; siblings share group
    channel_profile_id: UUID
    topic_lane_id: UUID | None
    lane_format_id: UUID | None               # which lane×format combination
    target_account_id: UUID

    # Discovery → Selection
    source: str                               # trend | competitor | inventory | manual | historical_winner
    title_seed: str
    prompt: str
    rationale_json: dict                      # explainability — why selected
    score_breakdown_json: dict                # per-factor scores
    portfolio_bucket: str                     # exploit | explore | wildcard

    # Production link
    autoflow_plan_id: UUID | None
    autoflow_run_id: UUID | None
    job_id: UUID | None

    # Scheduling
    scheduled_at: datetime | None             # when agent wants to publish
    priority: float

    # State
    state: str                                # see state machine §4.3
    state_updated_at: datetime
    failure_reason: str | None
    retry_count: int
    blocked_by_guard: str | None              # name of guard that blocked
    channel_config_version_snapshot: int      # so config edits don't retroactively affect in-flight tasks

    created_at: datetime
    updated_at: datetime
```

### 4.3 ProductionTask State Machine

```text
discovered          ── scored ──> scored
scored              ── selected ──> selected   (passes portfolio + guards)
scored              ── rejected ──> rejected   (terminal)
selected            ── planning ──> planning   (AutoFlow.plan)
planning            ── plan_failed ──> failed  (terminal until retry policy)
planning            ── producing ──> producing (AutoFlow.execute)
producing           ── produce_failed ──> failed
producing           ── uploaded_private ──> uploaded_private
uploaded_private    ── promoted ──> published
uploaded_private    ── promote_blocked ──> held  (manual gate)
published           ── measured ──> measured    (after metrics collected)
measured            ── learned ──> learned      (terminal — bandit updated)
* (any non-terminal) ── cancelled ──> cancelled  (admin or guard)
```

Tasks have **idempotent transitions** — each transition records the timestamp and the actor (`agent_tick:<id>` or `user:<id>` or `guard:<name>`).

### 4.4 Distribution Layer

```python
class PublicationRecord(Base):
    """Authoritative record per platform publication. Indexed for history queries."""
    id: UUID
    production_task_id: UUID
    platform: str
    account_id: UUID
    platform_content_id: str
    permalink: str | None

    title: str
    description: str
    tags_json: list[str]
    thumbnail_storage_path: str | None
    desired_privacy: str
    current_privacy: str
    publish_status: str                       # uploaded | scheduled | public | unlisted | private | removed
    uploaded_at: datetime
    scheduled_publish_at: datetime | None     # YouTube publishAt
    public_at: datetime | None

    compliance_disposition: str               # see §2
    quota_units_estimated: int

    last_metrics_polled_at: datetime | None


class TakedownEvent(Base):
    publication_id: UUID
    event_type: str                           # content_id_claim | strike | restriction | takedown
    detected_at: datetime
    severity: str                             # info | warning | severe
    raw_payload_json: dict
    auto_actions_taken_json: list[str]        # ["paused_lane:abc", "paused_account:xyz"]
```

### 4.5 Feedback Layer

```python
class FeedbackSnapshot(Base):
    publication_id: UUID
    collected_at: datetime
    views: int
    likes: int
    comments: int
    shares: int
    avg_view_duration_sec: float
    retention_curve_json: list[float] | None
    ctr: float | None
    impressions: int | None
    virality_score: float
    raw_json: dict


class LearningUpdate(Base):
    """Audit trail of every bandit / weight update."""
    id: UUID
    triggered_by_snapshot_id: UUID
    target_dimension: str                     # topic_lane | template | publish_hour | account
    target_id: str
    before_weight: float
    after_weight: float
    update_reason: str                        # bandit_thompson | manual_override | guard_reset
```

### 4.6 Material Intelligence

```python
class MaterialUsageLedger(Base):
    """Per-material × per-lane × per-account usage history. Existing AutoFlowUsedClip
    is per-asset only; we need finer-grain to enforce cooldowns properly."""
    id: UUID
    material_id: str
    asset_id: UUID | None
    channel_profile_id: UUID
    topic_lane_id: UUID | None
    publishing_account_id: UUID | None
    publication_id: UUID
    used_at: datetime
    segment_signature: str                    # so re-using a different segment of same video doesn't false-positive


class MaterialInventoryForecast(Base):
    """Computed periodically — answers 'how many days can this lane run before it runs dry'."""
    channel_profile_id: UUID
    topic_lane_id: UUID
    lane_format_id: UUID | None
    eligible_material_count: int
    weekly_consumption_rate: float
    days_of_supply: float
    last_computed_at: datetime
```

### 4.7 Operational Audit

```python
class AgentTickAudit(Base):
    """Every tick logged. Indispensable for debugging long-running agents."""
    id: UUID
    channel_profile_id: UUID
    tick_id: str                              # idempotency key
    started_at: datetime
    finished_at: datetime | None
    dry_run: bool
    ideas_discovered: int
    candidates_scored: int
    tasks_selected: int
    tasks_rejected: int
    guards_triggered_json: list[dict]         # which guard, what input, what action
    error_message: str | None


class AccountFingerprint(Base):
    publishing_account_id: UUID
    last_upload_at: datetime
    upload_timing_jitter_seconds: int         # randomization budget for next upload
    template_pool_recent_json: list[str]      # rotation enforcer
    daily_upload_count_json: dict             # per-date histogram (last 30 days)
    last_ip_class: str | None
```

### 4.8 What we are NOT introducing

- A separate `ContentIdea` table — merged into `ProductionTask`.
- A separate `WorkOrder` table — merged into `ProductionTask`.
- Renaming `AutoFlowUsedClip` (keep it; `MaterialUsageLedger` is additional, more granular).
- Replacing `AutoFlowRun.publish_json` (keep as run-internal snapshot; `PublicationRecord` is the agent-facing history).

## 5. Component Designs

### 5.1 Agent Tick Loop

```python
class ChannelAgent:
    async def tick(self, db, channel_id: UUID, *, dry_run: bool = False) -> AgentTickAudit:
        # 1. Acquire idempotent lock (Redis SETNX with TTL = 10 min)
        lock_key = f"channel_agent:tick:{channel_id}"
        async with redis_lock(lock_key, ttl=600) as acquired:
            if not acquired:
                logger.warning("tick already running for channel=%s; skipping", channel_id)
                return None

            audit = AgentTickAudit(channel_profile_id=channel_id, dry_run=dry_run, ...)

            # 2. Load state snapshot (config version is frozen for this tick)
            channel = await db.get(ChannelProfile, channel_id)
            lanes   = await self.lane_store.enabled_lanes(channel_id, db)
            accounts = await self.account_store.enabled_accounts(channel_id, db)
            inventory = await self.material_intelligence.summarize(channel_id, db)
            recent_metrics = await self.feedback.recent_window(channel_id, days=7, db=db)

            # 3. Guards (early — they may abort the tick)
            blocking = await self.guards.evaluate_pre_tick(channel, accounts, db=db)
            if blocking:
                audit.guards_triggered_json = blocking
                await self._persist(audit, db)
                return audit

            # 4. Discover candidates per lane × format
            candidates = []
            for lane in lanes:
                for combo in self.lane_format.combos(lane):
                    candidates.extend(
                        await self.discovery.discover(channel, lane, combo, recent_metrics, inventory)
                    )

            # 5. Score
            scored = [self.decision.score(c, channel, recent_metrics, inventory) for c in candidates]

            # 6. Portfolio selection (with bandit if Stage C, else expert weights)
            selected = self.decision.select_portfolio(
                scored,
                accounts=accounts,
                channel_config=channel,
                bucket_targets={"exploit": 0.7, "explore": 0.2, "wildcard": 0.1},
            )

            # 7. Per-account guards (cadence, quota, fingerprint, account_strike)
            tasks_to_create = []
            for cand in selected:
                rejection = await self.guards.evaluate_per_task(cand, db=db)
                if rejection:
                    audit.tasks_rejected += 1
                    continue
                tasks_to_create.append(cand)

            # 8. Persist as ProductionTask rows (or just log if dry_run)
            if not dry_run:
                for cand in tasks_to_create:
                    await self.task_store.create_selected(cand, db=db, tick_id=audit.tick_id)

            audit.tasks_selected = len(tasks_to_create)
            await self._persist(audit, db)
            return audit
```

Key properties:
- **Idempotent lock** — overlapping ticks no-op.
- **Config snapshot at tick start** — config edits during a tick don't take effect mid-run.
- **Dry-run = side-effect-free** — only writes `AgentTickAudit`. No tasks created.
- **Audit is mandatory** — every tick produces one row, success or failure.

### 5.2 Discovery Engine

Sources (extensible):

```python
class DiscoverySource(str, Enum):
    MANUAL_SEED        = "manual_seed"          # operator-injected topics
    TREND_INGEST       = "trend_ingest"         # automated trend feeds
    HISTORICAL_WINNER  = "historical_winner"    # past top performers, with refresh
    COMPETITOR         = "competitor"           # competitor channel monitoring
    NEWS_FEED          = "news_feed"            # RSS / Google Trends / etc.
    INVENTORY_DRIVEN   = "inventory_driven"     # surplus material in library suggests topic
```

**Trend Ingester** is the most-missing piece. New module `backend/app/channel_agent/trend_ingester.py`:

- YouTube: `search.list` with `videoCategoryId` + `regionCode` filter, sorted by `viewCount` over last 24h.
- Bilibili: `popular/series/one` and `web-interface/ranking/v2`.
- Google Trends: pytrends batch every N hours.
- RSS: pluggable list per channel.

Each ingester writes into `TrendSignal` (existing schema). The discovery engine reads from there — single sink, multiple sources.

**Cost guard**: cache ideas keyed on `(lane_id, trend_signal_hash)` for 4 hours; reuse unless trend changes. LLM idea-elaboration is expensive and rate-limited.

### 5.3 Decision Engine

V1 (cold-start, 0–100 publications): **expert-weighted scoring**. Hard-coded formula, similar to user's v1 §6.1 but explicitly versioned.

```python
def expert_score_v1(c, ctx) -> float:
    return (
        0.22 * c.trend_score
        + 0.18 * c.historical_lane_performance     # 0.5 baseline if no data
        + 0.16 * c.material_availability
        + 0.14 * c.novelty_score
        + 0.12 * c.account_fit
        + 0.10 * c.timing_score
        + 0.08 * c.title_potential
        - 0.18 * c.rights_risk
        - 0.14 * c.repetition_risk
        - 0.10 * c.production_cost
    )
```

V2 (after ≥100 publications/lane): **per-lane Thompson sampling** for the exploit bucket. Each lane has a Beta posterior over "good outcome probability"; selection samples a value per lane and ranks. Expert score remains the input to "what to make within the lane".

V3 (after ≥500 publications, optional): **contextual bandit** with features (hour-of-day, weekday, account, format) → reward (virality). Out of scope for first ship; data shape must support it.

**Portfolio buckets** are non-negotiable — without them the system locks onto local optima:

| Bucket | Share | Source |
|---|---|---|
| exploit | 70% | top expert-score within enabled lanes |
| explore | 20% | enabled-but-low-data lanes; or top score from previously-rejected sources |
| wildcard | 10% | random eligible candidate (anti-overfitting) |

### 5.4 Material Intelligence

Three responsibilities:

1. **Availability check** before idea is scored: `material_availability(lane) ∈ [0, 1]` — used as a scoring factor.
2. **Cooldown enforcement** when materials are picked: `MaterialUsageLedger` lookup; reject a selection that would violate the per-lane cooldown.
3. **Depletion forecasting**: nightly batch computes `MaterialInventoryForecast`; if `days_of_supply < 3` for a lane, agent emits an alert (observability §7) and may auto-pause that lane.

### 5.5 Publication Manager

Two-step publication is mandatory:

```text
job complete with upload artifact
    ↓
PublicationRecord created with status=uploaded, current_privacy=private
    ↓
PublicationManager.promote_eligible() runs on schedule
    ├── checks promote_policy: account default, time window, no recent strikes
    ├── if ok: set scheduled_publish_at OR public_at (depending on platform)
    └── else: state="held"; observability surfaces it for human review
```

**Decision: use YouTube `publishAt` for time-deferred public**, not in-process timers. YouTube's scheduler is reliable; our process restart shouldn't lose a scheduled publish. For other platforms without scheduling, fall back to a dedicated `publication_promoter` worker.

### 5.6 Metrics Collector & Learning

`MetricsCollector` is a new periodic worker:

- Every 1h for content < 24h old.
- Every 6h for content 24h–7d old.
- Every 24h for content 7d–30d old.
- Stop after 30d (configurable).

Polls YouTube Data API `videos.list(part="statistics,contentDetails")` + Reports API (if connected) for retention. Writes `FeedbackSnapshot`.

**Quota awareness**: each metrics poll costs ~1 unit; with 100 active publications polled hourly = 2,400/day — fits in 10k quota. Document this budget.

**Learning loop**:

```text
FeedbackSnapshot inserted
    ↓
LearningEngine: compute outcome label (good / neutral / bad) per task
    ↓
update bandit posterior for (lane, format, account, publish_hour)
    ↓
write LearningUpdate row
    ↓
update TopicLane.learned_weight (smoothed)
```

Outcome label is **not** raw virality_score. Use percentile rank within the channel's last 90-day window — robust to drift.

### 5.7 Thumbnail Generation (NEW)

YouTube custom thumbnails are the single largest CTR lever. Missing in v1; AutoFlow has only `thumbnail_text_candidates`, no image generation.

Two strategies, choose at lane level:

- **Strategy A — frame + text overlay** (cheap, deterministic):
  - Pick high-energy frame from `smart_trim` output (visual_motion + face_present score).
  - Overlay `thumbnail_text_candidates[0]` with stroke + drop shadow, 80pt min, max 5 words.
  - New node: `thumbnail_compose`.

- **Strategy B — AI image generation** (expensive, optional):
  - Call image-generation API with prompt derived from storyboard + title.
  - Cache results by prompt hash to control cost.

Both strategies produce an image artifact; `PublicationRecord.thumbnail_storage_path` is set. `YouTubeUploadHandler` is extended to call `thumbnails.set` after `videos.insert` (additional 50 quota units).

### 5.8 Multi-Platform Metadata Generators

A single LLM "describe the video" prompt cannot satisfy YouTube + X + 小红书 simultaneously:

- YouTube: 100-char title, 5000-char description (timestamps, links, CTAs), 500-char tag list.
- X: 280-char post, hashtag-aware.
- 小红书: emoji-heavy, tag-style, very different idiom.

Each platform gets its own `PlatformMetadataAdapter(platform=...)` that receives the generic `AutoFlowMetadata` and emits the platform-specific payload. `PublishingAccount.platform_specific_config_json` provides per-account overrides (signature, default CTA, tag pool).

## 6. Approval & Review Flow

AutoFlow currently has two-stage human approval (`approve` → `approve-public`). For agent-driven runs:

```text
agent creates plan → AutoFlowPlan.review_approved_at = now(), auto_approved_by_agent = True
                  → AutoFlowPlan.public_approved_at remains NULL by default
```

`public_approved_at` is only auto-set if **all** are true:
- `channel.risk_policy.auto_promote = True`
- task is in `exploit` bucket (not `explore`/`wildcard`)
- no recent strikes on this account
- no `held` state set by guards

Otherwise the publication is uploaded private, and the operator promotes from dashboard.

This is the "human in the loop, but not in the way" compromise. Operators see a queue of `held` publications each morning; one-click approve or reject.

## 7. Observability (NEW — was missing from v1)

Without observability, a long-running agent is uninvestigatable. Required surfaces:

### 7.1 Agent Health Dashboard (per channel)

- **Today**: ticks run, tasks selected, tasks failed at each stage, publications made, current quota remaining, current strikes.
- **7-day funnel**: discovered → scored → selected → planned → produced → uploaded → published → measured. Each transition's count and dropout rate.
- **Last 5 tick decisions**: which topics chosen, why (top 3 score factors), which guards triggered.
- **Lane health**: per-lane consumption rate, days-of-supply, learned weight trajectory.

### 7.2 Tick Audit Trail

Every `AgentTickAudit` row + JSON detail browsable; filter by `dry_run`, by channel, by date.

### 7.3 Decision Replay

Given an audit row, reconstruct: candidates, scores, why-selected, what-was-rejected. Necessary for debugging "why did the agent post this".

### 7.4 Alerts (push, not pull)

Trigger conditions → channel-configurable destination (Slack webhook, email):

- Account token expiring in < 7 days.
- Quota at < 20% with > 4 hours remaining in day.
- Strike or takedown event detected.
- Lane days-of-supply < 3.
- 3+ consecutive task failures on same template.
- 7-day virality below floor.

### 7.5 Read-Side APIs

```
GET /api/v1/channels/{id}/agent/health
GET /api/v1/channels/{id}/agent/ticks?limit=20
GET /api/v1/channels/{id}/agent/ticks/{tick_id}/details
GET /api/v1/channels/{id}/agent/lanes/{lane_id}/health
GET /api/v1/channels/{id}/agent/publications?status=held
POST /api/v1/channels/{id}/agent/publications/{pub_id}/promote
POST /api/v1/channels/{id}/agent/publications/{pub_id}/reject
POST /api/v1/channels/{id}/agent/lanes/{lane_id}/pause
POST /api/v1/channels/{id}/agent/lanes/{lane_id}/resume
```

## 8. Guards & Safety

Existing v1 guards (cadence, repetition, quota, rights) plus **new** required ones:

| Guard | Purpose | Trigger | Action |
|---|---|---|---|
| `IdempotencyGuard` | Prevent duplicate ticks | overlapping locks | abort tick |
| `ConfigVersionGuard` | Lock config during in-flight tasks | task snapshot ≠ current | use snapshot, log divergence |
| `AccountStrikeGuard` | YouTube strike protection | ≥2 strikes/90d | auto-pause account |
| `AccountFingerprintGuard` | Anti-spam-network | uniform upload timing across N+ accounts | inject jitter, require template diversity |
| `TokenHealthGuard` | OAuth pre-flight | token expires < 24h or last refresh failed | alert, skip account |
| `MaterialSupplyGuard` | Inventory depletion | days_of_supply < 3 | alert, throttle lane |
| `LaneStreakGuard` | Variety enforcement | same lane streak ≥ max | force different lane or insert wildcard |
| `PortfolioBudgetGuard` | Bucket balance | daily exploit/explore ratio drift > 15pp | rebalance next tick |
| `LearningStallGuard` | Learning loop health | no FeedbackSnapshot in 24h despite publications | alert (likely metrics collector failing) |

All guards write to `AgentTickAudit.guards_triggered_json` and emit observability events.

**Guard reset:** every guard-induced pause has either (a) a TTL after which it auto-clears, or (b) a manual reset endpoint that records a reason. No "stuck forever" states.

## 9. Cold Start & Learning Stages

| Stage | Threshold | Decision logic | Notes |
|---|---|---|---|
| **A — Bootstrap** | 0–100 publications | Pure explore: equal-weight lanes, randomized within each lane | Goal: build dataset, not optimize. Allow operator to mark "obviously broken" outcomes to fast-track removal. |
| **B — Epsilon-greedy** | 100–500 publications | 70% expert-score top, 20% random, 10% wildcard. No bandit yet. | Stable behavior; allow operator to override weights manually. |
| **C — Bandit-driven** | 500+ publications, ≥30 publications per active lane | Thompson sampling on lane selection; expert score within lane | Bandit only kicks in for lanes that have enough data; underfunded lanes remain in B mode. |

Stage transitions are **manual gates**, not automatic, in v1. Operator promotes after reviewing health. This avoids "agent advances to B with bad data".

## 10. Test Harness

A long-running autonomous system **cannot** be tested only manually.

### 10.1 Mock Platform Adapters

`tests/channel_agent/fakes/` provides:
- `FakeYouTubeClient` — accepts uploads, generates `video_id`, emits configurable metrics over time.
- `FakeTrendIngester` — returns scripted trend signals.
- `FakeBanditOracle` — deterministic Thompson sampler for tests.
- `FakeClock` — advances time, triggers schedulers without sleep.

### 10.2 E2E Scenarios (required CI coverage)

- Happy path: 1 channel × 2 lanes × 1 account for 7 simulated days. Assert: ≥6 publications, no guard breaches, learning_updates written.
- Strike scenario: inject `TakedownEvent` after 5 publications → assert lane auto-pauses, account does not auto-pause until 2nd strike.
- Inventory depletion: simulate library shrinking → assert `MaterialSupplyGuard` fires, alert emitted.
- Quota exhaustion: simulate quota at 95% → assert agent throttles, no upload failures.
- Token expiry: simulate token expiring tomorrow → assert `TokenHealthGuard` alerts pre-emptively.
- Cold start to Stage B promotion: 100 sim publications → operator advances stage → assert decision logic switches.

### 10.3 Dry-Run Mode Lifecycle

For first 7 calendar days of any new channel in production, agent runs `dry_run=True`. Operator reviews each day's `AgentTickAudit`, can flip `dry_run=False` per channel when satisfied. There is no automatic flip.

## 11. SLAs & Error Budget

Targets (initial, revise after 30d):

| Metric | Target |
|---|---|
| Tick success rate | ≥ 99% |
| Production task end-to-end success (selected → uploaded) | ≥ 90% |
| Upload failure rate | ≤ 5% per account-day |
| Metrics collection lag (publication → first snapshot) | < 2h |
| Mean tick wall-clock duration | < 60s |
| Dashboard page load | < 1s |

Error budget burn triggers a halt — if upload failure rate exceeds 15% in any 24h window, the channel auto-pauses pending operator review. Captured by `LearningStallGuard` and a sibling `ProductionStallGuard`.

## 12. Rollout Plan (revised)

| Phase | Scope | Acceptance Gate |
|---|---|---|
| **-1: Operator Acknowledgement** | Document risk acceptance (§2), define safe-lane defaults | Operator signs the doc |
| **0: Layer Repositioning** | Refactor module imports, no behavior change; carve out `channel_agent/` namespace | Tests still pass |
| **1a: Config Layer** | `ChannelProfile`, `TopicLane`, `PublishingAccount`, `LaneFormatMatrix` + CRUD API | Can configure a channel with 2 lanes via API + UI |
| **1b: Production Layer** | `ProductionTask` + state machine + `PublicationRecord` | Can manually create a task and watch it transition |
| **1c: Audit Layer** | `AgentTickAudit`, `MaterialUsageLedger`, `AccountFingerprint` | All writes traceable |
| **2: Discovery (manual sources only)** | `manual_seed` + `historical_winner` sources; LLM idea elaboration | Operator can seed → ideas appear |
| **3: Agent Tick (dry-run only)** | Loop + scoring + portfolio selection, `dry_run=True` enforced | 7-day dry-run audit reviewable |
| **4: Trend Ingester** | YouTube + RSS sources writing to `TrendSignal` | Real signals appear daily |
| **5: AutoFlow Integration** | Tasks call `AutoFlow.plan`/`execute`; uploads land in `PublicationRecord` (private) | End-to-end one task to private upload |
| **6a: Metrics Collector** | Poll YouTube API → `FeedbackSnapshot` | Snapshots accumulate within SLA |
| **6b: Learning Stage A** | Expert-weight only, log outcomes | 100 publications recorded |
| **7: Publication Manager** | Promote-eligible flow + `publishAt` scheduling | Held queue + auto-promote work |
| **8: Guards** | All §8 guards wired | Strike scenario test passes |
| **9: Observability** | Dashboard + alerts + APIs | Operator can audit any decision |
| **10: Bandit Stage B/C** | Epsilon-greedy then Thompson sampling | Manual stage promotion enabled |
| **11: Thumbnail + Multi-platform** | Image generation + per-platform metadata | YouTube CTR measurably improved |
| **12: Compliance Containment** | `TakedownEvent` listener + account-strike auto-actions | Strike scenario auto-handled |

Total: ~12–16 weeks for a single operator on a focused build. The "two commits and a doc" cadence will not deliver this — this is a real product.

## 13. Open Questions (explicit decisions deferred)

1. **Multi-tenant or single-tenant deployment?** Affects auth, isolation, billing. v1 doesn't say.
2. **A/B variant generation for the same task_group_id** — same prompt, different storyboard? In or out of scope for v1?
3. **Cross-channel material sharing** — if channel A buys a stock pack, can channel B use it?
4. **Bilibili / X / 小红书 publication parity** — feature-parity with YouTube or YouTube-first?
5. **Manual override priority** — if operator promotes a `held` task that the guard would have rejected, do we record the override and learn from it, or ignore?
6. **Localization** — same channel produces zh + en versions? Treated as one task or two?
7. **Operator UX scope** — full SPA, or terminal/Slack workflow first?
8. **Cost ceilings** — LLM + image gen + storage are non-trivial; do we expose `monthly_budget_usd` per channel and enforce?

These should be resolved before Phase 5 to avoid retrofitting.

## 14. Summary of Deltas from v1

| Area | v1 | v2 |
|---|---|---|
| Compliance | Phase-gate before everything | Risk-accepted; replaced by post-hoc containment |
| Tables | 7 (ChannelProfile, TopicLane, PublishingAccount, ContentIdea, WorkOrder, PublicationRecord, FeedbackSnapshot) | 11 (merged idea+work_order; added LaneFormatMatrix, MaterialUsageLedger, MaterialInventoryForecast, AgentTickAudit, AccountFingerprint, TakedownEvent, LearningUpdate) |
| Decision engine | One formula | Stage A/B/C with explicit promotion |
| Learning | "Update weights" — undefined | Thompson sampling per lane; expert score within |
| Observability | Not addressed | First-class chapter |
| Dry-run | Not addressed | Mandatory 7-day for any new channel |
| Approval flow conflict | Not addressed | `auto_approved_by_agent`; held queue for ambiguous |
| Thumbnail | Not addressed | New node + handler |
| Multi-platform metadata | Single generic | Per-platform adapters |
| Token / quota / OAuth health | Implicit | TokenHealthGuard + alerts |
| Test harness | Not addressed | Mock adapters + 6 required scenarios in CI |
| SLAs | Not addressed | Defined with auto-pause |

## 15. The Agent in One Paragraph

> ChannelOps Agent is a long-running, dry-runnable, observable, idempotent loop that turns a configured `ChannelProfile + TopicLanes + PublishingAccounts` into a steady stream of `ProductionTask`s. Each task is scored against an explainable expert formula (Stage A), then a bandit (Stage C), and selected into one of three portfolio buckets (exploit / explore / wildcard). Tasks flow through AutoFlow for production and YouTube (and friends) for private upload, then a PublicationManager promotes them to public when policy allows, with a held queue for operator review. A MetricsCollector polls platform APIs and writes FeedbackSnapshots that feed the bandit. Guards constrain the system at every step — quota, strikes, cadence, fingerprint, inventory — and every decision is captured in AgentTickAudit for replay. The system does not pretend compliance; it accepts known risks and instead invests in containing fallout when those risks materialize.

---

**Estimated implementation effort:** 12–16 weeks for one focused engineer, or 6–8 weeks for two. The single highest-risk component is the MetricsCollector + Learning loop — without it, the agent runs blind and the bandit never wakes up; budget for that early.
