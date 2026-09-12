# Native Owned Inventory: Offline Model and Activation Evidence

This is Task 6's offline portion at the reviewed C foundation. It adds no
activation switch, endpoint, runner, watcher, SQL authority or live action.
Task D's final-effect backstops and Task 5's stops/completion reporting remain
separate prerequisites. Passing these models is not implementation-wide GO,
a measured latency SLO, seven real uploads, or 168 hours of observation.

## Shared Window Model

Both test suites consume `backend/tests/fixtures/owned_inventory_window_model.json`.
The supplied operational window is UTC OPEN 08:00, DRAINING 13:00, CLOSED 14:00;
these tests do not verify or change the deployed schedule.

The model starts September 11 at 08:00 and expires September 18 at 08:00, with
seven fixed inputs, one outstanding item, and both native rolling 24-hour floors.
Eligibility just misses a polling boundary; rounding plus queue delay is bounded
by 2 minutes, admission-to-upload-completion by 20 minutes, and completion-to-normal
publication/reconcile settlement by 95 minutes. All three are assumptions to
measure before activation, not actual timing evidence.

| Case | Modeled admissions | Last modeled upload completion | Last modeled settlement | Result |
| --- | ---: | --- | --- | --- |
| Minute polling, worst phase + queue delay | 7 | Day 7 10:34 | Day 7 12:09 | Fits assumptions only |
| Hourly polling | 6 | Day 7 10:21 | Day 7 11:56 | Incomplete; rounding misses an OPEN window |
| Third OPEN window missed | 6 | Day 7 09:28 | Day 7 11:03 | Incomplete; no backfill |
| Upload completion takes 120 minutes | 6 | Day 7 14:06 | Day 7 15:41 | Outside assumptions/window; not qualified |
| Settlement takes 25 hours | 6 | Day 7 09:44 | Day 8 10:44 | Outstanding slot retained; not qualified |

The nominal recurrence is C1 <= 08:22 and Cn <= C(n-1) + 24h + 22m.
The seventh nominal settlement is still only 148h09m after the first OPEN;
it cannot satisfy the separate 168-hour actual-observation requirement.
Out-of-bound timestamps illustrate failed assumptions, not permission for a late
POST, an expiry bypass, or a prediction that the final-effect guards allow it.
The model does not turn a latency overrun into a new production hold rule.

Python exercises the actual profile scheduler, `read_phase`, and atomic `tick`
against in-memory SQLite. Only the DB clock, PostgreSQL-only evidence transport,
PDS and normal downstream lifecycle observations are synthetic. Every prior
modeled task remains in the A1 27-set snapshot; due metrics receive synthetic
normal success and future stages stay pending. Go exercises the actual profile
bucket and `assessOwnedInventory`/B1 history assessment with equivalent retained
facts, then applies test-only reservation state transitions. Go's transaction
commit is covered by the separately qualified B2 PG tests, not this model.

Tests assert literal shared traces, source/task uniqueness, lowest ordinal,
one outstanding item, exact completion+24h boundaries, attempt spacing, daily
OPEN/DRAINING/CLOSED refusal, exhaustion/expiry and no compressed catch-up.
Independent boundary probes are not a live controller or a simulated clock
advance used as acceptance evidence. The seventh item's durable finalization
remains Task 5's normal completion path, not a new Task 6 helper.

## Existing GET Surfaces and Completion Readout

Use the existing GET
`/api/v1/channel-agent/channels/{channel_id}/owned-seed-inventories/{inventory_id}`.
It currently returns `id`, `state`, `manifest`, `manifest_sha256`, approval and
revocation subject/time/reference, `hold_reason`, `succession_released_at`,
`items[{id,state,manual_seed_id,production_task_id}]`, and `closeout`.
The manifest already binds scope, UTC window, ordinals, asset IDs, source hashes
and provenance. V2 GET retains its existing operator authorization dependency.
Never expose credentials, tokens or raw sensitive account configuration.

`closeout.status=ready` currently concerns a narrowly verified unused revoked
inventory. It is not a used-inventory completion or seven-day success signal.
Do not infer settlement from `state=exhausted` or seven consumed items.

The existing `/internal/schedule/video/status` GET supplies `service_name`,
`state`, `guarded_job_id`, waiting/active job and queued/running node counts,
`updated_at`, `updated_by`, and `released_jobs`. Reuse it; `updated_at` is the
last schedule update, not the observation clock. Do not add a second schedule
reader or make an activation decision from two non-atomic GETs.

V2 GET now adds `assessment` only after the existing operator authorization,
using `check_owned_inventory_feedback(..., apply=False)`. V1 GET/output/auth and
all existing V2 manifest/items/closeout fields remain unchanged. This is a bounded
observation, not approval, a new classifier, or a mutation/commit/hold/enqueue path.

| Assessment field | Implemented meaning |
| --- | --- |
| `status` | `observed` when the helper returns without a hold reason; `blocked` when it returns a hold reason; `unavailable` when assessment raises. None means seven-day acceptance. |
| `observation_only` | Always `true`; no intake or publication authority is granted. |
| `hold_reason` | Existing helper reason, or the static `owned_inventory_assessment_unavailable` when assessment fails. The persisted top-level hold reason remains separate and unchanged. |
| `metrics` | Existing bounded intake/settlement/feedback statuses, item counts and due/future/succeeded metric counts. No duplicated stage classifier or new eligibility calculation. Empty when unavailable. |
| `completed_item_ids` | Existing helper's observed completion candidates; GET does not mark these reserved items completed. Empty when unavailable; not a replacement for persisted item state. |
| `observed_at` | Explicitly `null`: the existing helper does not expose its database observation clock. No wall-clock, approval-time or inferred timestamp is substituted. |

Stale/read-failed proof retains the helper's blocking reason/status; unexpected
assessment failure returns `unavailable`, never complete. Original Redis phase,
freshness and rollback behavior are reused, with no new external reader. The
existing inventory read still determines ordinary scope/manifest errors.
No account-wide watermarks, next eligibility, per-item receipt expansion or
continuous-observation timestamps are added by this small readout slice.
The operator's external evidence record still supplies actual observation
start/end and detailed receipt/audit references. Neither repeated GETs, approval
time nor snapshot completion establishes continuous 168-hour observation.

## Parent-Owned Activation and Acceptance

1. Complete D/Task 5 integration, required backend/Go checks, scoped lint/type
   checks, restricted-role disposable PG/Redis qualification and independent
   reviews. Confirm the reviewed ACK release and native settlement paths.
2. With native intake paused and global scheduling CLOSED, qualify seven actual
   distinct owned/generated video bytes/provenance and render QA, the exact
   account/actual YouTube channel, immutable manifest and retained history.
   Qualify real non-degraded PDS request/response evidence for candidate, plan
   and promotion; approved 150 vision/127 CPU placement with no 126; clean
   queues and fresh healthy native watcher. Named Redis observation must use
   the existing qualified credentials, never default-user fallback.
3. Measure scheduler/queue, render/upload and publication/reconcile timing
   against the modeled 2/20/95-minute bounds, or obtain a re-reviewed bounded
   model. Configure and approve the finite scope once through the existing
   operator API. No flag is enabled by this document or commit, including v2.
4. Parent alone enables the existing watcher/native intake after prerequisites.
   Other channels remain paused. Existing normal UTC window transitions govern
   admission; no exact-job canary loop, replacement POST, renewal, catch-up or
   implicit window extension is authorized.
5. Keep actual timestamped evidence for >=168 hours: seven distinct normal
   tasks, source bytes and unlisted videos; exact M/V/receipt/audit links;
   no duplicate/replacement POST; both rolling floors; real PDS; all then-due
   metrics/reconciliations healthy. Missing or held days mean incomplete
   acceptance, even if source tests and the nominal model pass.
6. Report intake settlement separately from full feedback completion. The last
   video's normal 7d metric stage can mature roughly another week later.
   Retain future queues and prior history. Existing submitted operations may
   settle after intake closes; never rewrite a failed result to manufacture
   completion. Public publishing remains outside this approval.

No live configuration, assets, PDS identity, service placement, watcher health,
Redis identity or timing has been qualified by this offline task.
