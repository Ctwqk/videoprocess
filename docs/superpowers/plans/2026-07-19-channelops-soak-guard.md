# ChannelOps Soak Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (\`- [ ]\`) syntax for tracking.

**Goal:** Build a repository-owned, push-deployed ChannelOps soak guard that remains disabled until explicitly activated and automatically quarantines one configured channel when long-running unlisted production becomes unsafe.

**Architecture:** A SQLAlchemy service evaluates channel-scoped database invariants and reuses the existing quarantine state machine. A small Python CLI runs that service from the deployed publisher image, while a host shell watcher contributes Swarm, placement, and Redis conditions. The VP deploy extension atomically installs the watcher and owns one marked cron block after service health succeeds.

**Tech Stack:** Python 3.12, SQLAlchemy async, Pydantic settings, pytest, Bash, Docker Swarm, Redis Streams, cron.

## Global Constraints

- The watcher is disabled unless an explicit state file enables one channel UUID.
- No task in this plan uploads, publishes, resumes a channel, or opens the video schedule.
- Publication privacy remains limited to \`private\` and \`unlisted\`; \`public\` is critical.
- External-platform asset automatic publication remains disabled and human review is required.
- Guard mutations may only reduce activity: halt, hold, cancel, dead-letter, and close the schedule.
- The installed script contains no credential fallback and never prints connection strings or tokens.
- VP services and guard commands never target 126.
- Existing APIs and quarantine defaults remain backward compatible.

---

### Task 1: Parameterized Atomic Quarantine

**Files:**
- Modify: \`backend/app/services/channelops_quarantine.py\`
- Modify: \`backend/tests/services/test_channelops_quarantine.py\`

**Interfaces:**
- Produces: \`quarantine_channelops_backlog(db, channel_id, *, apply=False, now=None, reason=QUARANTINE_REASON, close_schedule=False) -> dict[str, Any]\`.
- Produces: report key \`schedule\` with \`requested_close\`, \`changed\`, \`previous_state\`, and \`final_state\`.
- Preserves: existing default reason and existing changed/retained ID report shape.

- [ ] **Step 1: Add failing custom-reason and schedule-close tests**

Add \`RuntimeSchedule.__table__\` to the SQLite fixture and assert that a custom
reason propagates to the channel, task, job, node, queue item, and transition
history while the schedule closes in the same call:

\`\`\`python
SOAK_REASON = "automated_channelops_soak_guard"

report = await quarantine_channelops_backlog(
    quarantine_session,
    rows["target"].id,
    apply=True,
    now=NOW,
    reason=SOAK_REASON,
    close_schedule=True,
)
schedule = await quarantine_session.get(RuntimeSchedule, VIDEO_SCHEDULE_SERVICE)
assert schedule is not None
assert schedule.state == VideoScheduleState.CLOSED.value
assert schedule.updated_by == SOAK_REASON
assert report["schedule"] == {
    "requested_close": True,
    "changed": True,
    "previous_state": None,
    "final_state": "CLOSED",
}
assert rows["target"].halt_reason == SOAK_REASON
assert rows["active_task"].blocked_by_guard == SOAK_REASON
assert rows["active_job"].error_message == SOAK_REASON
assert rows["active_node"].error_message == SOAK_REASON
assert rows["running"].last_error == SOAK_REASON
\`\`\`

Also call it a second time and assert no duplicate transition and
\`schedule["changed"] is False\`. Add validation tests rejecting an empty reason
and a reason longer than 255 characters before mutation.

- [ ] **Step 2: Run the focused tests and verify failure**

\`\`\`bash
cd backend
.venv/bin/python -m pytest tests/services/test_channelops_quarantine.py -q
\`\`\`

Expected: calls fail because \`reason\` and \`close_schedule\` are not accepted.

- [ ] **Step 3: Implement the parameterized quarantine**

Validate the reason at function entry, use it in \`_already_quarantined\`,
\`_apply_channel_halt\`, \`_hold_task\`, \`_cancel_job\`, \`_cancel_node\`, and
\`_dead_letter_queue_item\`, and lock/create the \`RuntimeSchedule\` row inside the
existing \`db.begin()\` block when \`close_schedule=True\`. Do not call a helper
that commits independently.

The schedule mutation is:

\`\`\`python
schedule.state = VideoScheduleState.CLOSED.value
schedule.updated_by = reason
schedule.updated_at = changed_at
\`\`\`

When the row does not exist, add:

\`\`\`python
RuntimeSchedule(
    service_name=VIDEO_SCHEDULE_SERVICE,
    state=VideoScheduleState.CLOSED.value,
    updated_by=reason,
    updated_at=changed_at,
)
\`\`\`

- [ ] **Step 4: Run focused tests**

\`\`\`bash
cd backend
.venv/bin/python -m pytest tests/services/test_channelops_quarantine.py -q
\`\`\`

Expected: all quarantine tests pass.

- [ ] **Step 5: Commit**

\`\`\`bash
git add backend/app/services/channelops_quarantine.py \
  backend/tests/services/test_channelops_quarantine.py
git commit -m "feat: make channel quarantine guard-aware"
\`\`\`

### Task 2: Database Soak Assessment And CLI

**Files:**
- Create: \`backend/app/services/channelops_soak_guard.py\`
- Create: \`backend/app/channel_agent/soak_guard_cli.py\`
- Create: \`backend/tests/services/test_channelops_soak_guard.py\`
- Create: \`backend/tests/channel_agent/test_soak_guard_cli.py\`

**Interfaces:**
- Consumes: parameterized \`quarantine_channelops_backlog(..., reason=SOAK_GUARD_REASON, close_schedule=True)\`.
- Produces: immutable \`SoakGuardPolicy(channel_id, started_at, max_publications_per_24h=1, upload_stale_minutes=45, feedback_grace_hours=30)\`.
- Produces: immutable \`SoakGuardAssessment(critical_codes, metrics)\` with property \`healthy\`.
- Produces: \`assess_channelops_soak(db, policy, *, external_conditions=(), now=None) -> SoakGuardAssessment\`.
- Produces CLI: \`python -m app.channel_agent.soak_guard_cli --channel-id UUID --started-at RFC3339 [--external-condition CODE ...] [--apply]\`.

- [ ] **Step 1: Add failing assessment tests**

Build an in-memory SQLite graph with one enabled, non-dry-run channel, one
enabled account using \`unlisted\`, one enabled lane format using \`unlisted\`, and
an activation timestamp before all rows. Cover these exact codes:

\`\`\`python
assert (await assess_channelops_soak(db, policy, now=NOW)).critical_codes == ()

account.default_privacy = "public"
assert "unsafe_account_privacy" in assessment.critical_codes

account.external_asset_auto_publish = True
assert "external_asset_auto_publish_enabled" in assessment.critical_codes

publication.current_privacy = "public"
assert "unsafe_publication_privacy" in assessment.critical_codes

operation.status = "uncertain"
assert "ambiguous_upload_operation" in assessment.critical_codes

queue.status = "dead_lettered"
assert "channelops_queue_failure" in assessment.critical_codes

assert "publication_cadence_exceeded" in cadence_assessment.critical_codes
assert "feedback_missing_after_grace" in feedback_assessment.critical_codes
assert "service_unhealthy" in external_assessment.critical_codes
\`\`\`

Also cover missing/disabled/dry-run/halted channel, unsafe lane privacy,
stale \`reserved\`/\`submitted\` upload operations, failed/held tasks, and an
external-asset task reaching an upload/publication state without human
approval mode. Assert metrics contain counts only, never titles, prompts,
URLs, IDs other than the configured channel ID, or error payloads.

- [ ] **Step 2: Run assessment tests and verify import failure**

\`\`\`bash
cd backend
.venv/bin/python -m pytest tests/services/test_channelops_soak_guard.py -q
\`\`\`

Expected: import failure for \`app.services.channelops_soak_guard\`.

- [ ] **Step 3: Implement deterministic assessment**

Use SQLAlchemy \`select()\` expressions and channel-scoped joins. Normalize
timestamps to UTC before comparison. Return sorted, de-duplicated codes from a
fixed allowlist:

\`\`\`python
ALLOWED_EXTERNAL_CONDITIONS = frozenset({
    "forbidden_node_placement",
    "redis_group_missing",
    "redis_pending_exceeded",
    "service_missing",
    "service_unhealthy",
})
\`\`\`

Reject unknown external condition codes with \`ValueError\`. Allowed privacy is
\`{"private", "unlisted"}\`. Treat external-asset tasks as critical after they
reach \`uploaded_private\`, \`scheduled\`, \`published\`, or \`measured\` unless
\`approval_mode == "human"\`.

- [ ] **Step 4: Add failing CLI tests**

Patch the session factory and service calls. Assert healthy output returns
\`0\`, a critical assessment returns \`20\`, and \`--apply\` calls:

\`\`\`python
quarantine.assert_awaited_once_with(
    AnyAsyncSession,
    channel_id,
    apply=True,
    reason=SOAK_GUARD_REASON,
    close_schedule=True,
    now=ANY,
)
\`\`\`

Assert invalid UUID/timestamp/threshold/external code returns \`2\`, database
failure returns \`3\`, and JSON output contains no environment values.

- [ ] **Step 5: Implement the CLI orchestration**

Parse and validate arguments without importing settings at module import time.
Open one read session for assessment, close it, then open a fresh session for
quarantine when \`--apply\` and critical codes are present. Emit one JSON object
with \`status\`, \`critical_codes\`, \`metrics\`, and optional quarantine counts.
Never serialize exception text from database URLs.

- [ ] **Step 6: Run focused service and CLI tests**

\`\`\`bash
cd backend
.venv/bin/python -m pytest \
  tests/services/test_channelops_quarantine.py \
  tests/services/test_channelops_soak_guard.py \
  tests/channel_agent/test_soak_guard_cli.py -q
\`\`\`

Expected: all focused tests pass.

- [ ] **Step 7: Commit**

\`\`\`bash
git add backend/app/services/channelops_soak_guard.py \
  backend/app/channel_agent/soak_guard_cli.py \
  backend/tests/services/test_channelops_soak_guard.py \
  backend/tests/channel_agent/test_soak_guard_cli.py
git commit -m "feat: add channelops soak guard"
\`\`\`

### Task 3: Opt-In Host Watcher

**Files:**
- Create: \`deploy/swarm/channelops-soak-watch.sh\`
- Create: \`tests/test_channelops_soak_watch.sh\`

**Interfaces:**
- Consumes state: \`$DEPLOY_GITHUB_SYNC_ROOT/state/vp-soak-watch.env\`.
- Consumes deploy credential name: \`VP_PYTHON_WORKER_DATABASE_URL\`.
- Consumes CLI from Task 2 in the deployed publisher image.
- Produces fixed external condition arguments and concise key/value logs.

- [ ] **Step 1: Write failing shell contract tests**

Use a temporary root and fake \`docker\` binary. Verify missing state is a
successful no-op, invalid activation fails before Docker, healthy enabled state
inspects every required service and stream, and the Python module receives
\`--env DATABASE_URL\` without a connection string in command logs.

Also verify an unhealthy service and 126 placement become
\`--external-condition service_unhealthy\` and
\`--external-condition forbidden_node_placement\`,
\`VP_SOAK_AUTO_HOLD=false\` omits \`--apply\`, true includes it, unknown Redis
groups are critical, the \`youtube_publisher-workers\` group is checked, and the
watcher never issues upload, resume, enqueue, or schedule-open commands.

- [ ] **Step 2: Run the shell test and verify the watcher is missing**

\`\`\`bash
bash tests/test_channelops_soak_watch.sh
\`\`\`

Expected: failure because \`deploy/swarm/channelops-soak-watch.sh\` is absent.

- [ ] **Step 3: Implement the watcher**

Use \`set -euo pipefail\`, source the deploy environment without echoing it, and
validate the UUID, RFC3339 UTC timestamp, and positive integer thresholds.
Required services are the eight normal VP services plus
\`vp-feature-aggregator-swarm\`, \`vp-pds-swarm\`,
\`vp-ffmpeg-worker-gpu-swarm\`, and \`vp-youtube-publisher-swarm\`. Inspect
running task nodes and reject a configurable forbidden pattern defaulting to
\`CASPERs-Mac-mini|10.0.0.126\`.

Check these stream/group pairs with \`XINFO GROUPS\`:

\`\`\`text
vp:tasks:ffmpeg_go|ffmpeg_go-workers
vp:tasks:ffmpeg|ffmpeg-workers
vp:tasks:youtube_publisher|youtube_publisher-workers
vp:events|orchestrator
\`\`\`

Export \`DATABASE_URL="$VP_PYTHON_WORKER_DATABASE_URL"\` and pass only
\`--env DATABASE_URL\` to \`docker run\`. Treat CLI exit \`20\` as an observed
guard trip while preserving a nonzero watcher exit for cron.

- [ ] **Step 4: Run syntax and shell contract tests**

\`\`\`bash
bash -n deploy/swarm/channelops-soak-watch.sh
bash tests/test_channelops_soak_watch.sh
\`\`\`

Expected: both pass.

- [ ] **Step 5: Commit**

\`\`\`bash
git add deploy/swarm/channelops-soak-watch.sh tests/test_channelops_soak_watch.sh
git commit -m "feat: add opt-in channelops soak watcher"
\`\`\`

### Task 4: Push-Deployed Watcher And Managed Cron

**Files:**
- Modify: \`deploy/swarm/deploy-sync-extension.sh\`
- Modify: \`tests/test_vp_deploy_sync_extension.sh\`

**Interfaces:**
- Consumes: \`ROOT\`, \`REPO_ROOT\`, and \`UPDATE_SERVICES\` from the existing deploy controller.
- Produces: \`vp_install_soak_watch() -> 0|1\`.
- Produces: installed file \`$ROOT/bin/channelops-soak-watch.sh\` and one marked cron block.

- [ ] **Step 1: Add failing deployment contract tests**

Give the test harness a temporary \`ROOT\`, a fake crontab containing unrelated
entries plus the historical unmarked watcher line, and
\`VP_SOAK_WATCH_SOURCE="$ROOT_DIR/deploy/swarm/channelops-soak-watch.sh"\`.
After a successful \`deploy_vp_app_services\`, assert the installed file is
executable and byte-identical, one marked cron block exists, the historical
line is gone, exactly one watcher command remains, and unrelated entries are
unchanged.

Assert \`UPDATE_SERVICES=0\`, failed service convergence, and invalid watcher
syntax do not rewrite the fake crontab.

- [ ] **Step 2: Run the deploy contract and verify failure**

\`\`\`bash
bash tests/test_vp_deploy_sync_extension.sh
\`\`\`

Expected: missing \`vp_install_soak_watch\` behavior or installed file.

- [ ] **Step 3: Implement idempotent installation**

Add \`vp_install_soak_watch()\` which:

1. returns after a logged skip when \`UPDATE_SERVICES=0\`;
2. checks the source is readable and passes \`bash -n\`;
3. creates \`$ROOT/bin\`, \`$ROOT/logs\`, and \`$ROOT/state\`;
4. installs the source with \`install -m 0755\`;
5. removes any prior marked block and historical unmarked watcher line from a
   temporary crontab while preserving all other lines;
6. appends exactly one managed block and calls \`crontab\` with the temporary file;
7. cleans temporary files on both success and failure.

Call it only after all services in \`vp_apply_app_services()\` pass
\`swarm_service_running\`. Return failure so the deployment is not marked
successful when operations assets do not converge.

- [ ] **Step 4: Run deploy and watcher contracts**

\`\`\`bash
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_channelops_soak_watch.sh
bash -n deploy/swarm/deploy-sync-extension.sh
\`\`\`

Expected: all pass.

- [ ] **Step 5: Commit**

\`\`\`bash
git add deploy/swarm/deploy-sync-extension.sh tests/test_vp_deploy_sync_extension.sh
git commit -m "feat: deploy channelops soak guard automatically"
\`\`\`

### Task 5: Full Verification And Disabled Production Deployment

**Files:**
- Modify: \`deploy/four-machine-topology.md\`

**Interfaces:**
- Documents the managed watcher, activation file, fail-closed behavior, and 150-only cron ownership.
- Produces production evidence without enabling a channel or creating external side effects.

- [ ] **Step 1: Update the topology runbook**

Document the 150 scoped deploy controller as watcher owner, installed and state
paths, default-disabled behavior, quarantine plus \`CLOSED\` guard action, and a
separately approved successful unlisted canary as activation prerequisite. The
canary gate uses the 2026-07-22 atomic intake pause: success remains
intake-paused for downstream and mature metrics; failure becomes fully halted.
State explicitly that code deployment cannot activate or resume a channel and
that 126 remains forbidden.

- [ ] **Step 2: Run repository verification**

\`\`\`bash
cd backend
.venv/bin/python -m pytest
.venv/bin/python -m ruff check . || true
.venv/bin/python -m mypy app || true
cd ..
go test ./...
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_channelops_soak_watch.sh
git diff --check
\`\`\`

Expected: backend tests, Go tests, and both shell contracts pass. Record any
pre-existing Ruff or mypy baseline failures separately; no changed file may
introduce a new violation.

- [ ] **Step 3: Commit the runbook**

\`\`\`bash
git add deploy/four-machine-topology.md
git commit -m "docs: document managed soak guard"
\`\`\`

- [ ] **Step 4: Push and let scoped cron deploy naturally**

\`\`\`bash
git push origin main
\`\`\`

Do not invoke the deploy controller manually. Wait for the next scoped
15-minute cron and verify its log records the new commit for \`vp-app\`.

- [ ] **Step 5: Verify disabled production state**

From 150, verify the installed file is executable and syntax-valid, exactly one
managed cron entry exists, direct watcher output is \`status=disabled\`, all VP
services are at desired replicas, no VP task runs on 126, no enabled
non-dry-run unhalted channel exists, and upload-operation and publication counts
are unchanged from the pre-deploy baseline.

- [ ] **Step 6: Keep real canary gated**

Do not create \`vp-soak-watch.env\` and do not run the live canary. Report the
exact approval phrase required for the next attempt:

\`\`\`text
批准第四次 unlisted canary
\`\`\`
