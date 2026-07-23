# Active Redis Consumer Identity Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make canary startup and the managed soak watcher fail closed unless every production Redis stream has exactly one recently active consumer with the approved 127/150 identity.

**Architecture:** Extend the existing Redis pending audit with `XINFO CONSUMERS`, a fixed 120-second active window, and per-stream name allowlists. Reuse the same policy in the Bash soak watcher, mapping violations to one fixed critical external condition without deleting historical Redis consumers.

**Tech Stack:** Python 3.12, `redis.asyncio`, pytest, Bash, Redis 7 `XINFO`, Docker Swarm contract fakes.

## Global Constraints

- The audit is read-only and must never call `XGROUP DELCONSUMER`.
- A consumer is active when `idle <= 120000` milliseconds.
- Each managed stream must have exactly one active approved consumer.
- Historical consumers outside the active window are counted but do not fail.
- Canary failure occurs before media generation, schedule opening, enqueue, or upload.
- Disabled soak state remains a successful zero-side-effect exit.
- Unknown active identity uses only `redis_consumer_identity_invalid`; consumer names never become condition codes.

---

### Task 1: Python Redis Readiness Audit

**Files:**
- Modify: `scripts/run_vp_unlisted_canary.py`
- Test: `backend/tests/services/test_unlisted_canary_runner.py`

**Interfaces:**
- Produces: `REDIS_CONSUMER_ACTIVE_IDLE_MS = 120_000`
- Produces: `REDIS_ACTIVE_CONSUMER_PATTERNS: dict[str, re.Pattern[str]]`
- Produces: `redis_pending_audit(redis_url: str) -> dict[str, Any]`
- Produces: `assert_redis_readiness_audit(report: dict[str, Any]) -> None`

- [ ] **Step 1: Add failing audit tests**

Create a fake async Redis client whose `xpending` and `xinfo_consumers` results
cover one approved active consumer plus stale history:

```python
audit = {
    "available": True,
    "streams": {
        "vp:tasks:ffmpeg": {
            "group": "ffmpeg-workers",
            "pending": 0,
            "active_consumers": ["ffmpeg-worker@150-gpu:1"],
            "stale_consumer_count": 83,
        },
        "vp:tasks:ffmpeg_go": {
            "group": "ffmpeg_go-workers",
            "pending": 0,
            "active_consumers": ["ffmpeg_go-worker@colima-127:1"],
            "stale_consumer_count": 9,
        },
        "vp:tasks:youtube_publisher": {
            "group": "youtube_publisher-workers",
            "pending": 0,
            "active_consumers": ["youtube_publisher-worker@150-publisher:1"],
            "stale_consumer_count": 0,
        },
        "vp:events": {
            "group": "orchestrator",
            "pending": 0,
            "active_consumers": ["orchestrator-api-1"],
            "stale_consumer_count": 0,
        },
    },
}
runner.assert_redis_readiness_audit(audit)
```

Parameterize failure cases for no active consumer, two active consumers,
unknown active name, malformed `idle`, unavailable `XINFO CONSUMERS`, and
nonzero pending.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py \
  -k 'redis_readiness or redis_pending' -q
```

Expected: new tests fail because identity fields and
`assert_redis_readiness_audit` do not exist.

- [ ] **Step 3: Implement the audit**

Expand the managed stream list to all four groups and add:

```python
REDIS_CONSUMER_ACTIVE_IDLE_MS = 120_000
REDIS_ACTIVE_CONSUMER_PATTERNS = {
    "vp:tasks:ffmpeg": re.compile(r"^ffmpeg-worker@150-gpu:[1-9][0-9]*$"),
    "vp:tasks:ffmpeg_go": re.compile(
        r"^ffmpeg_go-worker@colima-127:[1-9][0-9]*$"
    ),
    "vp:tasks:youtube_publisher": re.compile(
        r"^youtube_publisher-worker@150-publisher:[1-9][0-9]*$"
    ),
    "vp:events": re.compile(r"^orchestrator-api-[1-9][0-9]*$"),
}
```

For each stream, call `xpending` and `xinfo_consumers`. Validate that every
consumer has string `name` and integer, nonnegative `pending` and `idle`.
Partition names by the active threshold and store sorted active names plus the
stale count. A malformed response is recorded as
`{"available": False, "reason": "<ExceptionType>"}` for that stream.

`assert_redis_readiness_audit` validates report shape, zero pending, exactly one
active consumer, and `fullmatch` against the stream policy.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run the command from Step 2.

Expected: selected tests pass.

---

### Task 2: Canary Startup And Final Audit Separation

**Files:**
- Modify: `scripts/run_vp_unlisted_canary.py`
- Test: `backend/tests/services/test_unlisted_canary_runner.py`

**Interfaces:**
- Consumes: `redis_pending_audit` and `assert_redis_readiness_audit`
- Produces evidence key: `redis_stream_startup_audit`
- Preserves final evidence key: `redis_stream_pending_audit`

- [ ] **Step 1: Add failing live ordering tests**

Patch readiness helpers and record calls. Require this ordering before the
first media operation:

```python
assert calls[:5] == [
    "schedule_status",
    "active_backlog",
    "deployment_readiness",
    "manager_readiness",
    "redis_pending_audit",
]
assert evidence["redis_stream_startup_audit"] == safe_audit
```

Add a failure case where an unknown active consumer raises `CanaryError` and
the media generator, schedule mutation, enqueue, and upload stubs remain
uninvoked.

- [ ] **Step 2: Run the live startup tests and verify RED**

Run:

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py \
  -k 'execute_canary and redis' -q
```

Expected: failure because live startup does not run the identity audit.

- [ ] **Step 3: Add startup and final evidence**

Immediately after manager readiness in `execute_canary`, add:

```python
startup_audit = await redis_pending_audit(args.redis_url)
evidence["redis_stream_startup_audit"] = startup_audit
assert_redis_readiness_audit(startup_audit)
```

In run finalization, always store a separate final audit for live mode:

```python
if mode == MODE_LIVE:
    evidence["redis_stream_pending_audit"] = await redis_pending_audit(
        runtime_args.redis_url
    )
elif "redis_stream_pending_audit" not in evidence:
    evidence["redis_stream_pending_audit"] = await redis_pending_audit(
        runtime_args.redis_url
    )
```

Do not let a final read failure replace the original live failure.

- [ ] **Step 4: Run the runner test file**

Run:

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py -q
```

Expected: all tests pass.

---

### Task 3: Soak Guard External Condition

**Files:**
- Modify: `backend/app/services/channelops_soak_guard.py`
- Test: `backend/tests/services/test_channelops_soak_guard.py`
- Test: `backend/tests/channel_agent/test_soak_guard_cli.py`

**Interfaces:**
- Produces condition: `redis_consumer_identity_invalid`
- Consumes: existing `external_conditions` CLI path

- [ ] **Step 1: Add a failing allowed-condition test**

Extend the allowed-condition assessment test:

```python
assessment = await assess_channelops_soak(
    soak_session,
    policy,
    external_conditions=("redis_consumer_identity_invalid",),
)
assert assessment.critical_codes == ("redis_consumer_identity_invalid",)
assert "redis_consumer_identity_invalid" in ALLOWED_EXTERNAL_CONDITIONS
```

Add a CLI parser test proving
`--external-condition redis_consumer_identity_invalid` is accepted.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
cd backend
.venv/bin/python -m pytest \
  tests/services/test_channelops_soak_guard.py \
  tests/channel_agent/test_soak_guard_cli.py \
  -k consumer_identity -q
```

Expected: failure because the condition is not allowlisted.

- [ ] **Step 3: Add the fixed critical condition**

Add `"redis_consumer_identity_invalid"` to
`ALLOWED_EXTERNAL_CONDITIONS`. Do not add dynamic identity strings.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run the command from Step 2.

Expected: selected tests pass.

---

### Task 4: Managed Soak Watcher Consumer Audit

**Files:**
- Modify: `deploy/swarm/channelops-soak-watch.sh`
- Test: `tests/test_channelops_soak_watch.sh`

**Interfaces:**
- Consumes: Redis raw `XINFO CONSUMERS <stream> <group>`
- Produces external condition: `redis_consumer_identity_invalid`
- Uses fixed threshold: `consumer_active_idle_ms=120000`

- [ ] **Step 1: Extend the Docker fake and add failing cases**

Make the fake return raw consumer records:

```text
name
ffmpeg-worker@150-gpu:1
pending
0
idle
500
inactive
500
```

Provide modes `unknown_consumer`, `missing_active_consumer`,
`duplicate_active_consumer`, and `malformed_consumer`. Assert each run invokes
the guard with:

```text
--external-condition|redis_consumer_identity_invalid
```

The healthy run must make exactly four `XINFO GROUPS` and four
`XINFO CONSUMERS` calls and must not invoke `XGROUP DELCONSUMER`.

- [ ] **Step 2: Run the watcher contract and verify RED**

Run:

```bash
bash tests/test_channelops_soak_watch.sh
```

Expected: failure because the watcher never queries consumers.

- [ ] **Step 3: Implement raw consumer parsing**

Change each stream policy row to:

```text
vp:tasks:ffmpeg|ffmpeg-workers|^ffmpeg-worker@150-gpu:[1-9][0-9]*$
vp:tasks:ffmpeg_go|ffmpeg_go-workers|^ffmpeg_go-worker@colima-127:[1-9][0-9]*$
vp:tasks:youtube_publisher|youtube_publisher-workers|^youtube_publisher-worker@150-publisher:[1-9][0-9]*$
vp:events|orchestrator|^orchestrator-api-[1-9][0-9]*$
```

Use `awk` to validate complete `name`, `pending`, and `idle` records, count
stale consumers, and emit active names. Require exactly one active name and an
ERE match. Any command, parse, count, or allowlist failure adds only
`redis_consumer_identity_invalid`.

- [ ] **Step 4: Run the watcher contract and syntax checks**

Run:

```bash
bash tests/test_channelops_soak_watch.sh
bash -n deploy/swarm/channelops-soak-watch.sh
```

Expected: both commands exit 0.

---

### Task 5: Verification, Review, And Deployment

**Files:**
- Modify: `deploy/four-machine-topology.md`
- Verify all files from Tasks 1-4

**Interfaces:**
- Produces deployment evidence at one exact commit SHA
- Preserves PDS independent deployment SHA

- [ ] **Step 1: Update topology documentation**

Document the four stream identity policies, the 120-second active window,
stale-record behavior, and the new fixed guard condition. State that this is an
operational guard and does not replace future signed worker registration.

- [ ] **Step 2: Run complete verification**

Run:

```bash
cd backend
.venv/bin/python -m pytest
.venv/bin/python -m ruff check app ../scripts/run_vp_unlisted_canary.py
cd ..
go test ./...
bash tests/test_channelops_soak_watch.sh
bash tests/test_vp_unlisted_canary_scripts.sh
bash tests/test_vp_deploy_sync_extension.sh
git diff --check
```

Expected: all required tests pass. Existing repository-wide advisory lint
findings must be reported separately and must not hide new findings.

- [ ] **Step 3: Request code review and address findings**

Review correctness at the Redis response boundary, failure ordering, Bash raw
record parsing, condition-code safety, and rollout compatibility. Repeat
focused tests after every fix.

- [ ] **Step 4: Commit and push only intended files**

Explicitly stage the design, plan, implementation, tests, and topology
documentation. Do not stage
`vp_autonomous_production_feedback_loop_plan.md`.

Use:

```bash
git commit -m "fix: fence production Redis consumer identities"
git push origin main
```

- [ ] **Step 5: Verify unattended deployment**

Require exact-SHA GitHub CI success, then wait for the 150 cron to deploy
`vp-app` and `vp-feature-aggregator`. Verify:

- all VideoProcess services are `1/1`;
- 127 and 150 use the exact image tag;
- PDS remains on its independent SHA;
- no VideoProcess task runs on 126;
- soak state remains disabled before a successful canary.

- [ ] **Step 6: Run production read-only preflight**

Load protected connection values without printing them and run:

```bash
PYTHONPATH=backend backend/.venv/bin/python scripts/run_vp_unlisted_canary.py \
  --preflight-only \
  --manager-ssh-jump 10.0.0.127 \
  --shared-services-ssh-host 10.0.0.127
```

Require one approved active consumer per stream, zero pending/lag, empty
backlog, `CLOSED` schedule, authenticated YouTubeManager, and `0600` evidence.
Do not run a live canary without a fresh single-attempt authorization.
