# Read-Only Unlisted Canary Preflight Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repeatable `--preflight-only` mode that proves live-canary readiness and writes sanitized evidence without changing production application state or contacting an upload endpoint.

**Architecture:** Keep the existing canary runner as the single readiness owner. Add a pure CLI mode selector, a read-only preflight executor that reuses existing readiness helpers, and mode-aware finalization so only the live path closes a schedule or performs canary cleanup.

**Tech Stack:** Python 3.12, asyncio, SQLAlchemy async sessions, httpx, pytest, Bash contract tests.

## Global Constraints

- `--preflight-only` and `--confirm-live-unlisted` are mutually exclusive and exactly one is required.
- Preflight may read PostgreSQL, Redis, Docker/SSH readiness, and YouTubeManager auth/quota state.
- Preflight must not generate media, upload assets, write application rows, enqueue work, mutate schedule state, or invoke YouTube upload/publication operations.
- Evidence remains sanitized and is written locally with mode `0600`.
- Existing live-canary cleanup and fail-closed schedule behavior must remain unchanged.

---

### Task 1: CLI Mode And Read-Only Executor

**Files:**
- Modify: `scripts/run_vp_unlisted_canary.py`
- Test: `backend/tests/services/test_unlisted_canary_runner.py`

**Interfaces:**
- Produces: `execution_mode(args: argparse.Namespace) -> str`
- Produces: `execute_preflight(args, db, client, evidence, path) -> None`
- Consumes: existing readiness and evidence helpers.

- [ ] **Step 1: Write failing mode-validation tests**

```python
from types import SimpleNamespace


@pytest.mark.parametrize(
    ("preflight", "live", "expected"),
    ((True, False, "preflight_only"), (False, True, "live_unlisted")),
)
def test_execution_mode_accepts_exactly_one_mode(preflight, live, expected):
    runner = load_runner()
    args = SimpleNamespace(
        preflight_only=preflight,
        confirm_live_unlisted=live,
    )
    assert runner.execution_mode(args) == expected


@pytest.mark.parametrize(("preflight", "live"), ((False, False), (True, True)))
def test_execution_mode_rejects_ambiguous_mode(preflight, live):
    runner = load_runner()
    args = SimpleNamespace(
        preflight_only=preflight,
        confirm_live_unlisted=live,
    )
    with pytest.raises(runner.CanaryError, match="exactly one"):
        runner.execution_mode(args)
```

- [ ] **Step 2: Run the mode tests and verify RED**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py -k execution_mode -q
```

Expected: FAIL because `execution_mode` and `preflight_only` do not exist.

- [ ] **Step 3: Add the CLI flag and minimal mode selector**

```python
MODE_PREFLIGHT = "preflight_only"
MODE_LIVE = "live_unlisted"


def execution_mode(args: argparse.Namespace) -> str:
    preflight = bool(args.preflight_only)
    live = bool(args.confirm_live_unlisted)
    if preflight == live:
        raise CanaryError(
            "exactly one of --preflight-only or --confirm-live-unlisted is required"
        )
    return MODE_PREFLIGHT if preflight else MODE_LIVE
```

Add `parser.add_argument("--preflight-only", action="store_true", default=False)` next to the existing confirmation flag.

- [ ] **Step 4: Run the mode tests and verify GREEN**

Run the command from Step 2.

Expected: all selected tests PASS.

- [ ] **Step 5: Write the failing read-only executor test**

```python
@pytest.mark.anyio
async def test_execute_preflight_reads_readiness_without_live_side_effects(
    db, monkeypatch, tmp_path
):
    runner = load_runner()
    calls = []

    async def schedule_status(*_args):
        calls.append("schedule_status")
        return {"state": "CLOSED", "active_jobs": 0}

    async def deployment_readiness(*_args):
        calls.append("deployment_readiness")
        return {"ready": True}

    async def manager_readiness(*_args):
        calls.append("manager_readiness")
        return {"authenticated": True}

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("preflight invoked a live mutation")

    monkeypatch.setattr(runner, "schedule_status", schedule_status)
    monkeypatch.setattr(runner, "deployment_readiness", deployment_readiness)
    monkeypatch.setattr(runner, "manager_readiness", manager_readiness)
    monkeypatch.setattr(runner, "mutate_schedule", forbidden)
    monkeypatch.setattr(runner, "close_schedule", forbidden)
    monkeypatch.setattr(runner, "execute_canary", forbidden)
    evidence = {
        "mode": runner.MODE_PREFLIGHT,
        "status": "running",
        "schedule": {"transitions": [], "final_state": None},
    }
    path = tmp_path / "preflight.json"

    await runner.execute_preflight(object(), db, object(), evidence, path)

    assert calls == [
        "schedule_status",
        "deployment_readiness",
        "manager_readiness",
    ]
    assert evidence["status"] == "succeeded"
    assert evidence["schedule"]["final_state"] == "CLOSED"
    assert evidence["preflight_backlog"]["active_job_ids"] == []
    assert path.exists()
```

- [ ] **Step 6: Run the executor test and verify RED**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py -k execute_preflight -q
```

Expected: FAIL because `execute_preflight` does not exist.

- [ ] **Step 7: Implement the minimal read-only executor**

```python
async def execute_preflight(args, db, client, evidence, path) -> None:
    initial_schedule = await schedule_status(args, client)
    record_schedule(evidence, "initial", initial_schedule)
    evidence["schedule"]["final_state"] = initial_schedule.get("state")
    if initial_schedule.get("state") != "CLOSED":
        raise CanaryError("global video schedule must be CLOSED before canary preflight")
    backlog = await active_backlog(db)
    evidence["preflight_backlog"] = backlog
    assert_no_preexisting_backlog(backlog)
    evidence["deployment"] = await deployment_readiness(args, client)
    evidence["manager"] = {"auth": await manager_readiness(args, client)}
    evidence["status"] = "succeeded"
    atomic_write_json(path, evidence)
```

- [ ] **Step 8: Run the executor and full runner test file**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py -q
```

Expected: all tests PASS.

### Task 2: Mode-Aware Run Finalization And Contract

**Files:**
- Modify: `scripts/run_vp_unlisted_canary.py`
- Modify: `tests/test_vp_unlisted_canary_scripts.sh`
- Test: `backend/tests/services/test_unlisted_canary_runner.py`

**Interfaces:**
- Consumes: `execution_mode(args)` from Task 1.
- Produces: `execute_selected_mode(mode, args, db, client, evidence, path) -> None`.
- Produces: `close_schedule_for_mode(mode, args, client, evidence) -> None`.
- Preserves: `run(args, database_url) -> Path` and `main() -> int`.

- [ ] **Step 1: Add a failing finalization test**

```python
from unittest.mock import AsyncMock


@pytest.mark.anyio
async def test_mode_aware_dispatch_and_schedule_close(monkeypatch):
    runner = load_runner()
    preflight = AsyncMock()
    canary = AsyncMock()
    close = AsyncMock()
    monkeypatch.setattr(runner, "execute_preflight", preflight)
    monkeypatch.setattr(runner, "execute_canary", canary)
    monkeypatch.setattr(runner, "close_schedule", close)
    values = (object(), object(), object(), {}, Path("evidence.json"))

    await runner.execute_selected_mode(runner.MODE_PREFLIGHT, *values)
    await runner.close_schedule_for_mode(
        runner.MODE_PREFLIGHT, values[0], values[2], values[3]
    )

    preflight.assert_awaited_once_with(*values)
    canary.assert_not_awaited()
    close.assert_not_awaited()

    await runner.execute_selected_mode(runner.MODE_LIVE, *values)
    await runner.close_schedule_for_mode(
        runner.MODE_LIVE, values[0], values[2], values[3]
    )

    canary.assert_awaited_once_with(*values)
    close.assert_awaited_once_with(values[0], values[2], values[3])
```

- [ ] **Step 2: Run the finalization test and verify RED**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py -k mode_aware -q
```

Expected: FAIL because the run path always assumes a live canary.

- [ ] **Step 3: Make `run` and `main` mode-aware**

Add these dispatch helpers:

```python
async def execute_selected_mode(mode, args, db, client, evidence, path) -> None:
    if mode == MODE_PREFLIGHT:
        await execute_preflight(args, db, client, evidence, path)
        return
    await execute_canary(args, db, client, evidence, path)


async def close_schedule_for_mode(mode, args, client, evidence) -> None:
    if mode == MODE_LIVE:
        await close_schedule(args, client, evidence)
```

Compute `mode = execution_mode(args)` before evidence creation. Record the mode and mode-specific safety contract. Replace the direct `execute_canary` call with `execute_selected_mode`. In `finally`, replace the direct `close_schedule` call with `close_schedule_for_mode`; always record the read-only Redis pending audit and completion timestamp. Restrict failure cleanup to `mode == MODE_LIVE`.

In `main`, validate the mode before reading `DATABASE_URL`, return exit 2 on ambiguity, and print mode-specific success/failure text.

- [ ] **Step 4: Extend the shell contract**

Add checks for `--preflight-only`, `MODE_PREFLIGHT`, and the live-mode guard around `close_schedule`. Keep the existing AST assertion that live close remains inside a `finally` block.

- [ ] **Step 5: Run targeted verification**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py -q
cd ..
bash tests/test_vp_unlisted_canary_scripts.sh
backend/.venv/bin/python -m py_compile scripts/run_vp_unlisted_canary.py
git diff --check
```

Expected: every command exits 0.

- [ ] **Step 6: Commit implementation**

```bash
git add scripts/run_vp_unlisted_canary.py \
  backend/tests/services/test_unlisted_canary_runner.py \
  tests/test_vp_unlisted_canary_scripts.sh \
  docs/superpowers/plans/2026-07-19-unlisted-canary-readonly-preflight.md
git commit -m "feat: add read-only YouTube canary preflight"
```

### Task 3: Full Verification, Deploy, And Production Preflight

**Files:**
- Runtime evidence: `.runtime/youtube-canary/unlisted-canary-preflight-<run-id>.json`

**Interfaces:**
- Consumes: deployed `scripts/run_vp_unlisted_canary.py --preflight-only`.
- Produces: sanitized E4 preflight evidence without an upload or publication.

- [ ] **Step 1: Run repository verification**

```bash
cd backend
.venv/bin/python -m pytest
cd ..
go test ./...
bash tests/test_vp_unlisted_canary_scripts.sh
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 2: Push and observe automatic deployment**

```bash
git push origin main
```

Wait for the existing 15-minute scoped deployment cron. Verify VP images use the new commit, every VP service is `1/1`, PDS is independently checked, and `colima-swarmbridged` has no VP task.

- [ ] **Step 3: Run one production read-only preflight**

Use a temporary SSH tunnel for Postgres and Redis only, set `DATABASE_URL` and `REDIS_URL` in the invoking process, then run:

```bash
backend/.venv/bin/python scripts/run_vp_unlisted_canary.py --preflight-only
```

Expected: exit 0 with a local evidence path; schedule remains `CLOSED`; active jobs/nodes/ChannelOps, uploads, and publications remain zero.

- [ ] **Step 4: Close the tunnel and audit evidence**

Validate JSON with `jq empty`, inspect file mode `0600`, confirm no secret URLs or tokens are present, and re-query production counts to prove no state was created.
