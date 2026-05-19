# ChannelOps Follow-Up Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the three merge-blocking ChannelOps hardening gaps: PDS outage alerts, independent interval-aware scheduling, and publish-time repetition protection.

**Architecture:** Keep the changes local to existing ChannelOps service boundaries. `ChannelAgentService` owns PDS decision health and publish-time material checks because it already owns PDS call sites and publication creation. `ChannelOpsScheduler` owns interval bucket calculation while `ChannelAgentRunner` owns running the queue consumer and scheduler loop together.

**Tech Stack:** Python 3, FastAPI service modules, SQLAlchemy async sessions, pytest, existing ChannelOps queue and model helpers.

---

## File Structure

- Modify `backend/app/channel_agent/service.py`: add PDS health state, `_decide_pds`, replace direct PDS decisions, add final publish-time material guard helpers.
- Modify `backend/app/channel_agent/scheduler.py`: add interval-aware bucket helper and use it in queue/audit idempotency keys.
- Modify `backend/app/channel_agent/runner.py`: pass PDS health monitoring into the service and run a scheduler loop from `run_forever()`.
- Modify `backend/app/config.py`: add `channel_agent_scheduler_poll_seconds`.
- Modify `backend/tests/channel_agent/test_service.py`: add PDS outage and publish-time repetition tests.
- Modify `backend/tests/channel_agent/test_scheduler.py`: add interval bucket behavior tests.
- Modify `backend/tests/channel_agent/test_runner.py`: add scheduler-loop independence test.

## Task 1: PDS Outage Alert Wiring

**Files:**
- Modify: `backend/app/channel_agent/service.py`
- Test: `backend/tests/channel_agent/test_service.py`

- [ ] **Step 1: Write failing PDS outage tests**

Add tests that construct `ChannelAgentService(pds_health_monitor_enabled=True)` with a fake PDS client returning fail-policy metadata:

```python
class SequencePDSClient:
    def __init__(self, decisions):
        self.decisions = list(decisions)
        self.requests = []

    async def decide(self, request):
        self.requests.append(request)
        if len(self.decisions) > 1:
            return self.decisions.pop(0)
        return self.decisions[0]


async def test_pds_unavailable_decision_enqueues_hourly_outage_alert(service_session):
    service = _service(
        pds=SequencePDSClient([
            PDSDecision(decision_id="", verdict="block", metadata={"warning": "pds_unavailable", "fail_policy": "block"}),
        ]),
        pds_health_monitor_enabled=True,
    )
    item = await _publishable_publication_promotion_item(service_session)

    await service.handle_promote_publication(service_session, item)

    alerts = (await service_session.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert"))).scalars().all()
    assert len(alerts) == 1
    assert alerts[0].payload_json["type"] == "pds_outage"
    assert alerts[0].payload_json["resource_id"] == "service:pds"


async def test_pds_outage_alert_is_deduped_per_hour(service_session):
    service = _service(
        pds=SequencePDSClient([
            PDSDecision(decision_id="", verdict="block", metadata={"warning": "pds_unavailable", "fail_policy": "block"}),
        ]),
        pds_health_monitor_enabled=True,
    )
    first = await _publishable_publication_promotion_item(service_session)
    second = await _publishable_publication_promotion_item(service_session)

    await service.handle_promote_publication(service_session, first)
    await service.handle_promote_publication(service_session, second)

    alert_count = await service_session.scalar(select(func.count()).select_from(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert"))
    assert alert_count == 1
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py -q -k "pds_outage"
```

Expected: tests fail because no `send_alert` queue item is created.

- [ ] **Step 3: Implement `_decide_pds`**

In `ChannelAgentService.__init__`, add `pds_health_monitor_enabled: bool = False` and fields:

```python
self.pds_health_monitor_enabled = pds_health_monitor_enabled
self._pds_last_success_at: datetime | None = None
self._pds_last_alert_bucket: str | None = None
```

Add `_decide_pds`:

```python
async def _decide_pds(self, db: AsyncSession, request: PDSDecisionRequest) -> PDSDecision:
    decision = await self.pds_client.decide(request)
    if _is_pds_fail_policy_decision(decision):
        await self._maybe_enqueue_pds_outage_alert(db, decision, request)
        return decision
    self._pds_last_success_at = self.clock.now()
    return decision
```

Add `_maybe_enqueue_pds_outage_alert` and `_is_pds_fail_policy_decision`. Import `should_enqueue_pds_outage_alert`.

- [ ] **Step 4: Replace direct PDS calls**

Replace direct `await self.pds_client.decide(...)` calls in candidate, plan approval, and promote publication paths with `await self._decide_pds(db, PDSDecisionRequest(...))`.

- [ ] **Step 5: Verify PDS tests pass**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py -q -k "pds_outage or pds"
```

Expected: PDS-related service tests pass.

## Task 2: Independent Interval-Aware Scheduler

**Files:**
- Modify: `backend/app/channel_agent/scheduler.py`
- Modify: `backend/app/channel_agent/runner.py`
- Modify: `backend/app/config.py`
- Test: `backend/tests/channel_agent/test_scheduler.py`
- Test: `backend/tests/channel_agent/test_runner.py`

- [ ] **Step 1: Write failing interval bucket tests**

In `test_scheduler.py`, import the new helper and add:

```python
def test_scheduler_bucket_respects_tick_interval_minutes():
    now = datetime(2026, 5, 19, 10, 37, tzinfo=timezone.utc)
    assert scheduler_bucket(now, 15) == "2026-05-19-10-30"
    assert scheduler_bucket(now, 30) == "2026-05-19-10-30"
    assert scheduler_bucket(now, 60) == "2026-05-19-10"
    assert scheduler_bucket(now, 240) == "2026-05-19-08"
```

Also update the existing scheduler test to assert a 240-minute channel uses an `agent_tick:{channel_id}:2026-05-19-08` idempotency key at 10:37 UTC.

- [ ] **Step 2: Write failing runner scheduler-loop test**

In `test_runner.py`, add a fake scheduler and monkeypatch `asyncio.sleep` to raise a local stop exception after the first loop:

```python
async def test_run_forever_starts_scheduler_loop(monkeypatch):
    runner = ChannelAgentRunner(worker_id="test-runner")
    calls = {"scheduler": 0}

    async def fake_run_scheduler_forever(*, poll_seconds):
        calls["scheduler"] += 1

    async def stop_after_one_sleep(seconds):
        raise StopAsyncIteration

    runner._run_scheduler_forever = fake_run_scheduler_forever
    monkeypatch.setattr(asyncio, "sleep", stop_after_one_sleep)

    with pytest.raises(StopAsyncIteration):
        await runner.run_forever(poll_seconds=0.01)

    assert calls["scheduler"] == 1
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_scheduler.py tests/channel_agent/test_runner.py -q
```

Expected: tests fail because `scheduler_bucket` and the scheduler loop do not exist.

- [ ] **Step 4: Implement scheduler bucket and loop**

Add `scheduler_bucket(value, interval_minutes)` to `scheduler.py`, use it in `run_once`, and set floored interval back on the channel. Add `channel_agent_scheduler_poll_seconds` to settings. In `runner.py`, add:

```python
async def run_forever(self, *, poll_seconds: float = 5.0) -> None:
    scheduler_task = asyncio.create_task(
        self._run_scheduler_forever(poll_seconds=settings.channel_agent_scheduler_poll_seconds)
    )
    try:
        while True:
            handled = await self.run_once(run_scheduler_when_idle=False)
            if not handled:
                await asyncio.sleep(poll_seconds)
    finally:
        scheduler_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler_task
```

Change `run_once` to accept `run_scheduler_when_idle: bool = True` so tests keep deterministic idle behavior.

- [ ] **Step 5: Verify scheduler tests pass**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_scheduler.py tests/channel_agent/test_runner.py -q
```

Expected: scheduler and runner tests pass.

## Task 3: Publish-Time Repetition Guard

**Files:**
- Modify: `backend/app/channel_agent/service.py`
- Test: `backend/tests/channel_agent/test_service.py`

- [ ] **Step 1: Write failing publish guard tests**

Add one lane-generated task test and one manual-seed override test:

```python
async def test_publish_task_holds_lane_task_when_material_recently_used(service_session):
    task, item, material_id = await _publishable_task_with_material(service_session, source="lane")
    service_session.add(MaterialUsageLedger(
        material_id=material_id,
        channel_profile_id=task.channel_profile_id,
        topic_lane_id=task.topic_lane_id,
        publishing_account_id=task.target_account_id,
        publication_id=uuid.uuid4(),
        used_at=datetime.now(timezone.utc),
        segment_signature=segment_signature(material_id, 0, 1000),
        metadata_json={},
    ))
    await service_session.commit()

    publication = await _service().handle_publish_task(service_session, item)

    await service_session.refresh(task)
    assert publication is None
    assert task.state == TASK_HELD
    assert task.blocked_by_guard == "repetition_rejected"


async def test_publish_task_manual_seed_repetition_override_is_recorded(service_session):
    task, item, material_id = await _publishable_task_with_material(service_session, source="manual_seed")
    service_session.add(MaterialUsageLedger(
        material_id=material_id,
        channel_profile_id=task.channel_profile_id,
        topic_lane_id=task.topic_lane_id,
        publishing_account_id=task.target_account_id,
        publication_id=uuid.uuid4(),
        used_at=datetime.now(timezone.utc),
        segment_signature=segment_signature(material_id, 0, 1000),
        metadata_json={},
    ))
    await service_session.commit()

    publication = await _service().handle_publish_task(service_session, item)

    await service_session.refresh(task)
    assert publication is not None
    assert task.rationale_json["material_usage_guard"]["repetition_rejected"] is True
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py -q -k "publish_task_holds_lane_task_when_material_recently_used or publish_task_manual_seed_repetition_override_is_recorded"
```

Expected: lane-generated publish is not held yet.

- [ ] **Step 3: Implement publish-time guard**

Refactor material reference extraction into a helper used by both publish guard and ledger writing:

```python
async def _material_references_for_publish(self, db, task, upload_metadata):
    plan_payload, run_payload = await self._material_payloads_for_task(db, task)
    return extract_material_references(plan_payload=plan_payload, run_payload=run_payload, upload_metadata=upload_metadata)
```

In `handle_publish_task`, before creating a new publication, call a new `_publish_time_material_usage_guard`. If it returns a blocked result for a non-manual task, set `TASK_HELD`, `blocked_by_guard`, `failure_reason`, and rationale metadata, commit, and return `None`. If it returns a blocked result for a manual seed, annotate rationale and continue.

- [ ] **Step 4: Verify publish guard tests pass**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py -q -k "material_usage or publish_task"
```

Expected: publish guard and existing publish/material tests pass.

## Task 4: Full Verification And Commit

**Files:**
- Modify: implementation files above.
- Test: focused and full backend suites.

- [ ] **Step 1: Run focused suites**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py tests/channel_agent/test_scheduler.py tests/channel_agent/test_runner.py -q
```

Expected: all focused tests pass.

- [ ] **Step 2: Run backend required checks**

Run:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: pytest passes; ruff/mypy may report missing modules in this environment and are allowed by project command.

- [ ] **Step 3: Run diff hygiene**

Run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors; only intended files changed.

- [ ] **Step 4: Commit and push**

Run:

```bash
git add backend/app/channel_agent/service.py backend/app/channel_agent/scheduler.py backend/app/channel_agent/runner.py backend/app/config.py backend/tests/channel_agent/test_service.py backend/tests/channel_agent/test_scheduler.py backend/tests/channel_agent/test_runner.py docs/superpowers/plans/2026-05-19-channel-ops-follow-up-hardening.md
git commit -m "fix: harden channel ops live loop"
git push
```

Expected: branch `codex/channel-ops-remaining-sprints` is updated on origin. No pull request is created.
