# Task 4 Report: Go Policy Scheduling And Queue Authority

## Scope

Implemented only the Task 4 Go policy parser, scheduler enqueue authority, queue kind, and channel authority classification. No discovery HTTP client, handler wiring, configuration, Python changes, deployment, provider calls, downloads, production-task creation, publication, public mode, or 126 work was added.

## RED Evidence

Command:

```bash
PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -run 'Discovery|SchedulerRunOnce' -count=1
```

Result: failed as expected before implementation. The package reported undefined `DiscoveryPolicyFromContentMix`, `DiscoveryPolicy`, `DiscoveryIdempotencyKey`, and `QueueIngestDiscovery` symbols.

Regression RED command:

```bash
PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -run '^TestDiscoveryPolicyFromContentMixParsesJSONBoundsAndLegacyRegion$' -count=1
```

Result: failed as expected while the Go parser validated an invalid legacy top-level `region_code` before the valid nested value. The failure was `region_code must be two uppercase ASCII letters` for the nested-precedence case.

## GREEN Evidence

Focused command:

```bash
PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -run 'Discovery|SchedulerRunOnce' -count=1
```

Result: `ok github.com/Ctwqk/videoprocess/internal/channelops`.

Full package command:

```bash
PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -count=1
```

Result: `ok github.com/Ctwqk/videoprocess/internal/channelops`.

Static checks:

```bash
PATH=/opt/homebrew/bin:$PATH go vet ./internal/channelops
git diff --check
```

Result: both exited successfully with no output.

## Self-Review

- `DiscoveryPolicyFromContentMix` defaults to disabled, uses JSON-decoded `float64` only for integral numeric fields, rejects bools/fractional values, applies all requested bounds, and preserves nested-over-legacy region precedence.
- Scheduler policy errors fail closed only for discovery; agent tick scheduling and its returned enqueue count are unchanged.
- Enabled discovery uses the UTC policy bucket, priority 80, the exact source/key format, channel-scoped metadata, and queue idempotency for repeated scheduler runs.
- SQL claim authority and Go execution-fence authority classify `ingest_discovery` with `agent_tick` and `learning_recompute`; tests cover matching, missing, and mismatched payload channels.
- The payload includes both `bucket` and `scheduler_bucket` because the approved design explicitly requires both, with no other ambiguous bucket field.

## Concerns

The requested `.superpowers/sdd/task-4-brief.md` was not present in this worktree or the parent repository. This implementation followed the matching pre-approved scheduler design and the committed Task 4 implementation-plan section instead.

## Review Fix Evidence

The disposable PostgreSQL test database was migrated with the repository's existing
Alembic revisions `001` through `029_channelops_discovery_ingestion_runs`:

```bash
cd backend
DATABASE_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
  .venv/bin/python -m alembic -c alembic.ini upgrade head
```

### RED

```text
$ go test ./internal/channelops -run '^TestSchedulerBucketDoesNotRestartNonDivisorIntervalsAtUTCMidnight$' -count=1 -v
=== RUN   TestSchedulerBucketDoesNotRestartNonDivisorIntervalsAtUTCMidnight
    scheduler_test.go:62: 1000-minute bucket restarted at UTC midnight: before = "2026-07-21-16-40", after = "2026-07-22-00"
--- FAIL: TestSchedulerBucketDoesNotRestartNonDivisorIntervalsAtUTCMidnight (0.00s)
FAIL
FAIL    github.com/Ctwqk/videoprocess/internal/channelops    0.351s
FAIL
```

```text
$ DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55439/videoprocess_test go test ./internal/channelops -run '^TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel$' -count=1 -v
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/matching_payload_is_channel_scoped
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/missing_payload_channel_is_not_global
    integration_test.go:2926: claimed invalid discovery item: &channelops.QueueItemRow{... Status:"running" ...}
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/malformed_payload_channel_is_not_claimable
    integration_test.go:2926: claimed invalid discovery item: &channelops.QueueItemRow{... Status:"running" ...}
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/mismatched_payload_channel_is_not_claimable
    integration_test.go:2926: claimed invalid discovery item: &channelops.QueueItemRow{... Status:"running" ...}
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/nil_stored_channel_is_not_claimable
    integration_test.go:2926: claimed invalid discovery item: &channelops.QueueItemRow{... Status:"running" ...}
--- FAIL: TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel (0.10s)
FAIL
FAIL    github.com/Ctwqk/videoprocess/internal/channelops    0.326s
FAIL
```

### GREEN: Focused PostgreSQL Tests

```text
$ DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55439/videoprocess_test go test ./internal/channelops -run '^(TestSchedulerBucketDoesNotRestartNonDivisorIntervalsAtUTCMidnight|TestSchedulerRunOnce.*|TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel)$' -count=1 -v
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/matching_payload_is_channel_scoped
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/missing_payload_channel_is_not_global
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/malformed_payload_channel_is_not_claimable
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/mismatched_payload_channel_is_not_claimable
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/nil_stored_channel_is_not_claimable
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/disabled_payload_channel_is_not_claimable
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/halted_payload_channel_is_not_claimable
=== RUN   TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/quarantined_payload_channel_is_not_claimable
--- PASS: TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel (0.08s)
    --- PASS: TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/matching_payload_is_channel_scoped (0.01s)
    --- PASS: TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/missing_payload_channel_is_not_global (0.00s)
    --- PASS: TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/malformed_payload_channel_is_not_claimable (0.00s)
    --- PASS: TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/mismatched_payload_channel_is_not_claimable (0.00s)
    --- PASS: TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/nil_stored_channel_is_not_claimable (0.00s)
    --- PASS: TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/disabled_payload_channel_is_not_claimable (0.00s)
    --- PASS: TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/halted_payload_channel_is_not_claimable (0.00s)
    --- PASS: TestDiscoveryQueueAuthorityRequiresMatchingPayloadChannel/quarantined_payload_channel_is_not_claimable (0.00s)
=== RUN   TestSchedulerBucketDoesNotRestartNonDivisorIntervalsAtUTCMidnight
--- PASS: TestSchedulerBucketDoesNotRestartNonDivisorIntervalsAtUTCMidnight (0.00s)
=== RUN   TestSchedulerRunOnceUsesIntervalAwareBuckets
--- PASS: TestSchedulerRunOnceUsesIntervalAwareBuckets (0.02s)
=== RUN   TestSchedulerRunOnceDoesNotRepeatSameFourHourBucket
--- PASS: TestSchedulerRunOnceDoesNotRepeatSameFourHourBucket (0.02s)
=== RUN   TestSchedulerRunOnceEnqueuesOperationalMaintenance
--- PASS: TestSchedulerRunOnceEnqueuesOperationalMaintenance (0.02s)
=== RUN   TestSchedulerRunOnceSchedulesEnabledDiscoveryOncePerPolicyBucket
--- PASS: TestSchedulerRunOnceSchedulesEnabledDiscoveryOncePerPolicyBucket (0.02s)
=== RUN   TestSchedulerRunOnceDiscoveryFailClosesWithoutChangingAgentTick
=== RUN   TestSchedulerRunOnceDiscoveryFailClosesWithoutChangingAgentTick/default_disabled
=== RUN   TestSchedulerRunOnceDiscoveryFailClosesWithoutChangingAgentTick/invalid_enabled_policy
--- PASS: TestSchedulerRunOnceDiscoveryFailClosesWithoutChangingAgentTick (0.03s)
    --- PASS: TestSchedulerRunOnceDiscoveryFailClosesWithoutChangingAgentTick/default_disabled (0.02s)
    --- PASS: TestSchedulerRunOnceDiscoveryFailClosesWithoutChangingAgentTick/invalid_enabled_policy (0.02s)
PASS
ok      github.com/Ctwqk/videoprocess/internal/channelops    0.513s
```

No focused test was skipped. `RunOnce` continues to report newly scheduled
agent ticks only; discovery is documented as maintenance and is deliberately
excluded from that count. The full non-skipped PostgreSQL package run also
passed:

```text
$ DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55439/videoprocess_test go test ./internal/channelops -count=1 -v
PASS
ok      github.com/Ctwqk/videoprocess/internal/channelops    6.247s
```
