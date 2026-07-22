# YouTube Discovery Scheduler Design

Status: pre-approved for implementation on 2026-07-21.

## Context

VideoProcess can already turn active `discovery_signals` into ChannelOps
candidates, but production never creates those signals automatically. The
existing `YouTubeTrendIngester` has a real YouTubeManager search client and
persists deduplicated signal metadata, yet it has no scheduler, queue kind, or
runtime owner.

The production topology now has one Go ChannelOps runner on 127, a Python API
on 127, shared services and the dedicated publisher on 150, and no VP work on
126. Discovery must preserve that ownership model. It must also remain
separate from rendering and publication: external-platform metadata can inform
topics, but external assets still require explicit human review and public
publication remains disabled.

## Goal

Add an opt-in, auditable YouTube discovery loop that:

1. is scheduled only by the Go ChannelOps runner;
2. calls the existing Python `YouTubeTrendIngester` through the internal API;
3. records one durable run per channel, source, and scheduler bucket;
4. bounds search frequency, lane queries, and results through channel policy;
5. retries safely without repeating a completed provider search;
6. creates or refreshes metadata-only `discovery_signals`;
7. never downloads a searched video, creates a production task directly,
   opens the video schedule, uploads, promotes, or publishes;
8. remains disabled for every existing channel until explicitly configured.

## Non-Goals

- No public publishing or unattended promotion.
- No download or reuse of a searched YouTube video.
- No automatic approval of an external-asset plan.
- No learned ranking, policy activation, or lane-weight mutation.
- No RSS, competitor, X, or multi-platform discovery.
- No host cron or second Python scheduler.
- No change to the per-attempt approval required for a live unlisted canary.
- No deployment or workload placement on 126.

## Approaches Considered

### Go scheduler plus Python internal service (selected)

The Go runner owns bucket scheduling and queue authority. A new queue handler
calls a narrow Python endpoint, which reuses the existing ingester and
YouTubeManager client. This preserves the single scheduler while avoiding a
second implementation of provider parsing and SQLAlchemy model writes.

The extra internal HTTP hop is acceptable because provider search is already
an external network operation. Durable run idempotency covers response loss or
runner restart after the Python service commits.

### Reimplement search and upsert in Go (rejected)

This avoids internal HTTP, but duplicates the existing Python provider
contract, signal expiration, upsert behavior, and channel policy parsing. The
two implementations would be likely to drift.

### Host cron invokes the Python ingester (rejected)

This is operationally simple but bypasses ChannelOps queue authority,
channel quarantine, queue retries, and the deployed Go scheduler. It would
also create a second scheduling owner on 150.

## Channel Policy

Discovery is configured inside the existing `content_mix_policy_json`:

```json
{
  "youtube_discovery": {
    "enabled": true,
    "interval_minutes": 360,
    "max_queries_per_run": 3,
    "max_results_per_query": 10,
    "min_view_count": 1000,
    "region_code": "US"
  }
}
```

Defaults and bounds are fail-closed:

| Field | Default | Accepted range |
|---|---:|---:|
| `enabled` | `false` | exact boolean |
| `interval_minutes` | `360` | `60..1440` |
| `max_queries_per_run` | `3` | `1..5` |
| `max_results_per_query` | `10` | `1..25` |
| `min_view_count` | `1000` | `0..1000000000` |
| `region_code` | `US` | two uppercase ASCII letters |

Invalid policy disables scheduling and causes an explicitly requested ingest
to fail validation. Existing top-level `region_code` remains a compatibility
fallback only when the nested value is absent.

The scheduler considers only enabled, non-halted channels already returned by
`ListSchedulableChannels`. Dry-run channels may collect discovery metadata;
dry-run continues to prevent live task execution and publication.

## Scheduler And Queue Ownership

`Scheduler.RunOnce` derives a discovery bucket using the bounded discovery
interval and enqueues:

```text
kind = ingest_discovery
idempotency_key = ingest_discovery:<channel_id>:youtube_search:<bucket>
priority = 80
payload = {
  channel_id,
  source: "youtube_search",
  bucket,
  scheduler_bucket: bucket
}
```

Priority 80 lets a due discovery item run before the normal priority-100
agent tick when both are newly queued. It is not a strict same-tick dependency:
if an older tick is already runnable, newly collected signals are consumed by
the next tick.

`ingest_discovery` is channel-scoped in the queue-authority CTE. A disabled,
halted, quarantined, mismatched, or missing authoritative channel cannot be
claimed. The Python endpoint independently verifies the committed queue row is
`running`, has kind `ingest_discovery`, and matches the requested channel and
bucket. The Go runner remains the only owner that completes or retries the
queue item.

The queue kind is non-publishing maintenance for canary backlog reporting, but
it is still quarantined with its channel and never treated as a global item.

## Internal Discovery API

The Python API exposes:

```text
POST /api/v1/channel-agent/internal/discovery/ingest
```

Request:

```json
{
  "channel_id": "uuid",
  "queue_item_id": "uuid",
  "source": "youtube_search",
  "scheduler_bucket": "2026-07-21-18"
}
```

The endpoint validates queue authority and channel policy before contacting
YouTubeManager. It returns the durable run ID, terminal status, query count,
created/refreshed/expired signal counts, and estimated search quota units. It
does not return raw provider payloads or connection details.

The Go client accepts only a matching channel, source, and bucket with status
`succeeded`. A response mismatch or non-terminal status is retryable and does
not mark the queue item done.

The Go runner uses `CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS`, default `120`, with
an accepted range of `30..300`. This timeout is separate from the shorter
AutoFlow planning timeout because one run may issue up to five bounded provider
queries. `ingest_discovery` is handled outside the long-lived queue-row
transaction; committed queue authority is checked by the Python endpoint and
the runner's lease-aware completion/retry update remains authoritative.

## Durable Run Model

Migration `029_channelops_discovery_ingestion_runs` adds
`discovery_ingestion_runs` with:

- UUID primary key;
- `channel_profile_id` with `ON DELETE CASCADE`;
- unique `queue_item_id` with `ON DELETE SET NULL`;
- `source`, fixed to `youtube_search` for v1;
- `scheduler_bucket` and unique `(channel_profile_id, source, scheduler_bucket)`;
- `query_version`, fixed to `youtube-lane-keyword-v1` for v1;
- `status` in `running`, `succeeded`, or `failed`;
- `attempt_count`, `query_count`, `created_count`, `refreshed_count`,
  `expired_count`, and `quota_units_estimated`;
- sanitized `policy_snapshot_json`;
- `started_at`, `finished_at`, and bounded `last_error_code`.

No credential, URL with credentials, provider response body, title, prompt, or
video metadata is copied into the run row. Provider item metadata remains only
in the existing `discovery_signals.raw_json` field.

## Idempotency And Concurrency

Before external search, the service atomically creates or claims the unique
run row and commits `running` state. Concurrent creation is resolved by the
database unique constraint.

- Existing `succeeded`: return the stored result without provider calls.
- Existing recent `running`: return a conflict so the Go queue retries later.
- Existing stale `running`: reclaim it and increment `attempt_count`.
- Existing `failed`: retry within the queue item's normal attempt budget.

The stale threshold is 15 minutes. Search and signal writes are committed with
the terminal run update. If the provider call fails, signal changes are rolled
back and the run is marked `failed` with a fixed error category. If the Python
commit succeeds but the HTTP response is lost, the next queue attempt observes
the succeeded run and performs no second search.

## Ingest Semantics

The ingester orders enabled lanes by weight and creation time, then applies
`max_queries_per_run`. Each lane uses its first keyword, falling back to the
lane name. The provider query covers the preceding 24 hours.

For each qualifying result, the ingester upserts the existing
`youtube_search` signal by channel and external ID. Refreshing an existing
signal updates its lane, observed/expiry times, title, summary, URL, keywords,
trend score, and raw metadata. It does not reset a `converted` signal to
`active`; only `expired` signals may be reactivated. Stale active signals are
expired before new results are applied.

The result reports separate created and refreshed counts so a successful run
with no novel video remains observable and is not mistaken for a failure.

## Error Handling

- Invalid or disabled policy: HTTP 409, no run and no provider call.
- Invalid queue authority: HTTP 409, no run and no provider call.
- Missing/halted channel: HTTP 404 or 409, no provider call.
- Recent concurrent run: HTTP 409; Go queue retries with normal backoff.
- Provider authentication, quota, timeout, or contract error: rollback signal
  changes, mark the run `failed` with a fixed category, and return non-2xx.
- Database failure before terminal commit: leave or roll back `running`; stale
  reclaim handles a later retry.
- Queue retry exhaustion: existing ChannelOps dead-letter behavior applies and
  the soak guard can halt the configured channel.

Error responses and run rows must not include tokens, credentials, database
URLs, provider response bodies, or searched titles.

## Safety

- Existing channels are unchanged because discovery defaults to disabled.
- Discovery writes only run audit rows and signal metadata.
- It never creates `production_tasks`; the existing deterministic agent tick
  remains the only conversion owner.
- A trend-derived task still passes PDS, deterministic AutoFlow planning,
  upload operation, publisher admission, and private/unlisted constraints.
- Any task that uses an external platform asset remains held for explicit
  human review.
- `PUBLIC_PUBLISH_ENABLED=false` remains required by the publisher and public
  promotion remains disabled.
- The feature adds no service, credential, build, or placement on 126.

## Deployment

The existing VideoProcess CI runs the migration, Python tests, Go tests, and
deploy contracts. The scoped `vp-app` deployment applies migration 029 and
updates the API and Go runner from the same exact successful commit.

Deployment does not enable discovery. Production activation is a separate
channel configuration change after the approved live unlisted canary. Initial
activation uses a six-hour interval, at most three lane queries, ten results
per query, and the existing low-frequency channel cadence.

## Verification

Automated coverage must prove:

- disabled and invalid policies never enqueue or call the provider;
- bucket/idempotency keys and policy bounds are deterministic;
- queue authority rejects channel mismatch and quarantine races;
- the Go client validates response identity and status;
- the API rejects forged/non-running queue items;
- concurrent/retried calls create one run and completed replay makes no second
  provider request;
- signal create, refresh, expiry, lane/query limits, and counts are correct;
- provider failure leaves no partial signal changes and records a sanitized
  failed run;
- migration upgrade/downgrade/upgrade and current head tests pass;
- full backend, Go, and deploy contract suites remain green.

Production verification before activation is read-only: migration head,
service replicas, exact commit, default-disabled policy, zero discovery queue
items/runs created by deployment, schedule `CLOSED`, no public rows, and zero
VP tasks on 126.

## Rollback

Set `youtube_discovery.enabled=false` to stop new scheduling without deleting
signals or audit rows. Quarantining the channel also prevents claims. Reverting
the code leaves migration 029 in place as an inert expand-first table. A schema
downgrade is allowed only after confirming no process references the table and
preserving any run evidence needed for review.
