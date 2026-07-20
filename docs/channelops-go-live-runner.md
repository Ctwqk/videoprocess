# ChannelOps Go Live Runner

Use the `channelops-runner-go` Compose service for Go live mode. It runs the
`channelops-runner` binary built from `cmd/channelops-runner` and includes the
`channelops-live-smoke` binary for manual smoke checks.

Do not run the Python legacy `channel-agent-runner` at the same time as the Go
runner. The Python runner is behind the `channelops-python` profile; the Go
runner is behind the `channelops-go` profile.

## Dev No-PDS Startup

For a local development run that bypasses PDS decisions against the repo's
default host database, start the Go runner with `CHANNEL_AGENT_DEV_ALLOW_ALL_PDS=true`:

```bash
CHANNEL_AGENT_DEV_ALLOW_ALL_PDS=true \
PDS_ENABLED=false \
docker compose --profile channelops-go up channelops-runner-go
```

This requires the API, host Postgres, and YouTube manager to be reachable. The
runner's default Compose AutoFlow endpoint is `http://api:8080`, and both API
and Go runner default to the same host database at
`host.docker.internal:5435/videoprocess`.

To use the Compose `postgres` service instead, explicitly override both API and
Go runner database URLs:

```bash
CHANNEL_AGENT_DEV_ALLOW_ALL_PDS=true \
PDS_ENABLED=false \
VP_DATABASE_URL=postgresql+asyncpg://vp:${VP_POSTGRES_PASSWORD:-vp_secret}@postgres:5432/videoprocess \
VP_DATABASE_URL_GO=postgres://vp:${VP_POSTGRES_PASSWORD:-vp_secret}@postgres:5432/videoprocess \
docker compose --profile standalone --profile channelops-go up channelops-runner-go
```

## Production-Like Startup

For production-like validation, keep PDS enabled and point the runner at the
real PDS, API, YouTube manager, and Postgres endpoints:

```bash
PDS_ENABLED=true \
PDS_BASE_URL=http://pds:8080 \
PDS_CLIENT_ID=videoprocess-channel-agent \
AUTOFLOW_BASE_URL=http://api:8080 \
YOUTUBE_MANAGER_URL=http://youtube-manager:8899 \
docker compose --profile channelops-go up channelops-runner-go
```

Do not include `--profile channelops-python` in the same run unless you are
explicitly comparing legacy behavior and have paused one of the runners.

## Live Smoke

The runner image also contains `channelops-live-smoke`. Run it against the same
environment as the live runner:

```bash
docker compose --profile standalone --profile channelops-go run --rm \
  --entrypoint channelops-live-smoke \
  channelops-runner-go -channel-id <channel_profile_id>
```

## AutoFlow And API Database Configuration

`channelops-runner-go` calls AutoFlow over HTTP with `AUTOFLOW_BASE_URL`.
In Compose this defaults to `http://api:8080`, so the Go runner uses the Python
API service as the AutoFlow owner.

The runner and API must point at the same database. AutoFlow creates plans,
pipelines, jobs, and run records through the API, while the Go runner reads and
updates ChannelOps task state directly through `DATABASE_URL`. If these point at
different databases, plan approval, execution, and job observation will not line
up.

The `channelops-runner-go` service defaults `PDS_ENABLED=false` for dev-safe
startup. Set `PDS_ENABLED=true` explicitly for production-like validation.

Relevant runner environment:

- `DATABASE_URL`
- `AUTOFLOW_BASE_URL`
- `AUTOFLOW_TIMEOUT_SECONDS`
- `YOUTUBE_MANAGER_URL`
- `PDS_ENABLED`
- `PDS_BASE_URL`
- `PDS_CLIENT_ID`
- `PDS_TIMEOUT_SECONDS`
- `CHANNEL_AGENT_DEV_ALLOW_ALL_PDS`
- `CHANNELOPS_RUNNER_POLL_SECONDS`
- `CHANNELOPS_SCHEDULER_POLL_SECONDS`
- `CHANNELOPS_THROTTLE_ENABLED`
- `CHANNELOPS_THROTTLE_TIME_ZONE`
- `CHANNELOPS_THROTTLE_START_HOUR`
- `CHANNELOPS_THROTTLE_END_HOUR`
- `CHANNELOPS_THROTTLE_RUNNER_POLL_SECONDS`
- `CHANNELOPS_THROTTLE_SCHEDULER_POLL_SECONDS`
- `CHANNELOPS_QUEUE_MAX_ATTEMPTS`
- `CHANNELOPS_METRICS_MAX_POLLS`
- `CHANNELOPS_METRICS_POLL_DELAY_MINUTES`
- `CHANNEL_AGENT_ALERT_SLACK_WEBHOOK_URL`
- `CHANNEL_AGENT_ALERT_EMAIL_TO`

## Low-Noise Production Trial

For the 10.0.0.150 shared-infra path, enable the daytime throttle during the
West Coast 08:00-24:00 window so ChannelOps polls shared Postgres/Redis less
often while people are using the machine:

```bash
PDS_ENABLED=true \
CHANNELOPS_THROTTLE_ENABLED=true \
CHANNELOPS_THROTTLE_TIME_ZONE=America/Los_Angeles \
CHANNELOPS_THROTTLE_START_HOUR=8 \
CHANNELOPS_THROTTLE_END_HOUR=24 \
CHANNELOPS_THROTTLE_RUNNER_POLL_SECONDS=300 \
CHANNELOPS_THROTTLE_SCHEDULER_POLL_SECONDS=1800 \
CHANNELOPS_METRICS_POLL_DELAY_MINUTES=120 \
docker compose --profile channelops-go up -d channelops-runner-go
```

Keep the trial channel at low cadence, with YouTube privacy `unlisted` or
`private`, and do not run the legacy Python runner concurrently.

External-platform assets are held after planning and before AutoFlow execution.
This is a pre-upload human-review gate for private and unlisted workflows too;
it is not only a public-promotion check. The Go runner must not execute, upload,
or promote those tasks until an explicit human review path releases them.

Release a task held by `human_approval_required` through the Channel Agent API:

```bash
curl -X POST "$VP_API_URL/api/v1/channel-agent/tasks/$TASK_ID/review-release" \
  -H 'Content-Type: application/json' \
  -d '{"human_actor":"operator@example.com","review_notes":"source and rights reviewed"}'
```

The API approves that task's exact AutoFlow plan, records both the current
`review_approved_at` token and `approved_revision_hash`, transitions the task
to `planning`, and enqueues `execute_task` in the same transaction. Any
execution-relevant AutoFlow change clears approval and makes the evidence
stale. Disabled or halted channels and unrelated holds cannot be released
through this endpoint.

Every Go `execute_task` retry sends
`channelops-execute:<task-id>:<plan-id>:<approved-revision-hash>` as the AutoFlow
execute idempotency key. A lost HTTP response therefore reuses the one durable
run, pipeline, and job rather than starting a second job.

After a reviewed external upload reaches `uploaded_private`, or when PDS has
explicitly held an uploaded publication for review, promote it with:

```bash
curl -X POST "$VP_API_URL/api/v1/channel-agent/publications/$PUBLICATION_ID/promote" \
  -H 'Content-Type: application/json' \
  -d '{"human_actor":"operator@example.com","review_notes":"publication reviewed"}'
```

The request body remains optional for compatibility and defaults the actor to
`channel_agent_operator`. Manual promotion records publication-specific review
evidence, restores only eligible PDS-held tasks to `uploaded_private`, clamps
visibility to `private` or `unlisted`, and still runs the PDS publication gate.
Quarantine, pre-upload review, metrics, platform-error, disabled, halted, and
rejected holds are not eligible.

## Soak Activation Window

The soak guard and watcher reject `VP_SOAK_STARTED_AT` values more than 300
seconds after assessment time. The five-minute tolerance is only for clock skew;
a value exactly 300 seconds ahead is accepted, while any positive fractional
part beyond 300 seconds is rejected. Future-window rejection is a configuration
error and occurs before topology checks or database assessment.

## Watcher Image CLI Smoke

Use the exact Python worker or publisher image selected by the watcher and an
isolated, migrated test database. This command runs the real module with no
`--apply` flag and a deliberately missing channel, so guard exit 20 proves the
image can reach the test database without activation, upload, or publication:

```bash
VP_SOAK_SMOKE_IMAGE=vp-ffmpeg-worker-python:<deployed-tag> \
VP_SOAK_SMOKE_DATABASE_URL=postgresql+asyncpg://vp:password@host.docker.internal:55432/videoprocess \
VP_SOAK_SMOKE_TEST_DATABASE=true \
bash tests/test_channelops_soak_image_smoke.sh
```
