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
- `CHANNEL_AGENT_ALERT_SLACK_WEBHOOK_URL`
- `CHANNEL_AGENT_ALERT_EMAIL_TO`
