# PDS Kafka Smoke Runbook

Run these commands from the VideoProcess worktree:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess
```

## Compose Check

Render the merged compose file before starting services:

```bash
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml config
```

The override mounts
`/home/taiwei/Constructure-repos/policy-decision-service/config` at `/etc/pds`
inside the scratch PDS image and sets
`PDS_RULES_PATH=/etc/pds/rules.example.yaml`. Keep the whole config directory
mounted because `rules.example.yaml` references `blocklist.example.txt`
relative to the rules file. The smoke override runs PDS as `0:0` so it can read
the local read-only bind mount even when the source checkout has restrictive
file permissions.

Start the PDS/Kafka smoke stack:

```bash
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml up -d --build redpanda pds vp-feature-aggregator event-outbox-relay
```

## Health, Ready, And Decide

The VP API is host-published on `${API_PORT:-18080}`:

```bash
curl -fsS "http://localhost:${API_PORT:-18080}/health"
```

PDS is service-local in `docker-compose.pds-kafka.yml`; it is not host-published
unless you add a local ports override. PDS exposes `/healthz` and `/readyz`,
while `/health` is the VP API endpoint above. Because the PDS image is
`scratch`, probe it from another Python-based service on the compose network:

```bash
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml exec vp-feature-aggregator \
  python - <<'PY'
from urllib.request import urlopen

for url in (
    "http://pds:8080/healthz",
    "http://pds:8080/readyz",
    "http://vp-feature-aggregator:8080/healthz",
    "http://vp-feature-aggregator:8080/readyz",
):
    print(url, urlopen(url, timeout=5).read().decode())
PY
```

Send a local decision through PDS:

```bash
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml exec vp-feature-aggregator \
  python - <<'PY'
import json
from urllib.request import Request, urlopen

body = {
    "actor_id": "smoke-actor",
    "action": {"type": "publish", "platform": "youtube"},
    "content": {"title": "smoke", "duration_s": 30, "tags": ["smoke"]},
    "context": {"request_source": "pds-kafka-smoke"},
}
req = Request(
    "http://pds:8080/v1/decide",
    data=json.dumps(body).encode("utf-8"),
    headers={"Content-Type": "application/json", "X-Client-Id": "vp-smoke"},
    method="POST",
)
print(urlopen(req, timeout=5).read().decode())
PY
```

If you add a host port override for PDS, equivalent host checks are:

```bash
curl -fsS http://localhost:18082/healthz
curl -fsS http://localhost:18082/readyz
```

## Proof Criteria

The smoke is proven when all of these are true:

- A ChannelOps action emits a `vp.actor.actions.v1` event into the VP
  `event_outbox` table.
- `event-outbox-relay` sends the event to Redpanda and marks the outbox row with
  a non-null `delivered_at`.
- `vp-feature-aggregator` returns non-zero actor features for the event actor,
  for example `publishes_5m`, `blocks_24h`, `flags_7d`, or `comment_burst_1m`.
- PDS `POST /v1/decide` returns a valid response with a stable non-empty
  `decision_id` and uses the aggregator feature fields without blocking on a
  missing host-exposed PDS port.

Useful inspection commands:

```bash
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml logs --tail=80 pds vp-feature-aggregator event-outbox-relay
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml exec vp-feature-aggregator \
  python - <<'PY'
from urllib.request import urlopen
print(urlopen("http://vp-feature-aggregator:8080/v1/features/smoke-actor", timeout=5).read().decode())
PY
```
