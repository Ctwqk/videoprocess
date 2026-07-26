# Production Redis ACL And Infrastructure Auto-Deploy Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development to execute each repository task in an
> isolated worktree with a task review before integration.

**Goal:** Make `constructure-runtime/main` deploy automatically to host 150
after exact-commit CI succeeds, migrate every production Redis client to a
least-privilege named ACL user, and disable unauthenticated `default`.

**Architecture:** Host 150 remains the pull-based deployment authority. The
runtime repository owns Redis server configuration and a two-phase ACL rollout;
VP and PDS own client secret loading and key/command contracts. Phase A adds and
verifies named users while default remains temporarily available. Phase B
disables default only after every client proves its expected identity.

**Repositories:** `Ctwqk/constructure-runtime`, `Ctwqk/videoprocess`,
`Ctwqk/policy-decision-service`.

## Global Constraints

- Complete and deploy durable worker registration before ACL enforcement.
- Run every repository task in its own isolated worktree and commit separately.
- Exact GitHub Actions success for the deployed 40-character commit is required.
- GitHub Actions receives no LAN SSH or production secret.
- Raw Redis credentials never enter Git, environment variables, arguments,
  logs, evidence, or application database rows.
- Host 126 is forbidden.
- No canary, upload, schedule opening, channel resume, soak activation, or
  public publication is part of this plan.
- Schedule closed, zero active jobs/nodes, and zero Redis pending entries are
  mandatory before either live ACL phase.

---

### Task 1: Add CI-Gated Inbound Deploy To Constructure Runtime

**Repository:** `constructure-runtime`

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `ops/deploy/deploy-main.sh`
- Create: `ops/deploy/install-cron.sh`
- Create: `tests/test_deploy_main.sh`
- Create: `tests/test_deploy_cron.sh`
- Modify: `ops/github/repos.tsv`
- Modify: `ops/schedule/install-cron.sh`
- Modify: `README.md`

**Requirements:**

- CI runs repository shell tests, `bash -n`, Compose rendering, and secret-path
  scans on pushes to `main` and pull requests.
- The host poller uses a nonblocking lock, clean-tree gate, exact-commit GitHub
  Actions gate, separate staging directory, allowlisted service changes,
  bounded health checks, atomic deployed marker, and prior-commit rollback.
- The runtime repository's outbound sync manifest changes to
  `auto_commit=false` and `auto_push=false` for itself. Dirty server drift
  blocks inbound deploy.
- Install exactly one marked 15-minute cron entry without changing VP/PDS cron.
- Dry-run performs no checkout, Compose, cron, marker, or service mutation.

**Verification:**

```bash
bash tests/test_sync_repos.sh
bash tests/test_deploy_main.sh
bash tests/test_deploy_cron.sh
bash -n ops/deploy/deploy-main.sh ops/deploy/install-cron.sh
docker compose -f infra/shared-infra/docker-compose.yml config >/dev/null
git diff --check
```

### Task 2: Add Generated Redis ACL Configuration And Rollout Engine

**Repository:** `constructure-runtime`

**Files:**
- Create: `infra/shared-infra/redis/users.json`
- Create: `ops/redis/render-acl.py`
- Create: `ops/redis/acl-rollout.sh`
- Create: `ops/redis/acl-preflight.sh`
- Create: `tests/test_redis_acl_render.py`
- Create: `tests/test_redis_acl_rollout.sh`
- Modify: `infra/shared-infra/docker-compose.yml`
- Modify: `.gitignore`
- Modify: `.env.example`

**Requirements:**

- The tracked manifest contains only user names, key/channel patterns, and
  command rules. Generated ACL files contain password hashes only and are
  ignored with mode `0600`.
- Generate independent versioned credentials for VP control, four VP workers,
  watcher, PDS, and feature aggregator.
- Mount the ACL file read-only and start Redis with an explicit `aclfile`.
- Implement `prepare`, `phase-a`, `phase-b`, `rotate`, `status`, and `rollback`
  modes with stable sanitized output.
- Phase A keeps `default on nopass`; Phase B renders `default off`.
- Validate ACL syntax in an isolated Redis 7 container before touching the live
  service.
- Rollback restores the prior ACL/config, verifies Redis health, and never
  changes VP schedule/channel/task/upload state.

**Verification:**

```bash
python3 -m unittest tests/test_redis_acl_render.py
bash tests/test_redis_acl_rollout.sh
docker compose -f infra/shared-infra/docker-compose.yml config >/dev/null
git diff --check
```

### Task 3: Add PDS Named-User Redis Contract

**Repository:** `policy-decision-service`

**Files:**
- Modify: PDS Redis configuration loader
- Modify: PDS startup/readiness tests
- Modify: PDS deployment extension contract tests
- Modify: `.env.example`
- Modify: `README.md`

**Requirements:**

- Production requires a bounded mode `0400` Redis URL secret file; environment-
  only credentials and default-user fallback fail before client construction.
- Define canonical PDS key prefixes and exact commands from source inventory.
- Readiness reports authenticated user mismatch without returning URLs,
  password hashes, keys, or payloads.
- Existing uncredentialed development remains available only outside production.
- PDS push remains independent and exact-commit CI-gated.

**Verification:**

```bash
go test ./...
go vet ./...
git diff --check
```

### Task 4: Add VP Control And Aggregator Named-User Contracts

**Repository:** `videoprocess`

**Files:**
- Modify: Go API/orchestrator Redis configuration
- Modify: Python API/event/ChannelOps Redis configuration
- Modify: `services/vp-feature-aggregator` Redis configuration
- Modify: related startup/readiness/deploy tests
- Modify: `deploy/swarm/deploy-sync-extension.sh`

**Requirements:**

- Production control-plane and aggregator services require mode `0400` Redis
  URL secrets before client construction.
- Inventory and test every command/key prefix. `+@all` and `~*` are forbidden.
- Aggregator uses its own canonical prefixes; database number 2 is compatibility
  routing, not the ACL boundary.
- Service readiness proves exact named identity and cross-prefix denial.
- Deploy transaction mounts versioned secrets, removes credential URLs from
  service environments, and rolls back to fresh prior-generation secrets.

**Verification:**

```bash
go test ./...
cd backend
python3 -m pytest
cd ..
bash tests/test_vp_deploy_sync_extension.sh
git diff --check
```

### Task 5: Add Per-Worker And Read-Only Watcher ACL Contracts

**Repository:** `videoprocess`

**Files:**
- Modify: Python worker secret configuration
- Modify: Go worker secret configuration
- Modify: worker lifecycle tests
- Modify: `deploy/swarm/channelops-soak-watch.sh`
- Modify: worker-registration guard/preflight
- Modify: deployment tests and runbooks

**Requirements:**

- Each worker identity can use only its exact task stream, `vp:events`, and the
  reviewed stream commands.
- Cross-worker stream access fails in Redis integration tests.
- Watcher permits only `PING`, `XINFO`, and `XPENDING` on four exact streams.
- Worker/watcher production startup rejects default user and environment-only
  Redis credentials before the first Redis command.
- Registration preflight fails on missing, duplicate, unexpected, or default
  Redis identities.

**Verification:**

```bash
go test ./internal/worker ./cmd/vp-ffmpeg-worker
cd backend
python3 -m pytest tests/worker tests/services/test_worker_registration_guard.py
cd ..
bash tests/test_channelops_soak_watch.sh
git diff --check
```

### Task 6: Deploy And Verify Phase A

**Repositories:** all three

**Requirements:**

- Merge/push each reviewed repository commit and require exact CI success.
- Let normal pull-based controllers deploy code and credential support.
- Close schedule and prove zero active jobs/nodes/pending entries.
- Run runtime ACL `prepare` then `phase-a`.
- Restart Redis once, then deploy every client with its named-user secret.
- Verify authenticated identity, allowed operations, cross-user denial,
  reconnect, service health, and data counts.
- Require zero clients reporting `default`; otherwise restore affected client
  generation and keep the schedule closed.
- Save a sanitized mode `0600` evidence bundle.

### Task 7: Enforce Phase B And Prove Persistence

**Repositories:** all three

**Requirements:**

- Re-run exact CI/deploy, closed schedule, idle database, and pending-zero gates.
- Apply `phase-b`, restart Redis, and prove `default` is off.
- Prove unauthenticated PING and all cross-prefix/cross-stream operations fail.
- Restart Redis a second time and prove ACL persistence and client reconnection.
- Run VP durable-registration and unknown-consumer preflights.
- Record exact repository/deployed commits and sanitized evidence.
- Keep schedule and channels closed after success. Operation resume and any
  canary remain separate explicit actions.

