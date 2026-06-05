# VP Feature Aggregator Merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `vp-feature-aggregator` into the VideoProcess repository so the video-generation application code is owned by two repos: `videoprocess` and `policy-decision-service`.

**Architecture:** Import the aggregator as a repo-local, independently built service under `services/vp-feature-aggregator`. Keep the runtime service name `vp-feature-aggregator`, the HTTP contract `GET /v1/features/{actor_id}`, and the Kafka topics unchanged. Rename the Python package from `app` to `feature_aggregator` so it cannot collide with VideoProcess backend's existing `backend/app` package.

**Tech Stack:** Python 3.12, FastAPI, Pydantic, aiokafka, Redis, asyncpg, Docker Compose, Redpanda, pytest, ruff.

---

## Direct Answer

It is not just "copy the folder and stop." Copying the source is the first mechanical step, but the migration also needs package renaming, Docker build path changes, compose path changes, documentation updates, and focused verification.

Plain copying is enough only if all of these are true:

- Git history for `vp-feature-aggregator` does not need to be preserved inside `videoprocess`.
- The copied service is still built from its own directory, not installed into `backend/app`.
- The Python package is renamed away from `app`.
- `docker-compose.pds-kafka.yml` points to the new in-repo service path.
- README and deployment docs stop pointing operators to the old aggregator repo.

If commit history must be preserved, use `git subtree add` instead of file copy. The recommended path for this repo is a normal copy plus a commit message recording the source repo and source commit, because the aggregator repo is small and its public GitHub repo can remain archived as historical reference.

## File Map

Create inside `videoprocess`:

- `services/vp-feature-aggregator/AGENTS.md`: service-local instructions and checks.
- `services/vp-feature-aggregator/README.md`: aggregator runbook updated for its new in-repo location.
- `services/vp-feature-aggregator/pyproject.toml`: standalone package metadata with package name `vp-feature-aggregator` and import package `feature_aggregator`.
- `services/vp-feature-aggregator/.gitignore`: service-local ignores copied from the source repo.
- `services/vp-feature-aggregator/feature_aggregator/`: renamed copy of source `app/`.
- `services/vp-feature-aggregator/schemas/`: copied JSON schemas for `vp.actor.actions.v1` and `pds.decisions.v1`.
- `services/vp-feature-aggregator/tests/`: copied tests with imports updated to `feature_aggregator`.
- `services/vp-feature-aggregator/deploy/Dockerfile`: standalone aggregator image build.

Modify inside `videoprocess`:

- `docker-compose.pds-kafka.yml`: build aggregator from `./services/vp-feature-aggregator`; keep service name `vp-feature-aggregator`.
- `README.md`: describe aggregator as an internal service directory instead of a sibling repo.
- `docs/pds-kafka-smoke.md`: update path notes and smoke commands where they imply an external aggregator checkout.
- `docs/constructure/infra-services.md`: update source-of-truth row for VP Feature Aggregator to the VP repo subdirectory.
- `deploy/four-machine-topology.md`: update task-to-entry mapping for feature aggregation to point to `services/vp-feature-aggregator`.

Do not modify in this migration:

- `backend/app/pds_client.py`: PDS still calls `http://vp-feature-aggregator:8080` through config.
- `backend/app/events/*`: the outbox and relay contract stays unchanged.
- `policy-decision-service` code: PDS depends on the HTTP and Kafka contracts, not the aggregator repo layout.

## Task 1: Preflight And Source Snapshot

**Files:**

- Inspect: `/home/kikuhiko/videoprocess`
- Inspect: `/home/kikuhiko/vp-feature-aggregator`

- [ ] **Step 1: Verify both worktrees are clean**

Run:

```bash
cd /home/kikuhiko/videoprocess
git status --short
cd /home/kikuhiko/vp-feature-aggregator
git status --short
```

Expected: both commands print no changed files. If either repo is dirty, stop and inspect the changed files before continuing.

- [ ] **Step 2: Record source commit**

Run:

```bash
cd /home/kikuhiko/vp-feature-aggregator
git rev-parse --short HEAD
git remote -v
```

Expected: source commit is recorded in notes and the remote is `https://github.com/Ctwqk/vp-feature-aggregator.git`.

- [ ] **Step 3: Create execution branch in VP**

Run:

```bash
cd /home/kikuhiko/videoprocess
git switch -c codex/merge-vp-feature-aggregator
```

Expected: branch `codex/merge-vp-feature-aggregator` is active.

## Task 2: Copy Aggregator Into VP

**Files:**

- Create: `/home/kikuhiko/videoprocess/services/vp-feature-aggregator/`
- Source: `/home/kikuhiko/vp-feature-aggregator/`

- [ ] **Step 1: Copy source files without nested git metadata**

Run:

```bash
cd /home/kikuhiko/videoprocess
mkdir -p services/vp-feature-aggregator
rsync -a \
  --exclude .git \
  --exclude .pytest_cache \
  --exclude __pycache__ \
  /home/kikuhiko/vp-feature-aggregator/ \
  services/vp-feature-aggregator/
```

Expected: `services/vp-feature-aggregator/pyproject.toml`, `services/vp-feature-aggregator/app/main.py`, `services/vp-feature-aggregator/schemas/vp.actor.actions.v1.json`, and `services/vp-feature-aggregator/tests/test_api.py` exist.

- [ ] **Step 2: Verify no nested repository was copied**

Run:

```bash
cd /home/kikuhiko/videoprocess
find services/vp-feature-aggregator -maxdepth 2 -name .git -print
```

Expected: no output.

## Task 3: Rename Aggregator Python Package

**Files:**

- Move: `/home/kikuhiko/videoprocess/services/vp-feature-aggregator/app/` to `/home/kikuhiko/videoprocess/services/vp-feature-aggregator/feature_aggregator/`
- Modify: `/home/kikuhiko/videoprocess/services/vp-feature-aggregator/pyproject.toml`
- Modify: `/home/kikuhiko/videoprocess/services/vp-feature-aggregator/deploy/Dockerfile`
- Modify: `/home/kikuhiko/videoprocess/services/vp-feature-aggregator/tests/*.py`
- Modify: `/home/kikuhiko/videoprocess/services/vp-feature-aggregator/feature_aggregator/**/*.py`

- [ ] **Step 1: Rename the package directory**

Run:

```bash
cd /home/kikuhiko/videoprocess/services/vp-feature-aggregator
mv app feature_aggregator
```

Expected: `feature_aggregator/main.py` exists and `app/` does not exist.

- [ ] **Step 2: Rewrite imports and uvicorn target**

Run:

```bash
cd /home/kikuhiko/videoprocess/services/vp-feature-aggregator
find feature_aggregator tests -type f -name '*.py' -print0 \
  | xargs -0 perl -0pi -e 's/from app\./from feature_aggregator./g; s/import app\./import feature_aggregator./g'
perl -0pi -e 's/include = \["app\*"\]/include = ["feature_aggregator*"]/g' pyproject.toml
perl -0pi -e 's/COPY app \.\/app/COPY feature_aggregator .\/feature_aggregator/g; s/app\.main:app/feature_aggregator.main:app/g' deploy/Dockerfile
perl -0pi -e 's/uvicorn app\.main:app/uvicorn feature_aggregator.main:app/g' README.md
```

Expected:

```bash
grep -R "from app\\.\\|import app\\.\\|app.main:app\\|COPY app" -n feature_aggregator tests pyproject.toml deploy/Dockerfile README.md
```

prints no output.

- [ ] **Step 3: Run focused aggregator tests**

Run:

```bash
cd /home/kikuhiko/videoprocess/services/vp-feature-aggregator
python3 -m pip install -e '.[dev]'
python3 -m pytest -q
```

Expected: all aggregator tests pass.

## Task 4: Update Compose Paths

**Files:**

- Modify: `/home/kikuhiko/videoprocess/docker-compose.pds-kafka.yml`

- [ ] **Step 1: Point aggregator build context at the in-repo service**

Change the `vp-feature-aggregator` service build block to:

```yaml
  vp-feature-aggregator:
    build:
      context: ./services/vp-feature-aggregator
      dockerfile: deploy/Dockerfile
```

Keep all existing `AGG_*`, `RISK_*`, `depends_on`, `extra_hosts`, and network settings unchanged.

- [ ] **Step 2: Make the PDS sibling repo path explicit and configurable**

Change the PDS build and config mount paths to:

```yaml
  pds:
    build:
      context: ${PDS_REPO_PATH:-../policy-decision-service}
      dockerfile: deploy/Dockerfile
    volumes:
      - ${PDS_CONFIG_PATH:-../policy-decision-service/config}:/etc/pds:ro
```

Keep `PDS_FEATURE_PROVIDER_URL: http://vp-feature-aggregator:8080` unchanged.

- [ ] **Step 3: Render the compose config**

Run:

```bash
cd /home/kikuhiko/videoprocess
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml config
```

Expected: config renders successfully, `vp-feature-aggregator` build context is `/home/kikuhiko/videoprocess/services/vp-feature-aggregator`, and PDS resolves to `/home/kikuhiko/policy-decision-service` unless overridden.

## Task 5: Update Docs And Operator References

**Files:**

- Modify: `/home/kikuhiko/videoprocess/README.md`
- Modify: `/home/kikuhiko/videoprocess/docs/pds-kafka-smoke.md`
- Modify: `/home/kikuhiko/videoprocess/docs/constructure/infra-services.md`
- Modify: `/home/kikuhiko/videoprocess/deploy/four-machine-topology.md`
- Modify: `/home/kikuhiko/videoprocess/services/vp-feature-aggregator/README.md`
- Modify: `/home/kikuhiko/videoprocess/services/vp-feature-aggregator/AGENTS.md`

- [ ] **Step 1: Update VP root README repository boundary text**

In `README.md`, replace the old boundary statement:

```text
The repository is a polyrepo-friendly monorepo: PDS and the feature aggregator live in their own repos and communicate over HTTP and Kafka topics, while this repository owns the media workflows, the channel-ops orchestrator, and the Python integration glue.
```

with:

```text
The repository is a polyrepo-friendly monorepo: PDS remains a standalone repo, while the VP feature aggregator now lives under `services/vp-feature-aggregator/` and still communicates with PDS over HTTP and Kafka topics.
```

Also update the related-repo bullet for `Ctwqk/vp-feature-aggregator` to say it is the archived source repo or remove that bullet after confirming the old GitHub repo has been archived.

- [ ] **Step 2: Update smoke runbook paths**

In `docs/pds-kafka-smoke.md`, update the PDS path notes to use:

```text
`${PDS_REPO_PATH:-../policy-decision-service}`
```

and add this sentence near the compose check section:

```text
`vp-feature-aggregator` now builds from `services/vp-feature-aggregator/` inside this repository.
```

- [ ] **Step 3: Update production source-of-truth docs**

In `docs/constructure/infra-services.md`, change the VP Feature Aggregator source location from:

```text
10.0.0.150:/home/taiwei/Constructure-repos/vp-feature-aggregator
```

to:

```text
VideoProcess repo subdirectory `services/vp-feature-aggregator`
```

In `deploy/four-machine-topology.md`, change the feature aggregation entry from:

```text
VP event schema plus `Ctwqk/vp-feature-aggregator`
```

to:

```text
VP event schema plus `services/vp-feature-aggregator`
```

- [ ] **Step 4: Update service-local AGENTS and README**

In `services/vp-feature-aggregator/AGENTS.md`, replace the old boundary line:

```text
Do not edit PDS, VideoProcess, k8s, or sibling Constructure repos from this workspace.
```

with:

```text
This service lives inside the VideoProcess repo but must remain independently buildable and testable. Do not move decision logic into this service, and do not mutate VideoProcess or PDS source-of-truth records.
```

In `services/vp-feature-aggregator/README.md`, update related repositories so `videoprocess` is described as the owning repo and `policy-decision-service` remains the external decision service.

## Task 6: Verify Aggregator Build And Compose Integration

**Files:**

- Verify: `/home/kikuhiko/videoprocess/services/vp-feature-aggregator`
- Verify: `/home/kikuhiko/videoprocess/docker-compose.pds-kafka.yml`

- [ ] **Step 1: Run aggregator checks**

Run:

```bash
cd /home/kikuhiko/videoprocess/services/vp-feature-aggregator
python3 -m pytest -q
python3 -m ruff check . || true
docker build -f deploy/Dockerfile -t vp-feature-aggregator:local .
```

Expected: pytest passes, ruff output is reviewed, and Docker image builds successfully.

- [ ] **Step 2: Render merged compose**

Run:

```bash
cd /home/kikuhiko/videoprocess
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml config
```

Expected: compose renders and the aggregator build context is in the VP repo.

- [ ] **Step 3: Run smoke stack if Docker is available**

Run:

```bash
cd /home/kikuhiko/videoprocess
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml \
  up -d --build redpanda pds vp-feature-aggregator event-outbox-relay
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml \
  exec vp-feature-aggregator python - <<'PY'
from urllib.request import urlopen

for url in (
    "http://vp-feature-aggregator:8080/healthz",
    "http://vp-feature-aggregator:8080/readyz",
):
    print(url, urlopen(url, timeout=5).read().decode())
PY
```

Expected: aggregator health checks return successful responses. If `AGG_ENABLE_CONSUMER=true` and Kafka is not ready, `/readyz` may fail; inspect logs before treating that as a code failure.

## Task 7: Commit And Decommission Old Repo Usage

**Files:**

- Stage: all modified and created VP files.
- Do not delete: `/home/kikuhiko/vp-feature-aggregator` during this task.

- [ ] **Step 1: Review VP diff**

Run:

```bash
cd /home/kikuhiko/videoprocess
git status --short
git diff -- docker-compose.pds-kafka.yml README.md docs/pds-kafka-smoke.md docs/constructure/infra-services.md deploy/four-machine-topology.md
git diff -- services/vp-feature-aggregator/pyproject.toml services/vp-feature-aggregator/deploy/Dockerfile
```

Expected: diff shows only aggregator import, package rename, path updates, and docs updates.

- [ ] **Step 2: Commit VP migration**

Run:

```bash
cd /home/kikuhiko/videoprocess
git add docker-compose.pds-kafka.yml README.md docs/pds-kafka-smoke.md docs/constructure/infra-services.md deploy/four-machine-topology.md services/vp-feature-aggregator
git commit -m "chore: vendor vp feature aggregator service"
```

Expected: commit succeeds. Include the source aggregator commit in the commit body if using a non-interactive commit flow that supports `-m` twice:

```bash
git commit \
  -m "chore: vendor vp feature aggregator service" \
  -m "Source: Ctwqk/vp-feature-aggregator at 8d1d79f."
```

- [ ] **Step 3: Freeze external aggregator repo after VP is merged**

Run only after the VP branch is merged and deployed:

```bash
cd /home/kikuhiko/vp-feature-aggregator
git status --short
```

Expected: clean working tree. Then update the external repo README or GitHub archive setting outside this VP implementation branch so future work starts in `videoprocess/services/vp-feature-aggregator`.

## Self-Review

- Spec coverage: the plan imports aggregator into VP, preserves service/runtime boundaries, keeps PDS as a separate repo, updates compose, updates docs, and defines verification.
- Placeholder scan: no implementation step depends on unresolved placeholders.
- Type consistency: the package name is consistently `feature_aggregator`; the Docker/uvicorn target is `feature_aggregator.main:app`; the service name remains `vp-feature-aggregator`.
