# CI-Gated Independent Deploy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make automatic VideoProcess and PDS deployment require successful GitHub Actions evidence for the exact commit while allowing either repository to deploy when the other fails.

**Architecture:** Each repository owns a push/PR workflow. The repository-owned VideoProcess deploy extension queries the authenticated GitHub CLI on 150 before image builds and fails closed unless the latest push workflow attempt for the exact SHA completed successfully. The 150 cron runs VideoProcess and PDS as separate controller invocations.

**Tech Stack:** GitHub Actions, Bash, GitHub CLI, Go 1.25, Python 3.12, PostgreSQL 16, Node.js 22.

## Global Constraints

- Do not open the VideoProcess schedule, activate a channel, enable the soak watcher, or call YouTube upload APIs.
- Do not add SSH or production credentials to GitHub Actions.
- Do not query, synchronize, build on, or schedule VideoProcess work on 126.
- Applying deployments have no CI bypass; missing, pending, failed, cancelled, or mismatched runs fail closed.
- Dry runs with both build and service updates disabled remain available without CI.
- Keep Ruff, mypy, and frontend lint advisory, matching `AGENTS.md`; tests and builds remain blocking.

---

### Task 1: VideoProcess CI Evidence Workflow

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `tests/test_ci_workflow_contract.sh`

**Interfaces:**
- Produces workflow file identifier: `ci.yml`.
- Produces workflow name: `VideoProcess CI`.
- Produces blocking jobs: `backend`, `go`, `frontend`, and `deploy-contracts`.

- [ ] **Step 1: Write the failing static workflow contract**

Create `tests/test_ci_workflow_contract.sh` and require:

```bash
workflow="$ROOT/.github/workflows/ci.yml"
test -f "$workflow"
grep -Fq 'name: VideoProcess CI' "$workflow"
grep -Fq 'python-version: "3.12"' "$workflow"
grep -Fq 'go-version-file: go.mod' "$workflow"
grep -Fq 'node-version: "22"' "$workflow"
grep -Fq 'CHANNEL_OPS_POSTGRES_TEST_URL:' "$workflow"
grep -Fq '.venv/bin/python -m pytest' "$workflow"
grep -Fq 'go test ./...' "$workflow"
grep -Fq 'npm run build' "$workflow"
grep -Fq 'bash tests/test_vp_deploy_sync_extension.sh' "$workflow"
grep -Fq 'actions/upload-artifact@v7' "$workflow"
```

- [ ] **Step 2: Run the contract and verify RED**

Run: `bash tests/test_ci_workflow_contract.sh`

Expected: non-zero because `.github/workflows/ci.yml` does not exist.

- [ ] **Step 3: Implement the workflow**

Use `actions/checkout@v6`, `actions/setup-python@v6`,
`actions/setup-go@v6`, `actions/setup-node@v7`, and
`actions/upload-artifact@v7`. Give the workflow read-only contents permission.
Run backend pytest with a PostgreSQL 16 service and
`CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres`.
Capture `alembic heads`, pytest, Go, frontend build, and deployment-contract
output beneath `ci-evidence/`, with `commit.txt` in every artifact.

- [ ] **Step 4: Run workflow contract and local equivalents**

Run:

```bash
bash tests/test_ci_workflow_contract.sh
go test ./...
cd backend && /Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest
cd ../frontend && npm run build
```

Expected: contract exits 0, Go passes, backend reports 664 passed with
environment-dependent skips, and frontend build exits 0.

- [ ] **Step 5: Commit the workflow**

```bash
git add .github/workflows/ci.yml tests/test_ci_workflow_contract.sh
git commit -m "ci: verify VideoProcess commits"
```

### Task 2: Exact-SHA Deploy Gate

**Files:**
- Modify: `deploy/swarm/deploy-sync-extension.sh`
- Create: `tests/test_vp_deploy_ci_gate.sh`
- Modify: `tests/test_vp_deploy_sync_extension.sh`

**Interfaces:**
- Produces: `vp_require_github_actions_success REPOSITORY WORKFLOW_FILE COMMIT`.
- Uses VideoProcess workflow: `Ctwqk/videoprocess`, `ci.yml`.
- Uses PDS workflow: `Ctwqk/policy-decision-service`, `ci.yml`.
- Overrides build entry points: `build_vp_app_images`, `build_feature_aggregator_images`, and `build_pds_images`.

- [ ] **Step 1: Write failing gate behavior tests**

The test supplies a fake `gh` executable and asserts:

```text
completed + success + exact head SHA -> build command executes
queued + null conclusion -> no build command
completed + failure -> no build command
no workflow run -> no build command
different returned head SHA -> no build command
invalid repository/workflow/SHA -> no gh call and no build command
BUILD_IMAGES=0 plus UPDATE_SERVICES=0 -> gh is not called
```

It must exercise all three build entry points and assert the CI call appears in
the trace before the first image build call.

- [ ] **Step 2: Run the gate test and verify RED**

Run: `bash tests/test_vp_deploy_ci_gate.sh`

Expected: non-zero because `vp_require_github_actions_success` and the PDS and
feature-aggregator guarded build overrides do not exist.

- [ ] **Step 3: Implement strict GitHub Actions lookup**

Validate inputs with Bash regular expressions. Query:

```bash
gh api --method GET \
  "repos/$repository/actions/workflows/$workflow/runs" \
  -f "head_sha=$commit" -f event=push -f per_page=20 --jq "$filter"
```

The jq filter returns either `missing` or the latest attempt's status,
conclusion, head SHA, and run ID. Proceed only for exact-head
`completed/success`. Do not log URLs, response bodies, or token-bearing state.

- [ ] **Step 4: Guard all repository build entry points**

Call the gate before the first build in `build_vp_app_images`. Add extension
overrides for `build_feature_aggregator_images` and `build_pds_images` that
preserve the controller's existing image tags and target paths but call the
matching repository workflow gate first.

- [ ] **Step 5: Pass focused deployment tests**

Run:

```bash
bash tests/test_vp_deploy_ci_gate.sh
bash tests/test_vp_deploy_sync_extension.sh
bash -n deploy/swarm/deploy-sync-extension.sh
```

Expected: all exit 0.

- [ ] **Step 6: Commit the deploy gate**

```bash
git add deploy/swarm/deploy-sync-extension.sh \
  tests/test_vp_deploy_ci_gate.sh tests/test_vp_deploy_sync_extension.sh
git commit -m "feat: gate automatic deploys on exact CI SHA"
```

### Task 3: Independent PDS CI

**Files:**
- Create in PDS repository: `.github/workflows/ci.yml`

**Interfaces:**
- Produces workflow file identifier: `ci.yml`.
- Produces workflow name: `Policy Decision Service CI`.

- [ ] **Step 1: Verify the workflow is absent**

Run in `/Users/wenjieliu/policy-decision-service`:

```bash
test ! -e .github/workflows/ci.yml
```

Expected: exit 0 before implementation.

- [ ] **Step 2: Add the PDS workflow**

Use `actions/checkout@v6`, `actions/setup-go@v6`, and
`actions/upload-artifact@v7`. Run with `set -o pipefail`:

```bash
go test ./... | tee ci-evidence/go-test.txt
go vet ./... | tee ci-evidence/go-vet.txt
go build -o /tmp/pds-ci-server ./cmd/server
```

Write `$GITHUB_SHA` to `ci-evidence/commit.txt` and upload the directory.

- [ ] **Step 3: Verify locally and commit**

Run:

```bash
go test ./...
go vet ./...
go build -o /tmp/pds-ci-server ./cmd/server
git add .github/workflows/ci.yml
git commit -m "ci: verify PDS commits"
```

Expected: all commands exit 0.

### Task 4: Push, Split Cron, And Verify Deployment

**Files:**
- Modify: `deploy/four-machine-topology.md`

**Interfaces:**
- VideoProcess cron invokes `--project vp-app --project vp-feature-aggregator`.
- PDS cron invokes `--project vp-pds` independently.

- [ ] **Step 1: Document the independent cron contract**

Replace the single three-project example with two marked commands and state
that each command requires exact-SHA CI success through the deploy extension.

- [ ] **Step 2: Run complete local verification**

Run the required backend, Go, frontend, deployment, soak, canary-script, and
syntax checks. Confirm the only root-main untracked file remains the user's
original plan.

- [ ] **Step 3: Push VideoProcess and wait for its workflow**

Fast-forward merge the worktree branch to local `main`, push `main`, query the
`ci.yml` workflow for the pushed SHA, and wait for `completed/success` before
applying any deployment.

- [ ] **Step 4: Push PDS and wait for its workflow**

Fast-forward PDS `main`, push, and require its `ci.yml` push run for that exact
SHA to report `completed/success`.

- [ ] **Step 5: Split the 150 cron transactionally**

Read the current crontab, replace only the marked `VIDEOPROCESS DEPLOY` block,
install it, read it back, and compare. Preserve the disabled all-project cron,
Constructure schedule, and soak watcher block byte-for-byte outside the
managed block.

- [ ] **Step 6: Dry-run and apply each project group**

Through the 127 jump host, run VideoProcess and PDS dry runs, then scoped
applies. Require exact source/deployment SHA parity, healthy service replicas,
PDS health, placement on 127/150 as designed, and zero VideoProcess tasks on
126.

- [ ] **Step 7: Re-run the read-only unlisted canary preflight**

Keep the schedule closed and perform no upload. Require empty runnable backlog,
zero public rows, no new upload operation, no new publication, and no enabled
soak state. A real canary still requires its separate exact approval phrase.
