# PDS Health-Gated Automatic Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every PDS `main` push deploy automatically through host 150 only after the exact image is healthy on host 127, with host 126 excluded.

**Architecture:** The PDS binary gains a bounded, dependency-free `probe` subcommand used by an exec-form Dockerfile healthcheck. The VideoProcess controller converges PDS placement and health timing, waits for the actual 127 container to inherit the image command and become healthy, and verifies rollback with the same gate.

**Tech Stack:** Go 1.25, `net/http`, Dockerfile, Docker Swarm, Bash, GitHub Actions, SSH.

## Global Constraints

- PDS runs on `colima-127`; the controller and shared services run on `ccttww-lap`.
- Host 126 is never selected, built on, probed, or used for rollback.
- The PDS image remains `scratch` and must not depend on a shell, curl, or wget.
- The probe performs one credential-free loopback `/readyz` request and prints only `status=ready` or `status=not_ready`.
- Deployment success is recorded only after exact image, placement, health command, timing, and status are verified.
- A real PDS source push, exact-SHA CI success, and the normal minute-7 poller must prove the final automatic chain.
- No sixth YouTube canary is allowed without separate explicit authorization.

---

## File Map

- PDS `cmd/server/main.go`: dispatch `probe` before server configuration is loaded.
- PDS `cmd/server/probe.go`: parse arguments, perform one strict readiness request, and map outcomes to stable exit codes/output.
- PDS `cmd/server/probe_test.go`: exercise the probe through real `httptest` servers.
- PDS `cmd/server/dockerfile_test.go`: lock the shell-free Docker health contract.
- PDS `deploy/Dockerfile`: declare the exec-form healthcheck.
- PDS `.github/workflows/ci.yml`: build and inspect the deployable image, run
  the probe fixture, and prove Swarm health inheritance.
- VP `deploy/swarm/deploy-sync-extension.sh`: converge PDS placement and perform bounded remote readiness/rollback verification.
- VP `tests/test_vp_deploy_sync_extension.sh`: simulate PDS container convergence, failure, and rollback.

### Task 1: PDS Probe Command

**Files:**
- Create: `/Users/wenjieliu/policy-decision-service/cmd/server/probe.go`
- Create: `/Users/wenjieliu/policy-decision-service/cmd/server/probe_test.go`
- Modify: `/Users/wenjieliu/policy-decision-service/cmd/server/main.go`

**Interfaces:**
- Produces: `dispatchCommand(args []string, stdout io.Writer) (handled bool, exitCode int)`
- Produces: `runProbe(args []string, stdout io.Writer) int`
- Produces: process command `pds probe --url <loopback-readyz-url> [--timeout 2s]`

- [x] **Step 1: Write the failing probe tests**

Use an `httptest.Server` table to require:

```go
tests := []struct {
    name       string
    handler    http.Handler
    wantCode   int
    wantOutput string
}{
    {"ready", jsonResponse(200, `{"status":"ready"}`), 0, "status=ready\n"},
    {"non-200", jsonResponse(503, `{"status":"not_ready"}`), 3, "status=not_ready\n"},
    {"malformed", jsonResponse(200, `{`), 3, "status=not_ready\n"},
    {"unknown field", jsonResponse(200, `{"status":"ready","detail":"secret"}`), 3, "status=not_ready\n"},
    {"trailing json", jsonResponse(200, `{"status":"ready"} {}`), 3, "status=not_ready\n"},
}
```

Add focused cases for a redirect, a server slower than `20ms`, missing URL,
HTTPS/non-loopback/wrong-path URL, zero/negative/over-`10s` timeout, unknown
flag, and a response containing a sentinel secret. Assert one request at most,
exact exit code, exact stable output, and absence of URL/body/error text.

- [x] **Step 2: Run the probe tests and verify RED**

Run:

```bash
cd /Users/wenjieliu/policy-decision-service
go test ./cmd/server -run 'Test(RunProbe|DispatchCommand)' -count=1
```

Expected: compilation fails because `runProbe` and `dispatchCommand` do not
exist.

- [x] **Step 3: Implement strict argument parsing and HTTP validation**

Implement these constants and functions in `probe.go`:

```go
const (
    probeReadyExit   = 0
    probeUsageExit   = 2
    probeFailureExit = 3
)

func dispatchCommand(args []string, stdout io.Writer) (bool, int)
func runProbe(args []string, stdout io.Writer) int
func validProbeURL(raw string) bool
func readyResponse(response *http.Response) bool
```

Use `flag.NewFlagSet("probe", flag.ContinueOnError)` with output discarded.
Require exactly zero positional arguments, a literal loopback `http` URL with
the exact `/readyz` path and no userinfo/query/fragment, and a timeout in
`(0, 10s]`. Configure `http.Client.Timeout` and return
`http.ErrUseLastResponse` from `CheckRedirect`. Read at most 1025 bytes,
reject bodies over 1024 bytes, and parse JSON tokens so unknown or duplicate
keys fail. Require exactly `{"status":"ready"}` followed by EOF. Every return
path writes exactly one stable status line.

- [x] **Step 4: Dispatch before configuration loading**

Change `main()` to:

```go
func main() {
    if handled, exitCode := dispatchCommand(os.Args[1:], os.Stdout); handled {
        os.Exit(exitCode)
    }
    runServer()
}

func runServer() {
    // Existing server startup body, unchanged.
}
```

Add a compiled-binary subprocess test proving `probe` completes with poisoned
server configuration, exact stdout, and empty stderr. Empty arguments start
the server; unknown commands return usage exit `2` instead of starting it.

- [x] **Step 5: Run formatting and focused tests**

Run:

```bash
cd /Users/wenjieliu/policy-decision-service
gofmt -w cmd/server/main.go cmd/server/probe.go cmd/server/probe_test.go
go test ./cmd/server -run 'Test(RunProbe|DispatchCommand)' -count=1
```

Expected: all focused tests pass.

- [x] **Step 6: Commit the probe**

```bash
cd /Users/wenjieliu/policy-decision-service
git add cmd/server/main.go cmd/server/probe.go cmd/server/probe_test.go
git commit -m "feat: add bounded readiness probe"
```

### Task 2: Shell-Free Container Health Contract

**Files:**
- Create: `/Users/wenjieliu/policy-decision-service/cmd/server/dockerfile_test.go`
- Modify: `/Users/wenjieliu/policy-decision-service/deploy/Dockerfile`
- Modify: `/Users/wenjieliu/policy-decision-service/.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `pds probe` from Task 1.
- Produces: image health command `["CMD","/usr/local/bin/pds","probe","--url","http://127.0.0.1:8080/readyz","--timeout","2s"]`.

- [x] **Step 1: Write the failing Dockerfile contract test**

Read `../../deploy/Dockerfile` and require this exact single-line instruction:

```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 CMD ["/usr/local/bin/pds", "probe", "--url", "http://127.0.0.1:8080/readyz", "--timeout", "2s"]
```

Also assert the final stage remains `FROM scratch` and that the health line
contains no `CMD-SHELL`, `curl`, `wget`, or `/bin/sh`.
Require the Go Alpine build stage by exact digest and reject `RUN apk add` or
an unpinned Alpine certificate stage.

- [x] **Step 2: Run the Dockerfile test and verify RED**

Run:

```bash
cd /Users/wenjieliu/policy-decision-service
go test ./cmd/server -run TestDockerfileHealthcheck -count=1
```

Expected: failure because the healthcheck is absent.

- [x] **Step 3: Add the exec-form healthcheck**

Insert the exact instruction after `COPY --from=build /out/pds
/usr/local/bin/pds` and before `USER 10001:10001`.

- [x] **Step 4: Run all PDS checks**

Run:

```bash
cd /Users/wenjieliu/policy-decision-service
gofmt -w cmd/server/dockerfile_test.go
go test ./...
go vet ./...
go build -o /tmp/pds-health-gated ./cmd/server
docker build -f deploy/Dockerfile -t vp-pds:health-gated-test .
docker image inspect vp-pds:health-gated-test --format '{{json .Config.Healthcheck}}'
```

Expected: checks pass; image inspection reports the exact `CMD` probe and
`10s`/`3s`/`10s`/`6` timing. CI additionally creates a disposable single-node
Swarm service with a stale shell health command, updates it with empty
`--health-cmd`, and inspects the running task container to prove exec-form
image inheritance through the production CLI path. It also builds and runs
the arm64 image used by host 127 under QEMU, with bounded job and Swarm update
timeouts.

- [x] **Step 5: Commit the image contract**

```bash
cd /Users/wenjieliu/policy-decision-service
git add cmd/server/dockerfile_test.go deploy/Dockerfile
git add .github/workflows/ci.yml
git commit -m "feat: healthcheck pds image"
```

### Task 3: PDS-Specific Swarm Readiness Gate

**Files:**
- Modify: `/Users/wenjieliu/videoprocess/tests/test_vp_deploy_sync_extension.sh`
- Modify: `/Users/wenjieliu/videoprocess/deploy/swarm/deploy-sync-extension.sh`

**Interfaces:**
- Consumes: the image health contract from Task 2.
- Produces: `vp_require_pds_ready(expectedImage string)` as a bounded Bash gate.
- Produces: `deploy_pds_services(image string)` with verified rollback.

- [x] **Step 1: Extend the fake deployment environment**

Add PDS state with safe defaults:

```bash
PDS_TASK_NODE=colima-127
PDS_TASK_STATE='Running 2 seconds ago'
PDS_READINESS_MODE=healthy
PDS_READINESS_CALLS=0
PDS_CURRENT_IMAGE=baseline-vp-pds-swarm:stable
PDS_EXPECTED_TEST='["CMD","/usr/local/bin/pds","probe","--url","http://127.0.0.1:8080/readyz","--timeout","2s"]'
```

Teach fake `docker service ps` to return the PDS node/state. Add fake
`remote_sh` that records only host and fixed command arguments, consumes the
remote script from stdin, and emits a sanitized snapshot:

```text
<image>|:8080|healthy|<expected-test>|10s|3s|10s|6
```

Modes must cover `starting-then-healthy`, container-set transition,
`missing`, `duplicate`, `wrong-image`, `wrong-http-addr`, `wrong-command`,
`wrong-timing`, `unhealthy`, malformed output, and `remote-error`.

- [x] **Step 2: Write failing deployment tests**

Require:

- the update removes stale constraints and adds both exact 127 constraints;
- constraint inspection failure stops before candidate or rollback mutation;
- the update replaces stale `PDS_HTTP_ADDR` values with exactly `:8080`;
- the update contains `--health-cmd ''`, `--health-interval 10s`,
  `--health-timeout 3s`, `--health-start-period 10s`, and
  `--health-retries 6`;
- `vp_require_service_node vp-pds-swarm colima-127` runs before remote health;
- starting health is retried and bounded;
- missing/duplicate/wrong image/wrong command/wrong timing/unhealthy/remote
  failure all fail closed;
- no remote call contains host 126, container IDs, database URLs, or test
  credential sentinels;
- the extracted production heredoc runs under `/bin/sh` with fake Docker and
  sanitizes missing/duplicate/inspect-race states without exposing IDs;
- a new-image readiness failure restores the baseline image with `stop-first`;
- rollback readiness is checked, and rollback readiness failure remains
  failed;
- success prints only `vp-pds-swarm`.

- [x] **Step 3: Run the deployment contract and verify RED**

Run:

```bash
cd /Users/wenjieliu/videoprocess
bash tests/test_vp_deploy_sync_extension.sh
```

Expected: failure at the first new PDS health assertion.

- [x] **Step 4: Converge service health inheritance**

For `vp-pds-swarm`, append these arguments in
`vp_update_runtime_service()`:

```bash
--health-cmd ""
--health-interval "10s"
--health-timeout "3s"
--health-retries "6"
--health-start-period "10s"
```

The empty service command removes stale `CMD-SHELL` commands; Docker merges
the image's exec-form test into the created container.

- [x] **Step 5: Implement bounded remote readiness**

Add a fixed remote script that:

1. sets `PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`;
2. lists running containers only for
   `com.docker.swarm.service.name=vp-pds-swarm`;
3. requires exactly one lowercase hexadecimal container ID;
4. uses one container inspect to capture image, environment, health status,
   health test, interval, timeout, start period, and retries;
5. returns only the sanitized pipe-delimited snapshot.

`vp_require_pds_ready()` must first call
`swarm_service_running vp-pds-swarm` and
`vp_require_service_node vp-pds-swarm colima-127`, then try the remote
snapshot at most 18 times with five-second sleeps. Accept only exact image,
exact listener/command/timing, and `healthy`. Retry only `starting` and
sanitized transient container-set snapshots; configuration mismatch, remote
command failure, or malformed output fails immediately.

- [x] **Step 6: Implement verified rollback**

Replace the generic PDS wrapper with a specialized `deploy_pds_services()`:

```bash
baseline_image="$(vp_service_values vp-pds-swarm '{{.Spec.TaskTemplate.ContainerSpec.Image}}')"
candidate_update_status=0
if vp_update_runtime_service vp-pds-swarm "$image" start-first; then
    if vp_require_pds_ready "$image"; then
        printf '%s\n' vp-pds-swarm
        return 0
    fi
else
    candidate_update_status=$?
    if [[ "$candidate_update_status" -eq "$VP_SERVICE_UPDATE_NOT_ATTEMPTED" ]]; then
        return 1
    fi
fi
vp_update_runtime_service vp-pds-swarm "$baseline_image" stop-first
vp_require_pds_ready "$baseline_image" || \
    echo "PDS image restore did not become ready" >&2
return 1
```

Keep the topology check before every mutation. The first rollout's legacy
baseline has no image healthcheck, so a failed first rollout may report an
unverified rollback; it must still remain failed. A failed candidate preflight
uses a distinct status and returns without rollback, while a Docker update
failure is treated as potentially partially applied and does roll back.

- [x] **Step 7: Run VP deployment checks**

Run:

```bash
cd /Users/wenjieliu/videoprocess
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_vp_deploy_ci_gate.sh
bash tests/test_ci_workflow_contract.sh
```

Expected: both deployment suites pass.

- [x] **Step 8: Commit the controller gate**

```bash
cd /Users/wenjieliu/videoprocess
git add deploy/swarm/deploy-sync-extension.sh tests/test_vp_deploy_sync_extension.sh \
  docs/superpowers/specs/2026-07-25-pds-health-gated-auto-deployment-design.md \
  docs/superpowers/plans/2026-07-25-pds-health-gated-auto-deployment.md
git commit -m "feat(deploy): health-gate pds rollout"
```

### Task 4: Full Verification And Automatic Production Proof

**Files:**
- No source changes expected.
- Evidence: `/Users/wenjieliu/videoprocess/.runtime/deploy-evidence/`

**Interfaces:**
- Consumes: committed VP controller gate and committed PDS image contract.
- Produces: exact-SHA CI, poller, image, node, and health evidence.

- [x] **Step 1: Run both repositories' full local checks**

Run:

```bash
cd /Users/wenjieliu/policy-decision-service
go test ./...
go vet ./...
go build ./cmd/server

cd /Users/wenjieliu/videoprocess/backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true

cd /Users/wenjieliu/videoprocess/frontend
npm install
npm run build
npm run lint || true

cd /Users/wenjieliu/videoprocess
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_vp_deploy_ci_gate.sh
bash tests/test_ci_workflow_contract.sh
```

Expected: required tests/builds pass; advisory lint/type output is recorded.

- [ ] **Step 2: Push VP first and wait for exact-SHA CI**

Push the VP `main` commit. Record its exact SHA and require the matching
`ci.yml` push run to finish successfully. Wait for the normal 150 minute-0/15
poller to install that exact controller revision; do not use `--force`.

- [ ] **Step 3: Push PDS and wait for exact-SHA CI**

Push PDS `main`, record the exact SHA, and require the matching
`Policy Decision Service CI` push run to finish successfully.

- [ ] **Step 4: Let the normal minute-7 PDS poller deploy**

Do not invoke the controller manually. Capture 150 logs proving it detected
the new PDS SHA, passed exact-SHA CI, built on 127, updated
`vp-pds-swarm`, and recorded success only after readiness.

- [ ] **Step 5: Verify final topology and container health**

From 150 require one desired/running PDS task on `colima-127`. On 127 inspect
the one running PDS container and require:

```text
image=vp-pds:deploy-<first 12 chars of exact PDS SHA>
health=healthy
test=["CMD","/usr/local/bin/pds","probe","--url","http://127.0.0.1:8080/readyz","--timeout","2s"]
http_addr=:8080
interval=10s timeout=3s start_period=10s retries=6
```

List every running `vp-*` task and prove all nodes are only `colima-127` or
`ccttww-lap`; assert no output contains `CASPERs-Mac-mini`,
`colima-swarmbridged`, or `10.0.0.126`.

- [ ] **Step 6: Preserve the canary boundary**

Run only read-only schedule, queue, auth, and upload/publication audits.
Do not close/open the schedule, create work, upload, or publish. Report that a
sixth unlisted canary still requires explicit authorization.
