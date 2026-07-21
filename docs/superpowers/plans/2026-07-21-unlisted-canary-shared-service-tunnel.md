# Unlisted Canary Shared-Service Tunnel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the guarded canary runner reach 150 PostgreSQL, Redis, and YouTubeManager through one lifecycle-managed SSH connection to 127.

**Architecture:** Add focused URL/forward helpers and a synchronous context manager inside the existing canary runner. The context manager starts before `run()` creates the async database engine, supplies rewritten in-memory endpoints, and always stops its SSH child; all existing canary safety gates remain unchanged.

**Tech Stack:** Python 3.12, SQLAlchemy URL parsing, `urllib.parse`, `socket`, `subprocess`, pytest, OpenSSH.

## Global Constraints

- Live upload still requires `--confirm-live-unlisted` and the separate exact operator approval.
- Preflight must not mutate application state or call upload/publication endpoints.
- Never persist or print credentials, complete connection URLs, or OAuth data.
- Keep public publication and external-asset automatic publication disabled.
- Keep all VideoProcess work off 126.
- Use test-driven development for every behavior change.

---

### Task 1: Structured Tunnel Endpoint And Command Contract

**Files:**
- Modify: `backend/tests/services/test_unlisted_canary_runner.py`
- Modify: `scripts/run_vp_unlisted_canary.py`

**Interfaces:**
- Consumes: database URL string, optional Redis URL, YouTubeManager base URL, SSH host.
- Produces: `TunnelForward`, `SharedServiceEndpoints`, `build_shared_service_endpoints()`, and `ssh_tunnel_command()`.

- [x] **Step 1: Write failing endpoint tests**

Add tests that require database credentials/path/query preservation, Redis DB
path preservation, HTTP path preservation, default PostgreSQL/Redis/HTTP
ports, and omission of an empty Redis URL. Assert rewritten endpoints use the
allocated `127.0.0.1` ports and forward targets retain the original hosts.

- [x] **Step 2: Run the endpoint tests and verify RED**

Run:

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py -k shared_service -q
```

Expected: failure because the shared-service helpers do not exist.

- [x] **Step 3: Implement structured endpoint helpers**

Use `sqlalchemy.engine.make_url()` for PostgreSQL and `urlsplit()` plus
`urlunsplit()` for Redis/HTTP. Return logical target names with host/port and
rewritten URLs. Reject missing hosts, unsupported URL forms, and missing ports
without interpolating a full URL into an error.

- [x] **Step 4: Add and pass exact command tests**

Require this argument shape:

```text
ssh -N -T -o BatchMode=yes -o ConnectTimeout=10 -o ExitOnForwardFailure=yes
  -L 127.0.0.1:<local>:<target-host>:<target-port> ... <ssh-host>
```

Run the same focused pytest command and expect all selected tests to pass.

### Task 2: Lifecycle And CLI Integration

**Files:**
- Modify: `backend/tests/services/test_unlisted_canary_runner.py`
- Modify: `scripts/run_vp_unlisted_canary.py`
- Modify: `tests/test_vp_unlisted_canary_scripts.sh`

**Interfaces:**
- Consumes: `--shared-services-ssh-host`, `DATABASE_URL`, `REDIS_URL`, and `--youtube-manager-url`.
- Produces: a context manager yielding rewritten endpoints and evidence-safe routing metadata.

- [x] **Step 1: Write failing lifecycle tests**

Use a fake process to prove startup exit raises `CanaryError`, normal exit calls
`terminate()` plus `wait()`, and a timeout escalates to `kill()` plus `wait()`.
Assert no process output is included in the exception.

- [x] **Step 2: Run lifecycle tests and verify RED**

Run the focused test module and confirm failures are caused by the absent
context manager.

- [x] **Step 3: Implement lifecycle management**

Reserve loopback ports, start OpenSSH with `stdin/stdout/stderr` detached, wait
briefly for startup failure, yield endpoints, and clean up in `finally`.

- [x] **Step 4: Wire CLI and evidence**

Validate the optional SSH hostname before database access. Wrap `asyncio.run`
in the context manager and pass rewritten URLs through a copied argparse
namespace. Record only enabled/host/logical targets in evidence.

- [x] **Step 5: Update and pass shell contract**

Require `--shared-services-ssh-host` in
`tests/test_vp_unlisted_canary_scripts.sh`, then run that test and the complete
canary runner test module.

### Task 3: Documentation, Verification, And Deployment

**Files:**
- Modify: `deploy/four-machine-topology.md`

**Interfaces:**
- Consumes: the tunnel-capable canary command.
- Produces: an operator command that needs no manually managed local forwards.

- [x] **Step 1: Document the command**

Add `--manager-ssh-jump 10.0.0.127 --shared-services-ssh-host 10.0.0.127`
to the read-only and approved-live command examples. State that the tunnel is
transport only and does not count as live approval.

- [x] **Step 2: Run repository verification**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py -q
.venv/bin/python -m ruff check ../scripts/run_vp_unlisted_canary.py tests/services/test_unlisted_canary_runner.py
cd ..
bash tests/test_vp_unlisted_canary_scripts.sh
python3 -m py_compile scripts/run_vp_unlisted_canary.py
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 3: Commit, push, and deploy**

Commit only the tunnel implementation, tests, design, plan, and topology
documentation. Push `main` and run the scoped controller for `vp-app`,
`vp-feature-aggregator`, and `vp-pds`.

- [ ] **Step 4: Prove production read-only behavior**

Fetch `DATABASE_URL` into the invoking environment without printing it and run:

```bash
PYTHONPATH=backend backend/.venv/bin/python scripts/run_vp_unlisted_canary.py \
  --preflight-only \
  --manager-ssh-jump 10.0.0.127 \
  --shared-services-ssh-host 10.0.0.127
```

Require successful evidence at the deployed commit, schedule `CLOSED`, empty
backlog, Redis pending zero, authenticated YouTubeManager quota, no upload or
publication count change, and evidence mode `0600` with no secrets.
