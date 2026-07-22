# CI-Gated Independent Deploy Design

**Date:** 2026-07-21

## Goal

Require a successful GitHub Actions run for the exact source commit before the
150 deploy controller builds or updates VideoProcess or PDS, while preserving
automatic polling deployment and keeping the two repositories operationally
independent.

This change does not open the production schedule, activate a channel, enable
the soak watcher, upload media, or change publication privacy.

## Current Problem

The scoped 150 cron polls `main` every 15 minutes and deploys changed commits,
but neither repository currently has a GitHub Actions workflow. The controller
also has no commit-status gate. A pushed commit can therefore reach build and
service update without durable CI evidence.

The current cron invokes VideoProcess, its feature aggregator, and PDS in one
shell process. Because the controller uses `set -e`, a VideoProcess failure can
prevent an unrelated PDS poll during that run.

## Chosen Approach

1. Add one required workflow per repository. The workflow run itself is the
   aggregate commit verdict: all jobs must succeed.
2. Make the repository-owned deploy extension query GitHub Actions for the
   exact 40-character commit before any project image build.
3. Split the 150 cron into a VideoProcess invocation and a PDS invocation so a
   failure in one repository does not suppress the other repository's poll.
4. Keep deployment pull-based from 150. GitHub Actions never receives SSH or
   production credentials.

Alternatives considered:

- Branch protection alone was rejected because it does not prove that the
  pull-based controller checked the deployed SHA and would interfere with the
  existing direct-push workflow.
- Running the full suite on 150 every 15 minutes was rejected because it
  duplicates CI, consumes production-host resources, and leaves no GitHub
  evidence artifact.
- GitHub Actions SSH deployment was rejected because it duplicates the proven
  controller and expands the credential boundary.

## VideoProcess Workflow

`.github/workflows/ci.yml` runs on pushes to `main` and pull requests:

- backend pytest against PostgreSQL 16, including live migration tests;
- Alembic head capture;
- complete Go tests;
- frontend `npm ci` and production build;
- deployment, canary-script, topology, and soak-watcher contract tests;
- advisory Ruff, mypy, and frontend lint using the repository's existing
  non-blocking policy.

Each blocking job uploads a short evidence artifact containing the commit and
its command output. The backend artifact also contains the Alembic head.

## PDS Workflow

The independent `policy-decision-service` repository receives
`.github/workflows/ci.yml`, running `go test ./...`, `go vet ./...`, and a
server build on pushes to `main` and pull requests. Its workflow is queried
independently from the VideoProcess workflow.

## Controller Gate

The deploy extension exposes a function that accepts only fixed-format inputs:

- repository: `owner/name`;
- workflow file name;
- exact lowercase 40-character commit SHA.

It uses the authenticated `gh` CLI on 150 to request push runs for that SHA and
selects the latest attempt. Deployment proceeds only when the run status is
`completed`, conclusion is `success`, and the returned head SHA exactly
matches the requested commit. Missing, queued, failed, cancelled, skipped, or
unreachable results fail closed before image build or service mutation.

Dry-run operations that disable both builds and service updates remain
available without CI so operators can inspect repository synchronization.
There is no environment-variable bypass for an applying production deploy.

## Failure Isolation

The 150 cron has two managed commands at the same 15-minute cadence, offset by
seven minutes so they do not contend for the controller lock:

- `vp-app` plus `vp-feature-aggregator` from `Ctwqk/videoprocess`;
- `vp-pds` from `Ctwqk/policy-decision-service`.

Both continue to use the same lock and controller, so overlapping runs exit
cleanly and retry on the next interval. A failed or pending workflow never
advances deployment markers. Existing services continue to run their last
successful images.

## Safety And Topology

- 150 remains the deployment controller, shared-infrastructure host, managed
  Python worker host, and YouTube publisher host.
- 127 remains the VideoProcess, feature-aggregator, and PDS runtime.
- 126 is not queried, synchronized, built on, or scheduled for these projects.
- Public publication remains disabled and external-platform asset review is
  unchanged.

## Verification

Repository tests must prove exact-SHA success, missing/pending/failed fail
closed behavior, dry-run behavior, and that all three build entry points call
the gate before build commands. Static workflow contracts verify trigger,
blocking commands, and evidence artifacts.

After push, GitHub workflow APIs must report success for both repository SHAs.
Then a 150 dry run and apply must show that the same SHAs are accepted, service
images and deployment markers converge, the two cron commands are independent,
and 126 has zero VideoProcess tasks.
