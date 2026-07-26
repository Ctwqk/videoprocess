# PDS Health-Gated Automatic Deployment Design

## Context

The independent `Ctwqk/policy-decision-service` repository is polled by the
deployment controller on host 150 every fifteen minutes. An exact successful
GitHub Actions run is required before its image is built on host 127 and
deployed as `vp-pds-swarm`.

The current chain proves source and image identity, but deployment acceptance
only proves that the Swarm task is `Running`. The PDS image has no healthcheck,
the service has no published HTTP port, and the current service has only the
`node.labels.vp.runtime==true` placement constraint. It is running on
`colima-127` today, but a stale label could move it elsewhere. Rollback restores
an image without proving that the restored task is ready.

## Decision

Add a bounded `probe` mode to the PDS binary and use it for the image and Swarm
service healthcheck:

```text
/usr/local/bin/pds probe --url http://127.0.0.1:8080/readyz --timeout 3s
```

The probe performs one HTTP GET, accepts only status `200` with the PDS ready
payload, and exits nonzero otherwise. It does not print response bodies,
connection details, configuration, or exception text.

The VideoProcess deployment extension will:

1. converge both `node.labels.vp.runtime==true` and
   `node.hostname==colima-127`;
2. converge the exact PDS healthcheck command and timing;
3. update the exact commit-tagged image;
4. require one running PDS task on `colima-127`;
5. remotely inspect the task container on host 127 and wait for Docker health
   status `healthy`;
6. record deployment success only after readiness is proven.

If the new image fails, rollback restores the captured baseline image with the
same placement and health contract, then proves the restored container is
healthy. A failed rollback readiness check is reported as a failed deployment;
the controller must not write a success marker.

## Alternatives Rejected

### External Curl Probe Image

An ephemeral curl container could reach the PDS service over the overlay
network, but it adds an external image dependency to the deployment control
plane and does not prove the PDS image itself has a usable health contract.

### Publish A Host Port

Publishing the PDS HTTP port would let host 150 probe it directly, but it
unnecessarily expands the service's network exposure. PDS remains internal to
the pipeline network.

## PDS Probe Contract

The server binary recognizes `probe` before loading server configuration. The
command accepts only:

- `--url`, required, HTTP scheme only;
- `--timeout`, optional, default `3s`, finite and positive.

Redirects are rejected. The response must be:

```json
{"status":"ready"}
```

with no additional dependency on an external shell, wget, or curl. Exit codes:

- `0`: ready;
- `2`: invalid arguments;
- `3`: request, status, payload, or timeout failure.

Output is a stable single line:

```text
status=ready
```

or:

```text
status=not_ready
```

## Container Health Contract

The PDS Dockerfile uses:

```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
  CMD ["/usr/local/bin/pds", "probe", "--url", "http://127.0.0.1:8080/readyz", "--timeout", "3s"]
```

The deployment extension also converges the same service-level health command
and timing so an old image or stale service definition cannot silently remove
the gate.

## Remote Readiness Verification

Host 150 resolves the service task from Swarm and requires:

- exactly one desired/running task;
- task node exactly `colima-127`;
- exactly one running container on host 127 with
  `com.docker.swarm.service.name=vp-pds-swarm`;
- container image equal to the expected release or rollback image;
- Docker health status `healthy` within a finite deadline.

The SSH command uses fixed service, host, and formatting values. It never
contains credentials or application URLs. Missing, duplicate, starting,
unhealthy, stopped, remote, or image-mismatched containers fail closed.

## Automatic Deployment And Shared Rule Changes

A PDS `main` push continues to be detected within fifteen minutes, gated by
exact-SHA CI, and deployed automatically.

Changes to the shared VideoProcess deployment extension can alter PDS
placement or readiness without changing the PDS source SHA. After this feature
is deployed, the operator performs one controller `--force --project vp-pds`
convergence. Future PDS source pushes naturally use the latest extension.
Normal cron remains marker-based and does not rebuild unchanged PDS every
fifteen minutes.

## Safety Boundaries

- PDS remains internal; no new published port is added.
- Host 126 is never labeled, selected, probed, built on, or used for rollback.
- Readiness performs no policy decision and no database mutation.
- The probe sends no credentials or client identity.
- Deployment state advances only after new-image readiness.
- Rollback state is not reported healthy until the restored container passes
  the same gate.

## Tests

PDS repository tests cover:

- ready response;
- non-200 response;
- malformed or unexpected JSON;
- redirect rejection;
- timeout;
- invalid URL and duration;
- output sanitization;
- Dockerfile healthcheck contract.

VideoProcess deployment tests cover:

- PDS update adds the exact runtime label and hostname constraints;
- stale placement constraints are removed;
- exact healthcheck command and timing are converged;
- running task must be on `colima-127`;
- remote container count, image, and health must match;
- update readiness failure invokes rollback;
- rollback readiness failure remains failed;
- success marker is written only after readiness;
- host 126 never appears in PDS build, update, readiness, or rollback calls.

## Success Criteria

- A PDS `main` push with successful exact-SHA CI automatically deploys within
  the configured polling interval.
- `vp-pds-swarm` runs only on `colima-127`.
- The deployed PDS container reports Docker health `healthy` from its own
  `/readyz` contract.
- New-image failure restores and verifies the prior healthy image.
- Controller state records success only after health-gated convergence.
- Hosts 127 and 150 remain the only VideoProcess/PDS participants.
