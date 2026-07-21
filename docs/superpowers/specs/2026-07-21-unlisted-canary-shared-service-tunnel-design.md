# Unlisted Canary Shared-Service Tunnel Design

**Date:** 2026-07-21

## Context

The guarded canary runner already reaches the 150 Swarm manager through the
127 SSH jump for read-only deployment checks. Its PostgreSQL, Redis, and
YouTubeManager clients still assume that the operator host can open direct TCP
connections to 150. Production verification exposed a real split: SSH to 150
through 127 was healthy while direct connections to `10.0.0.150:5435` and
`10.0.0.150:18999` failed with `No route to host`.

The operator can construct three local forwards manually, but that is fragile
for a guarded command that may run for twenty minutes and must always clean up.

## Decision

Add an optional `--shared-services-ssh-host` argument to
`scripts/run_vp_unlisted_canary.py`. When supplied, the runner opens one
temporary SSH process to that host with local forwards for the database,
Redis, and YouTubeManager endpoints. It rewrites only the in-memory connection
URLs to loopback ports and closes the SSH process in a context-manager
`finally` path.

For the active topology the value is `10.0.0.127`. The remote forwarding
targets remain the hosts and ports declared by `DATABASE_URL`, `REDIS_URL`,
and `--youtube-manager-url`, normally 150. The VP API remains a direct 127
connection, and manager Swarm checks continue to use the existing
`--manager-ssh-jump` behavior.

## Tunnel Contract

The runner:

1. parses the database URL with SQLAlchemy's structured URL parser;
2. parses Redis and HTTP endpoints with `urllib.parse` while preserving
   credentials, path, query, and fragment in memory;
3. requires a resolvable hostname and a known or explicit TCP port;
4. reserves one loopback port per configured shared service;
5. starts `ssh -N -T` with `BatchMode=yes`, `ConnectTimeout=10`, and
   `ExitOnForwardFailure=yes`;
6. passes every `-L` forward as an argument, never through a shell;
7. fails before opening the database if SSH exits during startup;
8. terminates and waits for SSH on success, failure, or signal, escalating to
   kill only if the process does not stop within five seconds.

Redis remains optional because the existing runner permits an unavailable
pending audit when no Redis URL is configured. PostgreSQL and YouTubeManager
are required and always receive forwards when the tunnel is enabled.

The current tunnel mode accepts `redis://` and `http://` only. Rewriting a
`rediss://` or `https://` URL to a loopback hostname would invalidate normal
TLS hostname verification, so those schemes fail closed until the client can
preserve the original TLS server name explicitly.

## Safety

- Tunnel activation is explicit; there is no silent network fallback.
- No password, token, complete URL, or SSH diagnostic containing connection
  data is written to evidence or standard output.
- SSH host-key policy is not weakened.
- The SSH process performs forwarding only and receives no remote command.
- `--preflight-only` keeps its existing read-only application contract.
- The tunnel does not satisfy or bypass the separate live-canary approval.
- Public publication and external-asset automatic publication remain disabled.
- `10.0.0.126`, `colima-swarmbridged`, and CASPER hostnames are rejected as
  both SSH endpoints and forwarding targets; 126 is not a build host, runtime,
  or failover target.

## Evidence

Canary evidence records only that shared-service forwarding was enabled, the
SSH host, and the logical services forwarded. Random loopback ports and
connection URLs are not persisted.

## Tests

Unit tests cover structured URL rewriting, default ports, omission of an empty
Redis URL, exact SSH command construction, startup failure, and unconditional
process cleanup. The shell contract requires the new CLI option. Production
verification reruns `--preflight-only` with the new option and without any
operator-created tunnel, then confirms schedule `CLOSED`, empty backlog, zero
new upload/publication rows, and a sanitized `0600` evidence file.
