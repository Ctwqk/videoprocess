# Scoped Worker Session Retirement

## Evidence and Scope

Release `22f0e541e28b7406aabee53c55733aa65a74300f` passed all four CI
jobs. The live CPU activation at 20:54:24 UTC failed with PostgreSQL
`permission denied to terminate process` inside `vp_worker_grant_activate`.
The existing non-superuser lifecycle tests do not keep an old worker database
session open during replacement. No worker was replaced by this attempt.

This repair addresses only that permission boundary. It does not enable uploads,
change publication review rules, or involve host 126. The user's standing
approval covers ideas, specifications, and implementation plans.

## Decision

Do not give the deploy principal `pg_signal_backend`, superuser, or usable
membership in worker login roles. Add a narrowly scoped SECURITY DEFINER
function in a bootstrap-owned private schema. Only the database owner may
execute it; ordinary operator logins reach it through the existing guarded
activation/revocation functions. The helper owner is the original PostgreSQL
bootstrap superuser, not a new inherited role available to deployment.

The helper accepts a managed service name and generation, never a PID, SQL
string, or database name. It derives the exact principal using the existing
SHA256 naming scheme and reads ONLY PostgreSQL-owned catalogs. It validates the
current database's owner, NOLOGIN and nonprivileged target attributes, and the
exact bootstrap-granted admin-only creator membership to that database owner.
It rejects every other target/member/grantor edge. It may signal only client
backends matching the exact database and role OIDs, excluding the caller.
The helper uses a fixed catalog search path and no deploy-controlled table,
view, type, trigger, function, index expression, or policy dependency.

Ruling: the private privilege boundary is isolated, NOLOGIN worker roles owned
by this deployment, not unforgeable business-table grant contents. The database
owner already controls that project's grants and login state; it must not gain
authority over other roles or databases. The ordinary non-superuser wrappers
continue to validate revoked grants and replacement authority before invoking
the helper. Direct helper calls by the trusted database owner remain bounded
to the same already-quarantined worker roles.

Complete replacement and role-graph validation before the first signal.
Preserve transaction rollback of authority, but do not promise that a database
rollback can resurrect terminated sessions. A post-commit scoped drain closes
the pre-commit reconnect window; revoked worker lease checks remain fail-closed
throughout. Coordinate with existing deployment locks and pin role identities
while signaling. Do not claim that one pre-commit enumeration proves that all
future authentication attempts have drained.

PostgreSQL exposes signaling by PID, not an atomic PID/start-time/database/role
comparison. The reviewed decision accepts its standard filtered-and-rechecked
signaling boundary with short serialized transactions and existing lease fences;
it does not claim to eliminate the final PID reuse race. Catalog SHARE locks
pin the validated identities and have a helper-local two-second lock timeout.
The operator CLI also bounds connection and command time, including its second,
separately committed drain.

Canonical role cleanup carries its validated service and generation into the
same helper after NOLOGIN and membership changes commit. It never reconstructs
generation from a hashed name or application rows. Generic, control, and marker
cleanup retain their existing privilege boundary. Forward deployment durably
records each existing worker attempt before activation: a failed drain or subsequent
marker gate must still select that worker for rollback, even if Docker has not
started its service update. Docker preflight cannot erase this prior intent.
Initially absent workers retain the prior pre-creation authority cleanup and
post-activation Docker-attempt recording. Existence comes from the validated,
captured baseline; missing, duplicate, invalid, or unreadable state fails closed.

One explicit administrator bootstrap installs the helper without broadening
deployment-role authority. A subsequent ordinary migration changes only the
existing activation/revocation function bodies to call it. Signatures, owner,
and EXECUTE ACLs remain unchanged. Missing or drifted bootstrap must fail closed.

## Verification

- Reproduce the current failure with an actual old worker connection and a
  non-superuser function owner on isolated PostgreSQL 16.
- Confirm replacement disconnects only the old revoked worker and preserves
  unrelated, new-generation, and other-database connections.
- Deny direct runtime/operator/public calls; reject login-enabled,
  privileged, noncanonical, membership-drifted, and foreign-role targets. The
  non-superuser wrapper rejects active/pending grant targets. Preserve old
  sessions when replacement validation fails before activation commits.
- Prove no privileged reads of substituted application objects; exercise
  post-commit draining, identity stability, disappearing sessions, and races.
- Verify helper ownership, function ACL, schema ACL, fixed search path, and
  absence of new deploy role memberships.
- Preserve the existing PG16 creator-edge lifecycle tests and migration head
  checks. Run backend tests and advisory lint/type checks, focused deployment
  contracts, exact-commit CI, then ordinary 127/150 deployment and read-only
  canary preflight. No sixth live canary is authorized by this change.
