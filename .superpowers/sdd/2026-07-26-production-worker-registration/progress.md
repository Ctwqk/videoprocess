# SDD ledger — plan: /Users/wenjieliu/videoprocess/.worktrees/worker-registration/docs/superpowers/plans/2026-07-26-production-worker-registration.md

Base: e51240f5c0f7f13bba3d97f198e4920afee8ad77
Design commits: 610386d, 2a73e4a, 570b801, 56d2a9c
Follow-up Redis ACL design commit: 9a3d8c4
Follow-up Redis ACL plan commit: faeb277
Baseline inherited from identical main SHA: Go `go test ./...` passed; backend
`1049 passed, 71 skipped`.
Task 1: implementation at 02149d7; review rejected (8 findings);
fix round 1 at 9abcbce; re-review rejected (3 findings);
fix round 2 at d37d26f; re-review rejected (1 finding);
fix round 3 at 07a3a8b; final re-review rejected (1 finding);
fix round 4 at 1a83c8a; final review approved; Task 1 complete
Task 2: implementation at ce25840; review rejected (6 findings);
fix round 1 complete at 38fc590; re-review rejected (6 findings);
fix round 2 at 2da204b; re-review rejected (8 findings);
fix round 3/5 (8 prior findings addressed, 8 open — source-task ACK lacks
durable event proof; restricted worker role cannot run Python row-lock path;
terminal recovery reset; preferred affinity reclaim gap; Task 4 broad worker
DML; ACK/cleanup lock inversion; URL cache normalization/retention; MinIO
janitor/readiness plan gap; commits 2da204b..0c8a188)
Task 2: fix round 4/5 (6 prior findings addressed, 6 open — registered YouTube
double-session self-lock; janitor readiness lacks cross-host transport and
recurring scheduler; prepared event lacks no-XADD replay; Python rejects
terminal/held_unresolved_event recovery outcomes; worker Redis ACL omits event
emission marker/script access; end-to-end restricted-role YouTube path remains
unproven; commit e1c5f0f)
Task 2: minor (deferred): preferred affinity reclaim processes the reclaimed
task inline, reducing consumer-loop fairness while claim fencing preserves
authority safety.
Task 2: fix round 5/5 (6 findings addressed, 1 open — Task 4 still lacks a
concrete Redis event-marker continuity readiness executable, exact
database-proof/principal marker janitor, recurring schedule, and conservative
operator repair command; commit 69d65cb)
Task 2: BLOCKED — the remaining Redis event-marker lifecycle finding is real
and load-bearing for Task 3/Task 4. The five-round breaker has tripped; do not
dispatch more fixes or build downstream worker/deployment work on this
incomplete idempotency protocol without explicit human direction.
Task 2 breaker recheck: RESOLVED — the explicitly authorized worker
event-marker lifecycle increment (352f3ba..d4975bb) now supplies the concrete
PostgreSQL/Redis readiness, janitor, repair, scheduler, role, and deployment
gate. Its sole final fix passed a scoped independent review with zero Critical
or Important findings; the mandatory breaker is closed and Task 3 may resume.
Task 2 follow-up (non-blocking for Task 3, required before production rollout):
on a native `flock` operational failure, invalidate any prior same-generation
readiness status so an independent `status` call cannot reuse it for the
remaining 90-second freshness window. The direct deploy transaction already
fails before `status`, so this does not reopen the Task 2 breaker.
Task 3: implementation at 0fe94a0; review rejected (6 Important — invalid
Redis ACL identity reaches client construction; store-proven registration loss
does not stop reads; PEL scan truncates after 50; registered affinity bounce
escapes the 20-second window; unknown modes permit environment credentials;
reclaimed/artifact work bypasses bounded shutdown; 1 Minor).
Task 3: minor (deferred): immediate-heartbeat startup failure leaves the
heartbeat completion channel open, so a direct `Close` may wait five seconds.
Task 3: fix round 1/5 (1 addressed, 6 open — protected callbacks can outlive
shared-fence rollback; one no-handler finalization path does not publish loss;
PEL pagination follows an unbounded moving tail; registered affinity duration
is still configurable; pre-database Redis validation differs from go-redis;
pool/callback/file shutdown abandons goroutines and resources; commits
0fe94a0..791e552)
Task 3: minor (deferred): exported `MarkLost` is zero-value safe but not
nil-receiver safe.
Task 3: fix round 2/5 (3 addressed, 4 open — a spare in-flight XREADGROUP can
accept a new PEL delivery after owned loss; later-page failure can discard an
already-XCLAIMed target; parser-accepted invalid Redis ranges pass the
pre-database gate; `CloseContext` has a zero-count-to-Close acquisition race;
commits 791e552..a0fa9ea)
Task 3: fix round 3/5 (0 addressed, 4 open — read cancellation still permits
post-proof PEL intake and has a dispatch/cause TOCTOU; visitor-phase failure
can discard an earlier successful XCLAIM; retry scalars still bypass the
pre-database Redis gate; pool tracking starts after underlying acquisition and
does not cover nil-gate Store construction or Hijack; commits
a0fa9ea..b176ee4)
Task 3: minor (deferred): the PostgreSQL pool lifecycle goroutine-count
assertion was flaky in 4 of 26 focused re-review runs.
Task 3: fix round 4/5 (1 addressed, 3 open — public registered reclaim helpers
bypass the second authority/local-loss handoff fence; Redis connection
lifetime/jitter scalars can still panic after database admission; external
`Store.Close` silently no-ops and direct `AcquireAllIdle` bypasses the pool
gate/statistics; commits b176ee4..2cf4494)
Task 3: fix round 5/5 (3 addressed, 0 open — public reclaim helpers now use
the second authority/local-loss handoff fence; unsafe Redis lifetime/jitter
values fail before database admission; external/owned pool close and
`AcquireAllIdle` behavior is explicit and fenced; commits 2cf4494..57be21b)
Task 3: complete — implementation 0fe94a0 plus fix rounds 1-5 passed an
independent final re-review with zero open Critical or Important findings.
The three deferred Minors above remain non-load-bearing follow-up items.
Task 4A: PostgreSQL admission-role implementation at 87dd5bd; independent
review rejected (4 Important, 1 Minor).
Task 4A: fix round 1/5 at 95b04e9; re-review rejected (3 Important).
Task 4A: fix round 2/5 at 66ce85c; re-review rejected (3 Important).
Task 4A: fix round 3/5 at de3aaeb; re-review rejected (3 Important).
Task 4A: fix round 4/5 at b0c3f1e; fresh re-review rejected (4 Important).
Task 4A: fix round 5/5 at 904f746; final fresh re-review rejected (2
Important — PostgreSQL 16 ADMIN OPTION/delegated stable-role grants are not
included in isolation and can keep an active grant after routine revoke
rolls back; complete credentials under an invalid runtime/control state root
fail without quarantining the still-login-capable principals).
Task 4A: BLOCKED — the five-round breaker has tripped with two load-bearing
role-isolation findings. Do not dispatch a sixth fix or build production
deployment on this role boundary without explicit human direction.
Task 4A breaker continuation: fix round 6 at 7b9a477 closed delegated
PostgreSQL 16 membership and initial invalid-root quarantine findings;
re-review rejected with two new Important findings.
Task 4A breaker continuation: fix round 7 at 28bb206 pinned authority
directories across DCL and bound each live database principal to one
service/generation. A fresh independent review passed with zero Critical,
Important, or Minor findings: 70 focused and 330 cumulative PostgreSQL 16
tests passed without skips, repeated directory/principal and concurrency
probes passed five rounds, all 62 descriptors closed, and no disposable
database or role remained. Task 4A and its breaker are RESOLVED.
Task 4B/4C recovery: the terminated broad Task 4 agent returned unstaged
worker-secret/startup, Docker, staging-janitor, and deploy-transaction changes.
They remain preserved and uncommitted; focused checks exposed an old
mode-0600 versus mode-0400 deployment-CLI contract and a marker freshness
fixture that does not isolate the new admission/janitor prerequisites.
Task 4: in progress on the preserved Task 4B/4C changes; no production
deployment is authorized by these partial changes.
Task 4B/4C: recovered implementation committed at 0c634ae; dual independent
review rejected it with 0 Critical, 17 Important, and 2 Minor findings.
Worker Track A has 7 Important findings (effective endpoint identity,
file-backed Redis production classification, post-loss Redis I/O, mutable
Python build commit, cross-generation MinIO cache, pre-validation Go database
I/O, and skipped/red real Go integration) plus 2 Minors (local storage MinIO
requirement and Go secret-reader ctime/race coverage).
Deploy Track B has 10 Important findings (first-deploy rollback, failed
rollback compensation, crash replay, credential identity separation,
constructure-runtime Redis secret use, secret-retirement identity,
per-mutation marker freshness, janitor service TOCTOU, durable janitor-install
rollback, and retirement-response binding).
Task 4B/4C: fix round 1/5 in progress, beginning with Worker Track A; fifth
unlisted canary authorization remains unused and no production mutation is
allowed before both tracks pass review.
Task 4B/4C Worker Track A: fix round 1/5 at 4d1f212; independent review
addressed 6 of the original 9 findings but left 4 Important findings open:
the provisioned `postgresql+asyncpg` Go URL is fingerprint-normalized but
passed unnormalized to pgx, pgx multi-host fallbacks are omitted from the
fingerprint, Python ACL WHOAMI is not itself registration-owned, and the
Python identity artifact remains root-writable in the actual image.
Task 4B/4C Worker Track A: minor (deferred): the new ctime helper lacks a
definition for Unix targets other than Linux/Darwin, regressing FreeBSD
cross-compilation.
Task 4B/4C Worker Track A: fix round 2/5 in progress; review base is 4d1f212.
Task 4B/4C Worker Track A: fix round 2/5 at 2a5b699 addressed all 4
round-one Important findings, but scoped review found 2 new Important
deployment-contract regressions: Python services/jobs still mount Swarm
secrets as root:root mode 0400 despite the image now running as UID/GID 10001,
and the staging-janitor evidence volume is not migrated from root ownership
before the non-root job writes status.
Task 4B/4C Worker Track A: minor (deferred): direct registration `close()`
does not cancel and drain registration-owned guarded tasks.
Task 4B/4C Worker Track A: fix round 3/5 in progress; integration scope is
expanded only to Python secret ownership and janitor evidence-volume
migration, and later Deploy Track B work must preserve this contract.
Task 4B/4C Worker Track A: fix round 3/5 at 9141cc9 addressed the Swarm
secret UID/GID contract but left the evidence-volume identity open and
introduced 2 Important findings: the root bootstrap recursively chowns
caller bind trees, breaking non-root controller read-back and allowing
source substitution, while the prepared named volume is unpinned between
prepare and service creation.
Task 4B/4C Worker Track A: minor (deferred): the exact-image secret gate does
not itself assert a root-owned source file, though an independent root-source
probe passed.
Task 4B/4C Worker Track A: fix round 4/5 in progress with a fresh implementer.
The architectural correction is to run control one-shots as the caller UID/GID
on a read-only root filesystem with narrow operation-owned output directories,
never recursively chown caller state, and to pin the janitor volume with a
transaction-owned holder container through service/task acquisition.
Task 4B/4C Worker Track A: fix round 4/5 at f713737 left both prior Important
findings open and introduced a third: production wrappers still run
path-following mkdir/chmod before the no-follow guard, SIGTERM leaves durable
copies of privileged database credentials and sentinels, and the holder
validator rejects the real CUDA image's inherited entrypoint.
Task 4B/4C Worker Track A: minor (deferred): the real named-volume prune probe
omits `--all`, so only direct remove/recreate currently proves pinning.
Task 4B/4C Worker Track A: fix round 5/5 in progress. Final architecture:
descriptor-relative no-follow creation owns every prospective bind path;
database credentials stream over container stdin into tmpfs instead of being
copied below durable admission state; holder creation explicitly clears and
then production-validates the inherited image entrypoint.
Task 4B/4C Worker Track A: fix round 5/5 at a0579d6 addressed bind-source
pre-mutation and the real CUDA holder path, and removed durable credential
copies, but final review left 1 Important finding open: one-shot operation
records have no exclusive/liveness owner, normal signals leave sentinel state,
and an overlapping invocation can delete a live operation and turn a
successful control mutation into a false failure.
Task 4B/4C Worker Track A: BLOCKED at the five-round breaker — I-R5-1 is real
and load-bearing; do not declare Track A complete or deploy this range.
Task 4B/4C breaker continuation: the user's standing instruction to continue
past protocol blocks and pre-approval of implementation plans authorizes
routing I-R5-1 into Deploy Track B substage 1. It shares the already-designed
exclusive `transaction.lock` and dead-owner-only reconciliation mechanism;
that substage must close I-R5-1 before any forward/rollback mutation work.
Task 4B/4C Deploy Track B stage 1: implementation at dbc792d plus marker-fake
contract fix f62e422; independent review rejected it with 6 Important
findings: inherited outer lock ownership admits concurrent child one-shots;
signal launch/status propagation has a race; principal probes are not bound
to the credential inodes recorded by the transaction; partial secret creation
does not immediately persist immutable IDs; phase/promotion and logical
retirement relations are under-validated; historical four-field worker
retirement journals have no strict v1-to-v2 hydration path.
Task 4B/4C Deploy Track B stage 1: fix round 1/5 in progress; stage 2 remains
blocked and no production mutation is authorized.
Task 4B/4C Deploy Track B stage 1: fix round 1/5 at a90290b addressed
credential-record binding, relational transaction validation, and strict v1
retirement hydration. Three Important findings remain: legitimate production
retirement queries still call one-shots from command-substitution children and
are blocked by the outer lock; a signal can arrive before the pending handler
and still allow Docker/later outer mutation; partial secret prefixes resume
without reminting but have no deterministic exact-ID abort cleanup.
Task 4B/4C Deploy Track B stage 1: fix round 2/5 in progress; stage 2 remains
blocked.
Task 4B/4C Deploy Track B stage 1: fix round 2/5 at a0e1afa left all three
prior findings open and added one Important finding: the query output pathname
can be replaced after stdout opens and before parsing; a signal after the last
gate can still be followed by Docker launch; fresh-process ABORTING replay
does not restore the control generation required for authority revoke; and
authority provisioned before the first secret is not durably represented, so
an empty-secret abort can archive without revoke evidence.
Task 4B/4C Deploy Track B stage 1: fix round 3/5 in progress; stage 2 remains
blocked.
Task 4B/4C Deploy Track B stage 1: fix round 3/5 at 1d13211 addressed all
four prior findings but introduced two Important cross-process continuity
gaps: a pre-release FIFO supervisor inherits both the FIFO write side and
transaction lock, so parent SIGKILL leaves an orphan that blocks replay; and
the authority WAL changes required schema-1 fields without a version bump,
migration, or stable quarantine for older PREPARING/ABORTING journals.
Task 4B/4C Deploy Track B stage 1: fix round 4/5 in progress with a fresh
implementer; stage 2 remains blocked.
Task 4B/4C Deploy Track B stage 1: fix round 4/5 at 86c59ac addressed the
parent-death launch gate but left journal compatibility open and introduced
two additional Important findings: legacy quarantine is classified only after
network inspect and four credential-bearing Docker probes; schema numeric
equality accepts forged 1.0/2.0 values; and the inherited-lock close helper
permanently redirects launch-child stderr to `/dev/null`.
Task 4B/4C Deploy Track B stage 1: fix round 5/5 in progress; stage 2 remains
blocked.
Task 4B/4C Deploy Track B stage 1: fix round 5/5 addressed all three inherited
production findings, but final review rejected the round because the rollback
test accidentally nested its credential-leak assertion after an unconditional
raise. The current product helper remained closed; the regression was in the
security test contract. The controller accepted the finding and applied a
minimal post-round correction under the user's standing continue authorization:
the assertion is unconditional again and a negative probe proves the failure
branch is reachable. Stage 2 remains blocked until fresh verification and
breaker adjudication complete.
Task 4B/4C Deploy Track B stage 1: APPROVED after independent breaker closure.
The exact prior credential-bearing canonical-plan reproducer now fails closed;
all six Stage 1 shell contracts and static checks pass. Stage 2 may begin.
Task 4B/4C Deploy Track B stage 2: in progress; no production mutation is
authorized by this transition.
Task 4B/4C Deploy Track B stage 2: post-review hardening closed four recovery
and identity findings. Durable promotion preconditions and marker receipts now
use same-directory unique 0600 temporaries, so a fixed `.tmp` left by a crash
cannot block replay. App snapshots retain exact service ID, image, and spec
digest through capture, baseline, hydration, and rollback; all snapshot IDs are
preflighted before mutation and ordinary/Go restores carry the expected ID into
the update boundary. Vision cutover records a distinct `final-safety` job and
database-secret purpose after managed vision readiness and immediately before
legacy retirement. Fixed-name marker jobs resolve name to ID once and use only
that ID for identity, task, remove, and convergence operations.
Task 4B/4C Deploy Track B stage 2: fresh verification passes worker deploy,
rollback, marker-control, macOS deploy-path, the full long deploy-sync contract,
backend pytest (`1426 passed, 125 skipped`), scoped Ruff/mypy, frontend build and
lint, shell syntax, helper compile/Ruff, and `git diff --check`. Full backend
Ruff/mypy still report the pre-existing baseline (15 lint findings; 61 type
findings), with none in the modified vision service or transaction helper.
Task 4B/4C Deploy Track B stage 2: fifth unlisted canary authorization is
recorded but remains unused. No push, remote deployment, YouTube upload, or
publication mutation has occurred in this hardening round. Independent final
review is in progress before commit/push and 150/127 deployment verification.
Task 4B/4C Deploy Track B stage 2: fresh read-only production topology probe
found 150 reachable through 127, Swarm manager `ccttww-lap` active/leader,
`colima-127` Ready/Active with `vp.runtime=true`, and both scoped deploy crons
active. `vp-app`/`vp-feature-aggregator` poll every 15 minutes; independent
`vp-pds` polls at minute 7 plus 15. Their clean checkouts remain at deployed
main SHAs `e51240f5c0f7...` and `6b8f8be32399...`. The historical
`colima-swarmbridged` node is still joined and Ready but has only `role=app`,
not `vp.runtime`, so the VideoProcess production constraints exclude it from
the 127 runtime path. Direct local routing to 150 still fails; the 127 SSH hop
is the working management path.
Task 4B/4C Deploy Track B stage 2: follow-up review found two Important crash
recovery gaps and one Moderate identity gap. Full `FORWARD_APPLYING` replay now
removes all durable vision cutover jobs before failed-forward capture or the
transition to rollback. A fresh process removes baseline-absent workers from
the hydrated `service|generation|service_id` candidate record, verifies the
name still resolves to that exact ID, and proves both ID and name absent after
removal; the process-local contract is only a same-process fallback when no
recovery records exist. Python, vision, and publisher restore helpers now
accept the immutable baseline service ID, validate it before node/service
mutation, reject create fallback, and revalidate at the registered-worker
mutation boundary. Red-green rollback contracts cover all three corrections;
worker deploy, rollback, marker-control, shell syntax, and `git diff --check`
are green. The full long deploy-sync contract and focused independent re-review
are still running. The fifth unlisted canary remains unused.
Task 4B/4C Deploy Track B stage 2: focused independent re-review returned no
P0/P1/P2/P3 findings and closed all three follow-up items on the latest diff,
including the real mutation chain with process-local worker contract forced to
fail. Residual notes concern only layered test granularity; exact-ID primitives
independently enforce the invariants. The full long deploy-sync contract remains
in progress. No production or YouTube mutation has occurred and the fifth
unlisted canary remains unused.
Task 4B/4C Deploy Track B stage 2: the full deploy-sync contract then exposed a
same-process partial-create recovery edge hidden by hydration. New forward
transactions now clear stale recovery candidates and capture exact in-memory
`service|generation|service_id` records before hydration; only the immediate
same-process rollback chain receives those records. Fresh-process replay remains
durable-only and fail-closed. When durable and process records both identify a
service they must agree exactly, and removal still requires a fresh marker,
matching immutable ID/name/generation labels, ID-targeted deletion, and verified
absence by both ID and name. The main test's rollback override now forwards this
fifth argument, and its fake Docker persists the injected partial-create
generation so the full-path assertion exercises the production identity model.
Task 4B/4C Deploy Track B stage 2: final candidate verification is green for the
full deploy-sync contract, worker deployment, rollback, Redis marker control,
macOS deploy paths, canary script safeguards, changed-shell syntax,
`git diff --check`, backend pytest (`1426 passed, 125 skipped, 17 warnings`),
scoped Ruff/mypy, frontend install/build/lint, and independent review. The
reviewer returned no P0/P1/P2/P3 findings after independently rerunning the full
contract. Full backend Ruff/mypy still expose repository-wide baseline findings
(15 lint, 62 type), with none in the modified Python files. The fifth unlisted
canary remains unused pending commit, push, and observed 150/127 auto-deployment;
no production or YouTube mutation has occurred in this hardening round.
Task 4B/4C Deploy Track B stage 2: the verified candidate was committed as
`5c106609a1c196bfe8dd02f941fa217d7033529d` and fast-forwarded to GitHub
`main`. The independent PDS cron automatically fetched that VideoProcess commit
at 17:07Z while correctly leaving the unchanged PDS service at
`6b8f8be32399...`. The 17:15Z app cron also detected `5c10660` but failed closed
before service mutation because GitHub Actions run `31960451359` concluded
failure; production VP services therefore remained on `e51240f`.
Task 4B/4C Deploy Track B stage 2: CI investigation found test-contract drift,
not a production deploy bypass. GNU grep returns SIGPIPE under `pipefail` when
the long deploy contract uses `grep | head`; all first-line selectors now use
`sed -n '1p'`. Three migration tests now compare Alembic head with the deploy
CLI's `EXPECTED_MIGRATION_HEAD` instead of stale revision 033. Two PostgreSQL
race tests now bind the real durable dispatch reconciler to their isolated
session factory and provide its idempotent Redis `eval` interface, rather than
letting it connect to default localhost:5435. Linux minimal reproduction proves
the pipeline correction (141 to 0), and fresh local deploy-sync, scoped Ruff,
shell syntax, backend pytest, and `git diff --check` pass. PostgreSQL 16
verification remains delegated to the next CI run. The fifth unlisted canary
remains unused.
Task 4B/4C Deploy Track B stage 2: the compatibility fixes were committed as
`a3944c8fda042ecd8c54047c758c2e601e2e1253` and fast-forwarded to GitHub
`main`. Actions run `31962299427` passed backend/migrations, Go, and frontend;
the deployment-contract job then exposed one Linux-only assertion-helper bug.
The parent-death safety behavior itself passed: the supervisor and producer
children exited, a fresh owner acquired the lock and reconciled stale state,
and no Docker call occurred. Only the FD audit rendered absent descriptors as
an empty string on `/proc`, while the cross-platform contract expected `-`.
`process_fd_access` now distinguishes an absent `/proc/<pid>/fdinfo/<fd>` from
an existing unreadable descriptor and returns the same `-` sentinel used by
the Darwin `lsof` path. A dedicated closed-FD assertion prevents regression.
Linux focused evidence reports closed/read/write as `-/r/w`; the full macOS
worker-admission contract, shell syntax, and `git diff --check` pass. The 17:45
and 18:00 app crons both observed `a3944c8` but failed closed while CI was not
successful, so production remains on `e51240f`. The fifth unlisted canary
remains unused pending the corrected CI run and observed automatic deployment.
Task 4B/4C Deploy Track B stage 2: the Linux FD-audit correction was committed
as `48ec37f8c923cd7d2ac4daae54831dc8c0776d0b`. Actions run `31964483742`
passed backend/migrations, Go, frontend, and the repaired absent-FD audit, then
exposed a separate Bash 5 runtime defect in real outer-lock retirement. Calling
the shell runner with function-level `>&15` caused Bash to reserve FD16 while
saving stdout, colliding with the fixed launch-gate FD16. Query capture is now
runner-owned through the internal `--query-output` switch: the runner accepts no
arbitrary output FD, validates the open and unlinked FD15 identity before
launch, and redirects only the final Docker command after the supervisor has
consumed the gate and closed inherited FD16/17. Both query callers and the test
double now enforce this contract. Direct regressions cover no-payload capture,
mismatched FD15 identity, and duplicate switch rejection. The full
worker-admission contract passes in the production worker image's Linux Bash
5.1 runtime and on macOS; rollback,
the full deploy-sync contract, Redis marker control, canary safeguards, macOS
paths, shell syntax, and `git diff --check` also pass. The 18:30 and 18:45 app
crons observed `48ec37f` but failed closed, so production remains on `e51240f`.
The fifth unlisted canary remains unused pending review, publication, successful
CI, and verified automatic deployment.
Task 5: pending
Task 6: pending
Live boundary: no sixth canary, upload, schedule opening, channel resume, soak
activation, policy activation, or public publication.
