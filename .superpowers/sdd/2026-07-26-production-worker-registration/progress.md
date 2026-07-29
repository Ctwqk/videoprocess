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
Task 5: pending
Task 6: pending
Live boundary: no sixth canary, upload, schedule opening, channel resume, soak
activation, policy activation, or public publication.
