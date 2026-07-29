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
Task 4: pending
Task 5: pending
Task 6: pending
Live boundary: no sixth canary, upload, schedule opening, channel resume, soak
activation, policy activation, or public publication.
