# Owned History Contract 1

These are synthetic offline fixtures, not uploaded artifacts, historical observations,
operator approvals, live certificates, or activation authority. In particular,
`retired_unassigned.json` uses the exact bounded retirement tuple with an artificial
four-node graph and artificial timestamps. It does not claim to reproduce or qualify
the actual eight-node c25 graph. No actual YouTube channel ID is inferred here.

## Consumers

`app.services.owned_seed_inventory_history` owns the Python codec, retained v1/v2
manifest decoding, immutable facts, complete bounded DB-only reader, and assessment.
Future Go/Python admission and effect guards must consume the same classifications,
authority digest, effect digest, and global retired hash sets. A successful assessment
is not permission to create a task or upload; schedule/PDS/consumption/final-effect
authority remain separate and are not implemented by this review unit.

Each JSON file contains `rows`, `platform_channel_id`, the actual-observation-shaped
`observed_at` and `now`, separate `redis_observations`, an exact `expected` assessment,
`snapshot_sha256`, and a canonical-encoding vector. All row arrays are ID-sorted;
`runtime_schedules` uses `service_name`. Missing tables, overflow, duplicate row IDs,
invalid JSON, and stale/future observations fail closed.

## Canonical Bytes

Use Python-compatible sorted-key, compact, ASCII-escaped JSON, with finite numbers
only. Preserve JSON numeric types: `1.0` is not `1`; the exponent vector is `1e-07`.
Do not deserialize into a representation which silently drops these distinctions
before hashing. Reject duplicate keys, invalid surrogates, non-string Python object
keys, trailing JSON, NaN and infinity. SHA-256 is over the resulting ASCII bytes.

Authority timestamps use exact UTC `datetime.isoformat()` spelling (`+00:00`, and
six fractional digits when present). Retained DB row timestamps are not rewritten;
native naive job/worker timestamps are interpreted as UTC for comparisons. Normal
queue idempotency timestamps use native RFC3339 seconds truncation (`Z`), while
`run_after` retains full DB precision.

## Evidence Types

- `HistoryOperationLocator`: untrusted operation/account/channel lookup IDs only.
- `QualifiedUploadFact`: one exact operation/M/V/actual-UC observation plus operation
  and receipt hashes. `UploadQualification.sanitized_facts` must bijectively cover
  sorted `qualified_operation_ids`; an outer representative M/V never proves others.
- `HistoryOnlyBinding`: raw historical account descriptor hash and immutable actual
  membership. Approved facts survive revocation/exhaustion/succession; conflicts and
  any new task or operation on a history-only account block.
- `RetiredPreuploadCertificate`: the exact singular c25 tuple, complete retained
  records, source asset/hash facts, complete typed terminal graph and transition hash.
  It has no UC field and produces no successful-effect timestamp or completed item.
- `RedisTerminalObservation`: exact task/event stream/group/message/payload identity;
  task dispatch key, matching marker (or absent marker for genuinely never-delivered
  cancellation), exact PEL result, and real observation time. Stored old Redis results
  are not fresh observations. Duplicate or additional observations fail.

The DB loader does not contact Redis. With a retirement certificate present, its
DB-only result cannot pass retirement until a server consumer supplies separately
observed immutable Redis facts. Both DB and Redis freshness limits are 60 seconds;
no caller clock advancement or manufactured timestamp is permitted.

## Stability and Fail-Closed Boundaries

Queue leases, normal publication/task status progress, and valid metric retry chains
are revalidated but excluded from the settled effect digest. The exact successful
manual promotion replacement retains its automatic cancellation, publish parent,
manual promotion and reconciliation records in that digest.

The retired graph keeps full original records. Comparison excludes only registration
heartbeat/lease-expiry/revocation bookkeeping and grant revocation/update bookkeeping;
claim identity, lease epoch, activation provenance, image, capabilities and all
execution/event/ACK facts remain pinned and freshly checked. Marker cleanup/repair
records and legacy-resolution variants are enumerated and rejected, not ignored or
implicitly treated as equivalent proof. They are not needed by the observed c25
cancelled-dispatch ACK branch.

The reader uses one PostgreSQL SELECT over 27 explicit existing models with per-table
4097-row sentinel reads. More than 4096 rows in any set or more than 16 MiB canonical
JSON is an error, not a partial snapshot. It owns no engine, lock, transaction,
network request, mutation, or migration. Worker token/lease-secret hashes are omitted.
Raw snapshots are private evidence; only static codes and sanitized assessments should
cross an operator error boundary. PostgreSQL execution qualification and all live
proof/approval/sealing remain later parent-owned work.
