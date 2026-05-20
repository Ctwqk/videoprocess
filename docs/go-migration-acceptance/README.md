# Go Migration Acceptance Evidence

Scope: non-Phase-6 completion for `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`.

Python remains authoritative for orchestration, event listening, schema migration, and rollback.

Evidence sections:

1. Registry parity.
2. Validator parity and unsupported graph refusal.
3. Per-node Go worker migration gate.
4. Per-route Go API write gate.
5. Docker health, readiness, and metrics.
6. Staging jobs, Redis pending, artifacts, p95, failure, cancellation, and rollback.

## Baseline

Commands run before non-Phase-6 completion work:

```bash
git status --short --branch
go test ./...
go vet ./...
cd backend && python3 -m pytest
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

Observed result:

```text
git branch: codex/go-partial-migration
go test ./...: pass
go vet ./...: pass
backend pytest: 331 passed, 8 warnings
ruff: /usr/bin/python3: No module named ruff
mypy: /usr/bin/python3: No module named mypy
```
