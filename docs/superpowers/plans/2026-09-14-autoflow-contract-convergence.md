# AutoFlow review convergence implementation plan

> Use Superpowers debugging, TDD, parallel independent problem investigation, and verification before completion. The user authorized implementation of the attached review on an isolated branch on 2026-09-14.

**Goal:** Resolve confirmed F01–F08 defects and record R01–R13 verification against current code.

**Architecture:** Retain Go ChannelOps ownership and deterministic pipeline compilation. Share candidate rights semantics across selection and conversion; represent missing material as a blocked plan; enforce explicit planning modes and report actual provider/fallback; require complete mandatory storyboards for upload.

**Tech stack:** Python 3.12, FastAPI/Pydantic, SQLAlchemy, Go, pytest.

**Review source:** `VP审查与收敛建议_697f622.md`, baseline 697f622; implementation baseline 95a2b8f. F06 is narrower than the report: production CLI admission already exists.

**Integration base:** During implementation, another task fast-forwarded the shared worktrees to `78baa79` (deployment rollback replay fix). This work preserves that update; final verification and the convergence commit use `78baa79` as their base.

## Global constraints

- Preserve existing APIs and migration history.
- All executable AutoFlow graphs pass `validate_pipeline()`.
- Default publication privacy remains private/unlisted; external sources require explicit human review before upload.
- Model output cannot assert rights or bypass capability-constrained deterministic compilation.
- Existing owned-inventory production profile keeps its strict generated provenance checks. Generic verified owned input supports original footage.
- No real upload or production deployment is part of local regression validation.

## Tasks

- [x] 1. Rights: `rights_policy.py`, `material_selector.py`, `search_service.py`. Add behavior tests rejecting unknown/blocked owned claims, preserving provenance and blocked states during conversion. Share `candidate_is_owned`, `candidate_is_licensed`, and rights metadata conversion helpers. Verify known owned/licensed/CC and external-review cases.
- [x] 2. Storyboard and parsing: `pipeline_builder.py`, `storyboard_generator.py`, `intent_parser.py`. Add required shot coverage tests before fixes; default `ShotSpec.required=True`. Formal material-library builds reject missing required shots; optional omissions are recorded with recomputed effective duration. Generation fulfills min/max or explicitly rejects unsupported counts, retaining ending when truncating. Exact X token matching and remove unreachable branch. Input smart_trim remains fail on missing matches for uploads.
- [x] 3. Cross-language/runtime: `internal/channelops/handlers.go`, Go request fixtures/tests, Python queue/runner. Forward versioned planning options from trusted config, preserving explicit false and keeping experimental opt-in. Filter Python claims by implemented kinds, keeping existing CLI admission and APIs. Behavior tests prove unsupported tasks remain queued.
- [x] 4. Service integration: `schemas/autoflow.py`, `autoflow/service.py`, `graph_planner.py`. No implicit demo provider: zero materials produces blocked no_material with no graph. Explicit template/storyboard/ai_graph has deterministic semantics; incompatible combinations reject clearly. Add persisted planning provenance (requested/effective/provider/fallback) and preserve rights evidence from DB assets and storyboard matches. Re-evaluate changed candidates and invalidate approvals. Red/green tests cover API and planner entry points.
- [x] 5. Evidence and docs: update current architecture entry and supersession pointers in relevant historical designs/runbook. Validate upload-timeout reconciliation, revision-bound approval, bounded rollback/restart recovery using existing focused behavioral suites and add missing regression where needed. Record verified limits, not runtime claims.
- [x] 6. Integration: full backend pytest, ruff, mypy; Go tests; frontend install/build/lint only if frontend files change. Independently review the complete diff, fix actionable issues, commit to the isolated branch and retain worktree for user review.

## Completion evidence

Final backend: 4048 passed, 577 environment-dependent skips. Go all packages passed. Expanded recovery/transaction suites: 716 passed plus 81 subtests; shell rollback: 14 passed. Ruff retains 11 pre-existing findings and mypy retains 61; comparison found no new static issues. Independent review findings were fixed and re-reviewed. No frontend changes or real platform/deployment actions. See [verification record](../../review-convergence-verification.md) for the R01–R13 mapping and limits.

## Test patterns

```python
assert not candidate_is_owned(AutoFlowClipCandidate(id='a', title='a', asset_id='a', source_type='asset', rights_status='unknown'))
assert RightsPolicy().evaluate(request, [blocked_candidate]).status == 'blocked'
assert plan.status == 'blocked' and plan.candidates == []  # empty search
assert plan.validation['planning']['effective_mode'] == 'storyboard'
assert len(storyboard.shots) >= request.min_shots  # or explicit ValueError
```

Run targeted tests before and after each fix. Full verification commands run from the worktree with its Python 3.12 virtual environment; retain exit codes and counts in the verification report.
