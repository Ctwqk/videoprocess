# Task 3 Report: Queue-Authority-Fenced Discovery Ingestion API

## RED

Command:

```bash
cd backend
.venv/bin/python -m pytest tests/api/test_channel_agent_discovery.py -q
```

Output:

```text
FFFFFFFFFFFFFFFFFF                                                       [100%]
...
assert {'detail': 'Not Found'} == {'detail': 'discovery_queue_item_not_found'}
...
18 failed in 0.65s
```

The route did not exist. The missing-queue assertion received FastAPI's
`404 Not Found`, and all remaining cases failed because the endpoint was not
registered.

## GREEN

Command:

```bash
cd backend
.venv/bin/python -m pytest tests/api/test_channel_agent_discovery.py -q
```

Output:

```text
.....................                                                    [100%]
21 passed in 0.62s
```

## Verification

```text
.venv/bin/python -m pytest tests/channel_agent/test_api.py -q
33 passed in 1.25s

.venv/bin/python -m pytest -q
746 passed, 55 skipped, 8 warnings in 66.37s (0:01:06)

.venv/bin/python -m ruff check app/api/channel_agent.py app/schemas/channel_agent.py tests/api/test_channel_agent_discovery.py
All checks passed!

.venv/bin/python -m mypy --follow-imports=skip app/api/channel_agent.py app/schemas/channel_agent.py
Success: no issues found in 2 source files

git diff --check
(no output; exit 0)
```

Project-wide static checks were also run as required by `AGENTS.md`:

```text
.venv/bin/python -m ruff check .
Found 17 errors.

.venv/bin/python -m mypy app
Found 66 errors in 24 files (checked 146 source files)
```

Those findings are pre-existing outside Task 3. The initial scoped mypy run
identified two literal-type mismatches in the new response construction; both
were fixed before the final scoped check above. The full test run emitted eight
pre-existing `datetime.utcnow()` deprecation warnings in orchestrator tests.

## Self-Review

- Request validation requires UUID identities, the `youtube_search` literal, a
  nonblank bounded bucket, and forbids extra properties.
- The endpoint reads the committed queue row and rejects missing, wrong-kind,
  non-running, channel-mismatched, or payload-mismatched requests before it
  creates the YouTube client.
- Channel availability and discovery policy are independently validated before
  provider construction. The existing runner's `_build_youtube_client()`
  wiring is reused lazily.
- Typed ingestion authority, policy, in-progress, and provider errors map to
  fixed detail codes. Provider details are not returned.
- Successful replays use the existing durable ingestion service and return the
  same run without a second ingester call. The endpoint never changes queue
  status, retry state, or completion ownership.
- No download, production-task, upload, publication, promotion, public-mode,
  scheduler, deployment, or 126 behavior was changed.

## Concerns

The full repository Ruff and mypy baselines remain nonzero for unrelated
files. Task 3's focused lint and type checks are clean, and all backend tests
pass.

## Review Fix: Lazy Provider Wiring

### RED

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_discovery_ingestion.py -q
```

```text
...FF.........
2 failed, 12 passed in 0.44s
```

The new lazy-factory tests failed because `DiscoveryIngestionService` did not
accept `youtube_client_factory`.

```bash
cd backend
.venv/bin/python -m pytest tests/api/test_channel_agent_discovery.py -q
```

```text
..............FFF..........
3 failed, 24 passed in 0.95s
```

The accepted/replay request invoked the eager factory twice, a factory
construction failure returned 500 instead of the fixed 502, and a negative
service counter was emitted as HTTP 200.

### GREEN

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_discovery_ingestion.py -q
```

```text
..............
14 passed in 0.38s
```

```bash
cd backend
.venv/bin/python -m pytest tests/api/test_channel_agent_discovery.py -q
```

```text
...........................
27 passed in 0.82s
```

### Fix Evidence

- `DiscoveryIngestionService` accepts either the compatible eager
  `youtube_client` or a `youtube_client_factory`; it invokes the factory only
  after a non-replay claim.
- Factory exceptions mark the committed run `failed` with only
  `provider_unavailable` and raise the existing typed provider error, which
  the endpoint maps to `502 {"detail":"discovery_provider_error"}`.
- All five response counters now require `ge=0`; the endpoint response-model
  test proves a negative result cannot return HTTP 200.
- API tests use a fresh request session, persist payload updates with a new
  dictionary, and cover exact source/bucket/channel, missing, and non-object
  payload mismatches before provider construction.
- `build_youtube_manager_client()` is the single public client builder used by
  both the runner and API; it reuses `YouTubeManagerClient` configuration
  handling without duplicating settings logic.

Focused verification:

```bash
cd backend
.venv/bin/python -m pytest tests/api/test_channel_agent_discovery.py tests/channel_agent/test_api.py -q
```

```text
60 passed in 1.76s
```

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_discovery_ingestion.py tests/channel_agent/test_runner.py tests/channel_agent/test_youtube_manager_client.py -q
```

```text
23 passed in 0.56s
```

```bash
cd backend
.venv/bin/python -m ruff check app/api/channel_agent.py app/channel_agent/clients.py app/channel_agent/runner.py app/schemas/channel_agent.py app/services/discovery_ingestion.py tests/api/test_channel_agent_discovery.py tests/services/test_discovery_ingestion.py tests/channel_agent/test_runner.py tests/channel_agent/test_youtube_manager_client.py
.venv/bin/python -m mypy --follow-imports=skip app/api/channel_agent.py app/channel_agent/clients.py app/channel_agent/runner.py app/schemas/channel_agent.py app/services/discovery_ingestion.py
```

```text
All checks passed!
Success: no issues found in 5 source files
```

Required full-suite verification:

```bash
cd backend
.venv/bin/python -m pytest -q
```

```text
754 passed, 55 skipped, 8 warnings in 66.51s (0:01:06)
```

Repository-wide Ruff still reports 17 unrelated pre-existing findings. The
repository-wide mypy baseline is now 64 unrelated findings in 23 files; the
two former `app/channel_agent/clients.py` findings are resolved by the local
type narrowing included with the shared-builder change.
