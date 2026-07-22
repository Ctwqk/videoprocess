# Task 5 Report

## RED Evidence

1. `PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -run 'DiscoveryClient' -count=1`
   failed before implementation with undefined `HTTPDiscoveryClient` and
   `DiscoveryIngestRequest` symbols in `discovery_client_test.go`.
2. `PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -run 'Discovery|Config' -count=1`
   failed before config/handler/runner implementation with missing
   `Config.DiscoveryTimeout`, `HandlerService.Discovery`, and
   `HandleIngestDiscovery` symbols.

## GREEN Evidence

1. `gofmt -w internal/channelops/discovery_client.go internal/channelops/discovery_client_test.go && PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -run 'DiscoveryClient' -count=1`
   passed: strict `httptest` request path/body/content type coverage and all
   required response rejection cases passed.
2. `gofmt -w internal/channelops/config.go internal/channelops/handlers.go internal/channelops/runner.go internal/channelops/config_test.go internal/channelops/handlers_test.go internal/channelops/runner_test.go && DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55439/videoprocess_test PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -run 'Discovery|Config' -count=1 -v`
   passed. The verbose output contains no `SKIP` entries.

## PostgreSQL Evidence

`DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55439/videoprocess_test PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -count=1 -v`

Passed in 6.122s with no skipped tests. This includes:

- `TestHandleIngestDiscoveryDoesNotHoldExecutionFenceDuringClientCall`, which
  obtains a concurrent `FOR UPDATE NOWAIT` channel lock during the fake HTTP
  call and confirms the handler leaves the queue item `running`.
- `TestRunnerDiscoveryQueueUsesLeaseAwareRetryAndCompletion`, which confirms a
  client error requeues with the lease cleared and a matching observation marks
  the leased item `succeeded`.

## Final Checks

- `PATH=/opt/homebrew/bin:$PATH go vet ./internal/channelops` passed.
- `git diff --check` passed.

## Self-Review

- The client posts only the four endpoint fields, uses the configured dedicated
  timeout, preserves the caller context, and caps successful response parsing
  at 1 MiB.
- Client failures are fixed messages or a numeric status only; response bodies,
  URLs, credentials, titles, and secrets are never returned in errors.
- The handler validates the canonical queue ID, stored/payload channel identity,
  literal source, and matching bucket before calling the client, then validates
  fake or real observations again.
- Discovery is optional for readiness and claimability. It is invoked before
  `WithQueueExecutionFence`, and neither the handler nor client changes queue
  status.
- No Python, scheduler/policy, deployment, download, publication, public-mode,
  or Task 4 production files were changed.

## Concerns

None unresolved. The Go client is deliberately limited to the internal API
contract; Python continues to own its independent committed-running authority
check and Go continues to own queue completion and retry.

## Review Fix RED Evidence

Before any production edits, the focused review suite was run with:

```bash
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55439/videoprocess_test PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -run 'Test(HandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall|HandleIngestDiscoverySanitizesClientError|DiscoveryClientIngestEnforcesDedicatedTimeoutWithCustomClient|DiscoveryClientIngestRejectsRedirectWithoutCallingTarget|DiscoveryClientIngestRejectsNonHTTPSchemes|LoadConfigStrictDiscoveryTimeoutEnv|NewRunnerHandlerServiceConfiguresDiscoveryDirectly|QueueLeaseReportsStaleCompletionAfterDeadLetter|RunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease)' -count=1 -v
```

The first compile RED recorded the missing production boundaries exactly:

```text
# github.com/Ctwqk/videoprocess/internal/channelops [github.com/Ctwqk/videoprocess/internal/channelops.test]
internal/channelops/handlers_test.go:153:21: undefined: ErrDiscoveryIngestFailed
internal/channelops/queue_test.go:237:23: undefined: ErrQueueLeaseLost
internal/channelops/runner_test.go:113:63: cfg.discoveryTimeoutParseFailed undefined (type *Config has no field or method discoveryTimeoutParseFailed)
internal/channelops/runner_test.go:245:51: undefined: ErrQueueLeaseLost
FAIL github.com/Ctwqk/videoprocess/internal/channelops [build failed]
FAIL
```

The tests were then decoupled from the not-yet-defined symbols, still without
production edits, and produced the behavioral RED evidence:

```text
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/empty_uses_default
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/minimum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/maximum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/below_minimum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/above_maximum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/malformed
    config_test.go:122: Validate did not reject discovery timeout environment value
--- FAIL: TestLoadConfigStrictDiscoveryTimeoutEnv (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/empty_uses_default (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/minimum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/maximum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/below_minimum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/above_maximum (0.00s)
    --- FAIL: TestLoadConfigStrictDiscoveryTimeoutEnv/malformed (0.00s)
=== RUN   TestDiscoveryClientIngestEnforcesDedicatedTimeoutWithCustomClient
=== RUN   TestDiscoveryClientIngestEnforcesDedicatedTimeoutWithCustomClient/zero_client_timeout
    discovery_client_test.go:136: Ingest did not return the fixed request failure
=== RUN   TestDiscoveryClientIngestEnforcesDedicatedTimeoutWithCustomClient/long_client_timeout
    discovery_client_test.go:136: Ingest did not return the fixed request failure
--- FAIL: TestDiscoveryClientIngestEnforcesDedicatedTimeoutWithCustomClient (0.61s)
    --- FAIL: TestDiscoveryClientIngestEnforcesDedicatedTimeoutWithCustomClient/zero_client_timeout (0.30s)
    --- FAIL: TestDiscoveryClientIngestEnforcesDedicatedTimeoutWithCustomClient/long_client_timeout (0.30s)
=== RUN   TestDiscoveryClientIngestRejectsRedirectWithoutCallingTarget
    discovery_client_test.go:166: Ingest did not reject the redirect response
--- FAIL: TestDiscoveryClientIngestRejectsRedirectWithoutCallingTarget (0.00s)
=== RUN   TestDiscoveryClientIngestRejectsNonHTTPSchemes
=== RUN   TestDiscoveryClientIngestRejectsNonHTTPSchemes/ftp
    discovery_client_test.go:180: Ingest did not reject the non-HTTP base scheme
=== RUN   TestDiscoveryClientIngestRejectsNonHTTPSchemes/gopher
    discovery_client_test.go:180: Ingest did not reject the non-HTTP base scheme
--- FAIL: TestDiscoveryClientIngestRejectsNonHTTPSchemes (0.00s)
    --- FAIL: TestDiscoveryClientIngestRejectsNonHTTPSchemes/ftp (0.00s)
    --- FAIL: TestDiscoveryClientIngestRejectsNonHTTPSchemes/gopher (0.00s)
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/queued_status
    handlers_test.go:123: HandleIngestDiscovery error = nil
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/missing_locked_by
    handlers_test.go:123: HandleIngestDiscovery error = nil
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/blank_locked_by
    handlers_test.go:123: HandleIngestDiscovery error = nil
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/missing_locked_at
    handlers_test.go:123: HandleIngestDiscovery error = nil
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/queue_id
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/stored_channel
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/payload_channel
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/source
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/missing_bucket
    handlers_test.go:123: HandleIngestDiscovery error = nil
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/blank_bucket
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/missing_scheduler_bucket
=== RUN   TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/mismatched_bucket
--- FAIL: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall (0.00s)
    --- FAIL: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/queued_status (0.00s)
    --- FAIL: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/missing_locked_by (0.00s)
    --- FAIL: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/blank_locked_by (0.00s)
    --- FAIL: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/missing_locked_at (0.00s)
    --- PASS: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/queue_id (0.00s)
    --- PASS: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/stored_channel (0.00s)
    --- PASS: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/payload_channel (0.00s)
    --- PASS: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/source (0.00s)
    --- FAIL: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/missing_bucket (0.00s)
    --- PASS: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/blank_bucket (0.00s)
    --- PASS: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/missing_scheduler_bucket (0.00s)
    --- PASS: TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall/mismatched_bucket (0.00s)
=== RUN   TestHandleIngestDiscoverySanitizesClientError
    handlers_test.go:154: HandleIngestDiscovery did not return the fixed discovery error
--- FAIL: TestHandleIngestDiscoverySanitizesClientError (0.00s)
=== RUN   TestQueueLeaseReportsStaleCompletionAfterDeadLetter
=== RUN   TestQueueLeaseReportsStaleCompletionAfterDeadLetter/success
    queue_test.go:237: stale success completion did not report queue lease loss
=== RUN   TestQueueLeaseReportsStaleCompletionAfterDeadLetter/retry
    queue_test.go:237: stale retry completion did not report queue lease loss
=== RUN   TestQueueLeaseReportsStaleCompletionAfterDeadLetter/reject
    queue_test.go:237: stale reject completion did not report queue lease loss
--- FAIL: TestQueueLeaseReportsStaleCompletionAfterDeadLetter (0.07s)
    --- FAIL: TestQueueLeaseReportsStaleCompletionAfterDeadLetter/success (0.01s)
    --- FAIL: TestQueueLeaseReportsStaleCompletionAfterDeadLetter/retry (0.01s)
    --- FAIL: TestQueueLeaseReportsStaleCompletionAfterDeadLetter/reject (0.01s)
=== RUN   TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly
=== RUN   TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/missing_base_URL
    runner_test.go:128: Discovery client configured for invalid discovery settings
=== RUN   TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/credential_base_URL
    runner_test.go:128: Discovery client configured for invalid discovery settings
=== RUN   TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/query_base_URL
    runner_test.go:128: Discovery client configured for invalid discovery settings
=== RUN   TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/invalid_scheme
    runner_test.go:128: Discovery client configured for invalid discovery settings
=== RUN   TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/invalid_timeout
=== RUN   TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/malformed_timeout
    runner_test.go:128: Discovery client configured for invalid discovery settings
--- FAIL: TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly (0.00s)
    --- FAIL: TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/missing_base_URL (0.00s)
    --- FAIL: TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/credential_base_URL (0.00s)
    --- FAIL: TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/query_base_URL (0.00s)
    --- FAIL: TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/invalid_scheme (0.00s)
    --- PASS: TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/invalid_timeout (0.00s)
    --- FAIL: TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly/malformed_timeout (0.00s)
=== RUN   TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease
=== RUN   TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/done
    runner_test.go:253: runOnce did not return the queue lease lost sentinel
=== RUN   TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/retry
    runner_test.go:253: runOnce did not return the queue lease lost sentinel
=== RUN   TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/deadletter
    runner_test.go:253: runOnce did not return the queue lease lost sentinel
--- FAIL: TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease (0.06s)
    --- FAIL: TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/done (0.03s)
    --- FAIL: TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/retry (0.02s)
    --- FAIL: TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/deadletter (0.02s)
FAIL
FAIL github.com/Ctwqk/videoprocess/internal/channelops 1.230s
FAIL
```

The independent discovery-wiring test was also RED before production edits:

```text
=== RUN   TestNewRunnerHandlerServiceDiscoveryIgnoresUnrelatedConfigValidation
    runner_test.go:145: unrelated invalid config disabled valid discovery settings
--- FAIL: TestNewRunnerHandlerServiceDiscoveryIgnoresUnrelatedConfigValidation (0.00s)
FAIL
FAIL github.com/Ctwqk/videoprocess/internal/channelops 0.372s
FAIL
```

## Review Fix PostgreSQL Evidence

The required named real-PostgreSQL run used:

```bash
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55439/videoprocess_test PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -run 'Test(HandleIngestDiscoveryDoesNotHoldExecutionFenceDuringClientCall|RunnerDiscoveryQueueUsesLeaseAwareRetryAndCompletion|RunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease)$' -count=1 -v
```

Exact output, with no `SKIP`:

```text
=== RUN   TestHandleIngestDiscoveryDoesNotHoldExecutionFenceDuringClientCall
--- PASS: TestHandleIngestDiscoveryDoesNotHoldExecutionFenceDuringClientCall (0.03s)
=== RUN   TestRunnerDiscoveryQueueUsesLeaseAwareRetryAndCompletion
=== RUN   TestRunnerDiscoveryQueueUsesLeaseAwareRetryAndCompletion/retry
=== RUN   TestRunnerDiscoveryQueueUsesLeaseAwareRetryAndCompletion/done
--- PASS: TestRunnerDiscoveryQueueUsesLeaseAwareRetryAndCompletion (0.04s)
    --- PASS: TestRunnerDiscoveryQueueUsesLeaseAwareRetryAndCompletion/retry (0.02s)
    --- PASS: TestRunnerDiscoveryQueueUsesLeaseAwareRetryAndCompletion/done (0.02s)
=== RUN   TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease
=== RUN   TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/done
=== RUN   TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/retry
=== RUN   TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/deadletter
--- PASS: TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease (0.06s)
    --- PASS: TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/done (0.02s)
    --- PASS: TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/retry (0.02s)
    --- PASS: TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease/deadletter (0.02s)
PASS
ok  github.com/Ctwqk/videoprocess/internal/channelops 0.372s
```

The full real-PostgreSQL package run used:

```bash
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55439/videoprocess_test PATH=/opt/homebrew/bin:$PATH go test ./internal/channelops -count=1
```

Exact output:

```text
ok  github.com/Ctwqk/videoprocess/internal/channelops 6.590s
```

## Review Fix Final Checks

- `gofmt -w internal/channelops/config.go internal/channelops/config_test.go internal/channelops/discovery_client.go internal/channelops/discovery_client_test.go internal/channelops/handlers.go internal/channelops/handlers_test.go internal/channelops/integration_test.go internal/channelops/queue.go internal/channelops/queue_test.go internal/channelops/runner.go internal/channelops/runner_test.go` exited 0 with no output.
- `PATH=/opt/homebrew/bin:$PATH go vet ./internal/channelops` exited 0 with no output.
- `git diff --check` exited 0 with no output.

## Task 5 Rereview RED/GREEN Evidence

Before the rereview production changes, the focused tests were run with:

```bash
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:55439/videoprocess_test go test ./internal/channelops -run 'TestRunnerRunContinuesAfterDiscoveryLeaseLoss|TestLoadConfigStrictDiscoveryTimeoutEnv' -count=1 -v
```

The behavioral RED was:

```text
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/empty_uses_default
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/minimum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/maximum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/below_minimum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/above_maximum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/malformed
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/duration_overflow
    config_test.go:124: Validate did not reject discovery timeout environment value
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/Atoi_overflow
--- FAIL: TestLoadConfigStrictDiscoveryTimeoutEnv (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/empty_uses_default (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/minimum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/maximum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/below_minimum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/above_maximum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/malformed (0.00s)
    --- FAIL: TestLoadConfigStrictDiscoveryTimeoutEnv/duration_overflow (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/Atoi_overflow (0.00s)
=== RUN   TestRunnerRunContinuesAfterDiscoveryLeaseLoss
=== RUN   TestRunnerRunContinuesAfterDiscoveryLeaseLoss/initial_poll
    runner_test.go:368: Run returned after lease loss: queue lease lost
=== RUN   TestRunnerRunContinuesAfterDiscoveryLeaseLoss/timer_poll
    runner_test.go:368: Run returned after lease loss: queue lease lost
--- FAIL: TestRunnerRunContinuesAfterDiscoveryLeaseLoss (1.10s)
    --- FAIL: TestRunnerRunContinuesAfterDiscoveryLeaseLoss/initial_poll (0.05s)
    --- FAIL: TestRunnerRunContinuesAfterDiscoveryLeaseLoss/timer_poll (1.05s)
FAIL
FAIL    github.com/Ctwqk/videoprocess/internal/channelops    1.403s
FAIL
```

After adding the integer bounds check before duration conversion and treating
only `ErrQueueLeaseLost` as a completed poll in both `Runner.Run` poll sites,
the same command was GREEN with no `SKIP` entries:

```text
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/empty_uses_default
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/minimum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/maximum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/below_minimum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/above_maximum
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/malformed
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/duration_overflow
=== RUN   TestLoadConfigStrictDiscoveryTimeoutEnv/Atoi_overflow
--- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/empty_uses_default (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/minimum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/maximum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/below_minimum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/above_maximum (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/malformed (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/duration_overflow (0.00s)
    --- PASS: TestLoadConfigStrictDiscoveryTimeoutEnv/Atoi_overflow (0.00s)
=== RUN   TestRunnerRunContinuesAfterDiscoveryLeaseLoss
=== RUN   TestRunnerRunContinuesAfterDiscoveryLeaseLoss/initial_poll
=== RUN   TestRunnerRunContinuesAfterDiscoveryLeaseLoss/timer_poll
--- PASS: TestRunnerRunContinuesAfterDiscoveryLeaseLoss (1.32s)
    --- PASS: TestRunnerRunContinuesAfterDiscoveryLeaseLoss/initial_poll (0.16s)
    --- PASS: TestRunnerRunContinuesAfterDiscoveryLeaseLoss/timer_poll (1.17s)
PASS
ok      github.com/Ctwqk/videoprocess/internal/channelops    1.630s
```
