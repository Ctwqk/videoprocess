package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCancelJobPreventsBlockedStaleFinalizerFromRevivingJob(t *testing.T) {
	ctx, fixtureStore, pipelineID := newCancellationRaceIntegrationFixture(t)
	jobID := uuid.NewString()
	if _, err := fixtureStore.Pool.Exec(ctx, `
		INSERT INTO jobs (id, pipeline_id, pipeline_snapshot, status, orchestrator_owner)
		VALUES ($1::uuid, $2::uuid, '{}'::json, 'RUNNING', 'go')
	`, jobID, pipelineID); err != nil {
		t.Fatalf("insert running job: %v", err)
	}
	finalNodeExecutionID := uuid.NewString()
	activeNodeExecutionID := uuid.NewString()
	artifactID := uuid.NewString()
	if _, err := fixtureStore.Pool.Exec(ctx, `
		INSERT INTO node_executions (id, job_id, node_id, node_type, status)
		VALUES
			($1::uuid, $3::uuid, 'final', 'encode', 'SUCCEEDED'),
			($2::uuid, $3::uuid, 'active', 'encode', 'RUNNING')
	`, finalNodeExecutionID, activeNodeExecutionID, jobID); err != nil {
		t.Fatalf("insert node executions: %v", err)
	}
	if _, err := fixtureStore.Pool.Exec(ctx, `
		INSERT INTO artifacts (
			id, job_id, node_execution_id, kind, filename, storage_backend, storage_path
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'INTERMEDIATE', 'final.mp4', 'local', 'race/final.mp4')
	`, artifactID, jobID, finalNodeExecutionID); err != nil {
		t.Fatalf("insert artifact: %v", err)
	}
	if _, err := fixtureStore.Pool.Exec(ctx, `
		UPDATE node_executions
		SET output_artifact_id = $2::uuid
		WHERE id = $1::uuid
	`, finalNodeExecutionID, artifactID); err != nil {
		t.Fatalf("attach output artifact: %v", err)
	}

	databaseURL := os.Getenv("DATABASE_URL")
	cancelApplicationName := "store-cancel-race-" + uuid.NewString()
	finalizeApplicationName := "store-finalize-race-" + uuid.NewString()
	cancelStore := openNamedIntegrationStore(t, ctx, databaseURL, cancelApplicationName)
	finalizeStore := openNamedIntegrationStore(t, ctx, databaseURL, finalizeApplicationName)

	nodeLock, err := fixtureStore.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin node lock: %v", err)
	}
	defer nodeLock.Rollback(context.Background())
	var lockedNodeID [16]byte
	if err := nodeLock.QueryRow(ctx, `
		SELECT id
		FROM node_executions
		WHERE id = $1::uuid
		FOR UPDATE
	`, activeNodeExecutionID).Scan(&lockedNodeID); err != nil {
		t.Fatalf("lock active node: %v", err)
	}

	type cancelResult struct {
		row JobDetailRow
		err error
	}
	cancelDone := make(chan cancelResult, 1)
	go func() {
		row, err := cancelStore.CancelJob(ctx, jobID)
		cancelDone <- cancelResult{row: row, err: err}
	}()
	waitForNamedDatabaseLock(t, ctx, fixtureStore, cancelApplicationName, "UPDATE node_executions")

	var statusWhileCancellationBlocked string
	if err := fixtureStore.Pool.QueryRow(ctx, `
		SELECT status::text FROM jobs WHERE id = $1::uuid
	`, jobID).Scan(&statusWhileCancellationBlocked); err != nil {
		t.Fatalf("read job while cancellation is blocked: %v", err)
	}

	finalizeDone := make(chan error, 1)
	go func() {
		finalizeDone <- finalizeStore.FinalizeGoJob(ctx, jobID, "SUCCEEDED", nil, []string{"final"})
	}()
	finalizerBlocked, earlyFinalizeErr := waitForNamedDatabaseLockOrResult(
		t,
		ctx,
		fixtureStore,
		finalizeApplicationName,
		finalizeDone,
	)

	if err := nodeLock.Rollback(ctx); err != nil {
		t.Fatalf("release active node lock: %v", err)
	}

	var cancelled cancelResult
	select {
	case cancelled = <-cancelDone:
	case <-time.After(2 * time.Second):
		t.Fatal("CancelJob did not finish after the active node lock was released")
	}
	finalizeErr := earlyFinalizeErr
	if finalizerBlocked {
		select {
		case finalizeErr = <-finalizeDone:
		case <-time.After(2 * time.Second):
			t.Fatal("FinalizeGoJob did not finish after cancellation committed")
		}
	}

	if cancelled.err != nil {
		t.Errorf("CancelJob error = %v", cancelled.err)
	} else if cancelled.row.Status != "CANCELLED" {
		t.Errorf("CancelJob returned status %q; want CANCELLED", cancelled.row.Status)
	}
	if finalizeErr != nil {
		t.Errorf("FinalizeGoJob error = %v; want terminal no-op", finalizeErr)
	}
	if statusWhileCancellationBlocked != "RUNNING" {
		t.Errorf("job status while cancellation was blocked = %q; want RUNNING", statusWhileCancellationBlocked)
	}
	if !finalizerBlocked {
		t.Error("FinalizeGoJob completed without waiting for the cancelling transaction's job lock")
	}

	var jobStatus string
	var activeNodeStatus string
	var artifactKind string
	if err := fixtureStore.Pool.QueryRow(ctx, `
		SELECT j.status::text, ne.status::text, a.kind::text
		FROM jobs j
		JOIN node_executions ne ON ne.job_id = j.id AND ne.id = $2::uuid
		JOIN artifacts a ON a.job_id = j.id AND a.id = $3::uuid
		WHERE j.id = $1::uuid
	`, jobID, activeNodeExecutionID, artifactID).Scan(&jobStatus, &activeNodeStatus, &artifactKind); err != nil {
		t.Fatalf("read final race state: %v", err)
	}
	if jobStatus != "CANCELLED" {
		t.Errorf("final job status = %q; want CANCELLED", jobStatus)
	}
	if activeNodeStatus != "CANCELLED" {
		t.Errorf("final active node status = %q; want CANCELLED", activeNodeStatus)
	}
	if artifactKind != "INTERMEDIATE" {
		t.Errorf("final artifact kind = %q; want INTERMEDIATE", artifactKind)
	}
}

func newCancellationRaceIntegrationFixture(t *testing.T) (context.Context, *Store, string) {
	t.Helper()
	if os.Getenv("CHANNELOPS_REQUIRE_DATABASE") != "1" {
		t.Skip("cancellation race integration requires CHANNELOPS_REQUIRE_DATABASE=1")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required when CHANNELOPS_REQUIRE_DATABASE=1")
	}
	ctx := context.Background()
	fixtureStore, err := Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open cancellation race integration store: %v", err)
	}
	pipelineID := uuid.NewString()
	if _, err := fixtureStore.Pool.Exec(ctx, `
		INSERT INTO pipelines (id, name, description, definition, is_template, template_tags)
		VALUES ($1::uuid, 'cancellation race test', '', '{}'::json, FALSE, '{}')
	`, pipelineID); err != nil {
		fixtureStore.Close()
		t.Fatalf("insert cancellation race pipeline: %v", err)
	}
	t.Cleanup(func() {
		_, _ = fixtureStore.Pool.Exec(ctx, `DELETE FROM jobs WHERE pipeline_id = $1::uuid`, pipelineID)
		_, _ = fixtureStore.Pool.Exec(ctx, `DELETE FROM pipelines WHERE id = $1::uuid`, pipelineID)
		fixtureStore.Close()
	})
	return ctx, fixtureStore, pipelineID
}

func openNamedIntegrationStore(
	t *testing.T,
	ctx context.Context,
	databaseURL string,
	applicationName string,
) *Store {
	t.Helper()
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse integration database config: %v", err)
	}
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open integration pool %q: %v", applicationName, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping integration pool %q: %v", applicationName, err)
	}
	t.Cleanup(pool.Close)
	return &Store{Pool: pool}
}

func waitForNamedDatabaseLock(
	t *testing.T,
	ctx context.Context,
	observer *Store,
	applicationName string,
	queryFragment string,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if namedDatabaseSessionIsWaiting(t, ctx, observer, applicationName, queryFragment) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("database session %q did not wait on %q", applicationName, queryFragment)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForNamedDatabaseLockOrResult(
	t *testing.T,
	ctx context.Context,
	observer *Store,
	applicationName string,
	done <-chan error,
) (bool, error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			return false, err
		default:
		}
		if namedDatabaseSessionIsWaiting(t, ctx, observer, applicationName, "jobs") {
			return true, nil
		}
		if time.Now().After(deadline) {
			t.Fatalf("database session %q neither completed nor waited on the job row", applicationName)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func namedDatabaseSessionIsWaiting(
	t *testing.T,
	ctx context.Context,
	observer *Store,
	applicationName string,
	queryFragment string,
) bool {
	t.Helper()
	var waiting bool
	if err := observer.Pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND application_name = $1
			  AND state = 'active'
			  AND wait_event_type = 'Lock'
			  AND query LIKE '%' || $2 || '%'
		)
	`, applicationName, queryFragment).Scan(&waiting); err != nil {
		t.Fatalf("inspect database lock for %q: %v", applicationName, err)
	}
	return waiting
}
