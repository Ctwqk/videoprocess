package orchestrator

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestRecoveryStartsPendingGoOwnedJobs(t *testing.T) {
	store := &fakeRecoveryStore{jobs: []JobView{
		{ID: "job-1", Status: "PENDING", OrchestratorOwner: "go"},
		{ID: "job-2", Status: "RUNNING", OrchestratorOwner: "go"},
	}}
	engine := &fakeRecoveryEngine{}
	runner := testRecoveryRunner(store, engine)

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(engine.started, []string{"job-1", "job-2"}) {
		t.Fatalf("started jobs = %#v; want [job-1 job-2]", engine.started)
	}
}

func TestRecoveryDoesNotTouchPythonOwnedJobs(t *testing.T) {
	store := &fakeRecoveryStore{jobs: []JobView{
		{ID: "job-go", Status: "PENDING", OrchestratorOwner: "go"},
		{ID: "job-python", Status: "PENDING", OrchestratorOwner: "python"},
	}}
	engine := &fakeRecoveryEngine{}
	runner := testRecoveryRunner(store, engine)

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(engine.started, []string{"job-go"}) {
		t.Fatalf("started jobs = %#v; want [job-go]", engine.started)
	}
	if !reflect.DeepEqual(store.resetJobIDs, []string{"job-go"}) {
		t.Fatalf("reset job IDs = %#v; want [job-go]", store.resetJobIDs)
	}
}

func TestRecoveryResetsStaleQueuedNode(t *testing.T) {
	now := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	store := &fakeRecoveryStore{jobs: []JobView{{ID: "job-1", Status: "RUNNING", OrchestratorOwner: "go"}}}
	engine := &fakeRecoveryEngine{}
	runner := testRecoveryRunner(store, engine)
	runner.Clock = func() time.Time { return now }
	runner.StaleNodeAge = 30 * time.Minute

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(store.resetBefore) != 1 {
		t.Fatalf("reset calls = %d; want 1", len(store.resetBefore))
	}
	if got, want := store.resetBefore[0], now.Add(-30*time.Minute); !got.Equal(want) {
		t.Fatalf("staleBefore = %s; want %s", got, want)
	}
}

func TestRecoveryFinalizesTerminalJobInsteadOfDispatching(t *testing.T) {
	store := &fakeRecoveryStore{jobs: []JobView{{
		ID:                "job-terminal",
		Status:            "RUNNING",
		OrchestratorOwner: "go",
		Nodes: []NodeExecutionView{
			{ID: "source-exec", NodeID: "source", Status: "SUCCEEDED", OutputArtifactID: "artifact-source"},
			{ID: "encode-exec", NodeID: "encode", Status: "SUCCEEDED", OutputArtifactID: "artifact-encode"},
		},
	}}}
	engine := &fakeRecoveryEngine{}
	runner := testRecoveryRunner(store, engine)

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(engine.started, []string{"job-terminal"}) {
		t.Fatalf("started jobs = %#v; want [job-terminal]", engine.started)
	}
	if engine.recoveryDispatches != 0 {
		t.Fatalf("recovery dispatches = %d; want 0", engine.recoveryDispatches)
	}
}

func TestRecoveryReturnsStartJobError(t *testing.T) {
	store := &fakeRecoveryStore{jobs: []JobView{{ID: "job-1", Status: "RUNNING", OrchestratorOwner: "go"}}}
	engine := &fakeRecoveryEngine{err: errors.New("dispatch failed")}
	runner := testRecoveryRunner(store, engine)

	err := runner.RunOnce(context.Background())
	if !errors.Is(err, engine.err) {
		t.Fatalf("RunOnce error = %v; want dispatch failed", err)
	}
}

func testRecoveryRunner(store *fakeRecoveryStore, engine *fakeRecoveryEngine) *RecoveryRunner {
	return &RecoveryRunner{
		Store:  store,
		Engine: engine,
		Clock: func() time.Time {
			return time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
		},
	}
}

type fakeRecoveryStore struct {
	jobs        []JobView
	resetJobIDs []string
	resetBefore []time.Time
}

func (s *fakeRecoveryStore) ListRecoverableGoJobs(_ context.Context) ([]JobView, error) {
	return append([]JobView(nil), s.jobs...), nil
}

func (s *fakeRecoveryStore) ResetStaleGoNodes(_ context.Context, jobID string, staleBefore time.Time) error {
	s.resetJobIDs = append(s.resetJobIDs, jobID)
	s.resetBefore = append(s.resetBefore, staleBefore)
	return nil
}

type fakeRecoveryEngine struct {
	started            []string
	recoveryDispatches int
	err                error
}

func (e *fakeRecoveryEngine) StartJob(_ context.Context, jobID string) error {
	e.started = append(e.started, jobID)
	return e.err
}
