package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

func TestStartJobDispatchesReadyNodeWithGoEventStream(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if store.job.Status != "RUNNING" {
		t.Fatalf("job status = %q; want RUNNING", store.job.Status)
	}
	if got := store.node("source").Status; got != "SUCCEEDED" {
		t.Fatalf("source status = %q; want SUCCEEDED", got)
	}
	if got := store.node("trim").Status; got != "QUEUED" {
		t.Fatalf("trim status = %q; want QUEUED", got)
	}
	if len(dispatcher.calls) != 1 {
		t.Fatalf("dispatch calls = %d; want 1", len(dispatcher.calls))
	}
	call := dispatcher.calls[0]
	if call.workerType != "ffmpeg_go" {
		t.Fatalf("worker type = %q; want ffmpeg_go", call.workerType)
	}
	assertGoPayload(t, call.payload)
	assertJSONMap(t, call.payload.InputArtifactsJSON, map[string]string{"input": "artifact-source-exec"})
	if got := store.planningPlan["topo_order"]; !reflect.DeepEqual(got, []string{"source", "trim", "encode"}) {
		t.Fatalf("topo_order = %#v", got)
	}
	if got := store.planningPlan["dependencies"]; !reflect.DeepEqual(got, map[string][]string{"source": []string{}, "trim": []string{"source"}, "encode": []string{"trim"}}) {
		t.Fatalf("dependencies = %#v", got)
	}
}

func TestStartJobSkipsDispatchWhenQueueClaimLost(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	store.queueClaim = false
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if got := store.node("trim").Status; got != "PENDING" {
		t.Fatalf("trim status = %q; want PENDING", got)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
}

func TestDispatchFailureReleasesQueueClaim(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	dispatchErr := errors.New("redis down")
	dispatcher := &fakeDispatcher{err: dispatchErr}
	engine := testEngine(store, dispatcher)

	err := engine.StartJob(context.Background(), "job-1")
	if !errors.Is(err, dispatchErr) {
		t.Fatalf("StartJob error = %v; want redis down", err)
	}

	if got := store.node("trim").Status; got != "PENDING" {
		t.Fatalf("trim status = %q; want PENDING", got)
	}
	if store.releaseCount != 1 {
		t.Fatalf("release count = %d; want 1", store.releaseCount)
	}
	if len(dispatcher.calls) != 1 {
		t.Fatalf("dispatch calls = %d; want 1", len(dispatcher.calls))
	}
}

func TestStartJobParksFreshJobWhenScheduleClosed(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	store.scheduleState = "CLOSED"
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if store.job.Status != "WAITING_WINDOW" {
		t.Fatalf("job status = %q; want WAITING_WINDOW", store.job.Status)
	}
	if store.waitingWindowCount != 1 {
		t.Fatalf("waiting window count = %d; want 1", store.waitingWindowCount)
	}
	if store.planningPlan != nil {
		t.Fatalf("planning plan = %#v; want nil", store.planningPlan)
	}
	if sourceStatus := store.node("source").Status; sourceStatus != "PENDING" {
		t.Fatalf("source status = %q; want PENDING", sourceStatus)
	}
	if trimStatus := store.node("trim").Status; trimStatus != "PENDING" {
		t.Fatalf("trim status = %q; want PENDING", trimStatus)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
}

func TestStartJobParksFreshJobWhenScheduleDraining(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	store.scheduleState = "DRAINING"
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if store.job.Status != "WAITING_WINDOW" {
		t.Fatalf("job status = %q; want WAITING_WINDOW", store.job.Status)
	}
	if store.waitingWindowCount != 1 {
		t.Fatalf("waiting window count = %d; want 1", store.waitingWindowCount)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
}

func TestStartJobRunsExactGuardedJob(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	store.scheduleGuardedJobID = "job-1"
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if store.job.Status != "RUNNING" {
		t.Fatalf("job status = %q; want RUNNING", store.job.Status)
	}
	if store.waitingWindowCount != 0 {
		t.Fatalf("waiting window count = %d; want 0", store.waitingWindowCount)
	}
	if store.planningPlan == nil {
		t.Fatal("planning plan is nil; guarded job did not start")
	}
	if len(dispatcher.calls) != 1 {
		t.Fatalf("dispatch calls = %d; want 1", len(dispatcher.calls))
	}
}

func TestStartJobParksGuardedMismatchBeforePlanningOrDispatch(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	store.scheduleGuardedJobID = "different-job"
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if store.job.Status != "WAITING_WINDOW" {
		t.Fatalf("job status = %q; want WAITING_WINDOW", store.job.Status)
	}
	if store.waitingWindowCount != 1 {
		t.Fatalf("waiting window count = %d; want 1", store.waitingWindowCount)
	}
	if store.planningPlan != nil {
		t.Fatalf("planning plan = %#v; want nil", store.planningPlan)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
}

func TestStartJobKeepsLegacyUnguardedOpenBehavior(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if store.job.Status != "RUNNING" || store.planningPlan == nil || len(dispatcher.calls) != 1 {
		t.Fatalf("job status=%q planning=%#v dispatch calls=%d", store.job.Status, store.planningPlan, len(dispatcher.calls))
	}
}

func TestStartJobRevalidatesAuthorityWhenPlanningClaimLosesRace(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	store.scheduleGuardedJobID = "job-1"
	store.claimAllowed = false
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if store.claimCount != 1 {
		t.Fatalf("planning claim count = %d; want 1", store.claimCount)
	}
	if store.job.Status != "WAITING_WINDOW" {
		t.Fatalf("job status = %q; want WAITING_WINDOW", store.job.Status)
	}
	if store.markPlanningCount != 0 || store.runningCount != 0 {
		t.Fatalf("ordinary planning calls=%d running calls=%d; want 0, 0", store.markPlanningCount, store.runningCount)
	}
	if store.planningPlan != nil {
		t.Fatalf("planning plan = %#v; want nil", store.planningPlan)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
}

func TestStartJobDoesNotReviveCancellationBetweenPlanningAndRunning(t *testing.T) {
	transitionErr := errors.New("running transition requires PLANNING")
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	store.cancelAfterClaim = true
	store.runningRejectedErr = transitionErr
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	err := engine.StartJob(context.Background(), "job-1")

	if !errors.Is(err, transitionErr) {
		t.Fatalf("StartJob error = %v; want running transition rejection", err)
	}
	if store.job.Status != "CANCELLED" {
		t.Fatalf("job status = %q; want CANCELLED", store.job.Status)
	}
	if sourceStatus := store.node("source").Status; sourceStatus != "PENDING" {
		t.Fatalf("source status = %q; want PENDING", sourceStatus)
	}
	if trimStatus := store.node("trim").Status; trimStatus != "PENDING" {
		t.Fatalf("trim status = %q; want PENDING", trimStatus)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
}

func TestStartJobFailsSourceWithoutAssetID(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNodeNoAsset("source-exec"), pendingNode("trim-exec", "trim"), pendingNode("encode-exec", "encode")))
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if got := store.node("source").Status; got != "FAILED" {
		t.Fatalf("source status = %q; want FAILED", got)
	}
	if !strings.Contains(store.node("source").ErrorMessage, "asset_id") {
		t.Fatalf("source error = %q; want asset_id message", store.node("source").ErrorMessage)
	}
	if got := store.node("trim").Status; got != "SKIPPED" {
		t.Fatalf("trim status = %q; want SKIPPED", got)
	}
	if store.finalStatus != "FAILED" {
		t.Fatalf("final status = %q; want FAILED", store.finalStatus)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
}

func TestStartJobFailsSourceWhenCreateArtifactFails(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "go", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim"), pendingNode("encode-exec", "encode")))
	store.createSourceErr = errors.New("asset lookup failed")
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if got := store.node("source").Status; got != "FAILED" {
		t.Fatalf("source status = %q; want FAILED", got)
	}
	if !strings.Contains(store.node("source").ErrorMessage, "asset lookup failed") {
		t.Fatalf("source error = %q; want create artifact error", store.node("source").ErrorMessage)
	}
	if got := store.node("trim").Status; got != "SKIPPED" {
		t.Fatalf("trim status = %q; want SKIPPED", got)
	}
	if store.finalStatus != "FAILED" {
		t.Fatalf("final status = %q; want FAILED", store.finalStatus)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
}

func TestNodeCompletedDispatchesDownstreamOnce(t *testing.T) {
	store := newFakeEngineStore(linearJob("RUNNING", "go",
		succeededNode("source-exec", "source", "artifact-source"),
		queuedNode("trim-exec", "trim", []string{"artifact-source"}),
		pendingNode("encode-exec", "encode"),
	))
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.OnNodeCompleted(context.Background(), "job-1", "trim-exec", "artifact-trim"); err != nil {
		t.Fatal(err)
	}
	if err := engine.OnNodeCompleted(context.Background(), "job-1", "trim-exec", "artifact-trim"); err != nil {
		t.Fatal(err)
	}

	if got := store.node("trim").Status; got != "SUCCEEDED" {
		t.Fatalf("trim status = %q; want SUCCEEDED", got)
	}
	if len(dispatcher.calls) != 1 {
		t.Fatalf("dispatch calls = %d; want 1", len(dispatcher.calls))
	}
	assertGoPayload(t, dispatcher.calls[0].payload)
	if dispatcher.calls[0].payload.NodeID != "encode" {
		t.Fatalf("dispatched node = %q; want encode", dispatcher.calls[0].payload.NodeID)
	}
	assertJSONMap(t, dispatcher.calls[0].payload.InputArtifactsJSON, map[string]string{"input": "artifact-trim"})
}

func TestNodeFailedRetriesOnce(t *testing.T) {
	store := newFakeEngineStore(linearJob("RUNNING", "go",
		succeededNode("source-exec", "source", "artifact-source"),
		runningNode("trim-exec", "trim", 0),
		pendingNode("encode-exec", "encode"),
	))
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.OnNodeFailed(context.Background(), "job-1", "trim-exec", "boom"); err != nil {
		t.Fatal(err)
	}

	trim := store.node("trim")
	if trim.RetryCount != 1 {
		t.Fatalf("retry count = %d; want 1", trim.RetryCount)
	}
	if trim.Status != "QUEUED" {
		t.Fatalf("trim status = %q; want QUEUED", trim.Status)
	}
	if len(dispatcher.calls) != 1 {
		t.Fatalf("dispatch calls = %d; want 1", len(dispatcher.calls))
	}
	assertGoPayload(t, dispatcher.calls[0].payload)
	assertJSONMap(t, dispatcher.calls[0].payload.InputArtifactsJSON, map[string]string{"input": "artifact-source"})
}

func TestNodeFailedSkipsDownstreamAfterRetryExhausted(t *testing.T) {
	store := newFakeEngineStore(linearJob("RUNNING", "go",
		succeededNode("source-exec", "source", "artifact-source"),
		runningNode("trim-exec", "trim", 1),
		pendingNode("encode-exec", "encode"),
	))
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.OnNodeFailed(context.Background(), "job-1", "trim-exec", "boom"); err != nil {
		t.Fatal(err)
	}

	if got := store.node("trim").Status; got != "FAILED" {
		t.Fatalf("trim status = %q; want FAILED", got)
	}
	if got := store.node("encode").Status; got != "SKIPPED" {
		t.Fatalf("encode status = %q; want SKIPPED", got)
	}
	if len(store.skippedNodeIDs) != 1 || store.skippedNodeIDs[0] != "encode" {
		t.Fatalf("skipped node IDs = %#v; want [encode]", store.skippedNodeIDs)
	}
	if store.finalStatus != "FAILED" {
		t.Fatalf("final status = %q; want FAILED", store.finalStatus)
	}
	if len(store.finalNodeIDs) != 0 {
		t.Fatalf("final artifact node IDs = %#v; want empty", store.finalNodeIDs)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
}

func TestFinalizeMarksSuccessfulLeafArtifacts(t *testing.T) {
	store := newFakeEngineStore(linearJob("RUNNING", "go",
		succeededNode("source-exec", "source", "artifact-source"),
		succeededNode("trim-exec", "trim", "artifact-trim"),
		queuedNode("encode-exec", "encode", []string{"artifact-trim"}),
	))
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.OnNodeCompleted(context.Background(), "job-1", "encode-exec", "artifact-encode"); err != nil {
		t.Fatal(err)
	}

	if store.finalStatus != "SUCCEEDED" {
		t.Fatalf("final status = %q; want SUCCEEDED", store.finalStatus)
	}
	if !reflect.DeepEqual(store.finalNodeIDs, []string{"encode"}) {
		t.Fatalf("final artifact node IDs = %#v; want [encode]", store.finalNodeIDs)
	}
}

func TestCancelledJobCompletionIsIgnored(t *testing.T) {
	store := newFakeEngineStore(linearJob("CANCELLED", "go",
		succeededNode("source-exec", "source", "artifact-source"),
		runningNode("trim-exec", "trim", 0),
		pendingNode("encode-exec", "encode"),
	))
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.OnNodeCompleted(context.Background(), "job-1", "trim-exec", "artifact-trim"); err != nil {
		t.Fatal(err)
	}

	if got := store.node("trim").Status; got != "RUNNING" {
		t.Fatalf("trim status = %q; want RUNNING", got)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
	if store.finalStatus != "" {
		t.Fatalf("final status = %q; want empty", store.finalStatus)
	}
}

func TestStartJobIgnoresNonGoOwner(t *testing.T) {
	store := newFakeEngineStore(linearJob("PENDING", "python", sourceNode("source-exec", "source-asset"), pendingNode("trim-exec", "trim")))
	dispatcher := &fakeDispatcher{}
	engine := testEngine(store, dispatcher)

	if err := engine.StartJob(context.Background(), "job-1"); err != nil {
		t.Fatal(err)
	}

	if store.job.Status != "PENDING" {
		t.Fatalf("job status = %q; want PENDING", store.job.Status)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch calls = %d; want 0", len(dispatcher.calls))
	}
}

func testEngine(store *fakeEngineStore, dispatcher *fakeDispatcher) *Engine {
	return &Engine{
		Store:       store,
		Dispatcher:  dispatcher,
		EventStream: "",
		Clock:       func() time.Time { return time.Unix(1779120000, 0).UTC() },
	}
}

func assertGoPayload(t *testing.T, payload TaskPayload) {
	t.Helper()
	values := payload.RedisValues()
	if values["event_stream"] != "vp:events:go" {
		t.Fatalf("event_stream = %#v; want vp:events:go", values["event_stream"])
	}
	if values["orchestrator_owner"] != "go" {
		t.Fatalf("orchestrator_owner = %#v; want go", values["orchestrator_owner"])
	}
}

func assertJSONMap(t *testing.T, raw string, want map[string]string) {
	t.Helper()
	var got map[string]string
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("unmarshal %q: %v", raw, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("input artifacts = %#v; want %#v", got, want)
	}
}

func linearJob(status string, owner string, nodes ...NodeExecutionView) JobView {
	return JobView{
		ID:                "job-1",
		Status:            status,
		OrchestratorOwner: owner,
		PipelineSnapshot: contracts.PipelineDefinition{
			Nodes: []contracts.PipelineNode{
				{ID: "source", Type: "source", Data: contracts.PipelineNodeData{Config: map[string]any{"asset_id": "source-asset"}}},
				{ID: "trim", Type: "trim", Data: contracts.PipelineNodeData{Config: map[string]any{"start": 1}}},
				{ID: "encode", Type: "encode", Data: contracts.PipelineNodeData{Config: map[string]any{"preset": "fast"}}},
			},
			Edges: []contracts.PipelineEdge{
				{ID: "edge-source-trim", Source: "source", Target: "trim", TargetHandle: "input"},
				{ID: "edge-trim-encode", Source: "trim", Target: "encode", TargetHandle: "input"},
			},
		},
		Nodes: nodes,
	}
}

func sourceNode(id string, assetID string) NodeExecutionView {
	return NodeExecutionView{
		ID:         id,
		NodeID:     "source",
		NodeType:   "source",
		NodeLabel:  "Source",
		Status:     "PENDING",
		NodeConfig: map[string]any{"asset_id": assetID},
	}
}

func sourceNodeNoAsset(id string) NodeExecutionView {
	return NodeExecutionView{
		ID:         id,
		NodeID:     "source",
		NodeType:   "source",
		NodeLabel:  "Source",
		Status:     "PENDING",
		NodeConfig: map[string]any{},
	}
}

func pendingNode(id string, nodeID string) NodeExecutionView {
	return NodeExecutionView{
		ID:         id,
		NodeID:     nodeID,
		NodeType:   nodeID,
		NodeLabel:  nodeID,
		Status:     "PENDING",
		NodeConfig: map[string]any{"name": nodeID},
	}
}

func queuedNode(id string, nodeID string, inputs []string) NodeExecutionView {
	node := pendingNode(id, nodeID)
	node.Status = "QUEUED"
	node.InputArtifactIDs = append([]string(nil), inputs...)
	return node
}

func runningNode(id string, nodeID string, retryCount int) NodeExecutionView {
	node := pendingNode(id, nodeID)
	node.Status = "RUNNING"
	node.RetryCount = retryCount
	return node
}

func succeededNode(id string, nodeID string, output string) NodeExecutionView {
	node := pendingNode(id, nodeID)
	node.Status = "SUCCEEDED"
	node.OutputArtifactID = output
	return node
}

type dispatchCall struct {
	workerType string
	payload    TaskPayload
}

type fakeDispatcher struct {
	calls []dispatchCall
	err   error
}

func (d *fakeDispatcher) Dispatch(_ context.Context, workerType string, payload TaskPayload) error {
	d.calls = append(d.calls, dispatchCall{workerType: workerType, payload: payload})
	return d.err
}

type fakeEngineStore struct {
	job                  JobView
	planningPlan         map[string]any
	skippedNodeIDs       []string
	finalStatus          string
	finalError           *string
	finalNodeIDs         []string
	scheduleState        string
	scheduleGuardedJobID string
	claimAllowed         bool
	claimCount           int
	cancelAfterClaim     bool
	markPlanningCount    int
	runningRejectedErr   error
	runningCount         int
	waitingWindowCount   int
	queueClaim           bool
	releaseCount         int
	createSourceErr      error
}

func newFakeEngineStore(job JobView) *fakeEngineStore {
	return &fakeEngineStore{job: cloneJob(job), scheduleState: "OPEN", claimAllowed: true, queueClaim: true}
}

func (s *fakeEngineStore) GetJobDetail(_ context.Context, _ string) (JobView, error) {
	return cloneJob(s.job), nil
}

func (s *fakeEngineStore) CreateSourceArtifact(_ context.Context, _ string, nodeExecutionID string, _ string) (string, error) {
	if s.createSourceErr != nil {
		return "", s.createSourceErr
	}
	artifactID := "artifact-" + nodeExecutionID
	node := s.nodeByExecutionID(nodeExecutionID)
	node.Status = "SUCCEEDED"
	node.OutputArtifactID = artifactID
	return artifactID, nil
}

func (s *fakeEngineStore) MarkGoJobPlanning(_ context.Context, _ string, executionPlan map[string]any) error {
	s.markPlanningCount++
	s.job.Status = "PLANNING"
	s.job.ExecutionPlan = executionPlan
	s.planningPlan = executionPlan
	return nil
}

func (s *fakeEngineStore) ClaimGoJobPlanning(_ context.Context, _ string, executionPlan map[string]any) (bool, error) {
	s.claimCount++
	if !s.claimAllowed {
		s.job.Status = "WAITING_WINDOW"
		return false, nil
	}
	s.job.Status = "PLANNING"
	s.job.ExecutionPlan = executionPlan
	s.planningPlan = executionPlan
	if s.cancelAfterClaim {
		s.job.Status = "CANCELLED"
	}
	return true, nil
}

func (s *fakeEngineStore) MarkGoJobRunning(_ context.Context, _ string) error {
	s.runningCount++
	if s.job.Status != "PLANNING" {
		if s.runningRejectedErr != nil {
			return s.runningRejectedErr
		}
		return errors.New("running transition requires PLANNING")
	}
	s.job.Status = "RUNNING"
	return nil
}

func (s *fakeEngineStore) GetVideoScheduleAuthority(_ context.Context) (VideoScheduleAuthority, error) {
	return VideoScheduleAuthority{
		State:        s.scheduleState,
		GuardedJobID: s.scheduleGuardedJobID,
	}, nil
}

func (s *fakeEngineStore) MarkGoJobWaitingWindow(_ context.Context, _ string) error {
	s.job.Status = "WAITING_WINDOW"
	s.waitingWindowCount++
	return nil
}

func (s *fakeEngineStore) MarkGoNodeQueued(_ context.Context, nodeExecutionID string, inputArtifactIDs []string) (bool, error) {
	if !s.queueClaim {
		return false, nil
	}
	node := s.nodeByExecutionID(nodeExecutionID)
	if node.Status != "PENDING" {
		return false, nil
	}
	node.Status = "QUEUED"
	node.InputArtifactIDs = append([]string(nil), inputArtifactIDs...)
	return true, nil
}

func (s *fakeEngineStore) ReleaseGoNodeQueueClaim(_ context.Context, nodeExecutionID string) error {
	s.releaseCount++
	node := s.nodeByExecutionID(nodeExecutionID)
	if node.Status == "QUEUED" {
		node.Status = "PENDING"
		node.InputArtifactIDs = nil
	}
	return nil
}

func (s *fakeEngineStore) MarkGoNodeSucceeded(_ context.Context, _ string, nodeExecutionID string, outputArtifactID string) error {
	node := s.nodeByExecutionID(nodeExecutionID)
	node.Status = "SUCCEEDED"
	node.OutputArtifactID = outputArtifactID
	return nil
}

func (s *fakeEngineStore) MarkGoNodeFailed(_ context.Context, _ string, nodeExecutionID string, errorMessage string) error {
	node := s.nodeByExecutionID(nodeExecutionID)
	node.Status = "FAILED"
	node.ErrorMessage = errorMessage
	return nil
}

func (s *fakeEngineStore) IncrementGoNodeRetry(_ context.Context, _ string, nodeExecutionID string) error {
	node := s.nodeByExecutionID(nodeExecutionID)
	node.RetryCount++
	node.Status = "PENDING"
	node.ErrorMessage = ""
	return nil
}

func (s *fakeEngineStore) SkipGoDownstreamNodes(_ context.Context, _ string, nodeIDs []string) error {
	s.skippedNodeIDs = append([]string(nil), nodeIDs...)
	for _, nodeID := range nodeIDs {
		node := s.node(nodeID)
		if node.Status != "SUCCEEDED" && node.Status != "FAILED" {
			node.Status = "SKIPPED"
		}
	}
	return nil
}

func (s *fakeEngineStore) FinalizeGoJob(_ context.Context, _ string, status string, errorMessage *string, finalArtifactNodeIDs []string) error {
	s.job.Status = status
	s.finalStatus = status
	s.finalError = errorMessage
	s.finalNodeIDs = append([]string(nil), finalArtifactNodeIDs...)
	return nil
}

func (s *fakeEngineStore) node(nodeID string) *NodeExecutionView {
	for i := range s.job.Nodes {
		if s.job.Nodes[i].NodeID == nodeID {
			return &s.job.Nodes[i]
		}
	}
	panic("missing node " + nodeID)
}

func (s *fakeEngineStore) nodeByExecutionID(id string) *NodeExecutionView {
	for i := range s.job.Nodes {
		if s.job.Nodes[i].ID == id {
			return &s.job.Nodes[i]
		}
	}
	panic("missing node execution " + id)
}

func cloneJob(job JobView) JobView {
	cloned := job
	cloned.ExecutionPlan = cloneAnyMap(job.ExecutionPlan)
	cloned.Nodes = make([]NodeExecutionView, len(job.Nodes))
	for i, node := range job.Nodes {
		cloned.Nodes[i] = node
		cloned.Nodes[i].NodeConfig = cloneAnyMap(node.NodeConfig)
		cloned.Nodes[i].InputArtifactIDs = append([]string(nil), node.InputArtifactIDs...)
	}
	return cloned
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
