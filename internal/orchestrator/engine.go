package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

const (
	defaultGoEventStream = "vp:events:go"
	goOrchestratorOwner  = "go"
	goWorkerType         = "ffmpeg_go"
)

type EngineStore interface {
	GetJobDetail(ctx context.Context, id string) (JobView, error)
	CreateSourceArtifact(ctx context.Context, jobID string, nodeExecutionID string, assetID string) (string, error)
	GetVideoScheduleAuthority(ctx context.Context) (VideoScheduleAuthority, error)
	MarkGoJobPlanning(ctx context.Context, jobID string, executionPlan map[string]any) error
	MarkGoJobRunning(ctx context.Context, jobID string) error
	MarkGoJobWaitingWindow(ctx context.Context, jobID string) error
	MarkGoNodeQueued(ctx context.Context, nodeExecutionID string, inputArtifactIDs []string) (bool, error)
	ReleaseGoNodeQueueClaim(ctx context.Context, nodeExecutionID string) error
	MarkGoNodeSucceeded(ctx context.Context, jobID string, nodeExecutionID string, outputArtifactID string) error
	MarkGoNodeFailed(ctx context.Context, jobID string, nodeExecutionID string, errorMessage string) error
	IncrementGoNodeRetry(ctx context.Context, jobID string, nodeExecutionID string) error
	SkipGoDownstreamNodes(ctx context.Context, jobID string, nodeIDs []string) error
	FinalizeGoJob(ctx context.Context, jobID string, status string, errorMessage *string, finalArtifactNodeIDs []string) error
}

type VideoScheduleAuthority struct {
	State        string
	GuardedJobID string
}

type Dispatcher interface {
	Dispatch(ctx context.Context, workerType string, payload TaskPayload) error
}

type Engine struct {
	Store       EngineStore
	Dispatcher  Dispatcher
	EventStream string
	Clock       func() time.Time
	Logger      *slog.Logger
}

type JobView struct {
	ID                string
	Status            string
	OrchestratorOwner string
	PipelineSnapshot  contracts.PipelineDefinition
	ExecutionPlan     map[string]any
	Nodes             []NodeExecutionView
}

type NodeExecutionView struct {
	ID               string
	NodeID           string
	NodeType         string
	NodeLabel        string
	Status           string
	RetryCount       int
	NodeConfig       map[string]any
	OutputArtifactID string
	InputArtifactIDs []string
	WorkerID         string
	ErrorMessage     string
}

func (e *Engine) StartJob(ctx context.Context, jobID string) (err error) {
	startResult := "error"
	defer func() {
		observeGoJobStarted(startResult)
	}()

	job, err := e.Store.GetJobDetail(ctx, jobID)
	if err != nil {
		return err
	}
	if !shouldProcessGoJob(job) {
		startResult = "skipped"
		return nil
	}
	if isTerminalJobStatus(job.Status) {
		startResult = "terminal"
		return nil
	}
	waiting, err := e.shouldWaitForSchedule(ctx, job)
	if err != nil {
		return err
	}
	if waiting {
		startResult = "waiting_window"
		return nil
	}

	depMap := DependencyMap(job.PipelineSnapshot)
	executionPlan := map[string]any{
		"topo_order":   TopologicalOrder(job.PipelineSnapshot),
		"dependencies": depMap,
	}
	if err := e.Store.MarkGoJobPlanning(ctx, jobID, executionPlan); err != nil {
		return err
	}
	job.ExecutionPlan = executionPlan
	job.Status = string(contracts.JobStatusPlanning)

	if err := e.Store.MarkGoJobRunning(ctx, jobID); err != nil {
		return err
	}
	job.Status = string(contracts.JobStatusRunning)

	if err := e.resolveSourceNodes(ctx, &job); err != nil {
		return err
	}
	if isTerminalJobStatus(job.Status) {
		startResult = "started"
		return nil
	}
	if err := e.dispatchReadyNodes(ctx, &job, depMap); err != nil {
		return err
	}
	if _, err := e.maybeFinalizeJob(ctx, job); err != nil {
		return err
	}
	startResult = "started"
	return err
}

func (e *Engine) shouldWaitForSchedule(ctx context.Context, job JobView) (bool, error) {
	authority, err := e.Store.GetVideoScheduleAuthority(ctx)
	if err != nil {
		return false, err
	}
	state := strings.ToUpper(strings.TrimSpace(authority.State))
	if state == "OPEN" && authority.GuardedJobID != "" && job.ID != authority.GuardedJobID {
		if job.Status != string(contracts.JobStatusWaitingWindow) {
			if err := e.Store.MarkGoJobWaitingWindow(ctx, job.ID); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	if state != "CLOSED" && state != "DRAINING" {
		return false, nil
	}
	alreadyWaiting := job.Status == string(contracts.JobStatusWaitingWindow)
	freshSubmission := job.Status == string(contracts.JobStatusPending)
	if state == "DRAINING" && !alreadyWaiting && !freshSubmission {
		return false, nil
	}
	if !alreadyWaiting {
		if err := e.Store.MarkGoJobWaitingWindow(ctx, job.ID); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (e *Engine) OnNodeCompleted(ctx context.Context, jobID string, nodeExecutionID string, outputArtifactID string) error {
	job, err := e.Store.GetJobDetail(ctx, jobID)
	if err != nil {
		return err
	}
	if !shouldProcessGoJob(job) || job.Status == string(contracts.JobStatusCancelled) || isTerminalJobStatus(job.Status) {
		return nil
	}
	node := findNodeByExecutionID(job.Nodes, nodeExecutionID)
	if node == nil {
		return nil
	}
	if node.Status == string(contracts.NodeStatusSucceeded) && node.OutputArtifactID == outputArtifactID {
		return nil
	}
	if isTerminalNodeStatus(node.Status) {
		return nil
	}

	if err := e.Store.MarkGoNodeSucceeded(ctx, jobID, nodeExecutionID, outputArtifactID); err != nil {
		return err
	}
	job, err = e.Store.GetJobDetail(ctx, jobID)
	if err != nil {
		return err
	}
	if !shouldProcessGoJob(job) || job.Status == string(contracts.JobStatusCancelled) || isTerminalJobStatus(job.Status) {
		return nil
	}

	depMap := dependenciesForJob(job)
	if err := e.dispatchReadyNodes(ctx, &job, depMap); err != nil {
		return err
	}
	_, err = e.maybeFinalizeJob(ctx, job)
	return err
}

func (e *Engine) OnNodeFailed(ctx context.Context, jobID string, nodeExecutionID string, errorMessage string) error {
	job, err := e.Store.GetJobDetail(ctx, jobID)
	if err != nil {
		return err
	}
	if !shouldProcessGoJob(job) || job.Status == string(contracts.JobStatusCancelled) || isTerminalJobStatus(job.Status) {
		return nil
	}
	node := findNodeByExecutionID(job.Nodes, nodeExecutionID)
	if node == nil || isTerminalNodeStatus(node.Status) {
		return nil
	}

	if node.RetryCount < 1 {
		if err := e.Store.IncrementGoNodeRetry(ctx, jobID, nodeExecutionID); err != nil {
			return err
		}
		observeGoRetry(node.NodeType)
		job, err = e.Store.GetJobDetail(ctx, jobID)
		if err != nil {
			return err
		}
		retryNode := findNodeByExecutionID(job.Nodes, nodeExecutionID)
		if retryNode == nil {
			return nil
		}
		if err := e.dispatchNode(ctx, &job, retryNode, dependenciesForJob(job)); err != nil {
			return err
		}
		return nil
	}

	if err := e.Store.MarkGoNodeFailed(ctx, jobID, nodeExecutionID, errorMessage); err != nil {
		return err
	}
	failedNodeID := node.NodeID
	depMap := dependenciesForJob(job)
	downstream := downstreamNodeIDs(failedNodeID, depMap)
	if err := e.Store.SkipGoDownstreamNodes(ctx, jobID, downstream); err != nil {
		return err
	}
	job, err = e.Store.GetJobDetail(ctx, jobID)
	if err != nil {
		return err
	}
	_, err = e.maybeFinalizeJob(ctx, job)
	return err
}

func (e *Engine) resolveSourceNodes(ctx context.Context, job *JobView) error {
	for i := range job.Nodes {
		node := &job.Nodes[i]
		if node.NodeType != "source" {
			continue
		}
		if node.Status == string(contracts.NodeStatusSucceeded) && node.OutputArtifactID != "" {
			continue
		}
		if node.Status != string(contracts.NodeStatusPending) {
			continue
		}
		assetID, ok := stringValue(node.NodeConfig["asset_id"])
		if !ok || assetID == "" {
			if err := e.failSourceNode(ctx, job, node, "source node missing asset_id"); err != nil {
				return err
			}
			continue
		}
		artifactID, err := e.Store.CreateSourceArtifact(ctx, job.ID, node.ID, assetID)
		if err != nil {
			if err := e.failSourceNode(ctx, job, node, err.Error()); err != nil {
				return err
			}
			continue
		}
		node.Status = string(contracts.NodeStatusSucceeded)
		node.OutputArtifactID = artifactID
	}
	return nil
}

func (e *Engine) failSourceNode(ctx context.Context, job *JobView, node *NodeExecutionView, errorMessage string) error {
	if err := e.Store.MarkGoNodeFailed(ctx, job.ID, node.ID, errorMessage); err != nil {
		return err
	}
	depMap := dependenciesForJob(*job)
	if err := e.Store.SkipGoDownstreamNodes(ctx, job.ID, downstreamNodeIDs(node.NodeID, depMap)); err != nil {
		return err
	}
	reloaded, err := e.Store.GetJobDetail(ctx, job.ID)
	if err != nil {
		return err
	}
	*job = reloaded
	finalized, err := e.maybeFinalizeJob(ctx, reloaded)
	if err != nil {
		return err
	}
	if finalized {
		finalizedJob, err := e.Store.GetJobDetail(ctx, job.ID)
		if err != nil {
			return err
		}
		*job = finalizedJob
	}
	return err
}

func (e *Engine) dispatchReadyNodes(ctx context.Context, job *JobView, depMap map[string][]string) error {
	if job.Status == string(contracts.JobStatusCancelled) || isTerminalJobStatus(job.Status) {
		return nil
	}
	nodesByID := nodesByNodeID(job.Nodes)
	for _, nodeID := range TopologicalOrder(job.PipelineSnapshot) {
		node := nodesByID[nodeID]
		if node == nil || node.Status != string(contracts.NodeStatusPending) || node.NodeType == "source" {
			continue
		}
		if !dependenciesSucceeded(nodesByID, depMap[nodeID]) {
			continue
		}
		if err := e.dispatchNode(ctx, job, node, depMap); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) dispatchNode(ctx context.Context, job *JobView, node *NodeExecutionView, depMap map[string][]string) error {
	nodesByID := nodesByNodeID(job.Nodes)
	if !dependenciesSucceeded(nodesByID, depMap[node.NodeID]) {
		return nil
	}
	inputArtifacts, inputArtifactIDs := inputArtifactsForNode(job.PipelineSnapshot, nodesByID, node.NodeID)
	inputArtifactsJSON, err := marshalJSONStringMap(inputArtifacts)
	if err != nil {
		return err
	}
	configJSON, err := marshalJSONAnyMap(node.NodeConfig)
	if err != nil {
		return err
	}
	preferredHostsJSON, err := json.Marshal(preferredHostsForNode(nodesByID, depMap[node.NodeID]))
	if err != nil {
		return err
	}

	claimed, err := e.Store.MarkGoNodeQueued(ctx, node.ID, inputArtifactIDs)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	node.Status = string(contracts.NodeStatusQueued)
	node.InputArtifactIDs = append([]string(nil), inputArtifactIDs...)

	payload := TaskPayload{
		JobID:              job.ID,
		NodeExecutionID:    node.ID,
		NodeID:             node.NodeID,
		NodeType:           node.NodeType,
		ConfigJSON:         configJSON,
		InputArtifactsJSON: string(inputArtifactsJSON),
		PreferredHostsJSON: string(preferredHostsJSON),
		AffinityEnqueuedAt: strconv.FormatInt(e.now().Unix(), 10),
		AffinityBounces:    "0",
		EventStream:        e.eventStream(),
		OrchestratorOwner:  goOrchestratorOwner,
	}
	if err := e.Dispatcher.Dispatch(ctx, goWorkerType, payload); err != nil {
		if releaseErr := e.Store.ReleaseGoNodeQueueClaim(ctx, node.ID); releaseErr != nil {
			return errors.Join(err, fmt.Errorf("release queue claim for node execution %s: %w", node.ID, releaseErr))
		}
		node.Status = string(contracts.NodeStatusPending)
		node.InputArtifactIDs = nil
		return err
	}
	observeGoDispatch(node.NodeType)
	return nil
}

func (e *Engine) maybeFinalizeJob(ctx context.Context, job JobView) (bool, error) {
	if isTerminalJobStatus(job.Status) {
		return true, nil
	}
	if hasActiveNode(job.Nodes) {
		return false, nil
	}

	successfulLeafIDs := successfulLeafNodeIDs(job)
	status := string(contracts.JobStatusSucceeded)
	var errorMessage *string
	if !allNodesSucceeded(job.Nodes) {
		if hasFailedLeaf(job) || len(successfulLeafIDs) == 0 {
			status = string(contracts.JobStatusFailed)
		} else {
			status = string(contracts.JobStatusPartiallyFailed)
		}
		msg := finalizationErrorMessage(job.Nodes)
		if msg != "" {
			errorMessage = &msg
		}
	}
	if err := e.Store.FinalizeGoJob(ctx, job.ID, status, errorMessage, successfulLeafIDs); err != nil {
		return false, err
	}
	observeGoFinalized(status)
	return true, nil
}

func (e *Engine) eventStream() string {
	if strings.TrimSpace(e.EventStream) != "" {
		return e.EventStream
	}
	return defaultGoEventStream
}

func (e *Engine) now() time.Time {
	if e.Clock != nil {
		return e.Clock()
	}
	return time.Now()
}

func shouldProcessGoJob(job JobView) bool {
	return job.OrchestratorOwner == goOrchestratorOwner
}

func dependenciesForJob(job JobView) map[string][]string {
	if deps, ok := job.ExecutionPlan["dependencies"].(map[string][]string); ok {
		return deps
	}
	return DependencyMap(job.PipelineSnapshot)
}

func dependenciesSucceeded(nodesByID map[string]*NodeExecutionView, deps []string) bool {
	for _, depID := range deps {
		upstream := nodesByID[depID]
		if upstream == nil || upstream.Status != string(contracts.NodeStatusSucceeded) {
			return false
		}
	}
	return true
}

func inputArtifactsForNode(def contracts.PipelineDefinition, nodesByID map[string]*NodeExecutionView, nodeID string) (map[string]string, []string) {
	inputs := map[string]string{}
	ids := make([]string, 0)
	for _, edge := range def.Edges {
		if edge.Target != nodeID {
			continue
		}
		upstream := nodesByID[edge.Source]
		if upstream == nil || upstream.OutputArtifactID == "" {
			continue
		}
		inputs[edge.TargetHandle] = upstream.OutputArtifactID
		ids = append(ids, upstream.OutputArtifactID)
	}
	return inputs, ids
}

func downstreamNodeIDs(nodeID string, depMap map[string][]string) []string {
	reverse := map[string][]string{}
	for target, deps := range depMap {
		for _, dep := range deps {
			reverse[dep] = append(reverse[dep], target)
		}
	}
	toSkip := make([]string, 0)
	seen := map[string]bool{}
	queue := append([]string(nil), reverse[nodeID]...)
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if seen[current] {
			continue
		}
		seen[current] = true
		toSkip = append(toSkip, current)
		queue = append(queue, reverse[current]...)
	}
	return toSkip
}

func successfulLeafNodeIDs(job JobView) []string {
	leaf := LeafNodeIDs(job.PipelineSnapshot)
	successByNodeID := map[string]bool{}
	for _, node := range job.Nodes {
		if node.Status == string(contracts.NodeStatusSucceeded) && node.OutputArtifactID != "" {
			successByNodeID[node.NodeID] = true
		}
	}
	ids := make([]string, 0)
	for _, node := range job.PipelineSnapshot.Nodes {
		if leaf[node.ID] && successByNodeID[node.ID] {
			ids = append(ids, node.ID)
		}
	}
	return ids
}

func hasFailedLeaf(job JobView) bool {
	leaf := LeafNodeIDs(job.PipelineSnapshot)
	for _, node := range job.Nodes {
		if leaf[node.NodeID] && isFailedNodeStatus(node.Status) {
			return true
		}
	}
	return false
}

func hasActiveNode(nodes []NodeExecutionView) bool {
	for _, node := range nodes {
		switch node.Status {
		case string(contracts.NodeStatusPending), string(contracts.NodeStatusQueued), string(contracts.NodeStatusRunning):
			return true
		}
	}
	return false
}

func allNodesSucceeded(nodes []NodeExecutionView) bool {
	if len(nodes) == 0 {
		return false
	}
	for _, node := range nodes {
		if node.Status != string(contracts.NodeStatusSucceeded) {
			return false
		}
	}
	return true
}

func finalizationErrorMessage(nodes []NodeExecutionView) string {
	names := make([]string, 0)
	for _, node := range nodes {
		if !isFailedNodeStatus(node.Status) {
			continue
		}
		name := node.NodeLabel
		if name == "" {
			name = node.NodeID
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return fmt.Sprintf("Failed nodes: %s", strings.Join(names, ", "))
}

func preferredHostsForNode(nodesByID map[string]*NodeExecutionView, deps []string) []string {
	counts := map[string]int{}
	for _, depID := range deps {
		upstream := nodesByID[depID]
		if upstream == nil {
			continue
		}
		host := extractWorkerHost(upstream.WorkerID)
		if host != "" {
			counts[host]++
		}
	}
	if len(counts) == 0 {
		return []string{}
	}
	maxCount := 0
	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
	}
	hosts := make([]string, 0)
	for host, count := range counts {
		if count == maxCount {
			hosts = append(hosts, host)
		}
	}
	sort.Strings(hosts)
	return hosts
}

func extractWorkerHost(workerID string) string {
	_, suffix, found := strings.Cut(workerID, "worker@")
	if !found {
		return ""
	}
	host, _, _ := strings.Cut(suffix, ":")
	return strings.TrimSpace(host)
}

func nodesByNodeID(nodes []NodeExecutionView) map[string]*NodeExecutionView {
	out := make(map[string]*NodeExecutionView, len(nodes))
	for i := range nodes {
		out[nodes[i].NodeID] = &nodes[i]
	}
	return out
}

func findNodeByExecutionID(nodes []NodeExecutionView, id string) *NodeExecutionView {
	for i := range nodes {
		if nodes[i].ID == id {
			return &nodes[i]
		}
	}
	return nil
}

func isTerminalJobStatus(status string) bool {
	switch status {
	case string(contracts.JobStatusSucceeded), string(contracts.JobStatusFailed), string(contracts.JobStatusCancelled), string(contracts.JobStatusPartiallyFailed):
		return true
	default:
		return false
	}
}

func isTerminalNodeStatus(status string) bool {
	switch status {
	case string(contracts.NodeStatusSucceeded), string(contracts.NodeStatusFailed), string(contracts.NodeStatusSkipped), string(contracts.NodeStatusCancelled):
		return true
	default:
		return false
	}
}

func isFailedNodeStatus(status string) bool {
	switch status {
	case string(contracts.NodeStatusFailed), string(contracts.NodeStatusSkipped), string(contracts.NodeStatusCancelled):
		return true
	default:
		return false
	}
}

func stringValue(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	default:
		return "", false
	}
}

func marshalJSONAnyMap(value map[string]any) (string, error) {
	if value == nil {
		value = map[string]any{}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func marshalJSONStringMap(value map[string]string) ([]byte, error) {
	if value == nil {
		value = map[string]string{}
	}
	return json.Marshal(value)
}
