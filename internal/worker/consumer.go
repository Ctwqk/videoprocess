package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var ErrConfirmedCancellation = errors.New("confirmed cancellation")

const registeredAffinityWait = 20 * time.Second
const registeredReadBlockLimit = 200 * time.Millisecond
const registeredReadDeadlineMargin = 100 * time.Millisecond

// Handler executes a single node's media transform. Each implementation is
// responsible for resolving input/output paths via the Storage backend and
// returning either nil on success or an error. Returning a wrapped
// ffmpeg.ErrCancelled tells the consumer to skip ack and event publication.
type Handler interface {
	NodeType() string
	Execute(ctx context.Context, task TaskMessage) (NodeResult, error)
}

type NodeResult struct {
	OutputArtifactID string
}

type RegisteredTaskStore interface {
	ClaimWorkerNode(
		context.Context,
		store.WorkerRegistrationLease,
		uuid.UUID,
		uuid.UUID,
		store.WorkerTaskDeliveryProof,
	) (store.WorkerNodeClaim, error)
	PrepareWorkerEvent(
		context.Context,
		store.WorkerNodeClaim,
		string,
		string,
		map[string]string,
	) (uuid.UUID, error)
	PublishPreparedWorkerEvent(
		context.Context,
		store.WorkerRegistrationLease,
		uuid.UUID,
		store.WorkerEventPublisher,
	) (store.WorkerNodeClaim, error)
	ListPreparedWorkerEventIDs(
		context.Context,
		store.WorkerRegistrationLease,
		int,
	) ([]uuid.UUID, error)
	AcknowledgeWorkerTask(
		context.Context,
		store.WorkerNodeClaim,
		store.WorkerTaskAcknowledger,
	) error
}

// Consumer drives the Redis Streams loop for one Go worker instance. The
// design mirrors the Python worker minus the heartbeat/PEL-reclaim helpers,
// which can be added in a follow-up alongside cancellation listeners.
type Consumer struct {
	Redis         *redis.Client
	WorkerType    string
	WorkerID      string
	ConsumerGroup string
	BlockTimeout  time.Duration
	Registration  *Registration
	cfg           Config
	taskStore     RegisteredTaskStore
	handlers      map[string]Handler
	log           *slog.Logger
	processingMu  sync.Mutex
	processing    map[string]struct{}
}

// NewConsumer wires a consumer with sensible defaults and the handler set
// supplied by the caller. The consumer group name is
// `{worker_type}-workers` to match the Python convention.
func NewConsumer(client *redis.Client, cfg Config, handlers ...Handler) *Consumer {
	registry := make(map[string]Handler, len(handlers))
	for _, h := range handlers {
		registry[h.NodeType()] = h
	}
	group := strings.TrimSpace(cfg.RedisGroup)
	if group == "" {
		group = cfg.WorkerType + "-workers"
	}
	return &Consumer{
		Redis:         client,
		WorkerType:    cfg.WorkerType,
		WorkerID:      cfg.WorkerID,
		ConsumerGroup: group,
		BlockTimeout:  5 * time.Second,
		cfg:           cfg,
		handlers:      registry,
		log:           slog.With("worker_id", cfg.WorkerID, "worker_type", cfg.WorkerType),
		processing:    make(map[string]struct{}),
	}
}

func NewRegisteredConsumer(
	client *redis.Client,
	cfg Config,
	taskStore RegisteredTaskStore,
	registration *Registration,
	handlers ...Handler,
) *Consumer {
	consumer := NewConsumer(client, cfg, handlers...)
	consumer.taskStore = taskStore
	consumer.Registration = registration
	return consumer
}

// EnsureGroup creates the consumer group if it does not yet exist. The
// `MKSTREAM` flag mirrors the Python implementation: a freshly booted system
// can start its workers before any task has ever been enqueued.
func (c *Consumer) EnsureGroup(ctx context.Context) error {
	stream := c.taskStream()
	if err := c.Redis.XGroupCreateMkStream(ctx, stream, c.ConsumerGroup, "$").Err(); err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("create consumer group %s: %w", c.ConsumerGroup, err)
		}
	}
	return nil
}

// Run blocks until ctx is done, claiming tasks from Redis Streams and
// dispatching them to the registered handlers. Each iteration claims at most
// one task to keep the loop deterministic for tests; production tuning of
// batch size is a follow-up.
func (c *Consumer) Run(ctx context.Context) error {
	if errors.Is(context.Cause(ctx), ErrRegistrationLost) {
		return ErrRegistrationLost
	}
	if err := c.EnsureGroup(ctx); err != nil {
		if errors.Is(context.Cause(ctx), ErrRegistrationLost) {
			return ErrRegistrationLost
		}
		return err
	}
	concurrency := c.cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 2
	}
	executions := newConsumerExecutions(c, ctx, concurrency)
	var reads *registeredReadFence
	if c.Registration != nil {
		reads = &registeredReadFence{}
		unregister, registered := c.Registration.registerLossGuard(reads.stop)
		if !registered {
			return ErrRegistrationLost
		}
		defer unregister()
	}
	if messages, err := c.claimPending(ctx); err != nil {
		c.log.Warn("initial pending reclaim failed", "error", err)
	} else {
		executions.dispatch(messages)
	}
	if messages, err := c.claimPreferredPending(ctx); err != nil {
		c.log.Warn("initial preferred pending reclaim failed", "error", err)
	} else {
		executions.dispatch(messages)
	}
	if err := c.ReconcilePreparedWorkerEvents(ctx); err != nil {
		if c.publishRegistrationLoss(err) {
			return c.waitForActive(ctx, &executions.wg)
		}
		c.log.Warn("initial prepared event reconciliation deferred", "error", err)
	}
	stream := c.taskStream()
	reclaimTicker := time.NewTicker(c.reclaimInterval())
	defer reclaimTicker.Stop()
	affinityTicker := time.NewTicker(time.Second)
	defer affinityTicker.Stop()
	eventTicker := time.NewTicker(5 * time.Second)
	defer eventTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return c.waitForActive(ctx, &executions.wg)
		case <-reclaimTicker.C:
			if messages, err := c.claimPending(ctx); err != nil {
				c.log.Warn("periodic pending reclaim failed", "error", err)
			} else {
				executions.dispatch(messages)
			}
			continue
		case <-affinityTicker.C:
			if messages, err := c.claimPreferredPending(ctx); err != nil {
				c.log.Warn("preferred pending reclaim failed", "error", err)
			} else {
				executions.dispatch(messages)
			}
			continue
		case <-eventTicker.C:
			if err := c.ReconcilePreparedWorkerEvents(ctx); err != nil {
				if c.publishRegistrationLoss(err) {
					return c.waitForActive(ctx, &executions.wg)
				}
				c.log.Warn("prepared event reconciliation deferred", "error", err)
			}
			continue
		case executions.sem <- struct{}{}:
		}
		readContext := ctx
		finishRead := func() {}
		blockTimeout := c.BlockTimeout
		if reads != nil {
			if blockTimeout <= 0 || blockTimeout > registeredReadBlockLimit {
				blockTimeout = registeredReadBlockLimit
			}
			var ready bool
			readContext, finishRead, ready = reads.begin(
				ctx,
				blockTimeout+registeredReadDeadlineMargin,
			)
			if !ready {
				<-executions.sem
				return c.waitForActive(ctx, &executions.wg)
			}
		}
		res, err := c.Redis.XReadGroup(readContext, &redis.XReadGroupArgs{
			Group:    c.ConsumerGroup,
			Consumer: c.WorkerID,
			Streams:  []string{stream, ">"},
			Block:    blockTimeout,
			Count:    1,
		}).Result()
		finishRead()
		if err != nil {
			<-executions.sem
			if reads != nil && reads.stopped() {
				return c.waitForActive(ctx, &executions.wg)
			}
			if errors.Is(err, context.Canceled) {
				return c.waitForActive(ctx, &executions.wg)
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			c.log.Warn("xreadgroup failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		if ctx.Err() != nil || reads != nil && reads.stopped() {
			<-executions.sem
			return c.waitForActive(ctx, &executions.wg)
		}
		dispatched := false
		for _, stream := range res {
			for _, msg := range stream.Messages {
				if ctx.Err() != nil || reads != nil && reads.stopped() {
					if !dispatched {
						<-executions.sem
					}
					return c.waitForActive(ctx, &executions.wg)
				}
				dispatched = true
				executions.startReserved(msg)
			}
		}
		if !dispatched {
			<-executions.sem
		}
	}
}

type registeredReadFence struct {
	mu          sync.Mutex
	stoppedFlag bool
	cancel      context.CancelFunc
	done        chan struct{}
}

func (f *registeredReadFence) begin(
	parent context.Context,
	timeout time.Duration,
) (context.Context, func(), bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stoppedFlag {
		return parent, func() {}, false
	}
	readContext, cancel := context.WithTimeout(parent, timeout)
	done := make(chan struct{})
	f.cancel = cancel
	f.done = done
	var once sync.Once
	return readContext, func() {
		once.Do(func() {
			cancel()
			f.mu.Lock()
			if f.done == done {
				f.cancel = nil
				f.done = nil
			}
			close(done)
			f.mu.Unlock()
		})
	}, true
}

func (f *registeredReadFence) stop() {
	f.mu.Lock()
	f.stoppedFlag = true
	cancel := f.cancel
	done := f.done
	f.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (f *registeredReadFence) stopped() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stoppedFlag
}

type consumerExecutions struct {
	consumer *Consumer
	ctx      context.Context
	sem      chan struct{}
	wg       sync.WaitGroup
}

func newConsumerExecutions(
	consumer *Consumer,
	ctx context.Context,
	concurrency int,
) *consumerExecutions {
	return &consumerExecutions{
		consumer: consumer,
		ctx:      ctx,
		sem:      make(chan struct{}, concurrency),
	}
}

func (executions *consumerExecutions) dispatch(messages []redis.XMessage) {
	for _, message := range messages {
		select {
		case executions.sem <- struct{}{}:
			executions.startReserved(message)
		case <-executions.ctx.Done():
			return
		}
	}
}

func (executions *consumerExecutions) startReserved(message redis.XMessage) {
	executions.wg.Add(1)
	go func() {
		defer executions.wg.Done()
		defer func() { <-executions.sem }()
		executions.consumer.handleMessage(executions.ctx, message)
	}()
}

func (c *Consumer) taskStream() string {
	if c.Registration != nil {
		if stream := strings.TrimSpace(c.cfg.RedisStream); stream != "" {
			return stream
		}
	}
	return redisstream.TaskStream(c.WorkerType)
}

func (c *Consumer) startTaskHeartbeat(
	ctx context.Context,
	messageID string,
) <-chan struct{} {
	if c.Registration == nil {
		return c.StartHeartbeat(ctx, messageID)
	}
	done := make(chan struct{})
	interval := c.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.Redis.XClaim(
					ctx,
					&redis.XClaimArgs{
						Stream:   c.taskStream(),
						Group:    c.ConsumerGroup,
						Consumer: c.WorkerID,
						MinIdle:  0,
						Messages: []string{messageID},
					},
				).Err(); err != nil && err != redis.Nil {
					c.log.Warn(
						"worker heartbeat failed",
						"msg_id",
						messageID,
						"error",
						err,
					)
					workerHeartbeatFailuresTotal.WithLabelValues(
						c.WorkerType,
					).Inc()
				}
			}
		}
	}()
	return done
}

func (c *Consumer) shouldDeferRegisteredForAffinity(
	task TaskMessage,
	now time.Time,
) bool {
	if len(task.PreferredHosts) == 0 {
		return false
	}
	host := strings.TrimSpace(c.cfg.WorkerHost)
	if host == "" {
		host = registeredWorkerHost(c.WorkerID)
	}
	for _, preferred := range task.PreferredHosts {
		if strings.EqualFold(strings.TrimSpace(preferred), host) {
			return false
		}
	}
	return registeredAffinityWindowActive(task, now)
}

func (c *Consumer) ReclaimPreferredPending(ctx context.Context) (int, error) {
	messages, err := c.claimPreferredPending(ctx)
	if err != nil {
		return 0, err
	}
	for _, message := range messages {
		c.handleMessage(ctx, message)
	}
	return len(messages), nil
}

func (c *Consumer) claimPreferredPending(
	ctx context.Context,
) ([]redis.XMessage, error) {
	if c.Registration == nil {
		return nil, nil
	}
	const minimumIdle = 500 * time.Millisecond
	claimed := make([]redis.XMessage, 0)
	now := time.Now().UTC()
	err := c.visitRegisteredPendingEntries(
		ctx,
		func(entry redis.XPendingExt) error {
			if entry.ID == "" || entry.Idle < minimumIdle {
				return nil
			}
			exact, err := c.Redis.XRangeN(
				ctx,
				c.taskStream(),
				entry.ID,
				entry.ID,
				1,
			).Result()
			if err != nil {
				return err
			}
			if len(exact) != 1 || exact[0].ID != entry.ID {
				return nil
			}
			task, err := decodeTask(exact[0].Values)
			if err != nil {
				return nil
			}
			if entry.Consumer == c.WorkerID {
				if len(task.PreferredHosts) == 0 ||
					c.registeredMessageIsProcessing(entry.ID) ||
					registeredAffinityWindowActive(task, now) {
					return nil
				}
				claimed = append(claimed, exact[0])
				return nil
			}
			if !c.taskPrefersCurrentHost(task) ||
				!registeredAffinityWindowActive(task, now) {
				return nil
			}
			messages, err := c.Redis.XClaim(
				ctx,
				&redis.XClaimArgs{
					Stream:   c.taskStream(),
					Group:    c.ConsumerGroup,
					Consumer: c.WorkerID,
					MinIdle:  minimumIdle,
					Messages: []string{entry.ID},
				},
			).Result()
			if err != nil && err != redis.Nil {
				return err
			}
			for _, message := range messages {
				if message.ID == entry.ID {
					claimed = append(claimed, message)
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

func (c *Consumer) visitRegisteredPendingEntries(
	ctx context.Context,
	visit func(redis.XPendingExt) error,
) error {
	const pageSize = int64(50)
	snapshot, err := c.Redis.XPending(
		ctx,
		c.taskStream(),
		c.ConsumerGroup,
	).Result()
	if err != nil {
		return err
	}
	if snapshot.Count == 0 || snapshot.Higher == "" {
		return nil
	}
	entries := make([]redis.XPendingExt, 0)
	start := "-"
	remaining := snapshot.Count
	for remaining > 0 {
		count := min(pageSize, remaining)
		page, err := c.Redis.XPendingExt(
			ctx,
			&redis.XPendingExtArgs{
				Stream: c.taskStream(),
				Group:  c.ConsumerGroup,
				Start:  start,
				End:    snapshot.Higher,
				Count:  count,
			},
		).Result()
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		entries = append(entries, page...)
		lastID := page[len(page)-1].ID
		remaining -= int64(len(page))
		if lastID == snapshot.Higher || len(page) < int(count) {
			break
		}
		nextStart := "(" + lastID
		if lastID == "" || nextStart == start {
			return errors.New("registered pending pagination is invalid")
		}
		start = nextStart
	}
	for _, entry := range entries {
		if err := visit(entry); err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) taskPrefersCurrentHost(task TaskMessage) bool {
	host := strings.TrimSpace(c.cfg.WorkerHost)
	if host == "" {
		host = registeredWorkerHost(c.WorkerID)
	}
	for _, preferred := range task.PreferredHosts {
		if strings.EqualFold(strings.TrimSpace(preferred), host) {
			return true
		}
	}
	return false
}

func registeredAffinityWindowActive(
	task TaskMessage,
	now time.Time,
) bool {
	enqueuedAt := parseAffinityTime(task.AffinityEnqueuedAt, now)
	age := now.Sub(enqueuedAt)
	return age < registeredAffinityWait
}

func registeredWorkerHost(workerID string) string {
	_, suffix, found := strings.Cut(workerID, "@")
	if !found {
		return workerID
	}
	host, _, found := strings.Cut(suffix, ":")
	if found {
		return host
	}
	return suffix
}

func (c *Consumer) beginRegisteredMessage(messageID string) bool {
	c.processingMu.Lock()
	defer c.processingMu.Unlock()
	if _, exists := c.processing[messageID]; exists {
		return false
	}
	c.processing[messageID] = struct{}{}
	return true
}

func (c *Consumer) endRegisteredMessage(messageID string) {
	c.processingMu.Lock()
	delete(c.processing, messageID)
	c.processingMu.Unlock()
}

func (c *Consumer) registeredMessageIsProcessing(messageID string) bool {
	c.processingMu.Lock()
	defer c.processingMu.Unlock()
	_, exists := c.processing[messageID]
	return exists
}

func (c *Consumer) reclaimInterval() time.Duration {
	if c.cfg.PELReclaimInterval > 0 {
		return c.cfg.PELReclaimInterval
	}
	return 60 * time.Second
}

func (c *Consumer) waitForActive(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timeout := c.cfg.ShutdownGracePeriod
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return consumerContextError(ctx)
	case <-timer.C:
		c.log.Warn("worker shutdown grace period expired")
		return consumerContextError(ctx)
	}
}

func consumerContextError(ctx context.Context) error {
	if errors.Is(context.Cause(ctx), ErrRegistrationLost) {
		return ErrRegistrationLost
	}
	return ctx.Err()
}

func (c *Consumer) handleMessage(ctx context.Context, msg redis.XMessage) {
	if c.Registration != nil {
		c.handleRegisteredMessage(ctx, msg)
		return
	}
	started := time.Now()
	task, err := decodeTask(msg.Values)
	if err != nil {
		c.log.Error("invalid task payload", "msg_id", msg.ID, "error", err)
		_ = c.publishFailed(ctx, task, fmt.Sprintf("invalid task payload: %v", err))
		workerTaskFailuresTotal.WithLabelValues(c.WorkerType, "unknown").Inc()
		observeTask(c.WorkerType, task, "failed", started)
		c.ack(ctx, msg.ID)
		return
	}

	if c.shouldDeferForAffinity(task, time.Now().UTC()) {
		if err := c.deferForAffinity(ctx, msg, task); err != nil {
			c.log.Warn("affinity defer failed; leaving message pending", "msg_id", msg.ID, "error", err)
		}
		return
	}

	handler, ok := c.handlers[task.NodeType]
	if !ok {
		c.log.Error("no handler", "msg_id", msg.ID, "node_type", task.NodeType)
		_ = c.publishFailed(ctx, task, fmt.Sprintf("no handler for node_type %q", task.NodeType))
		workerTaskFailuresTotal.WithLabelValues(c.WorkerType, task.NodeType).Inc()
		observeTask(c.WorkerType, task, "failed", started)
		c.ack(ctx, msg.ID)
		return
	}

	taskCtx, taskCancel := context.WithCancel(ctx)
	heartbeatDone := c.StartHeartbeat(taskCtx, msg.ID)
	result, err := handler.Execute(taskCtx, task)
	taskCancel()
	<-heartbeatDone
	switch {
	case err == nil:
		if strings.TrimSpace(result.OutputArtifactID) == "" {
			if pubErr := c.publishFailed(ctx, task, "handler succeeded without output_artifact_id"); pubErr != nil {
				c.log.Error("publish failed event failed; leaving message pending", "msg_id", msg.ID, "error", pubErr)
				return
			}
			workerTaskFailuresTotal.WithLabelValues(c.WorkerType, task.NodeType).Inc()
			observeTask(c.WorkerType, task, "failed", started)
			c.ack(ctx, msg.ID)
			return
		}
		if pubErr := c.publishCompleted(ctx, task, result.OutputArtifactID); pubErr != nil {
			c.log.Error("publish completed event failed; leaving message pending", "msg_id", msg.ID, "error", pubErr)
			return
		}
		observeTask(c.WorkerType, task, "succeeded", started)
		c.ack(ctx, msg.ID)
	case errors.Is(err, ErrConfirmedCancellation):
		c.log.Info("task cancelled by recorded job/node state, acking without event", "msg_id", msg.ID, "node_id", task.NodeID)
		workerTaskCancellationsTotal.WithLabelValues(c.WorkerType, task.NodeType).Inc()
		observeTask(c.WorkerType, task, "cancelled", started)
		c.ack(ctx, msg.ID)
	case errors.Is(err, context.Canceled):
		c.log.Info("worker context cancelled, leaving message pending", "msg_id", msg.ID, "node_id", task.NodeID)
	default:
		c.log.Error("handler failed", "msg_id", msg.ID, "node_id", task.NodeID, "error", err)
		if pubErr := c.publishFailed(ctx, task, err.Error()); pubErr != nil {
			c.log.Error("publish failed event failed; leaving message pending", "msg_id", msg.ID, "error", pubErr)
			return
		}
		workerTaskFailuresTotal.WithLabelValues(c.WorkerType, task.NodeType).Inc()
		observeTask(c.WorkerType, task, "failed", started)
		c.ack(ctx, msg.ID)
	}
}

func (c *Consumer) handleRegisteredMessage(
	ctx context.Context,
	msg redis.XMessage,
) {
	if !c.beginRegisteredMessage(msg.ID) {
		return
	}
	defer c.endRegisteredMessage(msg.ID)
	if c.taskStore == nil {
		c.log.Error("registered task store is unavailable", "msg_id", msg.ID)
		return
	}
	rawPayload, err := redisStringPayload(msg.Values)
	if err != nil {
		c.log.Error("invalid registered task payload", "msg_id", msg.ID)
		return
	}
	payloadHash, err := store.CanonicalRedisPayloadSHA256(rawPayload)
	if err != nil {
		c.log.Error("invalid registered task hash", "msg_id", msg.ID)
		return
	}
	task, err := decodeTask(msg.Values)
	if err != nil {
		c.log.Error("invalid registered task payload", "msg_id", msg.ID)
		return
	}
	dispatchKey, err := uuid.Parse(task.DispatchKey)
	if err != nil ||
		dispatchKey == uuid.Nil ||
		task.DispatchKey != dispatchKey.String() {
		c.log.Error("invalid registered task dispatch", "msg_id", msg.ID)
		return
	}
	if c.shouldDeferRegisteredForAffinity(task, time.Now().UTC()) {
		// The orchestrator-created delivery remains unchanged in the PEL.
		return
	}
	jobID, jobErr := uuid.Parse(task.JobID)
	nodeExecutionID, nodeErr := uuid.Parse(task.NodeExecutionID)
	if jobErr != nil ||
		nodeErr != nil ||
		jobID == uuid.Nil ||
		nodeExecutionID == uuid.Nil {
		c.log.Error("invalid registered task identity", "msg_id", msg.ID)
		return
	}
	proof := store.WorkerTaskDeliveryProof{
		RedisStream:   c.taskStream(),
		ConsumerGroup: c.ConsumerGroup,
		MessageID:     msg.ID,
		PayloadSHA256: payloadHash,
		DispatchKey:   dispatchKey,
	}

	taskContext, taskCancel := context.WithCancel(ctx)
	heartbeatDone := c.startTaskHeartbeat(taskContext, msg.ID)
	defer func() {
		taskCancel()
		<-heartbeatDone
	}()
	claim, err := c.taskStore.ClaimWorkerNode(
		taskContext,
		c.Registration.Lease(),
		jobID,
		nodeExecutionID,
		proof,
	)
	if err != nil {
		c.publishRegistrationLoss(err)
		c.log.Warn("registered task claim rejected", "msg_id", msg.ID)
		return
	}
	task.WorkerClaim = &claim
	handler, ok := c.handlers[task.NodeType]
	if !ok {
		if err := c.finishRegisteredTask(
			taskContext,
			claim,
			store.FailedWorkerEventValues(
				claim,
				proof,
				fmt.Sprintf("no handler for node_type %q", task.NodeType),
			),
			task.EventStream,
		); err != nil {
			c.publishRegistrationLoss(err)
			c.log.Warn("registered task failure deferred", "msg_id", msg.ID)
		}
		return
	}
	result, handlerErr := handler.Execute(taskContext, task)
	if errors.Is(context.Cause(taskContext), ErrRegistrationLost) {
		c.publishRegistrationLoss(ErrRegistrationLost)
		return
	}
	switch {
	case handlerErr == nil &&
		strings.TrimSpace(result.OutputArtifactID) != "":
		err = c.finishRegisteredTask(
			taskContext,
			claim,
			store.CompletedWorkerEventValues(
				claim,
				proof,
				result.OutputArtifactID,
			),
			task.EventStream,
		)
	case handlerErr == nil:
		err = c.finishRegisteredTask(
			taskContext,
			claim,
			store.FailedWorkerEventValues(
				claim,
				proof,
				"handler succeeded without output_artifact_id",
			),
			task.EventStream,
		)
	case errors.Is(handlerErr, ErrRegistrationLost):
		c.publishRegistrationLoss(handlerErr)
		return
	case errors.Is(handlerErr, ErrConfirmedCancellation),
		errors.Is(handlerErr, context.Canceled):
		return
	default:
		err = c.finishRegisteredTask(
			taskContext,
			claim,
			store.FailedWorkerEventValues(claim, proof, handlerErr.Error()),
			task.EventStream,
		)
	}
	if err != nil {
		c.publishRegistrationLoss(err)
		c.log.Warn("registered task finalization deferred", "msg_id", msg.ID)
	}
}

func (c *Consumer) finishRegisteredTask(
	ctx context.Context,
	claim store.WorkerNodeClaim,
	values map[string]string,
	eventStream string,
) error {
	if strings.TrimSpace(eventStream) == "" {
		eventStream = redisstream.EventStream
	}
	emissionID, err := c.taskStore.PrepareWorkerEvent(
		ctx,
		claim,
		eventStream,
		"orchestrator",
		values,
	)
	if err != nil {
		return err
	}
	publishedClaim, err := c.publishPreparedWorkerEventWithRetry(
		ctx,
		emissionID,
	)
	if err != nil {
		return err
	}
	return c.acknowledgeRegisteredTaskWithRetry(ctx, publishedClaim)
}

func (c *Consumer) publishPreparedWorkerEventWithRetry(
	ctx context.Context,
	emissionID uuid.UUID,
) (store.WorkerNodeClaim, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		claim, err := c.taskStore.PublishPreparedWorkerEvent(
			ctx,
			c.Registration.Lease(),
			emissionID,
			func(
				callbackContext context.Context,
				stream string,
				marker string,
				values map[string]string,
			) (string, error) {
				return redisstream.PublishIdempotentWorkerEvent(
					callbackContext,
					c.Redis,
					stream,
					marker,
					values,
				)
			},
		)
		if err == nil {
			return claim, nil
		}
		if c.publishRegistrationLoss(err) {
			return store.WorkerNodeClaim{}, ErrRegistrationLost
		}
		lastErr = err
		if !waitRegisteredRetry(ctx, attempt) {
			break
		}
	}
	return store.WorkerNodeClaim{}, lastErr
}

func (c *Consumer) acknowledgeRegisteredTaskWithRetry(
	ctx context.Context,
	claim store.WorkerNodeClaim,
) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		err := c.taskStore.AcknowledgeWorkerTask(
			ctx,
			claim,
			func(
				callbackContext context.Context,
				stream string,
				group string,
				messageID string,
			) (int64, error) {
				return redisstream.AcknowledgeWorkerTask(
					callbackContext,
					c.Redis,
					stream,
					group,
					messageID,
				)
			},
		)
		if err == nil {
			return nil
		}
		if c.publishRegistrationLoss(err) {
			return ErrRegistrationLost
		}
		lastErr = err
		if !waitRegisteredRetry(ctx, attempt) {
			break
		}
	}
	return lastErr
}

func (c *Consumer) ReconcilePreparedWorkerEvents(ctx context.Context) error {
	if c.Registration == nil || c.taskStore == nil {
		return nil
	}
	emissionIDs, err := c.taskStore.ListPreparedWorkerEventIDs(
		ctx,
		c.Registration.Lease(),
		100,
	)
	if err != nil {
		c.publishRegistrationLoss(err)
		return err
	}
	var lastErr error
	for _, emissionID := range emissionIDs {
		claim, err := c.publishPreparedWorkerEventWithRetry(ctx, emissionID)
		if err == nil {
			err = c.acknowledgeRegisteredTaskWithRetry(ctx, claim)
		}
		if err != nil {
			if c.publishRegistrationLoss(err) {
				return ErrRegistrationLost
			}
			lastErr = err
		}
	}
	return lastErr
}

func (c *Consumer) publishRegistrationLoss(err error) bool {
	if c.Registration == nil || !errors.Is(err, ErrRegistrationLost) {
		return false
	}
	c.Registration.MarkLost()
	return true
}

func waitRegisteredRetry(ctx context.Context, attempt int) bool {
	if attempt >= 2 {
		return false
	}
	timer := time.NewTimer(time.Duration(25*(1<<attempt)) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func redisStringPayload(values map[string]any) (map[string]string, error) {
	payload := make(map[string]string, len(values))
	for key, value := range values {
		text, ok := value.(string)
		if !ok || strings.TrimSpace(key) == "" {
			return nil, errors.New("registered task payload is invalid")
		}
		payload[key] = text
	}
	if len(payload) == 0 {
		return nil, errors.New("registered task payload is invalid")
	}
	return payload, nil
}

func (c *Consumer) ack(ctx context.Context, msgID string) {
	stream := c.taskStream()
	if err := c.Redis.XAck(ctx, stream, c.ConsumerGroup, msgID).Err(); err != nil {
		c.log.Warn("xack failed", "msg_id", msgID, "error", err)
	}
}

func (c *Consumer) publishCompleted(ctx context.Context, task TaskMessage, artifactID string) error {
	return redisstream.PublishNodeCompleted(ctx, c.Redis, redisstream.NodeEvent{
		EventStream:      task.EventStream,
		JobID:            task.JobID,
		NodeExecutionID:  task.NodeExecutionID,
		OutputArtifactID: artifactID,
	})
}

func (c *Consumer) publishFailed(ctx context.Context, task TaskMessage, errMsg string) error {
	return redisstream.PublishNodeFailed(ctx, c.Redis, redisstream.NodeEvent{
		EventStream:     task.EventStream,
		JobID:           task.JobID,
		NodeExecutionID: task.NodeExecutionID,
		Error:           errMsg,
	})
}

// decodeTask converts the Redis stream's string-valued map into a structured
// TaskMessage. JSON-valued fields (config, input_artifacts) are unmarshalled
// to preserve the Python orchestrator's payload semantics.
func decodeTask(values map[string]any) (TaskMessage, error) {
	get := func(key string) string {
		v, _ := values[key].(string)
		return v
	}
	task := TaskMessage{
		JobID:              get("job_id"),
		NodeExecutionID:    get("node_execution_id"),
		NodeID:             get("node_id"),
		NodeType:           get("node_type"),
		EventStream:        get("event_stream"),
		OrchestratorOwner:  get("orchestrator_owner"),
		DispatchKey:        get("dispatch_key"),
		AffinityEnqueuedAt: get("affinity_enqueued_at"),
		AffinityBounces:    get("affinity_bounces"),
	}
	if raw := get("config"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &task.Config); err != nil {
			return task, fmt.Errorf("decode config: %w", err)
		}
	}
	if raw := get("input_artifacts"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &task.InputArtifacts); err != nil {
			return task, fmt.Errorf("decode input_artifacts: %w", err)
		}
	}
	if raw := get("preferred_hosts"); raw != "" {
		_ = json.Unmarshal([]byte(raw), &task.PreferredHosts)
	}
	return task, nil
}

func encodeTask(task TaskMessage) (map[string]any, error) {
	if task.Config == nil {
		task.Config = map[string]any{}
	}
	if task.InputArtifacts == nil {
		task.InputArtifacts = map[string]any{}
	}
	if task.PreferredHosts == nil {
		task.PreferredHosts = []string{}
	}
	config, err := json.Marshal(task.Config)
	if err != nil {
		return nil, err
	}
	inputArtifacts, err := json.Marshal(task.InputArtifacts)
	if err != nil {
		return nil, err
	}
	preferredHosts, err := json.Marshal(task.PreferredHosts)
	if err != nil {
		return nil, err
	}
	values := map[string]any{
		"job_id":               task.JobID,
		"node_execution_id":    task.NodeExecutionID,
		"node_id":              task.NodeID,
		"node_type":            task.NodeType,
		"event_stream":         task.EventStream,
		"orchestrator_owner":   task.OrchestratorOwner,
		"config":               string(config),
		"input_artifacts":      string(inputArtifacts),
		"preferred_hosts":      string(preferredHosts),
		"affinity_enqueued_at": task.AffinityEnqueuedAt,
		"affinity_bounces":     task.AffinityBounces,
	}
	if task.DispatchKey != "" {
		values["dispatch_key"] = task.DispatchKey
	}
	return values, nil
}
