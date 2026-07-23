package channelops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrHandlerSnapshotStale = errors.New("handler preparation snapshot stale")

type preparedTaskSnapshot struct {
	Task   ProductionTaskRow
	Digest string
}

type preparedPublicationSnapshot struct {
	Publication PublicationRow
	Task        ProductionTaskRow
	Digest      string
}

type preparedAccountSnapshot struct {
	Account PublishingAccountRow
	Digest  string
}

type preparedMetricsSnapshot struct {
	Publication PublicationRow
	Task        ProductionTaskRow
	Schedule    *MetricScheduleRow
	Digest      string
}

func (h HandlerService) requireExternalPhase(kind string) error {
	if h.Store == nil {
		return errors.New("channelops handler store is not configured")
	}
	if h.Store.hasExecutionTransaction() {
		return fmt.Errorf("%s cannot call an external client while a database fence is held", kind)
	}
	return nil
}

func (h HandlerService) withQueueExecutionPhase(
	ctx context.Context,
	item QueueItemRow,
	dispatch func(HandlerService) error,
) error {
	if h.Store.Pool == nil {
		if h.Store.hasExecutionTransaction() {
			return errors.New("queue execution phase cannot reuse an existing transaction")
		}
		return dispatch(h)
	}
	return h.Store.WithQueueExecutionFence(ctx, item, func(fencedStore *Store) error {
		fencedHandler := h
		fencedHandler.Store = fencedStore
		return dispatch(fencedHandler)
	})
}

func newPreparedTaskSnapshot(task ProductionTaskRow) (preparedTaskSnapshot, error) {
	digest, err := handlerSnapshotDigest(task)
	if err != nil {
		return preparedTaskSnapshot{}, err
	}
	return preparedTaskSnapshot{Task: task, Digest: digest}, nil
}

func (p preparedTaskSnapshot) validate(task ProductionTaskRow) error {
	return validateHandlerSnapshot(p.Digest, task, "production task")
}

func (h HandlerService) revalidatePreparedTask(
	ctx context.Context,
	item QueueItemRow,
	prepared preparedTaskSnapshot,
) error {
	return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		task, err := fenced.Store.GetProductionTask(ctx, prepared.Task.ID)
		if err != nil {
			return err
		}
		return prepared.validate(task)
	})
}

func newPreparedPublicationSnapshot(
	publication PublicationRow,
	task ProductionTaskRow,
) (preparedPublicationSnapshot, error) {
	digest, err := handlerSnapshotDigest(struct {
		Publication PublicationRow
		Task        ProductionTaskRow
	}{Publication: publication, Task: task})
	if err != nil {
		return preparedPublicationSnapshot{}, err
	}
	return preparedPublicationSnapshot{
		Publication: publication,
		Task:        task,
		Digest:      digest,
	}, nil
}

func (p preparedPublicationSnapshot) validate(
	publication PublicationRow,
	task ProductionTaskRow,
) error {
	return validateHandlerSnapshot(p.Digest, struct {
		Publication PublicationRow
		Task        ProductionTaskRow
	}{Publication: publication, Task: task}, "publication and task")
}

func newPreparedAccountSnapshot(account PublishingAccountRow) (preparedAccountSnapshot, error) {
	digest, err := handlerSnapshotDigest(account)
	if err != nil {
		return preparedAccountSnapshot{}, err
	}
	return preparedAccountSnapshot{Account: account, Digest: digest}, nil
}

func (p preparedAccountSnapshot) validate(account PublishingAccountRow) error {
	return validateHandlerSnapshot(p.Digest, account, "publishing account")
}

func newPreparedMetricsSnapshot(
	publication PublicationRow,
	task ProductionTaskRow,
	schedule *MetricScheduleRow,
) (preparedMetricsSnapshot, error) {
	digest, err := handlerSnapshotDigest(struct {
		Publication PublicationRow
		Task        ProductionTaskRow
		Schedule    *MetricScheduleRow
	}{Publication: publication, Task: task, Schedule: schedule})
	if err != nil {
		return preparedMetricsSnapshot{}, err
	}
	return preparedMetricsSnapshot{
		Publication: publication,
		Task:        task,
		Schedule:    schedule,
		Digest:      digest,
	}, nil
}

func (p preparedMetricsSnapshot) validate(
	publication PublicationRow,
	task ProductionTaskRow,
	schedule *MetricScheduleRow,
) error {
	return validateHandlerSnapshot(p.Digest, struct {
		Publication PublicationRow
		Task        ProductionTaskRow
		Schedule    *MetricScheduleRow
	}{Publication: publication, Task: task, Schedule: schedule}, "metrics inputs")
}

func handlerSnapshotDigest(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode handler preparation snapshot: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func validateHandlerSnapshot(expected string, value any, label string) error {
	actual, err := handlerSnapshotDigest(value)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%w: %s changed", ErrHandlerSnapshotStale, label)
	}
	return nil
}
