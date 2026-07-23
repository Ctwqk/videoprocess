package channelops

import (
	"context"
	"errors"
	"time"
)

const queueClaimCleanupTimeout = 5 * time.Second

func completeCommittedQueueClaim(
	ctx context.Context,
	store *Store,
	item QueueItemRow,
) error {
	completionCtx, cancel := boundedQueueClaimContext(ctx)
	defer cancel()
	err := store.MarkQueueDone(completionCtx, item)
	if errors.Is(err, ErrQueueLeaseLost) {
		return nil
	}
	return err
}

func completeUncommittedQueueClaim(
	ctx context.Context,
	store *Store,
	item QueueItemRow,
	handlerErr error,
	reject bool,
) error {
	if ctx.Err() != nil || leaderFenceRejected(handlerErr) {
		return releaseExactQueueClaim(ctx, store, item)
	}

	completionCtx, cancel := context.WithTimeout(ctx, queueClaimCleanupTimeout)
	defer cancel()
	err := store.WithLeaderExecutionFence(completionCtx, func(fenced *Store) error {
		if reject {
			return fenced.MarkQueueRejected(completionCtx, item, handlerErr.Error())
		}
		return fenced.MarkQueueFailedOrRetry(completionCtx, item, handlerErr.Error())
	})
	if err == nil || errors.Is(err, ErrQueueLeaseLost) {
		return nil
	}
	if leaderFenceRejected(err) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return releaseExactQueueClaim(ctx, store, item)
	}
	return err
}

func releaseExactQueueClaim(
	ctx context.Context,
	store *Store,
	item QueueItemRow,
) error {
	releaseCtx, cancel := boundedQueueClaimContext(ctx)
	defer cancel()
	err := store.ReleaseQueueClaim(releaseCtx, item)
	if errors.Is(err, ErrQueueLeaseLost) {
		return nil
	}
	return err
}

func boundedQueueClaimContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), queueClaimCleanupTimeout)
}
