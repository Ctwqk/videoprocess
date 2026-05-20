package worker

import (
	"context"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/redis/go-redis/v9"
)

func (c *Consumer) ReclaimPending(ctx context.Context) (int, error) {
	minIdle := c.cfg.PELMinIdle
	if minIdle <= 0 {
		minIdle = 15 * time.Minute
	}
	stream := redisstream.TaskStream(c.WorkerType)
	messages, _, err := c.Redis.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    c.ConsumerGroup,
		Consumer: c.WorkerID,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    100,
	}).Result()
	if err != nil {
		return 0, err
	}
	if len(messages) > 0 {
		workerPendingReclaimsTotal.WithLabelValues(c.WorkerType).Add(float64(len(messages)))
	}
	for _, msg := range messages {
		c.handleMessage(ctx, msg)
	}
	return len(messages), nil
}
