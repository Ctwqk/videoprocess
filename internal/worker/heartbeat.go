package worker

import (
	"context"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/redis/go-redis/v9"
)

func (c *Consumer) StartHeartbeat(ctx context.Context, msgID string) <-chan struct{} {
	done := make(chan struct{})
	interval := c.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		stream := redisstream.TaskStream(c.WorkerType)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.Redis.XClaim(ctx, &redis.XClaimArgs{
					Stream:   stream,
					Group:    c.ConsumerGroup,
					Consumer: c.WorkerID,
					MinIdle:  0,
					Messages: []string{msgID},
				}).Err(); err != nil && err != redis.Nil {
					c.log.Warn("worker heartbeat failed", "msg_id", msgID, "error", err)
				}
			}
		}
	}()
	return done
}
