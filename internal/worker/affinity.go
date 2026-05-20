package worker

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/redis/go-redis/v9"
)

func (c *Consumer) shouldDeferForAffinity(task TaskMessage, now time.Time) bool {
	if len(task.PreferredHosts) == 0 {
		return false
	}
	host := workerHostFromID(c.WorkerID)
	for _, preferred := range task.PreferredHosts {
		if strings.EqualFold(strings.TrimSpace(preferred), host) {
			return false
		}
	}
	bounces := parseIntDefault(task.AffinityBounces, 0)
	maxBounces := c.cfg.AffinityMaxBounces
	if maxBounces <= 0 {
		maxBounces = 6
	}
	if bounces >= maxBounces {
		return false
	}
	enqueuedAt := parseAffinityTime(task.AffinityEnqueuedAt, now)
	wait := c.cfg.AffinityWait
	if wait <= 0 {
		wait = 20 * time.Second
	}
	return now.Sub(enqueuedAt) < wait
}

func (c *Consumer) deferForAffinity(ctx context.Context, msg redis.XMessage, task TaskMessage) error {
	task.AffinityBounces = strconv.Itoa(parseIntDefault(task.AffinityBounces, 0) + 1)
	if strings.TrimSpace(task.AffinityEnqueuedAt) == "" {
		task.AffinityEnqueuedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	values, err := encodeTask(task)
	if err != nil {
		return err
	}
	stream := redisstream.TaskStream(c.WorkerType)
	if err := c.Redis.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: values}).Err(); err != nil {
		return err
	}
	c.ack(ctx, msg.ID)
	return nil
}

func workerHostFromID(workerID string) string {
	parts := strings.Split(workerID, "@")
	if len(parts) != 2 {
		return workerID
	}
	hostPID := parts[1]
	if idx := strings.LastIndex(hostPID, ":"); idx >= 0 {
		return hostPID[:idx]
	}
	return hostPID
}

func parseAffinityTime(raw string, fallback time.Time) time.Time {
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw)); err == nil {
		return parsed
	}
	if seconds, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil && seconds > 0 {
		return time.Unix(seconds, 0).UTC()
	}
	return fallback
}

func parseIntDefault(raw string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return parsed
}
