package orchestrator

import (
	"context"
	"errors"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/redis/go-redis/v9"
)

type RedisDispatcher struct {
	Client *redis.Client
}

func (d RedisDispatcher) Dispatch(ctx context.Context, workerType string, payload TaskPayload) error {
	if d.Client == nil {
		return errors.New("redis dispatcher client is nil")
	}
	return d.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: redisstream.TaskStream(workerType),
		Values: payload.RedisValues(),
	}).Err()
}
