package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ctwqk/videoprocess/internal/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := worker.LoadConfig()
	slog.Info("starting vp-ffmpeg-worker-go",
		"worker_type", cfg.WorkerType,
		"worker_id", cfg.WorkerID,
	)

	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		slog.Error("invalid REDIS_URL", "error", err)
		os.Exit(1)
	}
	client := redis.NewClient(opts)
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Handler registration happens here so adding new node types is a
	// single-file change. Each handler must self-identify via NodeType().
	consumer := worker.NewConsumer(client, cfg /* handlers go here as they land */)

	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("vp-ffmpeg-worker-go stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("vp-ffmpeg-worker-go: shut down cleanly")
}
