package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Ctwqk/videoprocess/internal/channelops"
)

func main() {
	cfg := channelops.LoadConfig()
	if err := cfg.Validate(); err != nil {
		slog.Error("invalid ChannelOps runner config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runner, err := channelops.NewRunner(ctx, cfg)
	if err != nil {
		slog.Error("create ChannelOps runner", "error", err)
		os.Exit(1)
	}
	defer runner.Close()

	slog.Info("starting channelops-runner-go")
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("channelops-runner-go stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("channelops-runner-go stopped cleanly", "at", time.Now().UTC())
}
