package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/Ctwqk/videoprocess/internal/channelops"
)

func main() {
	channelID := flag.String("channel-id", "", "ChannelProfile id to smoke")
	flag.Parse()
	if *channelID == "" {
		fmt.Fprintln(os.Stderr, "-channel-id is required")
		os.Exit(2)
	}

	cfg := channelops.LoadConfig()
	if err := cfg.Validate(); err != nil {
		slog.Error("invalid smoke config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()
	runner, err := channelops.NewRunner(ctx, cfg)
	if err != nil {
		slog.Error("create runner", "error", err)
		os.Exit(1)
	}
	defer runner.Close()

	result, err := channelops.LiveSmoke{Store: runner.Store, Handler: runner.Handlers}.Run(ctx, *channelID)
	if err != nil {
		slog.Error("smoke failed", "error", err)
		os.Exit(1)
	}
	if err := result.Validate(); err != nil {
		slog.Error("smoke validation failed", "error", err, "result", result)
		os.Exit(1)
	}
	fmt.Printf("channelops live smoke passed: %+v\n", result)
}
