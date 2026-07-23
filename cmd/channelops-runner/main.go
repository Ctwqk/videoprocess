package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Ctwqk/videoprocess/internal/channelops"
)

type managedRunner interface {
	Run(context.Context) error
	Close()
	Handler() http.Handler
}

type channelOpsManagedRunner struct {
	runner *channelops.Runner
}

func (r channelOpsManagedRunner) Run(ctx context.Context) error {
	return r.runner.Run(ctx)
}

func (r channelOpsManagedRunner) Close() {
	r.runner.Close()
}

func (r channelOpsManagedRunner) Handler() http.Handler {
	return channelops.NewRunnerHTTPHandler(r.runner)
}

type runDependencies struct {
	loadConfig    func() channelops.Config
	signalContext func() (context.Context, context.CancelFunc)
	newRunner     func(context.Context, channelops.Config) (managedRunner, error)
	runHTTPServer func(context.Context, string, http.Handler) error
}

type componentResult struct {
	name string
	err  error
}

func main() {
	os.Exit(run())
}

func run() int {
	return runWithDependencies(productionRunDependencies())
}

func productionRunDependencies() runDependencies {
	return runDependencies{
		loadConfig: channelops.LoadConfig,
		signalContext: func() (context.Context, context.CancelFunc) {
			return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		},
		newRunner: func(ctx context.Context, cfg channelops.Config) (managedRunner, error) {
			runner, err := channelops.NewRunner(ctx, cfg)
			if err != nil {
				return nil, err
			}
			return channelOpsManagedRunner{runner: runner}, nil
		},
		runHTTPServer: channelops.RunHTTPServer,
	}
}

func runWithDependencies(deps runDependencies) int {
	cfg := deps.loadConfig()
	if err := cfg.Validate(); err != nil {
		slog.Error("invalid ChannelOps runner config", "error", err)
		return 1
	}

	ctx, cancel := deps.signalContext()
	defer cancel()

	runner, err := deps.newRunner(ctx, cfg)
	if err != nil {
		slog.Error("create ChannelOps runner", "error", err)
		return 1
	}
	defer runner.Close()

	resultCh := make(chan componentResult, 2)
	go func() {
		resultCh <- componentResult{
			name: "http server",
			err:  deps.runHTTPServer(ctx, fmt.Sprintf(":%d", cfg.HealthPort), runner.Handler()),
		}
	}()
	go func() {
		resultCh <- componentResult{name: "runner", err: runner.Run(ctx)}
	}()

	slog.Info("starting channelops-runner-go", "health_port", cfg.HealthPort, "runner_id", cfg.RunnerID)
	first := <-resultCh
	cancel()
	second := <-resultCh
	for _, result := range []componentResult{first, second} {
		if result.err != nil && !errors.Is(result.err, context.Canceled) {
			slog.Error("channelops-runner-go stopped", "component", result.name, "error", result.err)
			return 1
		}
	}
	slog.Info("channelops-runner-go stopped cleanly", "at", time.Now().UTC())
	return 0
}
