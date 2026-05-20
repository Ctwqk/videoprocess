package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Ctwqk/videoprocess/internal/config"
	"github.com/Ctwqk/videoprocess/internal/storage"
	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/Ctwqk/videoprocess/internal/worker"
	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
	handlerspkg "github.com/Ctwqk/videoprocess/internal/worker/handlers"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := worker.LoadConfig()
	appCfg := config.Load()
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

	openCtx, openCancel := context.WithTimeout(ctx, 10*time.Second)
	st, err := store.Open(openCtx, appCfg.DatabaseURL)
	openCancel()
	if err != nil {
		slog.Error("open worker database", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	storageBackend, err := storage.FromConfig(ctx, appCfg)
	if err != nil {
		slog.Error("open worker storage", "error", err)
		os.Exit(1)
	}

	runtimeEnv := worker.RuntimeEnv{
		Store:              st,
		Storage:            storageBackend,
		StorageBackend:     appCfg.StorageBackend,
		LocalRoot:          appCfg.StorageLocalRoot,
		WorkerID:           cfg.WorkerID,
		Logger:             slog.Default(),
		CancelPollInterval: cfg.CancelPollInterval,
	}
	runner := vpffmpeg.NewRunner()
	mediaHandlers := []worker.Handler{
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.TrimHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.TranscodeHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ExportHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.VerticalCropHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.WatermarkHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.TitleOverlayHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.BgmHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ReplaceAudioHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ConcatHorizontalHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ConcatVerticalHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ConcatManyHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ConcatTimelineHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ConcatVerticalTimelineHandler{Runner: runner}),
		worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.MontageAssemblerHandler{Runner: runner}),
	}

	// Handler registration happens here so adding new node types is a
	// single-file change. Each handler must self-identify via NodeType().
	consumer := worker.NewConsumer(client, cfg, mediaHandlers...)

	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("vp-ffmpeg-worker-go stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("vp-ffmpeg-worker-go: shut down cleanly")
}
