package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Ctwqk/videoprocess/internal/config"
	"github.com/Ctwqk/videoprocess/internal/storage"
	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/Ctwqk/videoprocess/internal/worker"
	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
	handlerspkg "github.com/Ctwqk/videoprocess/internal/worker/handlers"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cfg := worker.LoadConfig()
	stopMetrics := startMetricsServer(ctx, cfg.MetricsAddr)
	defer stopMetrics()
	if err := runWorker(
		ctx,
		workerEnvironment(),
		startupDependencies{},
	); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("vp-ffmpeg-worker-go stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("vp-ffmpeg-worker-go: shut down cleanly")
}

type startupDatabase interface {
	worker.RegistrationStore
	RequireWorkerRedisContinuity(context.Context, int) error
	Close()
}

type startupDependencies struct {
	loadSecrets          func(map[string]string) (worker.SecretConfig, error)
	openDatabase         func(context.Context, string) (startupDatabase, error)
	openStorage          func(context.Context, worker.Config) (storage.Backend, error)
	newRedis             func(*redis.Options) *redis.Client
	requireRedisIdentity func(
		context.Context,
		*redis.Client,
		*redis.Options,
	) error
	runConsumer func(
		context.Context,
		*redis.Client,
		worker.Config,
		startupDatabase,
		storage.Backend,
		*worker.Registration,
	) error
}

func (dependencies startupDependencies) defaults() startupDependencies {
	if dependencies.loadSecrets == nil {
		dependencies.loadSecrets = worker.LoadWorkerSecrets
	}
	if dependencies.openDatabase == nil {
		dependencies.openDatabase = func(
			ctx context.Context,
			databaseURL string,
		) (startupDatabase, error) {
			return store.Open(ctx, databaseURL)
		}
	}
	if dependencies.openStorage == nil {
		dependencies.openStorage = openWorkerStorage
	}
	if dependencies.newRedis == nil {
		dependencies.newRedis = redis.NewClient
	}
	if dependencies.requireRedisIdentity == nil {
		dependencies.requireRedisIdentity = requireWorkerRedisIdentity
	}
	if dependencies.runConsumer == nil {
		dependencies.runConsumer = runFFmpegConsumer
	}
	return dependencies
}

func runWorker(
	ctx context.Context,
	env map[string]string,
	dependencies startupDependencies,
) error {
	dependencies = dependencies.defaults()
	secrets, err := dependencies.loadSecrets(env)
	if err != nil {
		return err
	}
	cfg := worker.LoadConfigFromEnv(env)

	openContext, openCancel := context.WithTimeout(ctx, 10*time.Second)
	database, err := dependencies.openDatabase(
		openContext,
		secrets.DatabaseURL,
	)
	openCancel()
	if err != nil {
		return errors.New("worker database unavailable")
	}
	defer database.Close()

	instanceID := uuid.New()
	claims, err := worker.BuildRegistrationClaims(
		env,
		secrets.DatabaseURL,
		instanceID,
	)
	if err != nil {
		return err
	}
	registration := worker.NewRegistration(
		database,
		claims,
		secrets.AdmissionToken,
	)
	lease, err := registration.Start(ctx)
	if err != nil {
		return err
	}
	defer registration.Close(context.Background(), "shutdown")
	if err := database.RequireWorkerRedisContinuity(
		registration.Context(),
		worker.WorkerRedisContinuityMaxAge,
	); err != nil {
		_ = registration.Close(
			context.Background(),
			"worker_redis_continuity_unready",
		)
		return errors.New("worker_redis_continuity_unready")
	}
	if err := registrationContextError(registration.Context()); err != nil {
		return err
	}

	storageContext, storageCancel := context.WithTimeout(
		registration.Context(),
		30*time.Second,
	)
	storageBackend, err := dependencies.openStorage(storageContext, cfg)
	storageCancel()
	if err != nil {
		return errors.New("worker storage unavailable")
	}
	if err := registrationContextError(registration.Context()); err != nil {
		return err
	}

	options, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return errors.New("worker Redis configuration is invalid")
	}
	client := dependencies.newRedis(options)
	if client == nil {
		return errors.New("worker Redis client is unavailable")
	}
	defer client.Close()
	if err := registrationContextError(registration.Context()); err != nil {
		return err
	}
	identityContext, identityCancel := context.WithTimeout(
		registration.Context(),
		5*time.Second,
	)
	err = dependencies.requireRedisIdentity(
		identityContext,
		client,
		options,
	)
	identityCancel()
	if err != nil {
		return err
	}
	if err := registrationContextError(registration.Context()); err != nil {
		return err
	}

	cfg.WorkerType = claims.WorkerType
	cfg.WorkerHost = claims.WorkerHost
	cfg.WorkerID = lease.RedisConsumerID
	cfg.RedisStream = claims.RedisStream
	cfg.RedisGroup = claims.RedisGroup
	slog.Info(
		"starting vp-ffmpeg-worker-go",
		"worker_type",
		cfg.WorkerType,
		"worker_id",
		cfg.WorkerID,
	)
	return dependencies.runConsumer(
		registration.Context(),
		client,
		cfg,
		database,
		storageBackend,
		registration,
	)
}

func registrationContextError(ctx context.Context) error {
	if errors.Is(context.Cause(ctx), worker.ErrRegistrationLost) {
		return worker.ErrRegistrationLost
	}
	return ctx.Err()
}

func requireWorkerRedisIdentity(
	ctx context.Context,
	client *redis.Client,
	options *redis.Options,
) error {
	expected := strings.TrimSpace(options.Username)
	if expected == "" || expected == "default" {
		return errors.New("worker_redis_identity_unready")
	}
	observed, err := client.ACLWhoAmI(ctx).Result()
	if err != nil || observed != expected {
		return errors.New("worker_redis_identity_unready")
	}
	return nil
}

func openWorkerStorage(
	ctx context.Context,
	cfg worker.Config,
) (storage.Backend, error) {
	return storage.FromConfig(ctx, config.Config{
		StorageBackend:   cfg.StorageBackend,
		StorageLocalRoot: cfg.StorageLocalRoot,
		MinIOEndpoint:    cfg.MinIOEndpoint,
		MinIOAccessKey:   cfg.MinIOAccessKey,
		MinIOSecretKey:   cfg.MinIOSecretKey,
		MinIOBucket:      cfg.MinIOBucket,
		MinIOSecure:      cfg.MinIOSecure,
	})
}

func runFFmpegConsumer(
	ctx context.Context,
	client *redis.Client,
	cfg worker.Config,
	database startupDatabase,
	storageBackend storage.Backend,
	registration *worker.Registration,
) error {
	workerStore, ok := database.(*store.Store)
	if !ok {
		return fmt.Errorf("worker database implementation is unsupported")
	}
	runtimeEnv := worker.RuntimeEnv{
		Store:              workerStore,
		Storage:            storageBackend,
		StorageBackend:     cfg.StorageBackend,
		LocalRoot:          cfg.StorageLocalRoot,
		WorkerID:           registration.Lease().RedisConsumerID,
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
	consumer := worker.NewRegisteredConsumer(
		client,
		cfg,
		workerStore,
		registration,
		mediaHandlers...,
	)
	return consumer.Run(ctx)
}

func workerEnvironment() map[string]string {
	env := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			env[key] = value
		}
	}
	return env
}

func startMetricsServer(ctx context.Context, addr string) func() {
	if addr == "" {
		return func() {}
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		slog.Info("starting vp-ffmpeg-worker-go metrics", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("vp-ffmpeg-worker-go metrics stopped", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	return func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}
}
