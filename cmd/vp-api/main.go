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

	"github.com/Ctwqk/videoprocess/internal/config"
	"github.com/Ctwqk/videoprocess/internal/httpapi"
	"github.com/Ctwqk/videoprocess/internal/storage"
	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()

	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	redisProbe := redisReadinessProbe(cfg.RedisURL)
	storageBackend, storageErr := storage.FromConfig(rootCtx, cfg)
	storageProbe := storageReadinessProbe(storageBackend, storageErr)

	openCtx, openCancel := context.WithTimeout(rootCtx, 10*time.Second)
	st, err := store.Open(openCtx, cfg.DatabaseURL)
	openCancel()

	var server *httpapi.Server
	var pgProbe httpapi.ReadinessProbe
	if err != nil {
		// A missing DB during dev shouldn't crash the smoke binary, but log
		// loudly so it isn't silently mistaken for a real listener.
		slog.Error("vp-api-go: database unavailable", "error", err)
		dbErr := err
		pgProbe = func(context.Context) error { return dbErr }
		server = httpapi.NewServerWithOptions(nil, httpapi.ServerOptions{
			AllowStubStore: cfg.APIGoAllowStubStore,
			Storage:        storageBackend,
			StorageBackend: cfg.StorageBackend,
			Readiness: httpapi.ReadinessDeps{
				Postgres: pgProbe,
				Redis:    redisProbe,
				Storage:  storageProbe,
			},
		})
	} else {
		defer st.Close()
		pgProbe = st.Ping
		server = httpapi.NewServerWithOptions(st, httpapi.ServerOptions{
			AllowStubStore: cfg.APIGoAllowStubStore,
			Storage:        storageBackend,
			StorageBackend: cfg.StorageBackend,
			Readiness: httpapi.ReadinessDeps{
				Postgres: pgProbe,
				Redis:    redisProbe,
				Storage:  storageProbe,
			},
		})
	}

	addr := fmt.Sprintf("%s:%d", cfg.APIHost, cfg.APIPort)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server.Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-rootCtx.Done()
		slog.Info("vp-api-go: shutting down")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	slog.Info("starting vp-api-go", "addr", addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("vp-api-go stopped", "error", err)
		os.Exit(1)
	}
}

func redisReadinessProbe(redisURL string) httpapi.ReadinessProbe {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return func(context.Context) error { return err }
	}
	client := redis.NewClient(opts)
	return func(ctx context.Context) error {
		return client.Ping(ctx).Err()
	}
}

func storageReadinessProbe(backend storage.Backend, openErr error) httpapi.ReadinessProbe {
	if openErr != nil {
		return func(context.Context) error { return openErr }
	}
	return func(ctx context.Context) error {
		_, err := backend.Exists(ctx, ".")
		return err
	}
}
