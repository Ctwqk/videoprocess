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
	"github.com/Ctwqk/videoprocess/internal/store"
)

func main() {
	cfg := config.Load()

	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	openCtx, openCancel := context.WithTimeout(rootCtx, 10*time.Second)
	st, err := store.Open(openCtx, cfg.DatabaseURL)
	openCancel()

	var server *httpapi.Server
	if err != nil {
		// A missing DB during dev shouldn't crash the smoke binary, but log
		// loudly so it isn't silently mistaken for a real listener.
		slog.Error("vp-api-go: database unavailable, serving stub list endpoints", "error", err)
		server = httpapi.NewServer()
	} else {
		defer st.Close()
		server = httpapi.NewServerWithStore(st)
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
