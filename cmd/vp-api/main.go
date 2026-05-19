package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/Ctwqk/videoprocess/internal/config"
	"github.com/Ctwqk/videoprocess/internal/httpapi"
)

func main() {
	cfg := config.Load()
	server := httpapi.NewServer()
	addr := fmt.Sprintf("%s:%d", cfg.APIHost, cfg.APIPort)
	slog.Info("starting vp-api-go", "addr", addr)
	if err := http.ListenAndServe(addr, server.Router()); err != nil {
		slog.Error("vp-api-go stopped", "error", err)
		os.Exit(1)
	}
}
