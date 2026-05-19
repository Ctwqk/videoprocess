package main

import (
	"log/slog"

	"github.com/Ctwqk/videoprocess/internal/worker"
)

func main() {
	slog.Info("starting vp-ffmpeg-worker-go", "worker_type", worker.DefaultWorkerType())
	select {}
}
