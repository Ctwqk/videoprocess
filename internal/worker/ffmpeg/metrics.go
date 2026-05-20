package ffmpeg

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ffmpegRunsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vp_ffmpeg_runs_total",
		Help: "Total ffmpeg process executions from Go handlers.",
	})
	ffmpegFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vp_ffmpeg_failures_total",
		Help: "Total failed ffmpeg process executions from Go handlers.",
	})
	ffmpegGPUFallbacksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vp_ffmpeg_gpu_fallbacks_total",
		Help: "Total Go ffmpeg runs that detected hardware encoder capacity fallback conditions.",
	})
)
