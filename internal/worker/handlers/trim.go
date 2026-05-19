package handlers

import (
	"context"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type TrimHandler struct {
	Runner vpffmpeg.Runner
}

func (h TrimHandler) Args(inputPath, outputPath string, config map[string]any) []string {
	start := stringValue(config["start_time"], "00:00:00")
	end := stringValue(config["end_time"], "")
	duration := stringValue(config["duration"], "")

	args := []string{}
	if start != "" {
		args = append(args, "-ss", start)
	}
	args = append(args, "-i", inputPath)
	if end != "" {
		args = append(args, "-to", end)
	} else if duration != "" {
		args = append(args, "-t", duration)
	}
	args = append(args, "-map", "0:v:0", "-map", "0:a?")
	args = append(args, vpffmpeg.VideoEncodeArgs(vpffmpeg.EncodeConfig{
		Codec:         "libx264",
		Preset:        "slow",
		CRF:           18,
		MP4Compatible: true,
	})...)
	args = append(args, "-c:a", "aac", outputPath)
	return args
}

func (h TrimHandler) Execute(ctx context.Context, inputPath, outputPath string, config map[string]any) error {
	runner := h.Runner
	if runner.Binary == "" {
		runner = vpffmpeg.NewRunner()
	}
	_, err := runner.Run(ctx, h.Args(inputPath, outputPath, config))
	return err
}

func stringValue(value any, fallback string) string {
	if raw, ok := value.(string); ok {
		return raw
	}
	return fallback
}
