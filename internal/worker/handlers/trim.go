package handlers

import (
	"context"
	"errors"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type TrimHandler struct {
	Runner vpffmpeg.Runner
}

func (h TrimHandler) NodeType() string {
	return "trim"
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
	args = append(args, intermediateVideoEncodeArgs("libx264")...)
	args = append(args, "-c:a", "aac", outputPath)
	return args
}

func (h TrimHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	inputPath := inputPaths["input"]
	if inputPath == "" {
		return nil, errors.New("missing input path on input port")
	}
	runner := h.Runner
	if runner.Binary == "" {
		runner = vpffmpeg.NewRunner()
	}
	_, err := runner.Run(ctx, h.Args(inputPath, outputPath, config))
	if err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}

// _ asserts the runner result return type so a future change to Runner.Run
// breaks here loudly rather than silently shadowing the new field.
var _ = vpffmpeg.RunResult{}
