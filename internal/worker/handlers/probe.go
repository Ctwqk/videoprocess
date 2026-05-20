package handlers

import (
	"context"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type probeSummary struct {
	Duration float64
	HasAudio bool
}

func probePath(ctx context.Context, runner vpffmpeg.Runner, inputPath string) (probeSummary, error) {
	if runner.Binary == "" && runner.ProbeBinary == "" {
		runner = vpffmpeg.NewRunner()
	}
	result, err := runner.Probe(ctx, inputPath)
	if err != nil {
		return probeSummary{}, err
	}
	return probeSummary{
		Duration: result.DurationSeconds(),
		HasAudio: result.HasAudio(),
	}, nil
}
