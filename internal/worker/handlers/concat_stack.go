package handlers

import (
	"context"
	"errors"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type concatStackConfig struct {
	PrimaryLabel      string
	SecondaryLabel    string
	StackAxis         string
	ResizeMode        string
	PrimaryHasAudio   bool
	SecondaryHasAudio bool
}

func ConcatStackArgs(primaryPath string, secondaryPath string, outputPath string, config concatStackConfig) []string {
	filterComplex := concatStackVideoFilter(config)
	if config.PrimaryHasAudio && config.SecondaryHasAudio {
		filterComplex += ";[0:a][1:a]amix=inputs=2:duration=longest:dropout_transition=2[a]"
	}

	args := []string{
		"-i", primaryPath,
		"-i", secondaryPath,
		"-filter_complex", filterComplex,
		"-map", "[v]",
	}
	if config.PrimaryHasAudio && config.SecondaryHasAudio {
		args = append(args, "-map", "[a]", "-c:a", "aac")
	} else if config.PrimaryHasAudio {
		args = append(args, "-map", "0:a:0", "-c:a", "aac")
	} else if config.SecondaryHasAudio {
		args = append(args, "-map", "1:a:0", "-c:a", "aac")
	}
	args = append(args, intermediateVideoEncodeArgs("libx264")...)
	args = append(args, outputPath)
	return args
}

func concatStackVideoFilter(config concatStackConfig) string {
	if config.StackAxis == "horizontal" {
		switch config.ResizeMode {
		case "match_height":
			return "[0:v]scale=-2:480:flags=lanczos[" + config.PrimaryLabel + "];" +
				"[1:v]scale=-2:480:flags=lanczos[" + config.SecondaryLabel + "];" +
				"[" + config.PrimaryLabel + "][" + config.SecondaryLabel + "]hstack=inputs=2[v]"
		case "match_width":
			return "[0:v]scale=640:-2:flags=lanczos[" + config.PrimaryLabel + "];" +
				"[1:v]scale=640:-2:flags=lanczos[" + config.SecondaryLabel + "];" +
				"[" + config.PrimaryLabel + "][" + config.SecondaryLabel + "]hstack=inputs=2[v]"
		default:
			return "[0:v][1:v]hstack=inputs=2[v]"
		}
	}
	switch config.ResizeMode {
	case "match_width":
		return "[0:v]scale=640:-2:flags=lanczos[" + config.PrimaryLabel + "];" +
			"[1:v]scale=640:-2:flags=lanczos[" + config.SecondaryLabel + "];" +
			"[" + config.PrimaryLabel + "][" + config.SecondaryLabel + "]vstack=inputs=2[v]"
	case "match_height":
		return "[0:v]scale=-2:480:flags=lanczos[" + config.PrimaryLabel + "];" +
			"[1:v]scale=-2:480:flags=lanczos[" + config.SecondaryLabel + "];" +
			"[" + config.PrimaryLabel + "][" + config.SecondaryLabel + "]vstack=inputs=2[v]"
	default:
		return "[0:v][1:v]vstack=inputs=2[v]"
	}
}

func executeStackConcat(ctx context.Context, runner vpffmpeg.Runner, outputPath string, primaryPath string, secondaryPath string, config concatStackConfig) error {
	if primaryPath == "" {
		return errors.New("missing primary input path")
	}
	if secondaryPath == "" {
		return errors.New("missing secondary input path")
	}
	primaryProbe, err := probePath(ctx, runner, primaryPath)
	if err != nil {
		return err
	}
	secondaryProbe, err := probePath(ctx, runner, secondaryPath)
	if err != nil {
		return err
	}
	config.PrimaryHasAudio = primaryProbe.HasAudio
	config.SecondaryHasAudio = secondaryProbe.HasAudio
	return runFFmpeg(ctx, runner, ConcatStackArgs(primaryPath, secondaryPath, outputPath, config))
}
