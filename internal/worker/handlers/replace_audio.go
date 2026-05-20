package handlers

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type ReplaceAudioHandler struct {
	Runner vpffmpeg.Runner
}

func (h ReplaceAudioHandler) NodeType() string {
	return "replace_audio"
}

func ReplaceAudioArgs(videoPath string, audioPath string, outputPath string, config map[string]any, videoProbe probeSummary) []string {
	loopIfShorter := truthyValue(config["loop_if_shorter"], true)
	audioVolume := floatValue(config["audio_volume"], 1.0)

	inputArgs := []string{"-i", videoPath}
	if loopIfShorter {
		inputArgs = append(inputArgs, "-stream_loop", "-1", "-i", audioPath)
	} else {
		inputArgs = append(inputArgs, "-i", audioPath)
	}

	filterChain := "[1:a]volume=" + formatFloat(audioVolume)
	if !loopIfShorter {
		filterChain += ",apad"
	}
	filterChain += "[aout]"

	args := append([]string{}, inputArgs...)
	args = append(args,
		"-filter_complex", filterChain,
		"-map", "0:v:0",
		"-map", "[aout]",
		"-c:v", "copy",
		"-c:a", "aac",
		"-t", strconv.FormatFloat(videoProbe.Duration, 'f', 3, 64),
		outputPath,
	)
	return args
}

func (h ReplaceAudioHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	videoPath := inputPaths["video"]
	if videoPath == "" {
		return nil, errors.New("missing input path on video port")
	}
	audioPath := inputPaths["audio"]
	if audioPath == "" {
		return nil, errors.New("missing input path on audio port")
	}
	videoProbe, err := probePath(ctx, h.Runner, videoPath)
	if err != nil {
		return nil, err
	}
	if videoProbe.Duration <= 0 {
		return nil, fmt.Errorf("unable to determine input video duration")
	}
	if err := runFFmpeg(ctx, h.Runner, ReplaceAudioArgs(videoPath, audioPath, outputPath, config, videoProbe)); err != nil {
		return nil, err
	}
	return map[string]any{"audio_replaced": true, "video_duration": videoProbe.Duration}, nil
}
