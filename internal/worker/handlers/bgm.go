package handlers

import (
	"context"
	"errors"
	"strings"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type BgmHandler struct {
	Runner vpffmpeg.Runner
}

func (h BgmHandler) NodeType() string {
	return "bgm"
}

func BgmArgs(videoPath string, audioPath string, outputPath string, config map[string]any, videoProbe probeSummary) []string {
	volume := floatValue(config["volume"], 0.3)
	originalVolume := floatValue(config["original_volume"], 1.0)
	loop := truthyValue(config["loop"], true)
	fadeIn := floatValue(config["fade_in"], 0)
	fadeOut := floatValue(config["fade_out"], 0)

	bgmFilters := []string{"volume=" + formatFloat(volume)}
	if fadeIn > 0 {
		bgmFilters = append(bgmFilters, "afade=t=in:d="+formatFloat(fadeIn))
	}
	if fadeOut > 0 && videoProbe.Duration > 0 {
		fadeStart := videoProbe.Duration - fadeOut
		if fadeStart < 0 {
			fadeStart = 0
		}
		bgmFilters = append(bgmFilters, "afade=t=out:st="+formatFloat(fadeStart)+":d="+formatFloat(fadeOut))
	}

	bgmFilterChain := strings.Join(bgmFilters, ",")
	sidechainAudioFormat := "aformat=sample_fmts=fltp:channel_layouts=stereo"

	inputArgs := []string{"-i", videoPath}
	if loop {
		inputArgs = append(inputArgs, "-stream_loop", "-1", "-i", audioPath)
	} else {
		inputArgs = append(inputArgs, "-i", audioPath)
	}

	var filterComplex string
	if videoProbe.HasAudio {
		filterComplex = "[0:a]aresample=48000:async=1," + sidechainAudioFormat + "," +
			"volume=" + formatFloat(originalVolume) + ",asplit=2[orig_mix][orig_sidechain];" +
			"[1:a]aresample=48000:async=1," + sidechainAudioFormat + "," + bgmFilterChain + "[bgm];" +
			"[bgm][orig_sidechain]sidechaincompress=threshold=0.03:ratio=8:attack=200:release=800[ducked];" +
			"[orig_mix][ducked]amix=inputs=2:duration=first:normalize=0[mix];" +
			"[mix]loudnorm=I=-16:LRA=11:TP=-1.5[a]"
	} else {
		filterComplex = "[1:a]aresample=48000:async=1," + bgmFilterChain + ",loudnorm=I=-16:LRA=11:TP=-1.5[a]"
	}

	args := append([]string{}, inputArgs...)
	args = append(args,
		"-filter_complex", filterComplex,
		"-map", "0:v", "-map", "[a]",
		"-c:v", "copy",
		"-c:a", "aac",
		"-ar", "48000",
		"-ac", "2",
		"-shortest",
		outputPath,
	)
	return args
}

func (h BgmHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
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
	if err := runFFmpeg(ctx, h.Runner, BgmArgs(videoPath, audioPath, outputPath, config, videoProbe)); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}
