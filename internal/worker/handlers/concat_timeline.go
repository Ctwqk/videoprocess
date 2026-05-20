package handlers

import (
	"context"
	"errors"
	"os"
	"strings"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type ConcatTimelineHandler struct {
	Runner vpffmpeg.Runner
}

func (h ConcatTimelineHandler) NodeType() string {
	return "concat_timeline"
}

func (h ConcatTimelineHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	selectedItems := selectedVideoInputItems(inputPaths)
	if len(selectedItems) < 2 {
		return nil, errors.New("concat_timeline requires at least two video inputs")
	}
	selected := make([]string, 0, len(selectedItems))
	for _, item := range selectedItems {
		selected = append(selected, item.path)
	}

	transition := stringValue(config["transition"], "none")
	transitionDuration := floatValue(config["transition_duration"], 0.5)
	if transition == "none" || transitionDuration <= 0 {
		concatFile, err := writeConcatDemuxerTempFile(selected)
		if err != nil {
			return nil, err
		}
		defer safeRemove(concatFile)
		if err := runFFmpeg(ctx, h.Runner, concatTimelineDemuxerArgs(concatFile, outputPath)); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	}

	if len(selectedItems) > 2 {
		concatConfig := cloneHandlerConfig(config)
		if _, exists := concatConfig["normalize_resolution"]; !exists {
			concatConfig["normalize_resolution"] = true
		}
		args, err := ConcatManyArgs(inputPaths, outputPath, concatConfig)
		if err != nil {
			return nil, err
		}
		if err := runFFmpeg(ctx, h.Runner, args); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	}

	firstProbe, err := probePath(ctx, h.Runner, selected[0])
	if err != nil {
		return nil, err
	}
	if firstProbe.Duration <= 0 {
		firstProbe.Duration = 5
	}
	secondProbe, err := probePath(ctx, h.Runner, selected[1])
	if err != nil {
		return nil, err
	}
	if err := runFFmpeg(ctx, h.Runner, ConcatTimelineTransitionArgs(selected[0], selected[1], outputPath, config, firstProbe, secondProbe)); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}

func concatTimelineDemuxerArgs(concatFile string, outputPath string) []string {
	return []string{
		"-f", "concat", "-safe", "0",
		"-i", concatFile,
		"-c", "copy",
		outputPath,
	}
}

func ConcatTimelineTransitionArgs(firstPath string, secondPath string, outputPath string, config map[string]any, firstProbe probeSummary, secondProbe probeSummary) []string {
	transition := stringValue(config["transition"], "none")
	transitionDuration := floatValue(config["transition_duration"], 0.5)
	offset := firstProbe.Duration - transitionDuration
	if offset < 0 {
		offset = 0
	}
	transitionName := "dissolve"
	if transition == "fade" {
		transitionName = "fade"
	}

	normalizedFirst := "[0:v]" + scaleFilter("1080", "1920", "decrease") + ",pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,settb=AVTB[v0]"
	normalizedSecond := "[1:v]" + scaleFilter("1080", "1920", "decrease") + ",pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,settb=AVTB[v1]"
	filterComplex := []string{
		normalizedFirst,
		normalizedSecond,
		"[v0][v1]xfade=transition=" + transitionName + ":duration=" + formatFloat(transitionDuration) + ":offset=" + formatFloat(offset) + "[v]",
	}
	args := []string{"-i", firstPath, "-i", secondPath}
	if firstProbe.HasAudio && secondProbe.HasAudio {
		filterComplex = append(filterComplex, "[0:a][1:a]acrossfade=d="+formatFloat(transitionDuration)+"[a]")
	}
	args = append(args,
		"-filter_complex", strings.Join(filterComplex, ";"),
		"-map", "[v]",
	)
	args = append(args, intermediateVideoEncodeArgs("libx264")...)
	if firstProbe.HasAudio && secondProbe.HasAudio {
		args = append(args, "-map", "[a]", "-c:a", "aac")
	} else if firstProbe.HasAudio {
		args = append(args, "-map", "0:a:0", "-c:a", "aac")
	} else if secondProbe.HasAudio {
		args = append(args, "-map", "1:a:0", "-c:a", "aac")
	}
	args = append(args, outputPath)
	return args
}

func concatDemuxerFileContent(paths []string) string {
	var builder strings.Builder
	for _, path := range paths {
		builder.WriteString("file '")
		builder.WriteString(path)
		builder.WriteString("'\n")
	}
	return builder.String()
}

func writeConcatDemuxerTempFile(paths []string) (string, error) {
	file, err := os.CreateTemp("", "vp_concat_*.txt")
	if err != nil {
		return "", err
	}
	path := file.Name()
	if _, err := file.WriteString(concatDemuxerFileContent(paths)); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

func safeRemove(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

func cloneHandlerConfig(config map[string]any) map[string]any {
	clone := make(map[string]any, len(config))
	for key, value := range config {
		clone[key] = value
	}
	return clone
}
