package handlers

import (
	"context"
	"strconv"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type MontageAssemblerHandler struct {
	Runner vpffmpeg.Runner
}

func (h MontageAssemblerHandler) NodeType() string {
	return "montage_assembler"
}

func (h MontageAssemblerHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	montageConfig := cloneHandlerConfig(config)
	width, height := montageDimensions(montageConfig)
	if _, exists := montageConfig["width"]; !exists {
		montageConfig["width"] = width
	}
	if _, exists := montageConfig["height"]; !exists {
		montageConfig["height"] = height
	}
	if _, exists := montageConfig["normalize_resolution"]; !exists {
		montageConfig["normalize_resolution"] = true
	}
	if _, exists := montageConfig["input_count"]; !exists {
		montageConfig["input_count"] = montageInputCount(inputPaths)
	}
	args, err := ConcatManyArgs(inputPaths, outputPath, montageConfig)
	if err != nil {
		return nil, err
	}
	if err := runFFmpeg(ctx, h.Runner, args); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}

func montageDimensions(config map[string]any) (int, int) {
	width := intValue(config["width"], 0)
	height := intValue(config["height"], 0)
	if width > 0 && height > 0 {
		return width, height
	}
	aspectRatio := stringValue(config["aspect_ratio"], "9:16")
	switch aspectRatio {
	case "16:9":
		return 1920, 1080
	case "1:1":
		return 1080, 1080
	default:
		return 1080, 1920
	}
}

func montageInputCount(inputPaths map[string]string) int {
	count := 0
	for handle := range inputPaths {
		if stringsHasVideoNumericPrefix(handle) {
			count++
		}
	}
	return count
}

func stringsHasVideoNumericPrefix(value string) bool {
	if len(value) <= len("video_") || value[:len("video_")] != "video_" {
		return false
	}
	_, err := strconv.Atoi(value[len("video_"):])
	return err == nil
}
