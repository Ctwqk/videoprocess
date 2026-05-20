package handlers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type ConcatManyHandler struct {
	Runner vpffmpeg.Runner
}

type inputItem struct {
	handle string
	path   string
}

func (h ConcatManyHandler) NodeType() string {
	return "concat_many"
}

func (h ConcatManyHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	args, err := ConcatManyArgs(inputPaths, outputPath, config)
	if err != nil {
		return nil, err
	}
	if err := runFFmpeg(ctx, h.Runner, args); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}

func ConcatManyArgs(inputPaths map[string]string, outputPath string, config map[string]any) ([]string, error) {
	selectedItems := selectedVideoInputItems(inputPaths)
	if len(selectedItems) < 2 {
		return nil, errors.New("concat_many requires at least two video inputs")
	}

	handles := make([]string, 0, len(selectedItems))
	for _, item := range selectedItems {
		handles = append(handles, item.handle)
	}
	width, height := targetDimensions(config, handles)
	normalize := boolValue(config["normalize_resolution"], true)
	targetDuration := positiveFloatOrNone(config["target_duration"])
	silenceDuration := silenceDuration(config, selectedItems, targetDuration)

	args := []string{}
	for _, item := range selectedItems {
		args = append(args, "-i", item.path)
	}
	args = append(args, "-f", "lavfi", "-i", silenceSource(silenceDuration))

	filters := make([]string, 0, len(selectedItems)+1)
	for index := range selectedItems {
		if normalize {
			filters = append(filters, fmt.Sprintf(
				"[%d:v]%s,pad=%d:%d:(ow-iw)/2:(oh-ih)/2,setsar=1[v%d]",
				index,
				scaleFilter(strconv.Itoa(width), strconv.Itoa(height), "decrease"),
				width,
				height,
				index,
			))
		} else {
			filters = append(filters, fmt.Sprintf("[%d:v]setsar=1[v%d]", index, index))
		}
	}
	var concatInputs strings.Builder
	for index := range selectedItems {
		concatInputs.WriteString(fmt.Sprintf("[v%d]", index))
	}
	filters = append(filters, fmt.Sprintf("%sconcat=n=%d:v=1:a=0[v]", concatInputs.String(), len(selectedItems)))

	args = append(args,
		"-filter_complex", strings.Join(filters, ";"),
		"-map", "[v]",
		"-map", fmt.Sprintf("%d:a", len(selectedItems)),
	)
	args = append(args, intermediateVideoEncodeArgs("libx264")...)
	args = append(args, "-c:a", "aac", "-shortest")
	if targetDuration != nil {
		args = append(args, "-t", formatSeconds(*targetDuration))
	}
	args = append(args, outputPath)
	return args, nil
}

func selectedVideoInputItems(inputPaths map[string]string) []inputItem {
	indexed := map[int]inputItem{}
	for handle, path := range inputPaths {
		if strings.HasPrefix(handle, "video_") {
			raw := strings.TrimPrefix(handle, "video_")
			if index, err := strconv.Atoi(raw); err == nil {
				indexed[index] = inputItem{handle: handle, path: path}
				continue
			}
		}
		if handle == "video_first" {
			if _, exists := indexed[1]; !exists {
				indexed[1] = inputItem{handle: handle, path: path}
			}
		}
		if handle == "video_second" {
			if _, exists := indexed[2]; !exists {
				indexed[2] = inputItem{handle: handle, path: path}
			}
		}
	}
	keys := make([]int, 0, len(indexed))
	for key := range indexed {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	selected := make([]inputItem, 0, len(keys))
	for _, key := range keys {
		selected = append(selected, indexed[key])
	}
	return selected
}

func targetDimensions(config map[string]any, selectedHandles []string) (int, int) {
	aspectRatio := strings.ToLower(strings.TrimSpace(stringValue(config["aspect_ratio"], "9:16")))
	if aspectRatio == "auto" {
		if width, height, ok := autoDimensions(config, selectedHandles); ok {
			return width, height
		}
		return configuredOrDefaultDimensions(config, 1080, 1920)
	}
	width, height := dimensionsForAspectRatio(aspectRatio)
	return configuredOrDefaultDimensions(config, width, height)
}

func configuredOrDefaultDimensions(config map[string]any, defaultWidth int, defaultHeight int) (int, int) {
	width := positiveIntOrDefault(config["width"], defaultWidth)
	height := positiveIntOrDefault(config["height"], defaultHeight)
	return width, height
}

func dimensionsForAspectRatio(aspectRatio string) (int, int) {
	switch aspectRatio {
	case "16:9":
		return 1920, 1080
	case "1:1":
		return 1080, 1080
	default:
		return 1080, 1920
	}
}

func autoDimensions(config map[string]any, selectedHandles []string) (int, int, bool) {
	inputMeta, ok := config["_input_artifact_meta"].(map[string]any)
	if !ok {
		return 0, 0, false
	}
	type candidate struct {
		bucket string
		width  int
		height int
	}
	candidates := []candidate{}
	for _, handle := range selectedHandles {
		mediaInfo, ok := inputMeta[handle].(map[string]any)
		if !ok {
			continue
		}
		width := positiveIntOrDefault(mediaInfo["width"], 0)
		height := positiveIntOrDefault(mediaInfo["height"], 0)
		if width <= 0 || height <= 0 {
			continue
		}
		candidates = append(candidates, candidate{bucket: aspectBucket(width, height), width: width, height: height})
	}
	if len(candidates) == 0 {
		return 0, 0, false
	}
	counts := map[string]int{}
	for _, item := range candidates {
		counts[item.bucket]++
	}
	maxCount := 0
	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
	}
	for _, item := range candidates {
		if counts[item.bucket] == maxCount {
			return item.width, item.height, true
		}
	}
	return 0, 0, false
}

func aspectBucket(width int, height int) string {
	if width <= 0 || height <= 0 {
		return "unknown"
	}
	ratio := float64(width) / float64(height)
	if absFloat(ratio-16.0/9.0) < 0.08 {
		return "16:9"
	}
	if absFloat(ratio-9.0/16.0) < 0.08 {
		return "9:16"
	}
	if absFloat(ratio-1.0) < 0.08 {
		return "1:1"
	}
	return strconv.FormatFloat(roundTo(ratio, 3), 'f', -1, 64)
}

func silenceDuration(config map[string]any, selectedItems []inputItem, targetDuration *float64) *float64 {
	if targetDuration != nil {
		return targetDuration
	}
	inputMeta, ok := config["_input_artifact_meta"].(map[string]any)
	if !ok {
		return nil
	}
	total := 0.0
	for _, item := range selectedItems {
		mediaInfo, ok := inputMeta[item.handle].(map[string]any)
		if !ok {
			return nil
		}
		duration := positiveFloatOrNone(mediaInfo["duration"])
		if duration == nil {
			return nil
		}
		total += *duration
	}
	if total <= 0 {
		return nil
	}
	return &total
}

func silenceSource(duration *float64) string {
	source := "anullsrc=channel_layout=stereo:sample_rate=48000"
	if duration == nil {
		return source
	}
	return source + ":duration=" + formatSeconds(*duration)
}

func positiveIntOrDefault(value any, fallback int) int {
	numeric := intValue(value, fallback)
	if numeric <= 0 {
		return fallback
	}
	return numeric
}

func positiveFloatOrNone(value any) *float64 {
	if value == nil || value == "" {
		return nil
	}
	numeric := floatValue(value, 0)
	if !isFinite(numeric) || numeric <= 0 {
		return nil
	}
	return &numeric
}

func formatSeconds(value float64) string {
	if value == float64(int64(value)) {
		return strconv.FormatInt(int64(value), 10)
	}
	return strings.TrimRight(strings.TrimRight(strconv.FormatFloat(value, 'f', 3, 64), "0"), ".")
}

func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func roundTo(value float64, places int) float64 {
	factor := 1.0
	for i := 0; i < places; i++ {
		factor *= 10
	}
	if value >= 0 {
		return float64(int(value*factor+0.5)) / factor
	}
	return float64(int(value*factor-0.5)) / factor
}
