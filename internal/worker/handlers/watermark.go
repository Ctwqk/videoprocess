package handlers

import (
	"context"
	"errors"
	"strconv"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type WatermarkHandler struct {
	Runner vpffmpeg.Runner
}

func (h WatermarkHandler) NodeType() string {
	return "watermark"
}

func WatermarkArgs(videoPath string, overlayPath string, outputPath string, config map[string]any) []string {
	position := stringValue(config["position"], "bottom_right")
	opacity := floatValue(config["opacity"], 0.8)
	scale := floatValue(config["scale"], 0.15)
	margin := intValue(config["margin"], 10)

	overlayPos := map[string]string{
		"top_left":     strconv.Itoa(margin) + ":" + strconv.Itoa(margin),
		"top_right":    "W-w-" + strconv.Itoa(margin) + ":" + strconv.Itoa(margin),
		"bottom_left":  strconv.Itoa(margin) + ":H-h-" + strconv.Itoa(margin),
		"bottom_right": "W-w-" + strconv.Itoa(margin) + ":H-h-" + strconv.Itoa(margin),
		"center":       "(W-w)/2:(H-h)/2",
	}
	selected, ok := overlayPos[position]
	if !ok {
		selected = overlayPos["bottom_right"]
	}

	filterComplex := "[1:v]scale=iw*" + formatFloat(scale) + ":-1:flags=lanczos,format=rgba,colorchannelmixer=aa=" + formatFloat(opacity) + "[wm];[0:v][wm]overlay=" + selected + "[v]"
	args := []string{
		"-i", videoPath,
		"-i", overlayPath,
		"-filter_complex", filterComplex,
		"-map", "[v]",
		"-map", "0:a?",
	}
	args = append(args, intermediateVideoEncodeArgs("libx264")...)
	args = append(args, "-c:a", "copy", outputPath)
	return args
}

func (h WatermarkHandler) Args(videoPath, overlayPath, outputPath string, config map[string]any) []string {
	return WatermarkArgs(videoPath, overlayPath, outputPath, config)
}

func (h WatermarkHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	videoPath := inputPaths["video"]
	if videoPath == "" {
		return nil, errors.New("missing input path on video port")
	}
	overlayPath := inputPaths["overlay"]
	if overlayPath == "" {
		return nil, errors.New("missing input path on overlay port")
	}
	if err := runFFmpeg(ctx, h.Runner, h.Args(videoPath, overlayPath, outputPath, config)); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
