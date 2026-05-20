package handlers

import (
	"context"
	"errors"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type TitleOverlayHandler struct {
	Runner vpffmpeg.Runner
}

func (h TitleOverlayHandler) NodeType() string {
	return "title_overlay"
}

func TitleOverlayArgs(inputPath string, outputPath string, config map[string]any) []string {
	text := stringValue(config["text"], "")
	position := stringValue(config["position"], "top")
	start := floatValue(config["start_time"], 0)
	duration := floatValue(config["duration"], 3)
	fontSize := intValue(config["font_size"], 72)
	safeArea := boolValue(config["safe_area"], true)

	yExpr := "h*0.12"
	switch position {
	case "top":
		if !safeArea {
			yExpr = "h*0.06"
		}
	case "center":
		yExpr = "(h-text_h)/2"
	case "bottom":
		if safeArea {
			yExpr = "h-text_h-h*0.18"
		} else {
			yExpr = "h-text_h-h*0.08"
		}
	}

	enable := "between(t," + formatFloat(start) + "," + formatFloat(start+duration) + ")"
	drawtext := "drawtext=text='" + escapeDrawText(text) + "':fontcolor=white:fontsize=" + intString(fontSize, 72) +
		":box=1:boxcolor=black@0.45:boxborderw=18:x=(w-text_w)/2:y=" + yExpr + ":enable='" + enable + "'"
	args := []string{"-i", inputPath, "-vf", drawtext}
	args = append(args, intermediateVideoEncodeArgs("libx264")...)
	args = append(args, "-c:a", "aac", outputPath)
	return args
}

func (h TitleOverlayHandler) Args(inputPath, outputPath string, config map[string]any) []string {
	return TitleOverlayArgs(inputPath, outputPath, config)
}

func (h TitleOverlayHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	inputPath := inputPaths["input"]
	if inputPath == "" {
		return nil, errors.New("missing input path on input port")
	}
	if err := runFFmpeg(ctx, h.Runner, h.Args(inputPath, outputPath, config)); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}
