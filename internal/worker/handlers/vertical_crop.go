package handlers

import (
	"context"
	"errors"
	"strconv"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type VerticalCropHandler struct {
	Runner vpffmpeg.Runner
}

func (h VerticalCropHandler) NodeType() string {
	return "vertical_crop"
}

func VerticalCropArgs(inputPath string, outputPath string, config map[string]any) []string {
	width := intValue(config["width"], 1080)
	height := intValue(config["height"], 1920)
	mode := stringValue(config["mode"], "center_crop")
	widthText := strconv.Itoa(width)
	heightText := strconv.Itoa(height)

	if mode == "blur_bg" {
		vf := "[0:v]" + scaleFilter(widthText, heightText, "increase") + ",crop=" + widthText + ":" + heightText + ",boxblur=20:1[bg];" +
			"[0:v]" + scaleFilter(widthText, heightText, "decrease") + "[fg];" +
			"[bg][fg]overlay=(W-w)/2:(H-h)/2,setsar=1[v]"
		args := []string{
			"-i", inputPath,
			"-filter_complex", vf,
			"-map", "[v]",
			"-map", "0:a?",
		}
		args = append(args, intermediateVideoEncodeArgs("libx264")...)
		args = append(args, "-c:a", "aac", outputPath)
		return args
	}

	vf := scaleFilter(widthText, heightText, "increase") + ",crop=" + widthText + ":" + heightText + ",setsar=1"
	args := []string{"-i", inputPath, "-vf", vf}
	args = append(args, intermediateVideoEncodeArgs("libx264")...)
	args = append(args, "-c:a", "aac", outputPath)
	return args
}

func (h VerticalCropHandler) Args(inputPath, outputPath string, config map[string]any) []string {
	return VerticalCropArgs(inputPath, outputPath, config)
}

func (h VerticalCropHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	inputPath := inputPaths["input"]
	if inputPath == "" {
		return nil, errors.New("missing input path on input port")
	}
	if err := runFFmpeg(ctx, h.Runner, h.Args(inputPath, outputPath, config)); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}
