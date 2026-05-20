package handlers

import (
	"context"
	"errors"
	"strconv"
	"strings"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type TranscodeHandler struct {
	Runner vpffmpeg.Runner
}

func (h TranscodeHandler) NodeType() string {
	return "transcode"
}

func TranscodeArgs(inputPath string, outputPath string, config map[string]any) []string {
	videoCodec := preferredVideoCodec(stringValue(config["video_codec"], "libx264"))
	audioCodec := stringValue(config["audio_codec"], "aac")
	resolution := stringValue(config["resolution"], "")
	bitrate := stringValue(config["bitrate"], "")
	crf := intValue(config["crf"], 20)
	preset := stringValue(config["preset"], "medium")

	args := []string{"-i", inputPath}
	switch videoCodec {
	case "copy":
		args = append(args, "-c:v", "copy")
	case "libvpx-vp9":
		args = append(args, "-c:v", "libvpx-vp9", "-crf", strconv.Itoa(crf), "-b:v", "0")
		if bitrate != "" {
			args = append(args, "-b:v", bitrate)
		}
	default:
		args = append(args, videoEncodeArgs(videoCodec, preset, crf, bitrate, true)...)
	}

	if videoCodec != "copy" && resolution != "" && resolution != "original" {
		width, height, ok := strings.Cut(resolution, "x")
		if ok {
			args = append(args, "-vf", scaleFilter(width, height, ""))
		}
	}
	args = append(args, "-c:a", audioCodec, outputPath)
	return args
}

func (h TranscodeHandler) Args(inputPath, outputPath string, config map[string]any) []string {
	return TranscodeArgs(inputPath, outputPath, config)
}

func (h TranscodeHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	inputPath := inputPaths["input"]
	if inputPath == "" {
		return nil, errors.New("missing input path on input port")
	}
	if err := runFFmpeg(ctx, h.Runner, h.Args(inputPath, outputPath, config)); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}

func runFFmpeg(ctx context.Context, runner vpffmpeg.Runner, args []string) error {
	if runner.Binary == "" {
		runner = vpffmpeg.NewRunner()
	}
	_, err := runner.Run(ctx, args)
	return err
}
