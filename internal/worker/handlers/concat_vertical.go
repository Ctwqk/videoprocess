package handlers

import (
	"context"
	"errors"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type ConcatVerticalHandler struct {
	Runner vpffmpeg.Runner
}

func (h ConcatVerticalHandler) NodeType() string {
	return "concat_vertical"
}

func (h ConcatVerticalHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	topPath := inputPaths["video_top"]
	if topPath == "" {
		return nil, errors.New("missing input path on video_top port")
	}
	bottomPath := inputPaths["video_bottom"]
	if bottomPath == "" {
		return nil, errors.New("missing input path on video_bottom port")
	}
	if err := executeStackConcat(ctx, h.Runner, outputPath, topPath, bottomPath, concatStackConfig{
		PrimaryLabel:   "top",
		SecondaryLabel: "bottom",
		StackAxis:      "vertical",
		ResizeMode:     stringValue(config["resize_mode"], "match_width"),
	}); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}
