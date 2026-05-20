package handlers

import (
	"context"
	"errors"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type ConcatHorizontalHandler struct {
	Runner vpffmpeg.Runner
}

func (h ConcatHorizontalHandler) NodeType() string {
	return "concat_horizontal"
}

func (h ConcatHorizontalHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	leftPath := inputPaths["video_left"]
	if leftPath == "" {
		return nil, errors.New("missing input path on video_left port")
	}
	rightPath := inputPaths["video_right"]
	if rightPath == "" {
		return nil, errors.New("missing input path on video_right port")
	}
	if err := executeStackConcat(ctx, h.Runner, outputPath, leftPath, rightPath, concatStackConfig{
		PrimaryLabel:   "left",
		SecondaryLabel: "right",
		StackAxis:      "horizontal",
		ResizeMode:     stringValue(config["resize_mode"], "match_height"),
	}); err != nil {
		return nil, err
	}
	return map[string]any{}, nil
}
