package handlers

import (
	"context"
	"reflect"
	"testing"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

func TestTrimArgsMatchPythonHandler(t *testing.T) {
	handler := TrimHandler{Runner: vpffmpeg.Runner{Binary: "ffmpeg"}}
	got := handler.Args("/input.mp4", "/output.mp4", map[string]any{
		"start_time": "1.250",
		"duration":   "2.500",
	})
	want := []string{
		"-ss", "1.250",
		"-i", "/input.mp4",
		"-t", "2.500",
		"-map", "0:v:0",
		"-map", "0:a?",
		"-c:v", "libx264",
		"-crf", "18",
		"-preset", "slow",
		"-pix_fmt", "yuv420p",
		"-movflags", "+faststart",
		"-color_primaries", "bt709",
		"-color_trc", "bt709",
		"-colorspace", "bt709",
		"-c:a", "aac",
		"/output.mp4",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v", got)
	}
}

func TestTrimExecuteReadsInputPortAndReturnsNoMetadata(t *testing.T) {
	handler := TrimHandler{Runner: vpffmpeg.Runner{Binary: "true"}}
	metadata, err := handler.Execute(context.Background(), map[string]string{"input": "/input.mp4"}, "/output.mp4", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata) != 0 {
		t.Fatalf("metadata = %#v", metadata)
	}
}
