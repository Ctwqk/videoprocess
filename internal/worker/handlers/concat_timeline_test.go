package handlers

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

func TestConcatTimelineTransitionHandlesMixedVideoInputs(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not available")
	}
	root := t.TempDir()
	first := filepath.Join(root, "first.mp4")
	second := filepath.Join(root, "second.mp4")
	output := filepath.Join(root, "out.mp4")
	makeSyntheticVideo(t, first, "320x240", "24", "440")
	makeSyntheticVideo(t, second, "640x360", "30", "880")

	args := ConcatTimelineTransitionArgs(
		first,
		second,
		output,
		map[string]any{"transition": "fade", "transition_duration": 0.25},
		probeSummary{Duration: 1.0, HasAudio: true},
		probeSummary{HasAudio: true},
	)

	if err := runFFmpeg(context.Background(), vpffmpeg.NewRunner(), args); err != nil {
		t.Fatal(err)
	}
}

func makeSyntheticVideo(t *testing.T, path string, size string, rate string, frequency string) {
	t.Helper()
	cmd := exec.Command(
		"ffmpeg",
		"-y",
		"-hide_banner",
		"-loglevel",
		"error",
		"-f",
		"lavfi",
		"-i",
		"testsrc2=size="+size+":rate="+rate,
		"-f",
		"lavfi",
		"-i",
		"sine=frequency="+frequency+":sample_rate=48000",
		"-t",
		"1.2",
		"-c:v",
		"libx264",
		"-pix_fmt",
		"yuv420p",
		"-c:a",
		"aac",
		path,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make synthetic video: %v\n%s", err, output)
	}
}
