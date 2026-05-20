package handlers

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

func TestTrimExecuteRetriesHardwareCapacityFailureOnCPU(t *testing.T) {
	root := t.TempDir()
	logPath := filepath.Join(root, "args.log")
	markerPath := filepath.Join(root, "failed_once")
	scriptPath := filepath.Join(root, "fake-ffmpeg.sh")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> " + shellQuote(logPath) + "\n" +
		"if [ ! -e " + shellQuote(markerPath) + " ]; then\n" +
		"  touch " + shellQuote(markerPath) + "\n" +
		"  echo 'OpenEncodeSessionEx failed: out of memory' >&2\n" +
		"  exit 1\n" +
		"fi\n" +
		"exit 0\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VIDEO_USE_GPU", "1")

	handler := TrimHandler{Runner: vpffmpeg.Runner{Binary: scriptPath, PreArgs: nil}}
	_, err := handler.Execute(context.Background(), map[string]string{"input": "/input.mp4"}, "/output.mp4", map[string]any{})

	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(mustReadFile(t, logPath))), "\n")
	if len(lines) != 2 {
		t.Fatalf("run count = %d, lines=%#v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "h264_nvenc") {
		t.Fatalf("first run args = %q", lines[0])
	}
	if !strings.Contains(lines[1], "libx264") || strings.Contains(lines[1], "h264_nvenc") || strings.Contains(lines[1], "-cq:v") {
		t.Fatalf("retry args = %q", lines[1])
	}
}
