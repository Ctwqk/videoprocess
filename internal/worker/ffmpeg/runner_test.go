package ffmpeg

import (
	"context"
	"errors"
	"os/exec"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestRunCancelledReturnsErrCancelled(t *testing.T) {
	if _, err := exec.LookPath("sleep"); err != nil {
		t.Skip("sleep not available")
	}
	// Use `sleep` as a stand-in for a long ffmpeg run so the test stays
	// hermetic without depending on a real ffmpeg binary.
	r := Runner{Binary: "sleep", PreArgs: nil}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := r.Run(ctx, []string{"5"})
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if !errors.Is(err, ErrCancelled) {
		t.Fatalf("error = %v, want ErrCancelled", err)
	}
}

func TestRunMissingBinaryDoesNotPanic(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH semantics differ on Windows")
	}
	r := Runner{Binary: "vp-missing-binary-for-test"}
	_, err := r.Run(context.Background(), []string{"--version"})
	if err == nil {
		t.Fatal("expected error when binary is missing")
	}
	if errors.Is(err, ErrCancelled) {
		t.Fatalf("missing binary must not surface as ErrCancelled: %v", err)
	}
}

func TestIsGPUCapacityErrorDetectsKnownIndicators(t *testing.T) {
	cases := map[string]bool{
		"":                                false,
		"frame=  120 fps=30 q=23.0 size=": false,
		"OpenEncodeSessionEx failed: out of memory":     true,
		"No NVENC capable devices found":                true,
		"Cannot init CUDA":                              true,
		"videotoolbox encoder error":                    true,
		"Error while opening encoder for output stream": true,
	}
	for stderr, want := range cases {
		if got := IsGPUCapacityError(stderr); got != want {
			t.Fatalf("IsGPUCapacityError(%q) = %v want %v", stderr, got, want)
		}
	}
}

func TestRewriteHardwareArgsForCPUMapsNVENCToLibx264(t *testing.T) {
	in := []string{
		"-c:v", "h264_nvenc",
		"-rc:v", "vbr",
		"-cq:v", "23",
		"-preset", "fast",
		"-pix_fmt", "yuv420p",
	}
	got := RewriteHardwareArgsForCPU(in)
	want := []string{
		"-c:v", "libx264",
		"-crf", "21",
		"-preset", "fast",
		"-pix_fmt", "yuv420p",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rewrite = %#v\nwant   %#v", got, want)
	}
}

func TestRewriteHardwareArgsForCPUInsertsCRFAfterCodecInRealHandlerArgs(t *testing.T) {
	in := []string{
		"-i", "/input.mp4",
		"-map", "0:v:0",
		"-map", "0:a?",
		"-c:v", "h264_nvenc",
		"-rc:v", "vbr",
		"-cq:v", "23",
		"-preset", "slow",
		"/output.mp4",
	}
	got := RewriteHardwareArgsForCPU(in)
	want := []string{
		"-i", "/input.mp4",
		"-map", "0:v:0",
		"-map", "0:a?",
		"-c:v", "libx264",
		"-crf", "21",
		"-preset", "slow",
		"/output.mp4",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rewrite = %#v\nwant   %#v", got, want)
	}
}

func TestRewriteHardwareArgsForCPUPreservesExistingCRF(t *testing.T) {
	in := []string{"-c:v", "hevc_nvenc", "-cq:v", "26", "-crf", "20"}
	got := RewriteHardwareArgsForCPU(in)
	// -crf already present, -cq:v dropped, codec mapped.
	want := []string{"-c:v", "libx265", "-crf", "20"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rewrite = %#v\nwant   %#v", got, want)
	}
}
