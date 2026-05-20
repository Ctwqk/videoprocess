package handlers

import (
	"reflect"
	"testing"
)

func TestScaleFilter(t *testing.T) {
	got := scaleFilter("1080", "1920", "increase")
	want := "scale=1080:1920:force_original_aspect_ratio=increase:flags=lanczos"
	if got != want {
		t.Fatalf("scaleFilter() = %q, want %q", got, want)
	}
}

func TestDrawTextEscaping(t *testing.T) {
	got := escapeDrawText(`a:b'c\d`)
	want := `a\:b\'c\\d`
	if got != want {
		t.Fatalf("escapeDrawText() = %q, want %q", got, want)
	}
}

func TestIntermediateVideoEncodeArgs(t *testing.T) {
	got := intermediateVideoEncodeArgs("libx264")
	want := []string{
		"-c:v", "libx264",
		"-crf", "18",
		"-preset", "slow",
		"-pix_fmt", "yuv420p",
		"-movflags", "+faststart",
		"-color_primaries", "bt709",
		"-color_trc", "bt709",
		"-colorspace", "bt709",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v", got)
	}
}
