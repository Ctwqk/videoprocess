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

func TestBoolValueMatchesPythonParseBoolParam(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		fallback bool
		want     bool
	}{
		{name: "nil uses fallback true", value: nil, fallback: true, want: true},
		{name: "nil uses fallback false", value: nil, fallback: false, want: false},
		{name: "bool true", value: true, fallback: false, want: true},
		{name: "bool false", value: false, fallback: true, want: false},
		{name: "truthy string one", value: "1", fallback: false, want: true},
		{name: "truthy string true", value: "true", fallback: false, want: true},
		{name: "truthy string yes", value: "yes", fallback: false, want: true},
		{name: "truthy string on", value: "on", fallback: false, want: true},
		{name: "truthy string uppercase with spaces", value: " YES ", fallback: false, want: true},
		{name: "false string ignores fallback", value: "false", fallback: true, want: false},
		{name: "unknown string ignores fallback", value: "falsey", fallback: true, want: false},
		{name: "numeric zero ignores fallback", value: 0, fallback: true, want: false},
		{name: "numeric two ignores fallback", value: 2, fallback: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := boolValue(tt.value, tt.fallback); got != tt.want {
				t.Fatalf("boolValue(%#v, %v) = %v, want %v", tt.value, tt.fallback, got, tt.want)
			}
		})
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
