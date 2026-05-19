package ffmpeg

import (
	"reflect"
	"testing"
)

func TestVideoEncodeArgsCPU(t *testing.T) {
	got := VideoEncodeArgs(EncodeConfig{Codec: "libx264", Preset: "medium", CRF: 20, MP4Compatible: true})
	want := []string{"-c:v", "libx264", "-crf", "20", "-preset", "medium", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v", got)
	}
}

func TestVideoEncodeArgsNVENC(t *testing.T) {
	got := VideoEncodeArgs(EncodeConfig{UseGPU: true, Codec: "libx264", Preset: "medium", CRF: 20, MP4Compatible: true})
	want := []string{"-c:v", "h264_nvenc", "-rc:v", "vbr", "-cq:v", "20", "-preset", "medium", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v", got)
	}
}
