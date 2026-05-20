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

func TestBatch4AArgs(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		expect []string
	}{
		{
			name: "transcode default",
			args: TranscodeArgs("/in.mp4", "/out.mp4", map[string]any{}),
			expect: []string{
				"-i", "/in.mp4",
				"-c:v", "libx264",
				"-crf", "20",
				"-preset", "medium",
				"-pix_fmt", "yuv420p",
				"-movflags", "+faststart",
				"-color_primaries", "bt709",
				"-color_trc", "bt709",
				"-colorspace", "bt709",
				"-c:a", "aac",
				"/out.mp4",
			},
		},
		{
			name: "vertical crop center",
			args: VerticalCropArgs("/in.mp4", "/out.mp4", map[string]any{"width": 1080.0, "height": 1920.0, "mode": "center_crop"}),
			expect: []string{
				"-i", "/in.mp4",
				"-vf", "scale=1080:1920:force_original_aspect_ratio=increase:flags=lanczos,crop=1080:1920,setsar=1",
				"-c:v", "libx264",
				"-crf", "18",
				"-preset", "slow",
				"-pix_fmt", "yuv420p",
				"-movflags", "+faststart",
				"-color_primaries", "bt709",
				"-color_trc", "bt709",
				"-colorspace", "bt709",
				"-c:a", "aac",
				"/out.mp4",
			},
		},
		{
			name: "transcode nvenc custom crf bitrate",
			args: TranscodeArgs("/in.mp4", "/out.mp4", map[string]any{"video_codec": "h264_nvenc", "crf": 24.0, "preset": "p5", "bitrate": "4M"}),
			expect: []string{
				"-i", "/in.mp4",
				"-c:v", "h264_nvenc",
				"-rc:v", "vbr",
				"-cq:v", "24",
				"-preset", "p5",
				"-b:v", "4M",
				"-pix_fmt", "yuv420p",
				"-movflags", "+faststart",
				"-color_primaries", "bt709",
				"-color_trc", "bt709",
				"-colorspace", "bt709",
				"-c:a", "aac",
				"/out.mp4",
			},
		},
		{
			name: "transcode videotoolbox custom bitrate",
			args: TranscodeArgs("/in.mp4", "/out.mp4", map[string]any{"video_codec": "h264_videotoolbox", "bitrate": "5M"}),
			expect: []string{
				"-i", "/in.mp4",
				"-c:v", "h264_videotoolbox",
				"-b:v", "5M",
				"-pix_fmt", "yuv420p",
				"-movflags", "+faststart",
				"-color_primaries", "bt709",
				"-color_trc", "bt709",
				"-colorspace", "bt709",
				"-c:a", "aac",
				"/out.mp4",
			},
		},
		{
			name: "watermark bottom right",
			args: WatermarkArgs("/video.mp4", "/wm.png", "/out.mp4", map[string]any{"position": "bottom_right", "opacity": 0.8, "scale": 0.15, "margin": 10.0}),
			expect: []string{
				"-i", "/video.mp4",
				"-i", "/wm.png",
				"-filter_complex", "[1:v]scale=iw*0.15:-1:flags=lanczos,format=rgba,colorchannelmixer=aa=0.8[wm];[0:v][wm]overlay=W-w-10:H-h-10[v]",
				"-map", "[v]",
				"-map", "0:a?",
				"-c:v", "libx264",
				"-crf", "18",
				"-preset", "slow",
				"-pix_fmt", "yuv420p",
				"-movflags", "+faststart",
				"-color_primaries", "bt709",
				"-color_trc", "bt709",
				"-colorspace", "bt709",
				"-c:a", "copy",
				"/out.mp4",
			},
		},
		{
			name: "title overlay",
			args: TitleOverlayArgs("/in.mp4", "/out.mp4", map[string]any{"text": "Hello: A", "position": "top", "start_time": 0.0, "duration": 3.0, "font_size": 72.0, "safe_area": true}),
			expect: []string{
				"-i", "/in.mp4",
				"-vf", "drawtext=text='Hello\\: A':fontcolor=white:fontsize=72:box=1:boxcolor=black@0.45:boxborderw=18:x=(w-text_w)/2:y=h*0.12:enable='between(t,0,3)'",
				"-c:v", "libx264",
				"-crf", "18",
				"-preset", "slow",
				"-pix_fmt", "yuv420p",
				"-movflags", "+faststart",
				"-color_primaries", "bt709",
				"-color_trc", "bt709",
				"-colorspace", "bt709",
				"-c:a", "aac",
				"/out.mp4",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.args, tt.expect) {
				t.Fatalf("args = %#v", tt.args)
			}
		})
	}
}
