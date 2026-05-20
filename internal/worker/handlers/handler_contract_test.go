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

func TestIntermediateVideoEncodeArgsPrefersHardwareCodecFromEnv(t *testing.T) {
	t.Setenv("VIDEO_USE_GPU", "1")
	t.Setenv("VIDEO_USE_VIDEOTOOLBOX", "")
	got := intermediateVideoEncodeArgs("libx264")
	want := []string{
		"-c:v", "h264_nvenc",
		"-rc:v", "vbr",
		"-cq:v", "18",
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

func TestCopyFileRejectsSameFile(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "input.mp4")
	if err := os.WriteFile(src, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := copyFile(src, src); err == nil {
		t.Fatal("copyFile should reject same source and destination")
	}
	if got, err := os.ReadFile(src); err != nil {
		t.Fatal(err)
	} else if string(got) != "input" {
		t.Fatalf("source was modified: %q", string(got))
	}
}

type fakeExportQualityService struct {
	result ExportQualityResult
	calls  []fakeExportQualityCall
}

type fakeExportQualityCall struct {
	sourcePath string
	outputPath string
	config     map[string]any
}

func (s *fakeExportQualityService) QAExport(_ context.Context, sourcePath string, outputPath string, config map[string]any) (ExportQualityResult, error) {
	s.calls = append(s.calls, fakeExportQualityCall{sourcePath: sourcePath, outputPath: outputPath, config: config})
	return s.result, nil
}

func TestExportReturnsQualityReportMetadata(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input.mp4")
	output := filepath.Join(root, "artifact.mp4")
	exportDir := filepath.Join(root, "exports")
	if err := os.WriteFile(source, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}
	quality := &fakeExportQualityService{
		result: ExportQualityResult{Report: map[string]any{"enabled": true, "qa_action": "passed"}},
	}
	handler := ExportHandler{QualityService: quality}

	result, err := handler.Execute(context.Background(), map[string]string{"input": source}, output, map[string]any{"output_dir": exportDir, "filename": "final.mp4"})

	if err != nil {
		t.Fatal(err)
	}
	if got := string(mustReadFile(t, output)); got != "input" {
		t.Fatalf("output = %q", got)
	}
	if got := string(mustReadFile(t, filepath.Join(exportDir, "final.mp4"))); got != "input" {
		t.Fatalf("export = %q", got)
	}
	if !reflect.DeepEqual(result, map[string]any{"quality_report": map[string]any{"enabled": true, "qa_action": "passed"}}) {
		t.Fatalf("metadata = %#v", result)
	}
	if len(quality.calls) != 1 || quality.calls[0].sourcePath != source || quality.calls[0].outputPath != output {
		t.Fatalf("quality calls = %#v", quality.calls)
	}
}

func TestExportReplacesOutputWithRepairedFile(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input.mp4")
	repaired := filepath.Join(root, "repaired.mp4")
	output := filepath.Join(root, "artifact.mp4")
	exportDir := filepath.Join(root, "exports")
	if err := os.WriteFile(source, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(repaired, []byte("repaired"), 0o644); err != nil {
		t.Fatal(err)
	}
	quality := &fakeExportQualityService{
		result: ExportQualityResult{
			Report:       map[string]any{"qa_action": "reencoded_once"},
			RepairedPath: repaired,
		},
	}
	handler := ExportHandler{QualityService: quality}

	result, err := handler.Execute(context.Background(), map[string]string{"input": source}, output, map[string]any{"output_dir": exportDir, "filename": "final.mp4"})

	if err != nil {
		t.Fatal(err)
	}
	if got := string(mustReadFile(t, output)); got != "repaired" {
		t.Fatalf("output = %q", got)
	}
	if got := string(mustReadFile(t, filepath.Join(exportDir, "final.mp4"))); got != "repaired" {
		t.Fatalf("export = %q", got)
	}
	if result["quality_report"].(map[string]any)["qa_action"] != "reencoded_once" {
		t.Fatalf("metadata = %#v", result)
	}
	if _, err := os.Stat(repaired); !os.IsNotExist(err) {
		t.Fatalf("repaired temp should be removed, err=%v", err)
	}
}

func TestExportRepairRetriesHardwareCapacityFailureOnCPU(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "input.mp4")
	logPath := filepath.Join(root, "args.log")
	markerPath := filepath.Join(root, "failed_once")
	scriptPath := filepath.Join(root, "fake-ffmpeg.sh")
	if err := os.WriteFile(input, []byte("video"), 0o644); err != nil {
		t.Fatal(err)
	}
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

	service := MediaQualityService{Runner: vpffmpeg.Runner{Binary: scriptPath, PreArgs: nil}}
	repaired, err := service.repairExport(context.Background(), input, qualityQAConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(repaired)

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

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
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

func TestTranscodeArgsPrefersHardwareCodecFromEnv(t *testing.T) {
	t.Run("gpu maps libx264 to nvenc", func(t *testing.T) {
		t.Setenv("VIDEO_USE_GPU", "true")
		t.Setenv("VIDEO_USE_VIDEOTOOLBOX", "")
		got := TranscodeArgs("/in.mp4", "/out.mp4", map[string]any{"video_codec": "libx264"})
		want := []string{
			"-i", "/in.mp4",
			"-c:v", "h264_nvenc",
			"-rc:v", "vbr",
			"-cq:v", "20",
			"-preset", "medium",
			"-pix_fmt", "yuv420p",
			"-movflags", "+faststart",
			"-color_primaries", "bt709",
			"-color_trc", "bt709",
			"-colorspace", "bt709",
			"-c:a", "aac",
			"/out.mp4",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("args = %#v", got)
		}
	})

	t.Run("videotoolbox maps libx265 to hevc videotoolbox", func(t *testing.T) {
		t.Setenv("VIDEO_USE_GPU", "")
		t.Setenv("VIDEO_USE_VIDEOTOOLBOX", "on")
		got := TranscodeArgs("/in.mp4", "/out.mp4", map[string]any{"video_codec": "libx265"})
		want := []string{
			"-i", "/in.mp4",
			"-c:v", "hevc_videotoolbox",
			"-b:v", "4M",
			"-pix_fmt", "yuv420p",
			"-movflags", "+faststart",
			"-color_primaries", "bt709",
			"-color_trc", "bt709",
			"-colorspace", "bt709",
			"-c:a", "aac",
			"/out.mp4",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("args = %#v", got)
		}
	})
}

func TestRunFFmpegRetriesHardwareCapacityFailureOnCPU(t *testing.T) {
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

	err := runFFmpeg(context.Background(), vpffmpeg.Runner{Binary: scriptPath, PreArgs: nil}, []string{
		"-c:v", "h264_nvenc",
		"-rc:v", "vbr",
		"-cq:v", "23",
		"-preset", "fast",
	})

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

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
