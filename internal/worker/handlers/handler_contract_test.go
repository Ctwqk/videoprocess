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

func TestBatch4BInputPorts(t *testing.T) {
	tests := map[string]struct {
		handler interface{ NodeType() string }
		ports   []string
	}{
		"bgm":               {handler: BgmHandler{}, ports: []string{"video", "audio"}},
		"replace_audio":     {handler: ReplaceAudioHandler{}, ports: []string{"video", "audio"}},
		"concat_horizontal": {handler: ConcatHorizontalHandler{}, ports: []string{"video_left", "video_right"}},
		"concat_vertical":   {handler: ConcatVerticalHandler{}, ports: []string{"video_top", "video_bottom"}},
		"concat_many":       {handler: ConcatManyHandler{}, ports: []string{"video_1", "video_2"}},
	}
	for nodeType, tt := range tests {
		t.Run(nodeType, func(t *testing.T) {
			if tt.handler.NodeType() != nodeType {
				t.Fatalf("node type = %q", tt.handler.NodeType())
			}
			if len(tt.ports) < 2 {
				t.Fatalf("%s ports = %#v", nodeType, tt.ports)
			}
		})
	}
}

func TestBatch4BArgs(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		expect []string
	}{
		{
			name: "bgm with original audio ducking and fades",
			args: BgmArgs("/video.mp4", "/music.wav", "/out.mp4", map[string]any{
				"volume": 0.25, "original_volume": 0.8, "loop": true, "fade_in": 1.0, "fade_out": 2.0,
			}, probeSummary{Duration: 12.5, HasAudio: true}),
			expect: []string{
				"-i", "/video.mp4",
				"-stream_loop", "-1", "-i", "/music.wav",
				"-filter_complex", "[0:a]aresample=48000:async=1,aformat=sample_fmts=fltp:channel_layouts=stereo,volume=0.8,asplit=2[orig_mix][orig_sidechain];[1:a]aresample=48000:async=1,aformat=sample_fmts=fltp:channel_layouts=stereo,volume=0.25,afade=t=in:d=1,afade=t=out:st=10.5:d=2[bgm];[bgm][orig_sidechain]sidechaincompress=threshold=0.03:ratio=8:attack=200:release=800[ducked];[orig_mix][ducked]amix=inputs=2:duration=first:normalize=0[mix];[mix]loudnorm=I=-16:LRA=11:TP=-1.5[a]",
				"-map", "0:v", "-map", "[a]",
				"-c:v", "copy",
				"-c:a", "aac",
				"-ar", "48000",
				"-ac", "2",
				"-shortest",
				"/out.mp4",
			},
		},
		{
			name: "replace audio no loop pads to video duration",
			args: ReplaceAudioArgs("/video.mp4", "/audio.wav", "/out.mp4", map[string]any{
				"loop_if_shorter": false, "audio_volume": 0.7,
			}, probeSummary{Duration: 8.25}),
			expect: []string{
				"-i", "/video.mp4",
				"-i", "/audio.wav",
				"-filter_complex", "[1:a]volume=0.7,apad[aout]",
				"-map", "0:v:0",
				"-map", "[aout]",
				"-c:v", "copy",
				"-c:a", "aac",
				"-t", "8.250",
				"/out.mp4",
			},
		},
		{
			name: "horizontal stack mixes both audio tracks",
			args: ConcatStackArgs("/left.mp4", "/right.mp4", "/out.mp4", concatStackConfig{
				PrimaryLabel: "left", SecondaryLabel: "right", StackAxis: "horizontal", ResizeMode: "match_height",
				PrimaryHasAudio: true, SecondaryHasAudio: true,
			}),
			expect: []string{
				"-i", "/left.mp4",
				"-i", "/right.mp4",
				"-filter_complex", "[0:v]scale=-2:480:flags=lanczos[left];[1:v]scale=-2:480:flags=lanczos[right];[left][right]hstack=inputs=2[v];[0:a][1:a]amix=inputs=2:duration=longest:dropout_transition=2[a]",
				"-map", "[v]",
				"-map", "[a]",
				"-c:a", "aac",
				"-c:v", "libx264",
				"-crf", "18",
				"-preset", "slow",
				"-pix_fmt", "yuv420p",
				"-movflags", "+faststart",
				"-color_primaries", "bt709",
				"-color_trc", "bt709",
				"-colorspace", "bt709",
				"/out.mp4",
			},
		},
		{
			name: "vertical stack maps top audio track",
			args: ConcatStackArgs("/top.mp4", "/bottom.mp4", "/out.mp4", concatStackConfig{
				PrimaryLabel: "top", SecondaryLabel: "bottom", StackAxis: "vertical", ResizeMode: "match_width",
				PrimaryHasAudio: true, SecondaryHasAudio: false,
			}),
			expect: []string{
				"-i", "/top.mp4",
				"-i", "/bottom.mp4",
				"-filter_complex", "[0:v]scale=640:-2:flags=lanczos[top];[1:v]scale=640:-2:flags=lanczos[bottom];[top][bottom]vstack=inputs=2[v]",
				"-map", "[v]",
				"-map", "0:a:0",
				"-c:a", "aac",
				"-c:v", "libx264",
				"-crf", "18",
				"-preset", "slow",
				"-pix_fmt", "yuv420p",
				"-movflags", "+faststart",
				"-color_primaries", "bt709",
				"-color_trc", "bt709",
				"-colorspace", "bt709",
				"/out.mp4",
			},
		},
		{
			name: "concat many normalizes selected inputs with finite silence",
			args: mustConcatManyArgs(t, map[string]string{"video_2": "/2.mp4", "video_1": "/1.mp4"}, "/out.mp4", map[string]any{"target_duration": 5.5}),
			expect: []string{
				"-i", "/1.mp4",
				"-i", "/2.mp4",
				"-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000:duration=5.5",
				"-filter_complex", "[0:v]scale=1080:1920:force_original_aspect_ratio=decrease:flags=lanczos,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1[v0];[1:v]scale=1080:1920:force_original_aspect_ratio=decrease:flags=lanczos,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1[v1];[v0][v1]concat=n=2:v=1:a=0[v]",
				"-map", "[v]",
				"-map", "2:a",
				"-c:v", "libx264",
				"-crf", "18",
				"-preset", "slow",
				"-pix_fmt", "yuv420p",
				"-movflags", "+faststart",
				"-color_primaries", "bt709",
				"-color_trc", "bt709",
				"-colorspace", "bt709",
				"-c:a", "aac",
				"-shortest",
				"-t", "5.5",
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

func TestConcatManyAutoDimensionsAndMetadataSilence(t *testing.T) {
	config := map[string]any{
		"aspect_ratio": "auto",
		"_input_artifact_meta": map[string]any{
			"video_1": map[string]any{"width": 1920.0, "height": 1080.0, "duration": 1.25},
			"video_2": map[string]any{"width": 1280.0, "height": 720.0, "duration": 2.75},
		},
	}
	items := []inputItem{{handle: "video_1", path: "/1.mp4"}, {handle: "video_2", path: "/2.mp4"}}
	width, height := targetDimensions(config, []string{"video_1", "video_2"})
	if width != 1920 || height != 1080 {
		t.Fatalf("dimensions = %dx%d", width, height)
	}
	if got := silenceSource(silenceDuration(config, items, nil)); got != "anullsrc=channel_layout=stereo:sample_rate=48000:duration=4" {
		t.Fatalf("silence source = %q", got)
	}
}

func TestTimelineTempConcatFileContents(t *testing.T) {
	got := concatDemuxerFileContent([]string{"/a.mp4", "/b c.mp4"})
	want := "file '/a.mp4'\nfile '/b c.mp4'\n"
	if got != want {
		t.Fatalf("concat file = %q", got)
	}
}

func TestMontageDimensions(t *testing.T) {
	tests := []struct {
		config map[string]any
		width  int
		height int
	}{
		{map[string]any{}, 1080, 1920},
		{map[string]any{"aspect_ratio": "16:9"}, 1920, 1080},
		{map[string]any{"aspect_ratio": "1:1"}, 1080, 1080},
		{map[string]any{"width": 720.0, "height": 1280.0}, 720, 1280},
	}
	for _, tt := range tests {
		width, height := montageDimensions(tt.config)
		if width != tt.width || height != tt.height {
			t.Fatalf("dimensions = %dx%d", width, height)
		}
	}
}

func TestConcatTimelineTransitionArgs(t *testing.T) {
	got := ConcatTimelineTransitionArgs(
		"/first.mp4",
		"/second.mp4",
		"/out.mp4",
		map[string]any{"transition": "fade", "transition_duration": 0.5},
		probeSummary{Duration: 5, HasAudio: true},
		probeSummary{HasAudio: true},
	)
	want := []string{
		"-i", "/first.mp4",
		"-i", "/second.mp4",
		"-filter_complex", "[0:v]scale=1080:1920:force_original_aspect_ratio=decrease:flags=lanczos,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,settb=AVTB[v0];[1:v]scale=1080:1920:force_original_aspect_ratio=decrease:flags=lanczos,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1,fps=30,settb=AVTB[v1];[v0][v1]xfade=transition=fade:duration=0.5:offset=4.5[v];[0:a][1:a]acrossfade=d=0.5[a]",
		"-map", "[v]",
		"-c:v", "libx264",
		"-crf", "18",
		"-preset", "slow",
		"-pix_fmt", "yuv420p",
		"-movflags", "+faststart",
		"-color_primaries", "bt709",
		"-color_trc", "bt709",
		"-colorspace", "bt709",
		"-map", "[a]",
		"-c:a", "aac",
		"/out.mp4",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v", got)
	}
}

func TestVerticalTimelineSegmentArgsSynthesizesAudio(t *testing.T) {
	got := VerticalTimelineSegmentArgs("/active.mp4", "/still.png", "/segment.mp4", "bottom", 640, 360, "black", probeSummary{Duration: 3.25, HasAudio: false})
	want := []string{
		"-i", "/active.mp4",
		"-loop", "1",
		"-t", "3.250",
		"-i", "/still.png",
		"-f", "lavfi",
		"-t", "3.250",
		"-i", "anullsrc=r=48000:cl=stereo",
		"-filter_complex", "[1:v]scale=640:360:force_original_aspect_ratio=decrease:flags=lanczos,pad=640:360:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,fps=30[top];[0:v]scale=640:360:force_original_aspect_ratio=decrease:flags=lanczos,pad=640:360:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,fps=30[bottom];[top][bottom]vstack=inputs=2,setsar=1[v]",
		"-map", "[v]",
		"-map", "2:a:0",
		"-c:v", "libx264",
		"-crf", "18",
		"-preset", "slow",
		"-pix_fmt", "yuv420p",
		"-movflags", "+faststart",
		"-color_primaries", "bt709",
		"-color_trc", "bt709",
		"-colorspace", "bt709",
		"-c:a", "aac",
		"-shortest",
		"/segment.mp4",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v", got)
	}
}

func TestDefaultFrameIndex(t *testing.T) {
	tests := []struct {
		frameCount    int
		preferFromEnd bool
		want          int
	}{
		{0, true, 0},
		{10, true, 0},
		{20, true, 5},
		{10, false, 9},
		{20, false, 14},
	}
	for _, tt := range tests {
		got := defaultFrameIndex(tt.frameCount, tt.preferFromEnd)
		if got != tt.want {
			t.Fatalf("frame index for count=%d end=%v = %d", tt.frameCount, tt.preferFromEnd, got)
		}
	}
}

func TestConcatManySelectedInputOrder(t *testing.T) {
	inputs := map[string]string{
		"video_10":     "/10.mp4",
		"video_2":      "/2.mp4",
		"video_1":      "/1.mp4",
		"video_first":  "/legacy-first.mp4",
		"video_second": "/legacy-second.mp4",
	}
	got := selectedVideoInputItems(inputs)
	want := []inputItem{
		{handle: "video_1", path: "/1.mp4"},
		{handle: "video_2", path: "/2.mp4"},
		{handle: "video_10", path: "/10.mp4"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selected = %#v", got)
	}
}

func mustConcatManyArgs(t *testing.T, inputPaths map[string]string, outputPath string, config map[string]any) []string {
	t.Helper()
	args, err := ConcatManyArgs(inputPaths, outputPath, config)
	if err != nil {
		t.Fatal(err)
	}
	return args
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
