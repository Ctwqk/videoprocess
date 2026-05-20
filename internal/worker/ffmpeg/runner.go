package ffmpeg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// ErrCancelled is returned when ffmpeg exited because its context was
// cancelled. The orchestrator must NOT retry these — they are deliberate
// shutdown/cancellation, not transient failures.
var ErrCancelled = errors.New("ffmpeg cancelled")

// RunResult carries ffmpeg execution outcome details. Callers inspect
// GPUCapacity to decide whether to rewrite hardware-codec args and retry on
// CPU, matching `BaseHandler.run_ffmpeg` retry logic in Python.
type RunResult struct {
	Stderr      string
	GPUCapacity bool
}

type Runner struct {
	Binary string
	// ProbeBinary runs ffprobe for metadata lookup. Defaults to ffprobe when
	// unset, or a sibling ffprobe binary when Binary points at an ffmpeg path.
	ProbeBinary string
	// PreArgs is prepended before user-supplied args. Defaults to the
	// ffmpeg flags `-y -hide_banner` when the runner is constructed via
	// NewRunner; tests may set it to nil to use other binaries.
	PreArgs []string
}

func NewRunner() Runner {
	return Runner{Binary: "ffmpeg", ProbeBinary: "ffprobe", PreArgs: []string{"-y", "-hide_banner"}}
}

// Run executes ffmpeg with the supplied args. On non-zero exit it returns
// either ErrCancelled (when ctx is done) or an error wrapping stderr tail.
// RunResult.GPUCapacity is true when stderr looks like an NVENC/VideoToolbox
// capacity error, so the caller can fall back to a CPU encoder.
func (r Runner) Run(ctx context.Context, args []string) (RunResult, error) {
	ffmpegRunsTotal.Inc()
	binary := r.Binary
	if binary == "" {
		binary = "ffmpeg"
	}
	preArgs := r.PreArgs
	if preArgs == nil && binary == "ffmpeg" {
		preArgs = []string{"-y", "-hide_banner"}
	}
	fullArgs := append(append([]string{}, preArgs...), args...)
	cmd := exec.CommandContext(ctx, binary, fullArgs...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := RunResult{Stderr: stderr.String()}
	if err == nil {
		return result, nil
	}
	ffmpegFailuresTotal.Inc()

	// If the context was cancelled, exec returns either context.Canceled or
	// a *exec.ExitError after exec.CommandContext sent SIGKILL. Treat both
	// as cancellation so the orchestrator can skip retries.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return result, fmt.Errorf("%w: %v", ErrCancelled, ctxErr)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return result, fmt.Errorf("%w: %v", ErrCancelled, err)
	}

	result.GPUCapacity = IsGPUCapacityError(result.Stderr)
	if result.GPUCapacity {
		ffmpegGPUFallbacksTotal.Inc()
	}
	return result, fmt.Errorf("ffmpeg failed: %w: %s", err, tail(result.Stderr, 2000))
}

type ProbeResult struct {
	Streams []ProbeStream `json:"streams"`
	Format  ProbeFormat   `json:"format"`
}

type ProbeStream struct {
	CodecType string `json:"codec_type"`
	Width     int    `json:"width,omitempty"`
	Height    int    `json:"height,omitempty"`
}

type ProbeFormat struct {
	Duration string `json:"duration"`
}

func (r Runner) Probe(ctx context.Context, inputPath string) (ProbeResult, error) {
	binary := r.probeBinary()
	cmd := exec.CommandContext(ctx, binary,
		"-v", "error",
		"-show_streams",
		"-show_format",
		"-of", "json",
		inputPath,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ProbeResult{}, fmt.Errorf("%w: %v", ErrCancelled, ctxErr)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return ProbeResult{}, fmt.Errorf("%w: %v", ErrCancelled, err)
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return ProbeResult{}, nil
		}
		return ProbeResult{}, fmt.Errorf("ffprobe failed: %w: %s", err, tail(stderr.String(), 2000))
	}
	var result ProbeResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		return ProbeResult{}, nil
	}
	return result, nil
}

func (r Runner) probeBinary() string {
	if r.ProbeBinary != "" {
		return r.ProbeBinary
	}
	if r.Binary == "" || r.Binary == "ffmpeg" {
		return "ffprobe"
	}
	dir := filepath.Dir(r.Binary)
	base := filepath.Base(r.Binary)
	if base == "ffmpeg" {
		return filepath.Join(dir, "ffprobe")
	}
	if strings.Contains(base, "ffmpeg") {
		return filepath.Join(dir, strings.Replace(base, "ffmpeg", "ffprobe", 1))
	}
	return "ffprobe"
}

func (p ProbeResult) HasAudio() bool {
	for _, stream := range p.Streams {
		if stream.CodecType == "audio" {
			return true
		}
	}
	return false
}

func (p ProbeResult) DurationSeconds() float64 {
	if p.Format.Duration == "" {
		return 0
	}
	value, err := strconv.ParseFloat(p.Format.Duration, 64)
	if err != nil {
		return 0
	}
	return value
}

func (r Runner) CountVideoFrames(ctx context.Context, inputPath string) (int, error) {
	binary := r.probeBinary()
	cmd := exec.CommandContext(ctx, binary,
		"-v", "error",
		"-select_streams", "v:0",
		"-count_frames",
		"-show_entries", "stream=nb_read_frames",
		"-of", "default=nokey=1:noprint_wrappers=1",
		inputPath,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, fmt.Errorf("%w: %v", ErrCancelled, ctxErr)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, fmt.Errorf("%w: %v", ErrCancelled, err)
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return 0, nil
		}
		return 0, fmt.Errorf("ffprobe count frames failed: %w: %s", err, tail(stderr.String(), 2000))
	}
	raw := strings.TrimSpace(stdout.String())
	count, err := strconv.Atoi(raw)
	if err != nil {
		return 0, nil
	}
	return count, nil
}

// gpuCapacityIndicators mirrors the keyword list in
// backend/worker/handlers/base.py `_is_gpu_capacity_error`. Keep in sync when
// either side changes.
var gpuCapacityIndicators = []string{
	"openencodesessionex failed",
	"no nvenc capable devices found",
	"device busy",
	"resource temporarily unavailable",
	"cannot init cuda",
	"cuda_error_out_of_memory",
	"out of memory",
	"nvenc",
	"videotoolbox",
	"videotoolbox encoder",
	"hardware encoder may be busy",
	"error while opening encoder for output stream",
}

// IsGPUCapacityError detects ffmpeg stderr fragments that indicate an
// exhausted/busy hardware encoder rather than a content problem.
func IsGPUCapacityError(stderrText string) bool {
	if stderrText == "" {
		return false
	}
	lowered := strings.ToLower(stderrText)
	for _, fragment := range gpuCapacityIndicators {
		if strings.Contains(lowered, fragment) {
			return true
		}
	}
	return false
}

// RewriteHardwareArgsForCPU drops NVENC/VideoToolbox-specific flags and
// switches the codec back to its CPU equivalent. NVENC `-cq:v` is mapped to a
// libx264-friendly CRF when no explicit `-crf` was supplied. Mirrors
// `BaseHandler._rewrite_hardware_args_for_cpu` in Python.
func RewriteHardwareArgsForCPU(args []string) []string {
	cpuFor := func(c string) string {
		switch c {
		case "h264_nvenc", "h264_videotoolbox":
			return "libx264"
		case "hevc_nvenc", "hevc_videotoolbox":
			return "libx265"
		}
		return c
	}
	rewritten := make([]string, 0, len(args))
	var removedCQ string
	hasCRF := false
	for i := 0; i < len(args); i++ {
		tok := args[i]
		var next string
		if i+1 < len(args) {
			next = args[i+1]
		}
		switch tok {
		case "-c:v":
			if next == "" {
				rewritten = append(rewritten, tok)
				continue
			}
			rewritten = append(rewritten, tok, cpuFor(next))
			i++
		case "-crf":
			hasCRF = true
			rewritten = append(rewritten, tok, next)
			i++
		case "-cq:v":
			removedCQ = next
			i++
		case "-rc:v":
			i++
		default:
			rewritten = append(rewritten, tok)
		}
	}
	if removedCQ != "" && !hasCRF {
		mapped := nvencCQToLibx264CRF(removedCQ)
		insertAt := len(rewritten)
		for i := 0; i+1 < len(rewritten); i++ {
			if rewritten[i] == "-c:v" && (rewritten[i+1] == "libx264" || rewritten[i+1] == "libx265") {
				insertAt = i + 2
				break
			}
		}
		rewritten = append(rewritten[:insertAt], append([]string{"-crf", mapped}, rewritten[insertAt:]...)...)
	}
	return rewritten
}

// nvencCQToLibx264CRF mirrors Python `_nvenc_cq_to_libx264_crf`: NVENC CQ
// roughly maps to a libx264 CRF two steps lower (slightly lower-quality), so
// we subtract 2 with a floor of 18.
func nvencCQToLibx264CRF(cq string) string {
	n := 0
	for i := 0; i < len(cq); i++ {
		c := cq[i]
		if c < '0' || c > '9' {
			return "20"
		}
		n = n*10 + int(c-'0')
	}
	if n < 20 {
		return "18"
	}
	mapped := n - 2
	if mapped < 18 {
		mapped = 18
	}
	out := []byte{}
	for mapped > 0 {
		out = append([]byte{byte('0' + mapped%10)}, out...)
		mapped /= 10
	}
	if len(out) == 0 {
		return "0"
	}
	return string(out)
}

func tail(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[len(value)-max:]
}
