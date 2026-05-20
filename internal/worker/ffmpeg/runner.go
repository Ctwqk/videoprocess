package ffmpeg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
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
	// PreArgs is prepended before user-supplied args. Defaults to the
	// ffmpeg flags `-y -hide_banner` when the runner is constructed via
	// NewRunner; tests may set it to nil to use other binaries.
	PreArgs []string
}

func NewRunner() Runner {
	return Runner{Binary: "ffmpeg", PreArgs: []string{"-y", "-hide_banner"}}
}

// Run executes ffmpeg with the supplied args. On non-zero exit it returns
// either ErrCancelled (when ctx is done) or an error wrapping stderr tail.
// RunResult.GPUCapacity is true when stderr looks like an NVENC/VideoToolbox
// capacity error, so the caller can fall back to a CPU encoder.
func (r Runner) Run(ctx context.Context, args []string) (RunResult, error) {
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
	return result, fmt.Errorf("ffmpeg failed: %w: %s", err, tail(result.Stderr, 2000))
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
