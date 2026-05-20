package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type ExportHandler struct {
	Runner         vpffmpeg.Runner
	QualityService ExportQualityService
}

type ExportQualityService interface {
	QAExport(ctx context.Context, sourcePath string, outputPath string, config map[string]any) (ExportQualityResult, error)
}

type ExportQualityResult struct {
	Report       map[string]any
	RepairedPath string
}

func (h ExportHandler) NodeType() string {
	return "export"
}

func (h ExportHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	inputPath := inputPaths["input"]
	if inputPath == "" {
		return nil, errors.New("missing input path on input port")
	}

	outputDir := stringValue(config["output_dir"], "/tmp/vp_export")
	filename := stringValue(config["filename"], "")
	if filename == "" {
		filename = filepath.Base(inputPath)
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create export directory: %w", err)
	}

	exportPath := filepath.Join(outputDir, filename)
	if err := copyFile(inputPath, exportPath); err != nil {
		return nil, fmt.Errorf("copy export artifact: %w", err)
	}
	if err := copyFile(inputPath, outputPath); err != nil {
		return nil, fmt.Errorf("copy tracked artifact: %w", err)
	}
	qualityService := h.QualityService
	if qualityService == nil {
		qualityService = MediaQualityService{Runner: h.Runner}
	}
	qaResult, err := qualityService.QAExport(ctx, inputPath, outputPath, config)
	if err != nil {
		return nil, fmt.Errorf("export quality check: %w", err)
	}
	if qaResult.RepairedPath != "" {
		defer os.Remove(qaResult.RepairedPath)
		if err := copyFile(qaResult.RepairedPath, exportPath); err != nil {
			return nil, fmt.Errorf("copy repaired export artifact: %w", err)
		}
		if err := copyFile(qaResult.RepairedPath, outputPath); err != nil {
			return nil, fmt.Errorf("copy repaired tracked artifact: %w", err)
		}
	}
	return map[string]any{"quality_report": qaResult.Report}, nil
}

func copyFile(src string, dst string) error {
	if same, err := sameFilePath(src, dst); err != nil {
		return err
	} else if same {
		return fmt.Errorf("source and destination are the same file: %s", src)
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func sameFilePath(src string, dst string) (bool, error) {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return false, err
	}
	dstInfo, err := os.Stat(dst)
	if err == nil {
		return os.SameFile(srcInfo, dstInfo), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	absSrc, srcErr := filepath.Abs(src)
	if srcErr != nil {
		return false, srcErr
	}
	absDst, dstErr := filepath.Abs(dst)
	if dstErr != nil {
		return false, dstErr
	}
	return absSrc == absDst, nil
}

type MediaQualityService struct {
	Runner vpffmpeg.Runner
}

type qualityQAConfig struct {
	Enabled           bool
	GateMode          string
	VMAFMinScore      float64
	LoudnormTargetI   float64
	LoudnormTargetLRA float64
	LoudnormTargetTP  float64
}

func qualityConfig(config map[string]any) qualityQAConfig {
	return qualityQAConfig{
		Enabled:           boolValue(config["enable_quality_qa"], true),
		GateMode:          stringValue(config["quality_gate_mode"], "soft_repair_once"),
		VMAFMinScore:      finiteFloatValue(config["vmaf_min_score"], 80),
		LoudnormTargetI:   finiteFloatValue(config["loudnorm_target_i"], -16),
		LoudnormTargetLRA: finiteFloatValue(config["loudnorm_target_lra"], 11),
		LoudnormTargetTP:  finiteFloatValue(config["loudnorm_target_tp"], -1.5),
	}
}

func (s MediaQualityService) QAExport(ctx context.Context, sourcePath string, outputPath string, config map[string]any) (ExportQualityResult, error) {
	qaConfig := qualityConfig(config)
	report := baseQualityReport(qaConfig)
	if !qaConfig.Enabled {
		report["qa_action"] = "disabled"
		return ExportQualityResult{Report: report}, nil
	}

	vmafScore, err := s.measureVMAF(ctx, sourcePath, outputPath)
	if err != nil {
		appendQualityWarning(report, "vmaf_unavailable")
	} else if vmafScore == nil {
		appendQualityWarning(report, "vmaf_unavailable")
	} else {
		report["vmaf_score"] = round3(*vmafScore)
	}

	loudnormStats, err := s.measureLoudnorm(ctx, outputPath, qaConfig)
	if err != nil {
		appendQualityWarning(report, "loudnorm_measure_failed")
	}
	if loudnormStats != nil && loudnormStatsAreFinite(loudnormStats) {
		report["audio_lufs"] = finiteFloatString(loudnormStats["input_i"], 0)
		report["audio_true_peak"] = finiteFloatString(loudnormStats["input_tp"], 0)
		report["audio_lra"] = finiteFloatString(loudnormStats["input_lra"], 0)
	} else if loudnormStats != nil {
		loudnormStats = nil
		appendQualityWarning(report, "loudnorm_non_finite")
	}

	needsRepair := qualityNeedsRepair(report, qaConfig)
	if !needsRepair || qaConfig.GateMode != "soft_repair_once" {
		if needsRepair {
			report["qa_action"] = "warning_only"
		} else {
			report["qa_action"] = "passed"
		}
		return ExportQualityResult{Report: report}, nil
	}

	report["reencode_attempted"] = true
	repairedPath, err := s.repairExport(ctx, outputPath, qaConfig, loudnormStats)
	if err != nil {
		report["qa_action"] = "repair_failed"
		appendQualityWarning(report, "repair_failed")
		return ExportQualityResult{Report: report}, nil
	}
	report["qa_action"] = "reencoded_once"
	return ExportQualityResult{Report: report, RepairedPath: repairedPath}, nil
}

func (s MediaQualityService) run(ctx context.Context, args []string) (vpffmpeg.RunResult, error) {
	runner := s.Runner
	if runner.Binary == "" {
		runner = vpffmpeg.NewRunner()
	}
	return runner.Run(ctx, args)
}

func (s MediaQualityService) measureVMAF(ctx context.Context, sourcePath string, outputPath string) (*float64, error) {
	if sourcePath == "" || outputPath == "" {
		return nil, nil
	}
	if _, err := os.Stat(sourcePath); err != nil {
		return nil, nil
	}
	if _, err := os.Stat(outputPath); err != nil {
		return nil, nil
	}
	logFile, err := os.CreateTemp("", "vp_vmaf_*.json")
	if err != nil {
		return nil, err
	}
	logPath := logFile.Name()
	if err := logFile.Close(); err != nil {
		_ = os.Remove(logPath)
		return nil, err
	}
	defer os.Remove(logPath)

	_, err = s.run(ctx, []string{
		"-i", outputPath,
		"-i", sourcePath,
		"-lavfi", "libvmaf=log_fmt=json:log_path=" + logPath,
		"-f", "null",
		"-",
	})
	if err != nil {
		return nil, err
	}
	return parseVMAFScore(logPath)
}

func parseVMAFScore(logPath string) (*float64, error) {
	raw, err := os.ReadFile(logPath)
	if err != nil {
		return nil, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, nil
	}
	pooled, ok := payload["pooled_metrics"].(map[string]any)
	if !ok {
		return nil, nil
	}
	vmaf, ok := pooled["vmaf"].(map[string]any)
	if !ok {
		return nil, nil
	}
	mean, ok := vmaf["mean"].(float64)
	if !ok {
		return nil, nil
	}
	return &mean, nil
}

func (s MediaQualityService) measureLoudnorm(ctx context.Context, outputPath string, config qualityQAConfig) (map[string]string, error) {
	result, err := s.run(ctx, []string{
		"-i", outputPath,
		"-af", fmt.Sprintf(
			"loudnorm=I=%s:LRA=%s:TP=%s:print_format=json",
			formatNumber(config.LoudnormTargetI),
			formatNumber(config.LoudnormTargetLRA),
			formatNumber(config.LoudnormTargetTP),
		),
		"-f", "null",
		"-",
	})
	if err != nil {
		return nil, err
	}
	return parseLoudnormJSON(result.Stderr), nil
}

var jsonObjectStart = regexp.MustCompile(`\{`)

func parseLoudnormJSON(stderr string) map[string]string {
	required := []string{"input_i", "input_tp", "input_lra", "input_thresh", "target_offset"}
	for _, loc := range jsonObjectStart.FindAllStringIndex(stderr, -1) {
		decoder := json.NewDecoder(strings.NewReader(stderr[loc[0]:]))
		var payload map[string]any
		if err := decoder.Decode(&payload); err != nil {
			continue
		}
		stats := map[string]string{}
		ok := true
		for _, key := range required {
			value, exists := payload[key]
			if !exists {
				ok = false
				break
			}
			stats[key] = fmt.Sprint(value)
		}
		if ok {
			return stats
		}
	}
	return nil
}

func (s MediaQualityService) repairExport(ctx context.Context, outputPath string, config qualityQAConfig, loudnormStats map[string]string) (string, error) {
	ext := filepath.Ext(outputPath)
	if ext == "" {
		ext = ".mp4"
	}
	repaired, err := os.CreateTemp("", "vp_export_repaired_*"+ext)
	if err != nil {
		return "", err
	}
	repairedPath := repaired.Name()
	if err := repaired.Close(); err != nil {
		_ = os.Remove(repairedPath)
		return "", err
	}
	args := []string{"-i", outputPath}
	if loudnormStats != nil {
		args = append(args, "-af", buildLoudnormApplyFilter(loudnormStats, config))
	}
	args = append(args,
		"-map", "0:v:0",
		"-map", "0:a?",
	)
	args = append(args, intermediateVideoEncodeArgs("libx264")...)
	args = append(args,
		"-c:a", "aac",
		"-ar", "48000",
		"-ac", "2",
		repairedPath,
	)
	if _, err := s.run(ctx, args); err != nil {
		_ = os.Remove(repairedPath)
		return "", err
	}
	return repairedPath, nil
}

func buildLoudnormApplyFilter(stats map[string]string, config qualityQAConfig) string {
	return fmt.Sprintf(
		"loudnorm=I=%s:LRA=%s:TP=%s:measured_I=%s:measured_LRA=%s:measured_TP=%s:measured_thresh=%s:offset=%s:linear=true:print_format=summary",
		formatNumber(config.LoudnormTargetI),
		formatNumber(config.LoudnormTargetLRA),
		formatNumber(config.LoudnormTargetTP),
		stats["input_i"],
		stats["input_lra"],
		stats["input_tp"],
		stats["input_thresh"],
		stats["target_offset"],
	)
}

func baseQualityReport(config qualityQAConfig) map[string]any {
	return map[string]any{
		"enabled":            config.Enabled,
		"gate_mode":          config.GateMode,
		"qa_action":          "not_run",
		"reencode_attempted": false,
		"vmaf_score":         nil,
		"audio_lufs":         nil,
		"audio_true_peak":    nil,
		"audio_lra":          nil,
		"thresholds": map[string]any{
			"vmaf_min_score":      config.VMAFMinScore,
			"loudnorm_target_i":   config.LoudnormTargetI,
			"loudnorm_target_lra": config.LoudnormTargetLRA,
			"loudnorm_target_tp":  config.LoudnormTargetTP,
		},
		"warnings": []any{},
	}
}

func appendQualityWarning(report map[string]any, warning string) {
	warnings, ok := report["warnings"].([]any)
	if !ok {
		warnings = []any{}
	}
	report["warnings"] = append(warnings, warning)
}

func qualityNeedsRepair(report map[string]any, config qualityQAConfig) bool {
	if score, ok := report["vmaf_score"].(float64); ok && score < config.VMAFMinScore {
		return true
	}
	if audioLUFS, ok := report["audio_lufs"].(float64); ok && math.Abs(audioLUFS-config.LoudnormTargetI) > 1 {
		return true
	}
	if truePeak, ok := report["audio_true_peak"].(float64); ok && truePeak > config.LoudnormTargetTP+0.5 {
		return true
	}
	return false
}

func loudnormStatsAreFinite(stats map[string]string) bool {
	for _, key := range []string{"input_i", "input_tp", "input_lra", "input_thresh", "target_offset"} {
		value, err := strconv.ParseFloat(stats[key], 64)
		if err != nil || !isFinite(value) {
			return false
		}
	}
	return true
}

func finiteFloatValue(value any, fallback float64) float64 {
	if value == nil || value == "" {
		return fallback
	}
	numeric := floatValue(value, fallback)
	if !isFinite(numeric) {
		return fallback
	}
	return numeric
}

func finiteFloatString(value string, fallback float64) float64 {
	numeric, err := strconv.ParseFloat(value, 64)
	if err != nil || !isFinite(numeric) {
		return fallback
	}
	return numeric
}

func round3(value float64) float64 {
	return math.Round(value*1000) / 1000
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func formatNumber(value float64) string {
	if value == math.Trunc(value) {
		return strconv.Itoa(int(value))
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}
