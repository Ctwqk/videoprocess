package handlers

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func scaleFilter(width string, height string, forceOriginalAspectRatio string) string {
	parts := []string{"scale=" + width + ":" + height}
	if forceOriginalAspectRatio != "" {
		parts = append(parts, "force_original_aspect_ratio="+forceOriginalAspectRatio)
	}
	parts = append(parts, "flags=lanczos")
	return strings.Join(parts, ":")
}

func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case nil:
		return fallback
	case int:
		return typed
	case int8:
		return int(typed)
	case int16:
		return int(typed)
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case uint:
		return int(typed)
	case uint8:
		return int(typed)
	case uint16:
		return int(typed)
	case uint32:
		return int(typed)
	case uint64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		if parsed, err := typed.Int64(); err == nil {
			return int(parsed)
		}
		if parsed, err := typed.Float64(); err == nil {
			return int(parsed)
		}
	case string:
		trimmed := strings.TrimSpace(typed)
		if parsed, err := strconv.Atoi(trimmed); err == nil {
			return parsed
		}
		if parsed, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return int(parsed)
		}
	}
	return fallback
}

func intString(value any, fallback int) string {
	return strconv.Itoa(intValue(value, fallback))
}

func floatValue(value any, fallback float64) float64 {
	switch typed := value.(type) {
	case nil:
		return fallback
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int8:
		return float64(typed)
	case int16:
		return float64(typed)
	case int32:
		return float64(typed)
	case int64:
		return float64(typed)
	case uint:
		return float64(typed)
	case uint8:
		return float64(typed)
	case uint16:
		return float64(typed)
	case uint32:
		return float64(typed)
	case uint64:
		return float64(typed)
	case json.Number:
		if parsed, err := typed.Float64(); err == nil {
			return parsed
		}
	case string:
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func boolValue(value any, fallback bool) bool {
	switch typed := value.(type) {
	case nil:
		return fallback
	case bool:
		return typed
	}
	return strings.Contains("|1|true|yes|on|", "|"+strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))+"|")
}

func stringValue(value any, fallback string) string {
	switch typed := value.(type) {
	case nil:
		return fallback
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case json.Number:
		return typed.String()
	}
	return fallback
}

func escapeDrawText(text string) string {
	text = strings.ReplaceAll(text, `\`, `\\`)
	text = strings.ReplaceAll(text, ":", `\:`)
	text = strings.ReplaceAll(text, "'", `\'`)
	return text
}

func videoEncodeArgs(codec string, preset string, crf int, bitrate string, mp4Compatible bool) []string {
	if codec == "" {
		codec = "libx264"
	}
	if preset == "" {
		preset = "medium"
	}
	args := []string{"-c:v", codec}
	switch codec {
	case "libx264", "libx265":
		args = append(args, "-crf", strconv.Itoa(crf), "-preset", preset)
	case "h264_nvenc", "hevc_nvenc":
		args = append(args, "-rc:v", "vbr", "-cq:v", strconv.Itoa(crf), "-preset", preset)
	case "h264_videotoolbox", "hevc_videotoolbox":
		args = append(args, "-b:v", defaultVideotoolboxBitrate(codec, bitrate))
	}
	if bitrate != "" && codec != "h264_videotoolbox" && codec != "hevc_videotoolbox" {
		args = append(args, "-b:v", bitrate)
	}
	if mp4Compatible && isMP4CompatibleCodec(codec) {
		args = append(args,
			"-pix_fmt", "yuv420p",
			"-movflags", "+faststart",
			"-color_primaries", "bt709",
			"-color_trc", "bt709",
			"-colorspace", "bt709",
		)
	}
	return args
}

func intermediateVideoEncodeArgs(codec string) []string {
	return videoEncodeArgs(codec, "slow", 18, "", true)
}

func finalVideoEncodeArgs(codec string) []string {
	return videoEncodeArgs(codec, "medium", 20, "", true)
}

func defaultVideotoolboxBitrate(codec string, bitrate string) string {
	if bitrate != "" {
		return bitrate
	}
	if codec == "hevc_videotoolbox" {
		return "4M"
	}
	return "6M"
}

func isMP4CompatibleCodec(codec string) bool {
	switch codec {
	case "libx264", "libx265", "h264_nvenc", "hevc_nvenc", "h264_videotoolbox", "hevc_videotoolbox":
		return true
	default:
		return false
	}
}
