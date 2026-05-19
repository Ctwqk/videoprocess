package ffmpeg

import "strconv"

type EncodeConfig struct {
	UseGPU          bool
	UseVideotoolbox bool
	Codec           string
	Preset          string
	CRF             int
	Bitrate         string
	MP4Compatible   bool
}

func VideoEncodeArgs(cfg EncodeConfig) []string {
	codec := preferredCodec(cfg)
	args := []string{"-c:v", codec}
	if codec == "libx264" || codec == "libx265" {
		args = append(args, "-crf", itoa(cfg.CRF), "-preset", defaultString(cfg.Preset, "medium"))
	}
	if codec == "h264_nvenc" || codec == "hevc_nvenc" {
		args = append(args, "-rc:v", "vbr", "-cq:v", itoa(cfg.CRF), "-preset", defaultString(cfg.Preset, "medium"))
	}
	if codec == "h264_videotoolbox" || codec == "hevc_videotoolbox" {
		args = append(args, "-b:v", defaultString(cfg.Bitrate, "6M"))
	}
	if cfg.Bitrate != "" && codec != "h264_videotoolbox" && codec != "hevc_videotoolbox" {
		args = append(args, "-b:v", cfg.Bitrate)
	}
	if cfg.MP4Compatible {
		args = append(args, "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709")
	}
	return args
}

func preferredCodec(cfg EncodeConfig) string {
	codec := defaultString(cfg.Codec, "libx264")
	if cfg.UseGPU {
		if codec == "libx264" {
			return "h264_nvenc"
		}
		if codec == "libx265" {
			return "hevc_nvenc"
		}
	}
	if cfg.UseVideotoolbox {
		if codec == "libx264" {
			return "h264_videotoolbox"
		}
		if codec == "libx265" {
			return "hevc_videotoolbox"
		}
	}
	return codec
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func itoa(value int) string {
	if value == 0 {
		value = 20
	}
	return strconv.Itoa(value)
}
