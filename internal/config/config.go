package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DeployMode                           string
	DatabaseURL                          string
	RedisURL                             string
	StorageBackend                       string
	StorageLocalRoot                     string
	MinIOEndpoint                        string
	MinIOAccessKey                       string
	MinIOSecretKey                       string
	MinIOBucket                          string
	MinIOSecure                          bool
	APIHost                              string
	APIPort                              int
	APIGoAllowStubStore                  bool
	ExoWatchdogURL                       string
	YouTubeManagerURL                    string
	PlatformBrowserManagerURL            string
	XPlatformBrowserManagerURL           string
	BilibiliPlatformBrowserManagerURL    string
	XiaohongshuPlatformBrowserManagerURL string
	EmbeddingGatewayURL                  string
	QdrantURL                            string
	MaterialQdrantCollection             string
	VisionEmbeddingURL                   string
	SmartTrimDefaultWorkerType           string
	VideoScheduleDefaultState            string
	VideoUseGPU                          bool
	VideoUseVideotoolbox                 bool
	VideoGPUFallbackToCPU                bool
}

func Load() Config {
	return Config{
		DeployMode:                           env("DEPLOY_MODE", "shared"),
		DatabaseURL:                          env("DATABASE_URL", "postgresql://vp:vp_secret@localhost:5435/videoprocess"),
		RedisURL:                             env("REDIS_URL", "redis://localhost:6379/0"),
		StorageBackend:                       env("STORAGE_BACKEND", "local"),
		StorageLocalRoot:                     env("STORAGE_LOCAL_ROOT", "/tmp/vp_storage"),
		MinIOEndpoint:                        env("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey:                       env("MINIO_ACCESS_KEY", "minioadmin"),
		MinIOSecretKey:                       env("MINIO_SECRET_KEY", "minioadmin"),
		MinIOBucket:                          env("MINIO_BUCKET", "videoprocess"),
		MinIOSecure:                          boolEnv("MINIO_SECURE", false),
		APIHost:                              env("API_HOST", "0.0.0.0"),
		APIPort:                              intEnv("API_PORT", 8080),
		APIGoAllowStubStore:                  boolEnv("VP_API_GO_ALLOW_STUB_STORE", false),
		ExoWatchdogURL:                       env("EXO_WATCHDOG_URL", "http://localhost:8000"),
		YouTubeManagerURL:                    env("YOUTUBE_MANAGER_URL", "http://localhost:8899"),
		PlatformBrowserManagerURL:            env("PLATFORM_BROWSER_MANAGER_URL", "http://localhost:8898"),
		XPlatformBrowserManagerURL:           env("X_PLATFORM_BROWSER_MANAGER_URL", ""),
		BilibiliPlatformBrowserManagerURL:    env("BILIBILI_PLATFORM_BROWSER_MANAGER_URL", ""),
		XiaohongshuPlatformBrowserManagerURL: env("XIAOHONGSHU_PLATFORM_BROWSER_MANAGER_URL", ""),
		EmbeddingGatewayURL:                  env("EMBEDDING_GATEWAY_URL", "http://localhost:8080"),
		QdrantURL:                            env("QDRANT_URL", "http://localhost:6333"),
		MaterialQdrantCollection:             env("MATERIAL_QDRANT_COLLECTION", "videoprocess_material_clips"),
		VisionEmbeddingURL:                   env("VISION_EMBEDDING_URL", ""),
		SmartTrimDefaultWorkerType:           env("SMART_TRIM_DEFAULT_WORKER_TYPE", "vision"),
		VideoScheduleDefaultState:            env("VIDEO_SCHEDULE_DEFAULT_STATE", "OPEN"),
		VideoUseGPU:                          boolEnv("VIDEO_USE_GPU", false),
		VideoUseVideotoolbox:                 boolEnv("VIDEO_USE_VIDEOTOOLBOX", false),
		VideoGPUFallbackToCPU:                boolEnv("VIDEO_GPU_FALLBACK_TO_CPU", true),
	}
}

func env(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func boolEnv(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func intEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
