package config

import "testing"

func TestLoadUsesPythonCompatibleDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "")
	t.Setenv("STORAGE_BACKEND", "")

	cfg := Load()

	if cfg.StorageBackend != "local" {
		t.Fatalf("StorageBackend = %q", cfg.StorageBackend)
	}
	if cfg.StorageLocalRoot != "/tmp/vp_storage" {
		t.Fatalf("StorageLocalRoot = %q", cfg.StorageLocalRoot)
	}
	if cfg.VideoGPUFallbackToCPU != true {
		t.Fatalf("VideoGPUFallbackToCPU = false")
	}
}

func TestBoolEnvAcceptsPythonStyleTruth(t *testing.T) {
	t.Setenv("VIDEO_USE_GPU", "yes")

	cfg := Load()

	if !cfg.VideoUseGPU {
		t.Fatalf("VideoUseGPU = false")
	}
}
