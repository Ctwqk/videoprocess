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

func TestAPIGoAllowStubStoreDefaultsFalse(t *testing.T) {
	t.Setenv("VP_API_GO_ALLOW_STUB_STORE", "")

	cfg := Load()

	if cfg.APIGoAllowStubStore {
		t.Fatal("APIGoAllowStubStore must default false so production read APIs fail closed")
	}
}

func TestAPIGoAllowStubStoreReadsTruthyValues(t *testing.T) {
	t.Setenv("VP_API_GO_ALLOW_STUB_STORE", "true")

	cfg := Load()

	if !cfg.APIGoAllowStubStore {
		t.Fatal("APIGoAllowStubStore should read true")
	}
}

func TestGoOrchestratorFlagsDefaultClosed(t *testing.T) {
	t.Setenv("VP_GO_ORCHESTRATOR_ENABLED", "")
	t.Setenv("VP_GO_ORCHESTRATOR_JOB_WRITES", "")
	t.Setenv("VP_GO_EVENT_STREAM", "")
	t.Setenv("VP_GO_ORCHESTRATOR_RECOVERY_INTERVAL_SECONDS", "")
	t.Setenv("VP_GO_ORCHESTRATOR_STALE_NODE_SECONDS", "")

	cfg := Load()

	if cfg.GoOrchestratorEnabled {
		t.Fatal("GoOrchestratorEnabled must default false")
	}
	if cfg.GoOrchestratorJobWrites {
		t.Fatal("GoOrchestratorJobWrites must default false")
	}
	if cfg.GoEventStream != "vp:events:go" {
		t.Fatalf("GoEventStream = %q", cfg.GoEventStream)
	}
	if cfg.GoOrchestratorRecoveryIntervalSeconds != 60 {
		t.Fatalf("GoOrchestratorRecoveryIntervalSeconds = %d", cfg.GoOrchestratorRecoveryIntervalSeconds)
	}
	if cfg.GoOrchestratorStaleNodeSeconds != 600 {
		t.Fatalf("GoOrchestratorStaleNodeSeconds = %d", cfg.GoOrchestratorStaleNodeSeconds)
	}
}
