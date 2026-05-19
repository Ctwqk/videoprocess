package worker

import (
	"testing"
	"time"
)

func TestLoadConfigIncludesDatabaseAndStorage(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://vp:test@localhost:5432/videoprocess")
	t.Setenv("STORAGE_BACKEND", "local")
	t.Setenv("STORAGE_LOCAL_ROOT", "/tmp/vp-test")

	cfg := LoadConfig()

	if cfg.DatabaseURL == "" || cfg.StorageLocalRoot != "/tmp/vp-test" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestLoadConfigProductionRuntimeDefaults(t *testing.T) {
	t.Setenv("WORKER_TYPE", "")
	t.Setenv("WORKER_CONCURRENCY", "")
	t.Setenv("WORKER_PEL_MIN_IDLE_MS", "")
	t.Setenv("WORKER_PEL_RECLAIM_INTERVAL_SECONDS", "")
	t.Setenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", "")
	t.Setenv("WORKER_AFFINITY_WAIT_SECONDS", "")
	t.Setenv("WORKER_AFFINITY_MAX_BOUNCES", "")
	t.Setenv("WORKER_SHUTDOWN_GRACE_SECONDS", "")
	t.Setenv("WORKER_CANCEL_POLL_SECONDS", "")

	cfg := LoadConfig()

	if cfg.WorkerType != "ffmpeg_go" {
		t.Fatalf("WorkerType = %q", cfg.WorkerType)
	}
	if cfg.Concurrency != 2 {
		t.Fatalf("Concurrency = %d", cfg.Concurrency)
	}
	if cfg.PELMinIdle != 15*time.Minute {
		t.Fatalf("PELMinIdle = %s", cfg.PELMinIdle)
	}
	if cfg.HeartbeatInterval != 15*time.Second {
		t.Fatalf("HeartbeatInterval = %s", cfg.HeartbeatInterval)
	}
	if cfg.AffinityMaxBounces != 6 {
		t.Fatalf("AffinityMaxBounces = %d", cfg.AffinityMaxBounces)
	}
}

func TestLoadConfigProductionRuntimeOverrides(t *testing.T) {
	t.Setenv("WORKER_CONCURRENCY", "4")
	t.Setenv("WORKER_PEL_MIN_IDLE_MS", "120000")
	t.Setenv("WORKER_PEL_RECLAIM_INTERVAL_SECONDS", "5")
	t.Setenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", "3")
	t.Setenv("WORKER_AFFINITY_WAIT_SECONDS", "7")
	t.Setenv("WORKER_AFFINITY_MAX_BOUNCES", "2")
	t.Setenv("WORKER_SHUTDOWN_GRACE_SECONDS", "9")
	t.Setenv("WORKER_CANCEL_POLL_SECONDS", "1")

	cfg := LoadConfig()

	if cfg.Concurrency != 4 || cfg.PELMinIdle != 2*time.Minute || cfg.PELReclaimInterval != 5*time.Second {
		t.Fatalf("config = %#v", cfg)
	}
	if cfg.HeartbeatInterval != 3*time.Second || cfg.AffinityWait != 7*time.Second {
		t.Fatalf("config = %#v", cfg)
	}
	if cfg.ShutdownGracePeriod != 9*time.Second || cfg.CancelPollInterval != time.Second {
		t.Fatalf("config = %#v", cfg)
	}
}
