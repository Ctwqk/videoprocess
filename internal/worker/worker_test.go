package worker

import "testing"

func TestLoadConfigIncludesDatabaseAndStorage(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://vp:test@localhost:5432/videoprocess")
	t.Setenv("STORAGE_BACKEND", "local")
	t.Setenv("STORAGE_LOCAL_ROOT", "/tmp/vp-test")

	cfg := LoadConfig()

	if cfg.DatabaseURL == "" || cfg.StorageLocalRoot != "/tmp/vp-test" {
		t.Fatalf("cfg = %#v", cfg)
	}
}
