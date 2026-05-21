package channelops

import (
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		DatabaseURL:             "postgresql://vp:vp@localhost:5432/vp",
		YouTubeManagerURL:       "http://youtube:8899",
		LiveMode:                true,
		PDSTimeout:              500 * time.Millisecond,
		RunnerPollSeconds:       5,
		SchedulerPollSeconds:    60,
		MaxQueueAttempts:        3,
		MetricsPollMaxAttempts:  24,
		MetricsPollDelayMinutes: 60,
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://vp:vp@localhost:5432/vp")
	t.Setenv("YOUTUBE_MANAGER_URL", "http://youtube:8899")
	cfg := LoadConfig()
	if cfg.DatabaseURL != "postgresql://vp:vp@localhost:5432/vp" {
		t.Fatalf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.YouTubeManagerURL != "http://youtube:8899" {
		t.Fatalf("YouTubeManagerURL = %q", cfg.YouTubeManagerURL)
	}
	if cfg.RunnerPollSeconds != 5 {
		t.Fatalf("RunnerPollSeconds = %v", cfg.RunnerPollSeconds)
	}
	if cfg.SchedulerPollSeconds != 60 {
		t.Fatalf("SchedulerPollSeconds = %v", cfg.SchedulerPollSeconds)
	}
	if cfg.MaxQueueAttempts != 3 {
		t.Fatalf("MaxQueueAttempts = %v, want Python default 3", cfg.MaxQueueAttempts)
	}
	if cfg.DevAllowAllPDS {
		t.Fatal("DevAllowAllPDS default should be false")
	}
}

func TestValidateLiveRequiresYouTubeManagerURL(t *testing.T) {
	cfg := validConfig()
	cfg.YouTubeManagerURL = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to reject missing YouTubeManagerURL")
	}
	cfg.YouTubeManagerURL = "http://youtube:8899"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestValidateRejectsNonPositivePollsAndAttempts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "runner poll seconds",
			mutate: func(cfg *Config) {
				cfg.RunnerPollSeconds = 0
			},
		},
		{
			name: "scheduler poll seconds",
			mutate: func(cfg *Config) {
				cfg.SchedulerPollSeconds = 0
			},
		},
		{
			name: "max queue attempts",
			mutate: func(cfg *Config) {
				cfg.MaxQueueAttempts = 0
			},
		},
		{
			name: "metrics poll max attempts",
			mutate: func(cfg *Config) {
				cfg.MetricsPollMaxAttempts = 0
			},
		},
		{
			name: "metrics poll delay minutes",
			mutate: func(cfg *Config) {
				cfg.MetricsPollDelayMinutes = 0
			},
		},
		{
			name: "pds timeout",
			mutate: func(cfg *Config) {
				cfg.PDSTimeout = 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected Validate to reject non-positive config")
			}
		})
	}
}
