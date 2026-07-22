package channelops

import (
	"context"
	"strings"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		DatabaseURL:                  "postgresql://vp:vp@localhost:5432/vp",
		YouTubeManagerURL:            "http://youtube:8899",
		AutoFlowBaseURL:              "http://api:8080",
		AutoFlowTimeout:              10 * time.Second,
		DiscoveryTimeout:             120 * time.Second,
		LiveMode:                     true,
		PDSTimeout:                   500 * time.Millisecond,
		RunnerPollSeconds:            5,
		SchedulerPollSeconds:         60,
		HealthPort:                   8080,
		ThrottleTimeZone:             "America/Los_Angeles",
		ThrottleStartHour:            8,
		ThrottleEndHour:              24,
		ThrottleRunnerPollSeconds:    300,
		ThrottleSchedulerPollSeconds: 1800,
		MaxQueueAttempts:             3,
		MetricsPollMaxAttempts:       24,
		MetricsPollDelayMinutes:      60,
		RetentionQueueDays:           30,
		RetentionAuditDays:           90,
		RetentionFeedbackDays:        365,
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
	if cfg.AutoFlowBaseURL != "http://api:8080" {
		t.Fatalf("AutoFlowBaseURL = %q", cfg.AutoFlowBaseURL)
	}
	if cfg.AutoFlowTimeout != 10*time.Second {
		t.Fatalf("AutoFlowTimeout = %v", cfg.AutoFlowTimeout)
	}
	if cfg.DiscoveryTimeout != 120*time.Second {
		t.Fatalf("DiscoveryTimeout = %v", cfg.DiscoveryTimeout)
	}
	if cfg.RunnerPollSeconds != 5 {
		t.Fatalf("RunnerPollSeconds = %v", cfg.RunnerPollSeconds)
	}
	if cfg.SchedulerPollSeconds != 60 {
		t.Fatalf("SchedulerPollSeconds = %v", cfg.SchedulerPollSeconds)
	}
	if cfg.HealthPort != 8080 {
		t.Fatalf("HealthPort = %v", cfg.HealthPort)
	}
	if cfg.ThrottleEnabled {
		t.Fatal("ThrottleEnabled default should be false")
	}
	if cfg.ThrottleTimeZone != "America/Los_Angeles" {
		t.Fatalf("ThrottleTimeZone = %q", cfg.ThrottleTimeZone)
	}
	if cfg.MaxQueueAttempts != 3 {
		t.Fatalf("MaxQueueAttempts = %v, want Python default 3", cfg.MaxQueueAttempts)
	}
	if cfg.RetentionQueueDays != 30 || cfg.RetentionAuditDays != 90 || cfg.RetentionFeedbackDays != 365 {
		t.Fatalf("retention defaults = %d/%d/%d, want 30/90/365", cfg.RetentionQueueDays, cfg.RetentionAuditDays, cfg.RetentionFeedbackDays)
	}
	if cfg.DevAllowAllPDS {
		t.Fatal("DevAllowAllPDS default should be false")
	}
}

func TestValidateDiscoveryTimeoutRange(t *testing.T) {
	for _, timeout := range []time.Duration{29 * time.Second, 301 * time.Second} {
		cfg := validConfig()
		cfg.DiscoveryTimeout = timeout
		if err := cfg.Validate(); err == nil {
			t.Fatalf("Validate accepted DiscoveryTimeout %s", timeout)
		}
	}
	for _, timeout := range []time.Duration{30 * time.Second, 300 * time.Second} {
		cfg := validConfig()
		cfg.DiscoveryTimeout = timeout
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate(%s): %v", timeout, err)
		}
	}
}

func TestLoadConfigStrictDiscoveryTimeoutEnv(t *testing.T) {
	for _, tt := range []struct {
		name      string
		value     string
		want      time.Duration
		wantError bool
	}{
		{name: "empty uses default", value: "", want: 120 * time.Second},
		{name: "minimum", value: "30", want: 30 * time.Second},
		{name: "maximum", value: "300", want: 300 * time.Second},
		{name: "below minimum", value: "29", want: 29 * time.Second, wantError: true},
		{name: "above maximum", value: "301", want: 301 * time.Second, wantError: true},
		{name: "malformed", value: "not-an-integer", want: 120 * time.Second, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS", tt.value)
			t.Setenv("CHANNELOPS_LIVE_MODE", "false")
			cfg := LoadConfig()
			if cfg.DiscoveryTimeout != tt.want {
				t.Fatalf("DiscoveryTimeout = %s, want %s", cfg.DiscoveryTimeout, tt.want)
			}
			err := cfg.Validate()
			if tt.wantError {
				if err == nil || !strings.Contains(err.Error(), "CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS") {
					t.Fatal("Validate did not reject discovery timeout environment value")
				}
				if tt.name == "malformed" {
					if _, runnerErr := NewRunner(context.Background(), cfg); runnerErr == nil || !strings.Contains(runnerErr.Error(), "CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS") {
						t.Fatal("NewRunner did not reject malformed discovery timeout")
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
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

func TestValidateLiveRequiresAutoFlowBaseURL(t *testing.T) {
	cfg := validConfig()
	cfg.AutoFlowBaseURL = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to reject missing AutoFlowBaseURL")
	}
	cfg.AutoFlowBaseURL = "http://api:8080"
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
			name: "health port",
			mutate: func(cfg *Config) {
				cfg.HealthPort = 0
			},
		},
		{
			name: "throttle start hour",
			mutate: func(cfg *Config) {
				cfg.ThrottleEnabled = true
				cfg.ThrottleStartHour = -1
			},
		},
		{
			name: "throttle end hour",
			mutate: func(cfg *Config) {
				cfg.ThrottleEnabled = true
				cfg.ThrottleEndHour = 25
			},
		},
		{
			name: "throttle runner poll seconds",
			mutate: func(cfg *Config) {
				cfg.ThrottleEnabled = true
				cfg.ThrottleRunnerPollSeconds = 0
			},
		},
		{
			name: "throttle scheduler poll seconds",
			mutate: func(cfg *Config) {
				cfg.ThrottleEnabled = true
				cfg.ThrottleSchedulerPollSeconds = 0
			},
		},
		{
			name: "queue retention days",
			mutate: func(cfg *Config) {
				cfg.RetentionQueueDays = 0
			},
		},
		{
			name: "audit retention days",
			mutate: func(cfg *Config) {
				cfg.RetentionAuditDays = 0
			},
		},
		{
			name: "feedback retention days",
			mutate: func(cfg *Config) {
				cfg.RetentionFeedbackDays = 0
			},
		},
		{
			name: "pds timeout",
			mutate: func(cfg *Config) {
				cfg.PDSTimeout = 0
			},
		},
		{
			name: "autoflow timeout",
			mutate: func(cfg *Config) {
				cfg.AutoFlowTimeout = 0
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
