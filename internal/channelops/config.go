package channelops

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL             string
	YouTubeManagerURL       string
	AutoFlowBaseURL         string
	AutoFlowTimeout         time.Duration
	PDSEnabled              bool
	PDSBaseURL              string
	PDSClientID             string
	PDSTimeout              time.Duration
	DevAllowAllPDS          bool
	RunnerPollSeconds       int
	SchedulerPollSeconds    int
	SlackWebhookURL         string
	AlertEmailTo            string
	LiveMode                bool
	MaxQueueAttempts        int
	MetricsPollMaxAttempts  int
	MetricsPollDelayMinutes int
}

func LoadConfig() Config {
	return Config{
		DatabaseURL:             env("DATABASE_URL", "postgresql://vp:vp_secret@localhost:5435/videoprocess"),
		YouTubeManagerURL:       env("YOUTUBE_MANAGER_URL", ""),
		AutoFlowBaseURL:         env("AUTOFLOW_BASE_URL", "http://api:8080"),
		AutoFlowTimeout:         time.Duration(floatEnv("AUTOFLOW_TIMEOUT_SECONDS", 10) * float64(time.Second)),
		PDSEnabled:              boolEnv("PDS_ENABLED", false),
		PDSBaseURL:              env("PDS_BASE_URL", "http://pds:8080"),
		PDSClientID:             env("PDS_CLIENT_ID", "videoprocess-channel-agent"),
		PDSTimeout:              time.Duration(floatEnv("PDS_TIMEOUT_SECONDS", 0.5) * float64(time.Second)),
		DevAllowAllPDS:          boolEnv("CHANNEL_AGENT_DEV_ALLOW_ALL_PDS", false),
		RunnerPollSeconds:       intEnv("CHANNELOPS_RUNNER_POLL_SECONDS", 5),
		SchedulerPollSeconds:    intEnv("CHANNELOPS_SCHEDULER_POLL_SECONDS", 60),
		SlackWebhookURL:         env("CHANNEL_AGENT_ALERT_SLACK_WEBHOOK_URL", ""),
		AlertEmailTo:            env("CHANNEL_AGENT_ALERT_EMAIL_TO", ""),
		LiveMode:                boolEnv("CHANNELOPS_LIVE_MODE", true),
		MaxQueueAttempts:        intEnv("CHANNELOPS_QUEUE_MAX_ATTEMPTS", 3),
		MetricsPollMaxAttempts:  intEnv("CHANNELOPS_METRICS_MAX_POLLS", 24),
		MetricsPollDelayMinutes: intEnv("CHANNELOPS_METRICS_POLL_DELAY_MINUTES", 60),
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return errors.New("DATABASE_URL is required")
	}
	if c.LiveMode && strings.TrimSpace(c.YouTubeManagerURL) == "" {
		return errors.New("YOUTUBE_MANAGER_URL is required in live ChannelOps mode")
	}
	if c.LiveMode && strings.TrimSpace(c.AutoFlowBaseURL) == "" {
		return errors.New("AUTOFLOW_BASE_URL is required in live ChannelOps mode")
	}
	if c.AutoFlowTimeout <= 0 {
		return errors.New("AUTOFLOW_TIMEOUT_SECONDS must be positive")
	}
	if c.PDSTimeout <= 0 {
		return errors.New("PDS_TIMEOUT_SECONDS must be positive")
	}
	if c.RunnerPollSeconds <= 0 {
		return errors.New("CHANNELOPS_RUNNER_POLL_SECONDS must be positive")
	}
	if c.SchedulerPollSeconds <= 0 {
		return errors.New("CHANNELOPS_SCHEDULER_POLL_SECONDS must be positive")
	}
	if c.MaxQueueAttempts <= 0 {
		return errors.New("CHANNELOPS_QUEUE_MAX_ATTEMPTS must be positive")
	}
	if c.MetricsPollMaxAttempts <= 0 {
		return errors.New("CHANNELOPS_METRICS_MAX_POLLS must be positive")
	}
	if c.MetricsPollDelayMinutes <= 0 {
		return errors.New("CHANNELOPS_METRICS_POLL_DELAY_MINUTES must be positive")
	}
	return nil
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

func floatEnv(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
