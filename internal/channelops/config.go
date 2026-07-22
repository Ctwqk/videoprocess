package channelops

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL                  string
	YouTubeManagerURL            string
	AutoFlowBaseURL              string
	AutoFlowTimeout              time.Duration
	DiscoveryTimeout             time.Duration
	PDSEnabled                   bool
	PDSBaseURL                   string
	PDSClientID                  string
	PDSTimeout                   time.Duration
	DevAllowAllPDS               bool
	RunnerPollSeconds            int
	SchedulerPollSeconds         int
	HealthPort                   int
	ThrottleEnabled              bool
	ThrottleTimeZone             string
	ThrottleStartHour            int
	ThrottleEndHour              int
	ThrottleRunnerPollSeconds    int
	ThrottleSchedulerPollSeconds int
	SlackWebhookURL              string
	AlertEmailTo                 string
	LiveMode                     bool
	MaxQueueAttempts             int
	MetricsPollMaxAttempts       int
	MetricsPollDelayMinutes      int
	RetentionQueueDays           int
	RetentionAuditDays           int
	RetentionFeedbackDays        int
	discoveryTimeoutParseFailed  bool
}

func LoadConfig() Config {
	discoveryTimeout, discoveryTimeoutParseFailed := discoveryTimeoutEnv()
	return Config{
		DatabaseURL:                  env("DATABASE_URL", "postgresql://vp:vp_secret@localhost:5435/videoprocess"),
		YouTubeManagerURL:            env("YOUTUBE_MANAGER_URL", ""),
		AutoFlowBaseURL:              env("AUTOFLOW_BASE_URL", "http://api:8080"),
		AutoFlowTimeout:              time.Duration(floatEnv("AUTOFLOW_TIMEOUT_SECONDS", 10) * float64(time.Second)),
		DiscoveryTimeout:             discoveryTimeout,
		PDSEnabled:                   boolEnv("PDS_ENABLED", false),
		PDSBaseURL:                   env("PDS_BASE_URL", "http://pds:8080"),
		PDSClientID:                  env("PDS_CLIENT_ID", "videoprocess-channel-agent"),
		PDSTimeout:                   time.Duration(floatEnv("PDS_TIMEOUT_SECONDS", 0.5) * float64(time.Second)),
		DevAllowAllPDS:               boolEnv("CHANNEL_AGENT_DEV_ALLOW_ALL_PDS", false),
		RunnerPollSeconds:            intEnv("CHANNELOPS_RUNNER_POLL_SECONDS", 5),
		SchedulerPollSeconds:         intEnv("CHANNELOPS_SCHEDULER_POLL_SECONDS", 60),
		HealthPort:                   intEnv("CHANNELOPS_HEALTH_PORT", 8080),
		ThrottleEnabled:              boolEnv("CHANNELOPS_THROTTLE_ENABLED", false),
		ThrottleTimeZone:             env("CHANNELOPS_THROTTLE_TIME_ZONE", "America/Los_Angeles"),
		ThrottleStartHour:            intEnv("CHANNELOPS_THROTTLE_START_HOUR", 8),
		ThrottleEndHour:              intEnv("CHANNELOPS_THROTTLE_END_HOUR", 24),
		ThrottleRunnerPollSeconds:    intEnv("CHANNELOPS_THROTTLE_RUNNER_POLL_SECONDS", 300),
		ThrottleSchedulerPollSeconds: intEnv("CHANNELOPS_THROTTLE_SCHEDULER_POLL_SECONDS", 1800),
		SlackWebhookURL:              env("CHANNEL_AGENT_ALERT_SLACK_WEBHOOK_URL", ""),
		AlertEmailTo:                 env("CHANNEL_AGENT_ALERT_EMAIL_TO", ""),
		LiveMode:                     boolEnv("CHANNELOPS_LIVE_MODE", true),
		MaxQueueAttempts:             intEnv("CHANNELOPS_QUEUE_MAX_ATTEMPTS", 3),
		MetricsPollMaxAttempts:       intEnv("CHANNELOPS_METRICS_MAX_POLLS", 24),
		MetricsPollDelayMinutes:      intEnv("CHANNELOPS_METRICS_POLL_DELAY_MINUTES", 60),
		RetentionQueueDays:           intEnv("CHANNELOPS_RETENTION_QUEUE_DAYS", 30),
		RetentionAuditDays:           intEnv("CHANNELOPS_RETENTION_AUDIT_DAYS", 90),
		RetentionFeedbackDays:        intEnv("CHANNELOPS_RETENTION_FEEDBACK_DAYS", 365),
		discoveryTimeoutParseFailed:  discoveryTimeoutParseFailed,
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
	if c.discoveryTimeoutParseFailed {
		return errors.New("CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS must be an integer between 30 and 300")
	}
	if !validDiscoveryTimeout(c.DiscoveryTimeout) {
		return errors.New("CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS must be between 30 and 300")
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
	if c.HealthPort <= 0 {
		return errors.New("CHANNELOPS_HEALTH_PORT must be positive")
	}
	if c.ThrottleEnabled {
		if _, err := time.LoadLocation(c.ThrottleTimeZone); err != nil {
			return errors.New("CHANNELOPS_THROTTLE_TIME_ZONE must be a valid IANA time zone")
		}
		if c.ThrottleStartHour < 0 || c.ThrottleStartHour > 23 {
			return errors.New("CHANNELOPS_THROTTLE_START_HOUR must be between 0 and 23")
		}
		if c.ThrottleEndHour < 1 || c.ThrottleEndHour > 24 {
			return errors.New("CHANNELOPS_THROTTLE_END_HOUR must be between 1 and 24")
		}
		if c.ThrottleStartHour >= c.ThrottleEndHour {
			return errors.New("CHANNELOPS_THROTTLE_START_HOUR must be before CHANNELOPS_THROTTLE_END_HOUR")
		}
		if c.ThrottleRunnerPollSeconds <= 0 {
			return errors.New("CHANNELOPS_THROTTLE_RUNNER_POLL_SECONDS must be positive")
		}
		if c.ThrottleSchedulerPollSeconds <= 0 {
			return errors.New("CHANNELOPS_THROTTLE_SCHEDULER_POLL_SECONDS must be positive")
		}
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
	if c.RetentionQueueDays <= 0 {
		return errors.New("CHANNELOPS_RETENTION_QUEUE_DAYS must be positive")
	}
	if c.RetentionAuditDays <= 0 {
		return errors.New("CHANNELOPS_RETENTION_AUDIT_DAYS must be positive")
	}
	if c.RetentionFeedbackDays <= 0 {
		return errors.New("CHANNELOPS_RETENTION_FEEDBACK_DAYS must be positive")
	}
	return nil
}

func (c Config) EffectiveRunnerPollSeconds(now time.Time) int {
	if c.throttleActiveAt(now) {
		return c.ThrottleRunnerPollSeconds
	}
	return c.RunnerPollSeconds
}

func (c Config) EffectiveSchedulerPollSeconds(now time.Time) int {
	if c.throttleActiveAt(now) {
		return c.ThrottleSchedulerPollSeconds
	}
	return c.SchedulerPollSeconds
}

func (c Config) throttleActiveAt(now time.Time) bool {
	if !c.ThrottleEnabled {
		return false
	}
	loc, err := time.LoadLocation(c.ThrottleTimeZone)
	if err != nil {
		return false
	}
	hour := now.In(loc).Hour()
	return hour >= c.ThrottleStartHour && hour < c.ThrottleEndHour
}

func metricsPollDelay(c Config) time.Duration {
	minutes := c.MetricsPollDelayMinutes
	if minutes <= 0 {
		minutes = 60
	}
	return time.Duration(minutes) * time.Minute
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

func discoveryTimeoutEnv() (time.Duration, bool) {
	value := strings.TrimSpace(os.Getenv("CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS"))
	if value == "" {
		return defaultDiscoveryTimeout, false
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultDiscoveryTimeout, true
	}
	if parsed < 30 || parsed > 300 {
		return defaultDiscoveryTimeout, true
	}
	return time.Duration(parsed) * time.Second, false
}

func validDiscoveryTimeout(timeout time.Duration) bool {
	return timeout >= 30*time.Second && timeout <= 300*time.Second
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
