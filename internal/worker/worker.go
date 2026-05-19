package worker

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultWorkerType returns the Go ffmpeg worker's queue type. The new
// `ffmpeg_go` value keeps Go and Python workers on separate Redis streams so
// neither can claim the other's tasks until a node registry entry is
// explicitly migrated.
func DefaultWorkerType() string {
	return "ffmpeg_go"
}

// Config captures the runtime parameters a Go worker needs to participate in
// the Redis Streams task/event protocol. Field names mirror the env vars
// expected by `backend/worker/main.py` so deployment templates port directly.
type Config struct {
	WorkerType          string
	WorkerID            string
	RedisURL            string
	DatabaseURL         string
	StorageBackend      string
	StorageLocalRoot    string
	Concurrency         int
	PELMinIdle          time.Duration
	PELReclaimInterval  time.Duration
	HeartbeatInterval   time.Duration
	AffinityWait        time.Duration
	AffinityMaxBounces  int
	ShutdownGracePeriod time.Duration
	CancelPollInterval  time.Duration
}

// LoadConfig builds a Config from environment, applying defaults that match
// the Python worker:
//
//   - WORKER_TYPE: defaults to "ffmpeg_go".
//   - REDIS_URL: defaults to redis://localhost:6379/0.
//   - WORKER_HOST: defaults to the OS hostname.
//   - WorkerID is computed as "<worker_type>-worker@<host>:<pid>".
func LoadConfig() Config {
	workerType := strings.TrimSpace(os.Getenv("WORKER_TYPE"))
	if workerType == "" {
		workerType = DefaultWorkerType()
	}
	host := strings.TrimSpace(os.Getenv("WORKER_HOST"))
	if host == "" {
		host, _ = os.Hostname()
	}
	if host == "" {
		host = "unknown"
	}
	redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		databaseURL = "postgresql://vp:vp_secret@localhost:5435/videoprocess"
	}
	storageBackend := strings.TrimSpace(os.Getenv("STORAGE_BACKEND"))
	if storageBackend == "" {
		storageBackend = "local"
	}
	storageLocalRoot := strings.TrimSpace(os.Getenv("STORAGE_LOCAL_ROOT"))
	if storageLocalRoot == "" {
		storageLocalRoot = "/tmp/vp_storage"
	}
	return Config{
		WorkerType:          workerType,
		WorkerID:            fmt.Sprintf("%s-worker@%s:%d", workerType, host, os.Getpid()),
		RedisURL:            redisURL,
		DatabaseURL:         databaseURL,
		StorageBackend:      storageBackend,
		StorageLocalRoot:    storageLocalRoot,
		Concurrency:         intEnv("WORKER_CONCURRENCY", 2),
		PELMinIdle:          durationMillisEnv("WORKER_PEL_MIN_IDLE_MS", 15*time.Minute),
		PELReclaimInterval:  durationSecondsEnv("WORKER_PEL_RECLAIM_INTERVAL_SECONDS", 60*time.Second),
		HeartbeatInterval:   durationSecondsEnv("WORKER_HEARTBEAT_INTERVAL_SECONDS", 15*time.Second),
		AffinityWait:        durationSecondsEnv("WORKER_AFFINITY_WAIT_SECONDS", 20*time.Second),
		AffinityMaxBounces:  intEnv("WORKER_AFFINITY_MAX_BOUNCES", 6),
		ShutdownGracePeriod: durationSecondsEnv("WORKER_SHUTDOWN_GRACE_SECONDS", 30*time.Second),
		CancelPollInterval:  durationSecondsEnv("WORKER_CANCEL_POLL_SECONDS", 2*time.Second),
	}
}

func intEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func durationSecondsEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return time.Duration(parsed) * time.Second
}

func durationMillisEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return time.Duration(parsed) * time.Millisecond
}
