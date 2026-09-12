package worker

import (
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
)

const MaxWorkerSecretBytes = 4096

type SecretConfig struct {
	DatabaseURL    string
	AdmissionToken string
	RedisURL       string
	MinIOAccessKey string
	MinIOSecretKey string
}

type WorkerSecretError struct {
	message string
}

type secretChangeTime struct {
	seconds     int64
	nanoseconds int64
}

func (e *WorkerSecretError) Error() string {
	if e == nil || e.message == "" {
		return "worker secret configuration is invalid"
	}
	return e.message
}

func ReadMode0400Secret(path string, label string) (string, error) {
	return readMode0400Secret(path, label, nil)
}

func readMode0400Secret(
	path string,
	label string,
	beforeFinalIdentityCheck func() error,
) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", &WorkerSecretError{message: label + " could not be opened"}
	}
	before, err := os.Lstat(path)
	if err != nil {
		return "", &WorkerSecretError{message: label + " could not be opened"}
	}
	if !before.Mode().IsRegular() {
		return "", &WorkerSecretError{message: label + " must be a regular file"}
	}
	if !isExactMode0400(before.Mode()) {
		return "", &WorkerSecretError{message: label + " must use mode 0400"}
	}
	if before.Size() > MaxWorkerSecretBytes {
		return "", &WorkerSecretError{message: label + " is too large"}
	}

	descriptor, err := unix.Open(
		path,
		unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return "", &WorkerSecretError{message: label + " could not be opened"}
	}
	file := os.NewFile(uintptr(descriptor), "")
	if file == nil {
		_ = unix.Close(descriptor)
		return "", &WorkerSecretError{message: label + " could not be opened"}
	}
	defer file.Close()

	opened, err := file.Stat()
	if err != nil {
		return "", &WorkerSecretError{message: label + " changed while being opened"}
	}
	beforeChangeTime, beforeChangeTimeOK := secretFileChangeTime(before)
	openedChangeTime, openedChangeTimeOK := secretFileChangeTime(opened)
	if !opened.Mode().IsRegular() ||
		!isExactMode0400(opened.Mode()) ||
		!beforeChangeTimeOK ||
		!openedChangeTimeOK ||
		beforeChangeTime != openedChangeTime ||
		!os.SameFile(before, opened) {
		return "", &WorkerSecretError{message: label + " changed while being opened"}
	}
	if opened.Size() < 0 || opened.Size() > MaxWorkerSecretBytes {
		return "", &WorkerSecretError{message: label + " is too large"}
	}
	raw, err := readExactSecret(file, opened.Size())
	if err != nil {
		return "", &WorkerSecretError{message: label + " changed while being read"}
	}
	if beforeFinalIdentityCheck != nil {
		if err := beforeFinalIdentityCheck(); err != nil {
			return "", &WorkerSecretError{message: label + " changed while being read"}
		}
	}
	after, err := file.Stat()
	if err != nil {
		return "", &WorkerSecretError{message: label + " changed while being read"}
	}
	afterChangeTime, afterChangeTimeOK := secretFileChangeTime(after)
	if !after.Mode().IsRegular() ||
		!isExactMode0400(after.Mode()) ||
		after.Size() != opened.Size() ||
		!after.ModTime().Equal(opened.ModTime()) ||
		!afterChangeTimeOK ||
		afterChangeTime != openedChangeTime ||
		!os.SameFile(opened, after) {
		return "", &WorkerSecretError{message: label + " changed while being read"}
	}
	if !utf8.Valid(raw) {
		return "", &WorkerSecretError{message: label + " is not valid UTF-8"}
	}
	value := string(raw)
	if strings.HasSuffix(value, "\n") {
		value = strings.TrimSuffix(value, "\n")
	}
	if value == "" || strings.ContainsRune(value, '\x00') {
		return "", &WorkerSecretError{message: label + " is empty or invalid"}
	}
	return value, nil
}

func readExactSecret(reader io.Reader, expectedSize int64) ([]byte, error) {
	raw := make([]byte, expectedSize)
	if len(raw) > 0 {
		if _, err := io.ReadFull(reader, raw); err != nil {
			return nil, err
		}
	}
	extra := make([]byte, 1)
	if count, err := reader.Read(extra); count != 0 ||
		err != nil && !errors.Is(err, io.EOF) {
		return nil, errors.New("secret changed while being read")
	}
	return raw, nil
}

func isExactMode0400(mode os.FileMode) bool {
	const special = os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	return mode&(os.ModePerm|special) == 0o400
}

func LoadWorkerSecrets(env map[string]string) (SecretConfig, error) {
	deployMode := strings.ToLower(strings.TrimSpace(env["DEPLOY_MODE"]))
	switch deployMode {
	case "", "shared", "production", "local", "development", "test":
	default:
		return SecretConfig{}, &WorkerSecretError{
			message: "worker deploy mode is invalid",
		}
	}
	preliminaryProduction := deployMode == "" ||
		deployMode == "shared" ||
		deployMode == "production"
	redisPath := strings.TrimSpace(env["WORKER_REDIS_URL_FILE"])
	redisEnvironment := strings.TrimSpace(env["REDIS_URL"])
	var redisURL string
	var err error
	if preliminaryProduction {
		if redisEnvironment != "" {
			return SecretConfig{}, &WorkerSecretError{
				message: "production workers must not receive REDIS_URL through the environment",
			}
		}
		if redisPath == "" {
			return SecretConfig{}, &WorkerSecretError{
				message: "production workers require WORKER_REDIS_URL_FILE",
			}
		}
		redisURL, err = ReadMode0400Secret(
			redisPath,
			"worker Redis URL",
		)
	} else if redisPath != "" {
		redisURL, err = ReadMode0400Secret(
			redisPath,
			"worker Redis URL",
		)
	} else if redisEnvironment != "" {
		redisURL = redisEnvironment
	} else {
		return SecretConfig{}, &WorkerSecretError{
			message: "worker Redis URL is not configured",
		}
	}
	if err != nil {
		return SecretConfig{}, err
	}
	redisOptions, err := ParseWorkerRedisOptions(redisURL)
	if err != nil {
		return SecretConfig{}, err
	}
	allowEnvironmentFallback, err := workerAllowsEnvironmentSecretFallback(
		env,
		redisOptions,
	)
	if err != nil {
		return SecretConfig{}, err
	}
	production := !allowEnvironmentFallback
	if production && redisEnvironment != "" {
		return SecretConfig{}, &WorkerSecretError{
			message: "production workers must not receive REDIS_URL through the environment",
		}
	}
	if production && redisPath == "" {
		return SecretConfig{}, &WorkerSecretError{
			message: "production workers require WORKER_REDIS_URL_FILE",
		}
	}
	databasePath := strings.TrimSpace(env["WORKER_DATABASE_URL_FILE"])
	databaseEnvironment := strings.TrimSpace(env["DATABASE_URL"])
	var databaseURL string
	if production {
		if databaseEnvironment != "" {
			return SecretConfig{}, &WorkerSecretError{
				message: "production workers must not receive DATABASE_URL through the environment",
			}
		}
		if databasePath == "" {
			return SecretConfig{}, &WorkerSecretError{
				message: "production workers require WORKER_DATABASE_URL_FILE",
			}
		}
		databaseURL, err = ReadMode0400Secret(
			databasePath,
			"worker database URL",
		)
	} else if databasePath != "" {
		databaseURL, err = ReadMode0400Secret(
			databasePath,
			"worker database URL",
		)
	} else if databaseEnvironment != "" {
		databaseURL = databaseEnvironment
	} else {
		return SecretConfig{}, &WorkerSecretError{
			message: "worker database URL is not configured",
		}
	}
	if err != nil {
		return SecretConfig{}, err
	}

	tokenPath := strings.TrimSpace(env["WORKER_ADMISSION_TOKEN_FILE"])
	tokenEnvironment := strings.TrimSpace(env["WORKER_ADMISSION_TOKEN"])
	if production && tokenEnvironment != "" {
		return SecretConfig{}, &WorkerSecretError{
			message: "production workers must not receive WORKER_ADMISSION_TOKEN through the environment",
		}
	}
	var admissionToken string
	if tokenPath != "" {
		admissionToken, err = ReadMode0400Secret(
			tokenPath,
			"worker admission token",
		)
	} else if production {
		return SecretConfig{}, &WorkerSecretError{
			message: "production workers require WORKER_ADMISSION_TOKEN_FILE",
		}
	} else {
		admissionToken = tokenEnvironment
		if admissionToken == "" {
			return SecretConfig{}, &WorkerSecretError{
				message: "worker admission token is not configured",
			}
		}
	}
	if err != nil {
		return SecretConfig{}, err
	}
	minioAccessPath := strings.TrimSpace(env["WORKER_MINIO_ACCESS_KEY_FILE"])
	minioSecretPath := strings.TrimSpace(env["WORKER_MINIO_SECRET_KEY_FILE"])
	minioAccessEnvironment := strings.TrimSpace(env["MINIO_ACCESS_KEY"])
	minioSecretEnvironment := strings.TrimSpace(env["MINIO_SECRET_KEY"])
	if !production &&
		strings.ToLower(strings.TrimSpace(env["STORAGE_BACKEND"])) != "minio" &&
		minioAccessPath == "" &&
		minioSecretPath == "" &&
		minioAccessEnvironment == "" &&
		minioSecretEnvironment == "" {
		return SecretConfig{
			DatabaseURL:    databaseURL,
			AdmissionToken: admissionToken,
			RedisURL:       redisURL,
		}, nil
	}
	if production &&
		(minioAccessEnvironment != "" || minioSecretEnvironment != "") {
		return SecretConfig{}, &WorkerSecretError{
			message: "production workers must not receive MINIO_ACCESS_KEY or MINIO_SECRET_KEY through the environment",
		}
	}
	var minioAccessKey string
	var minioSecretKey string
	if minioAccessPath != "" || minioSecretPath != "" {
		if minioAccessPath == "" ||
			minioSecretPath == "" ||
			minioAccessPath == minioSecretPath {
			return SecretConfig{}, &WorkerSecretError{
				message: "worker MinIO credentials require independent secret files",
			}
		}
		minioAccessKey, err = ReadMode0400Secret(
			minioAccessPath,
			"worker MinIO access key",
		)
		if err == nil {
			minioSecretKey, err = ReadMode0400Secret(
				minioSecretPath,
				"worker MinIO secret key",
			)
		}
	} else if production {
		return SecretConfig{}, &WorkerSecretError{
			message: "production workers require WORKER_MINIO_ACCESS_KEY_FILE and WORKER_MINIO_SECRET_KEY_FILE",
		}
	} else {
		minioAccessKey = minioAccessEnvironment
		minioSecretKey = minioSecretEnvironment
		if minioAccessKey == "" || minioSecretKey == "" {
			return SecretConfig{}, &WorkerSecretError{
				message: "worker MinIO credentials are not configured",
			}
		}
	}
	if err != nil {
		return SecretConfig{}, err
	}
	return SecretConfig{
		DatabaseURL:    databaseURL,
		AdmissionToken: admissionToken,
		RedisURL:       redisURL,
		MinIOAccessKey: minioAccessKey,
		MinIOSecretKey: minioSecretKey,
	}, nil
}

func ParseWorkerRedisOptions(rawRedisURL string) (*redis.Options, error) {
	rawRedisURL = strings.TrimSpace(rawRedisURL)
	if rawRedisURL == "" {
		rawRedisURL = "redis://localhost:6379/0"
	}
	durationQuery, durationsValid := parseWorkerRedisConnectionDurationQuery(
		rawRedisURL,
	)
	options, err := redis.ParseURL(rawRedisURL)
	if err == nil && durationsValid {
		if durationQuery.lifetimeSet {
			options.ConnMaxLifetime = durationQuery.lifetime
		}
		if durationQuery.jitterSet {
			options.ConnMaxLifetimeJitter = durationQuery.jitter
			if options.ConnMaxLifetimeJitter > options.ConnMaxLifetime {
				options.ConnMaxLifetimeJitter = options.ConnMaxLifetime
			}
		}
	}
	if err != nil || !durationsValid || !validWorkerRedisOptions(options) {
		return nil, &WorkerSecretError{
			message: "worker Redis configuration is invalid",
		}
	}
	options.ContextTimeoutEnabled = true
	return options, nil
}

func validWorkerRedisOptions(options *redis.Options) bool {
	if options == nil ||
		options.Network != "tcp" ||
		options.DB < 0 ||
		options.MaxRetries < -1 ||
		options.MaxRetries == int(^uint(0)>>1) ||
		!validWorkerRedisRetryBackoff(options.MinRetryBackoff) ||
		!validWorkerRedisRetryBackoff(options.MaxRetryBackoff) ||
		!validWorkerRedisConnectionDurations(options) ||
		options.PoolSize < 0 ||
		options.MinIdleConns < 0 ||
		options.MaxIdleConns < 0 ||
		options.MaxActiveConns < 0 ||
		options.MaxConcurrentDials < 0 ||
		options.DialTimeout < 0 ||
		options.ReadTimeout < 0 ||
		options.WriteTimeout < 0 ||
		options.PoolTimeout < 0 {
		return false
	}
	if options.Protocol != 0 &&
		options.Protocol != 2 &&
		options.Protocol != 3 {
		return false
	}
	const maxRedisPoolCount = int64(1<<31 - 1)
	for _, count := range []int{
		options.PoolSize,
		options.MinIdleConns,
		options.MaxIdleConns,
		options.MaxActiveConns,
		options.MaxConcurrentDials,
	} {
		if int64(count) > maxRedisPoolCount {
			return false
		}
	}
	host, portText, err := net.SplitHostPort(options.Addr)
	if err != nil || strings.TrimSpace(host) == "" {
		return false
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return false
	}
	return !strings.Contains(host, ":") || net.ParseIP(host) != nil
}

func validWorkerRedisRetryBackoff(backoff time.Duration) bool {
	return backoff == -1 || backoff >= 0
}

type workerRedisConnectionDurationQuery struct {
	lifetimeSet bool
	lifetime    time.Duration
	jitterSet   bool
	jitter      time.Duration
}

func parseWorkerRedisConnectionDurationQuery(
	rawRedisURL string,
) (workerRedisConnectionDurationQuery, bool) {
	parsed, err := url.Parse(rawRedisURL)
	if err != nil {
		return workerRedisConnectionDurationQuery{}, false
	}
	query := parsed.Query()
	if value, ok := workerRedisDurationQueryValue(
		query,
		"conn_max_idle_time",
		"idle_timeout",
	); ok {
		idle, valid := parseWorkerRedisURLDuration(value)
		if !valid || idle < 0 && idle != -time.Nanosecond {
			return workerRedisConnectionDurationQuery{}, false
		}
	}
	result := workerRedisConnectionDurationQuery{}
	if value, ok := workerRedisDurationQueryValue(
		query,
		"conn_max_lifetime",
		"max_conn_age",
	); ok {
		lifetime, valid := parseWorkerRedisURLDuration(value)
		if !valid || lifetime < 0 {
			return workerRedisConnectionDurationQuery{}, false
		}
		result.lifetimeSet = true
		result.lifetime = lifetime
	}
	if value, ok := workerRedisDurationQueryValue(
		query,
		"conn_max_lifetime_jitter",
		"",
	); ok {
		jitter, valid := parseWorkerRedisURLDuration(value)
		if !valid || jitter < 0 {
			return workerRedisConnectionDurationQuery{}, false
		}
		result.jitterSet = true
		result.jitter = jitter
	}
	return result, true
}

func workerRedisDurationQueryValue(
	query url.Values,
	name string,
	legacyName string,
) (string, bool) {
	values := query[name]
	if len(values) == 0 && legacyName != "" {
		values = query[legacyName]
	}
	if len(values) == 0 || values[len(values)-1] == "" {
		return "", false
	}
	return values[len(values)-1], true
}

func parseWorkerRedisURLDuration(value string) (time.Duration, bool) {
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return time.Duration(seconds), true
		}
		const maxDuration = time.Duration(1<<63 - 1)
		if int64(seconds) > int64(maxDuration/time.Second) {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	duration, err := time.ParseDuration(value)
	return duration, err == nil
}

func validWorkerRedisConnectionDurations(options *redis.Options) bool {
	if options == nil ||
		options.ConnMaxIdleTime < 0 &&
			options.ConnMaxIdleTime != -time.Nanosecond ||
		options.ConnMaxLifetime < 0 ||
		options.ConnMaxLifetimeJitter < 0 {
		return false
	}
	effectiveJitter := options.ConnMaxLifetimeJitter
	if effectiveJitter > options.ConnMaxLifetime {
		effectiveJitter = options.ConnMaxLifetime
	}
	if effectiveJitter <= 0 || options.ConnMaxLifetime <= 0 {
		return true
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if effectiveJitter > maxDuration/2 {
		return false
	}
	maxPositiveOffset := effectiveJitter - time.Nanosecond
	return options.ConnMaxLifetime <= maxDuration-maxPositiveOffset
}

func workerAllowsEnvironmentSecretFallback(
	env map[string]string,
	redisOptions *redis.Options,
) (bool, error) {
	deployMode := strings.ToLower(strings.TrimSpace(env["DEPLOY_MODE"]))
	if deployMode == "" {
		return false, nil
	}
	switch deployMode {
	case "shared", "production":
		return false, nil
	case "local", "development", "test":
	default:
		return false, &WorkerSecretError{
			message: "worker deploy mode is invalid",
		}
	}
	host, _, err := net.SplitHostPort(redisOptions.Addr)
	if err != nil {
		return false, &WorkerSecretError{
			message: "worker Redis configuration is invalid",
		}
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "" ||
		host == "localhost" ||
		host == "0.0.0.0" ||
		host == "::1" {
		return true, nil
	}
	address := net.ParseIP(host)
	if address != nil {
		return address.IsLoopback() || address.IsUnspecified(), nil
	}
	return false, nil
}
