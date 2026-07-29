package worker

import (
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
)

const MaxWorkerSecretBytes = 4096

type SecretConfig struct {
	DatabaseURL    string
	AdmissionToken string
}

type WorkerSecretError struct {
	message string
}

func (e *WorkerSecretError) Error() string {
	if e == nil || e.message == "" {
		return "worker secret configuration is invalid"
	}
	return e.message
}

func ReadMode0400Secret(path string, label string) (string, error) {
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
	if err != nil ||
		!opened.Mode().IsRegular() ||
		!isExactMode0400(opened.Mode()) ||
		!os.SameFile(before, opened) {
		return "", &WorkerSecretError{message: label + " changed while being opened"}
	}
	if opened.Size() < 0 || opened.Size() > MaxWorkerSecretBytes {
		return "", &WorkerSecretError{message: label + " is too large"}
	}
	raw := make([]byte, opened.Size())
	if len(raw) > 0 {
		if _, err := io.ReadFull(file, raw); err != nil {
			return "", &WorkerSecretError{message: label + " changed while being read"}
		}
	}
	extra := make([]byte, 1)
	if count, readErr := file.Read(extra); count != 0 ||
		readErr != nil && !errors.Is(readErr, io.EOF) {
		return "", &WorkerSecretError{message: label + " changed while being read"}
	}
	after, err := file.Stat()
	if err != nil ||
		!after.Mode().IsRegular() ||
		!isExactMode0400(after.Mode()) ||
		after.Size() != opened.Size() ||
		!after.ModTime().Equal(opened.ModTime()) ||
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

func isExactMode0400(mode os.FileMode) bool {
	const special = os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	return mode&(os.ModePerm|special) == 0o400
}

func LoadWorkerSecrets(env map[string]string) (SecretConfig, error) {
	redisOptions, err := ParseWorkerRedisOptions(env["REDIS_URL"])
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
	return SecretConfig{
		DatabaseURL:    databaseURL,
		AdmissionToken: admissionToken,
	}, nil
}

func ParseWorkerRedisOptions(rawRedisURL string) (*redis.Options, error) {
	rawRedisURL = strings.TrimSpace(rawRedisURL)
	if rawRedisURL == "" {
		rawRedisURL = "redis://localhost:6379/0"
	}
	options, err := redis.ParseURL(rawRedisURL)
	if err != nil || !validWorkerRedisOptions(options) {
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
