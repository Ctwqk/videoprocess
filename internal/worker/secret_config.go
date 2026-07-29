package worker

import (
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"unicode/utf8"

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
	production := isProductionWorkerEnv(env)
	databasePath := strings.TrimSpace(env["WORKER_DATABASE_URL_FILE"])
	databaseEnvironment := strings.TrimSpace(env["DATABASE_URL"])
	var databaseURL string
	var err error
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

func isProductionWorkerEnv(env map[string]string) bool {
	deployMode := strings.ToLower(strings.TrimSpace(env["DEPLOY_MODE"]))
	if deployMode == "" {
		deployMode = "shared"
	}
	switch deployMode {
	case "shared", "production":
		return true
	}
	rawRedisURL := strings.TrimSpace(env["REDIS_URL"])
	if rawRedisURL == "" {
		rawRedisURL = "redis://localhost:6379/0"
	}
	parsed, err := url.Parse(rawRedisURL)
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if host == "" ||
		host == "localhost" ||
		host == "0.0.0.0" ||
		host == "::1" {
		return false
	}
	address := net.ParseIP(host)
	if address != nil {
		return !address.IsLoopback() && !address.IsUnspecified()
	}
	return true
}
