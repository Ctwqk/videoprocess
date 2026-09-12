package worker

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRegistrationProductionSecretsRequireBoundedMode0400Files(t *testing.T) {
	databasePath := writeWorkerSecret(t, "database-url", "postgresql://runtime:database-secret@vp-postgres/videoprocess\n", 0o400)
	tokenPath := writeWorkerSecret(t, "admission-token", "admission-secret\n", 0o400)
	redisPath := writeWorkerSecret(t, "redis-url", "redis://go-worker:redis-secret@vp-redis/3\n", 0o400)
	minioAccessPath := writeWorkerSecret(t, "minio-access", "minio-access\n", 0o400)
	minioSecretPath := writeWorkerSecret(t, "minio-secret", "minio-password\n", 0o400)
	env := map[string]string{
		"DEPLOY_MODE":                  "production",
		"WORKER_DATABASE_URL_FILE":     databasePath,
		"WORKER_ADMISSION_TOKEN_FILE":  tokenPath,
		"WORKER_REDIS_URL_FILE":        redisPath,
		"WORKER_MINIO_ACCESS_KEY_FILE": minioAccessPath,
		"WORKER_MINIO_SECRET_KEY_FILE": minioSecretPath,
	}

	secrets, err := LoadWorkerSecrets(env)
	if err != nil {
		t.Fatalf("LoadWorkerSecrets: %v", err)
	}
	if secrets.DatabaseURL != "postgresql://runtime:database-secret@vp-postgres/videoprocess" {
		t.Fatalf("database URL was not read exactly from the secret file")
	}
	if secrets.AdmissionToken != "admission-secret" {
		t.Fatalf("admission token was not read exactly from the secret file")
	}
	if secrets.RedisURL != "redis://go-worker:redis-secret@vp-redis/3" {
		t.Fatalf("Redis URL was not read exactly from the secret file")
	}
	if secrets.MinIOAccessKey != "minio-access" ||
		secrets.MinIOSecretKey != "minio-password" {
		t.Fatalf("MinIO credentials were not read exactly from secret files")
	}

	env["DATABASE_URL"] = "postgresql://leaked:environment-secret@db/videoprocess"
	_, err = LoadWorkerSecrets(env)
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("production environment database credential error = %v", err)
	}
	for _, secret := range []string{
		"database-secret",
		"admission-secret",
		"environment-secret",
		databasePath,
		tokenPath,
	} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("sanitized error exposed secret material %q: %v", secret, err)
		}
	}

	delete(env, "DATABASE_URL")
	env["WORKER_ADMISSION_TOKEN"] = "environment-admission-secret"
	_, err = LoadWorkerSecrets(env)
	if err == nil ||
		!strings.Contains(err.Error(), "WORKER_ADMISSION_TOKEN") {
		t.Fatalf("production environment admission credential error = %v", err)
	}
	if strings.Contains(err.Error(), "environment-admission-secret") {
		t.Fatalf("admission environment error exposed secret material: %v", err)
	}

	delete(env, "WORKER_ADMISSION_TOKEN")
	env["REDIS_URL"] = "redis://go-worker:environment-secret@vp-redis/3"
	_, err = LoadWorkerSecrets(env)
	if err == nil || !strings.Contains(err.Error(), "REDIS_URL") {
		t.Fatalf("production environment Redis credential error = %v", err)
	}
	if strings.Contains(err.Error(), "environment-secret") {
		t.Fatalf("Redis environment error exposed secret material: %v", err)
	}

	delete(env, "REDIS_URL")
	env["MINIO_ACCESS_KEY"] = "environment-minio-secret"
	_, err = LoadWorkerSecrets(env)
	if err == nil || !strings.Contains(err.Error(), "MINIO_ACCESS_KEY") {
		t.Fatalf("production environment MinIO credential error = %v", err)
	}
	if strings.Contains(err.Error(), "environment-minio-secret") {
		t.Fatalf("MinIO environment error exposed secret material: %v", err)
	}
}

func TestRegistrationProductionRedisEnvironmentIsRejectedBeforeFileRead(t *testing.T) {
	_, err := LoadWorkerSecrets(map[string]string{
		"DEPLOY_MODE":           "production",
		"WORKER_REDIS_URL_FILE": filepath.Join(t.TempDir(), "missing-redis-url"),
		"REDIS_URL":             "redis://go-worker:environment-secret@vp-redis/3",
	})
	if err == nil || !strings.Contains(err.Error(), "REDIS_URL") {
		t.Fatalf("production environment Redis credential error = %v", err)
	}
	if strings.Contains(err.Error(), "environment-secret") {
		t.Fatalf("Redis environment error exposed secret material: %v", err)
	}
}

func TestRegistrationSecretReaderRejectsWrongModeOversizeAndSymlink(t *testing.T) {
	wrongMode := writeWorkerSecret(t, "wrong-mode", "secret", 0o600)
	if _, err := ReadMode0400Secret(wrongMode, "worker secret"); err == nil ||
		!strings.Contains(err.Error(), "mode 0400") {
		t.Fatalf("wrong-mode error = %v", err)
	}
	specialMode := writeWorkerSecret(
		t,
		"setuid-mode",
		"secret",
		0o400|os.ModeSetuid,
	)
	if _, err := ReadMode0400Secret(
		specialMode,
		"worker secret",
	); err == nil || !strings.Contains(err.Error(), "mode 0400") {
		t.Fatalf("special-mode error = %v", err)
	}

	oversize := writeWorkerSecret(t, "oversize", strings.Repeat("x", MaxWorkerSecretBytes+1), 0o400)
	if _, err := ReadMode0400Secret(oversize, "worker secret"); err == nil ||
		!strings.Contains(err.Error(), "too large") {
		t.Fatalf("oversize error = %v", err)
	}

	target := writeWorkerSecret(t, "target", "secret", 0o400)
	link := filepath.Join(t.TempDir(), "secret-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink secret: %v", err)
	}
	if _, err := ReadMode0400Secret(link, "worker secret"); err == nil {
		t.Fatal("symlink secret was accepted")
	}
}

type shortSecretReader struct {
	reader *strings.Reader
}

func (r *shortSecretReader) Read(buffer []byte) (int, error) {
	if len(buffer) > 2 {
		buffer = buffer[:2]
	}
	return r.reader.Read(buffer)
}

func TestRegistrationSecretReaderHandlesShortReads(t *testing.T) {
	raw, err := readExactSecret(
		&shortSecretReader{reader: strings.NewReader("complete-secret")},
		int64(len("complete-secret")),
	)
	if err != nil {
		t.Fatalf("readExactSecret: %v", err)
	}
	if got := string(raw); got != "complete-secret" {
		t.Fatalf("short-read secret = %q; want complete-secret", got)
	}
}

func TestRegistrationSecretReaderRejectsGrowthDuringFinalIdentityCheck(
	t *testing.T,
) {
	path := writeWorkerSecret(t, "growing-secret", "secret", 0o400)
	_, err := readMode0400Secret(
		path,
		"worker secret",
		func() error {
			if chmodErr := os.Chmod(path, 0o600); chmodErr != nil {
				return chmodErr
			}
			handle, openErr := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if openErr != nil {
				return openErr
			}
			_, writeErr := handle.WriteString("-growth")
			closeErr := handle.Close()
			if chmodErr := os.Chmod(path, 0o400); chmodErr != nil {
				return chmodErr
			}
			if writeErr != nil {
				return writeErr
			}
			return closeErr
		},
	)
	if err == nil || !strings.Contains(err.Error(), "changed while being read") {
		t.Fatalf("growth mutation error = %v; want changed-while-read", err)
	}
}

func TestRegistrationSecretReaderRejectsSameIdentityMutationWithRestoredMtime(
	t *testing.T,
) {
	path := writeWorkerSecret(t, "mutating-secret", "abcdef", 0o400)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat secret before mutation: %v", err)
	}
	_, err = readMode0400Secret(
		path,
		"worker secret",
		func() error {
			if chmodErr := os.Chmod(path, 0o600); chmodErr != nil {
				return chmodErr
			}
			if writeErr := os.WriteFile(path, []byte("UVWXYZ"), 0o600); writeErr != nil {
				return writeErr
			}
			if chmodErr := os.Chmod(path, 0o400); chmodErr != nil {
				return chmodErr
			}
			return os.Chtimes(path, before.ModTime(), before.ModTime())
		},
	)
	if err == nil || !strings.Contains(err.Error(), "changed while being read") {
		t.Fatalf(
			"same-identity mutation error = %v; want changed-while-read",
			err,
		)
	}
}

func TestRegistrationNonProductionSecretsAllowExplicitFallbacks(t *testing.T) {
	secrets, err := LoadWorkerSecrets(map[string]string{
		"DEPLOY_MODE":            "local",
		"REDIS_URL":              "redis://127.0.0.1:6379/14",
		"DATABASE_URL":           "postgresql://dev:dev@127.0.0.1/videoprocess",
		"WORKER_ADMISSION_TOKEN": "development-token",
	})
	if err != nil {
		t.Fatalf("LoadWorkerSecrets: %v", err)
	}
	if secrets.DatabaseURL == "" || secrets.AdmissionToken != "development-token" {
		t.Fatalf("non-production secrets = %#v", secrets)
	}
}

func TestRegistrationMissingDeployModeFailsClosedToSecretFiles(t *testing.T) {
	_, err := LoadWorkerSecrets(map[string]string{
		"REDIS_URL":              "redis://127.0.0.1:6379/14",
		"DATABASE_URL":           "postgresql://dev:dev@127.0.0.1/videoprocess",
		"WORKER_ADMISSION_TOKEN": "development-token",
	})
	if err == nil ||
		!strings.Contains(err.Error(), "REDIS_URL") {
		t.Fatalf("missing-mode production error = %v", err)
	}
}

func TestRegistrationEnvironmentFallbackRequiresExplicitValidDevelopmentMode(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name       string
		deployMode *string
		redisURL   string
	}{
		{
			name:       "misspelled production mode",
			deployMode: workerSecretStringPointer("prodution"),
			redisURL:   "redis://127.0.0.1:6379/14",
		},
		{
			name:       "unknown mode",
			deployMode: workerSecretStringPointer("sandbox"),
			redisURL:   "redis://127.0.0.1:6379/14",
		},
		{
			name:       "local mode malformed endpoint",
			deployMode: workerSecretStringPointer("local"),
			redisURL:   "redis://[::1",
		},
		{
			name:       "test mode malformed endpoint",
			deployMode: workerSecretStringPointer("test"),
			redisURL:   "://malformed",
		},
		{
			name:       "missing mode",
			deployMode: nil,
			redisURL:   "redis://127.0.0.1:6379/14",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			env := map[string]string{
				"REDIS_URL": testCase.redisURL,
				"DATABASE_URL": "postgresql://unsafe:" +
					"database-environment-secret@127.0.0.1/videoprocess",
				"WORKER_ADMISSION_TOKEN": "admission-environment-secret",
			}
			if testCase.deployMode != nil {
				env["DEPLOY_MODE"] = *testCase.deployMode
			}
			_, err := LoadWorkerSecrets(env)
			if err == nil {
				t.Fatal("unsafe environment credential fallback was accepted")
			}
			for _, secret := range []string{
				"database-environment-secret",
				"admission-environment-secret",
			} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("secret error exposed credential %q: %v", secret, err)
				}
			}
		})
	}
}

func TestRegistrationExplicitDevelopmentAndTestModesAllowLocalFallback(
	t *testing.T,
) {
	for _, mode := range []string{"local", "development", "test"} {
		t.Run(mode, func(t *testing.T) {
			secrets, err := LoadWorkerSecrets(map[string]string{
				"DEPLOY_MODE":            mode,
				"REDIS_URL":              "redis://127.0.0.1:6379/14",
				"DATABASE_URL":           "postgresql://dev:dev@127.0.0.1/videoprocess",
				"WORKER_ADMISSION_TOKEN": "development-token",
			})
			if err != nil {
				t.Fatalf("LoadWorkerSecrets: %v", err)
			}
			if secrets.DatabaseURL == "" ||
				secrets.AdmissionToken != "development-token" {
				t.Fatalf("development secrets = %#v", secrets)
			}
		})
	}
}

func TestRegistrationRedisValidationUsesStartupParserSemantics(t *testing.T) {
	for _, redisURL := range []string{
		"redis://worker:synthetic@127.0.0.1:6379/not-a-db",
		"redis://worker:synthetic@127.0.0.1:6379/14?unknown_option=1",
		"redis://worker:synthetic@127.0.0.1:6379/14?dial_timeout=0",
		"redis://worker:synthetic@127.0.0.1:6379/14?read_timeout=0",
		"redis://worker:synthetic@127.0.0.1:6379/14?write_timeout=0",
		"redis://worker:synthetic@127.0.0.1:6379/14?pool_timeout=0",
	} {
		t.Run(redisURL, func(t *testing.T) {
			_, err := LoadWorkerSecrets(map[string]string{
				"DEPLOY_MODE":            "development",
				"REDIS_URL":              redisURL,
				"DATABASE_URL":           "postgresql://dev:synthetic@127.0.0.1/videoprocess",
				"WORKER_ADMISSION_TOKEN": "synthetic-admission",
			})
			if err == nil {
				t.Fatal("go-redis-invalid endpoint accepted environment credentials")
			}
			for _, credential := range []string{
				"synthetic-admission",
				"postgresql://dev:synthetic",
			} {
				if strings.Contains(err.Error(), credential) {
					t.Fatalf(
						"Redis validation exposed credential %q: %v",
						credential,
						err,
					)
				}
			}
		})
	}
}

func TestRegistrationRedisOptionsEnforceContextCancellation(t *testing.T) {
	options, err := ParseWorkerRedisOptions(
		"redis://worker:synthetic@127.0.0.1:6379/14",
	)
	if err != nil {
		t.Fatalf("ParseWorkerRedisOptions: %v", err)
	}
	if !options.ContextTimeoutEnabled {
		t.Fatal("ContextTimeoutEnabled = false; want registration-owned cancellation")
	}
}

func TestRegistrationRedisOptionsRejectUnsafeConstructorRanges(t *testing.T) {
	for _, redisURL := range []string{
		"redis://worker:synthetic@127.0.0.1:6379/-1",
		"redis://worker:synthetic@127.0.0.1:6379/14?max_retries=-2",
		"redis://worker:synthetic@127.0.0.1:6379/14?max_retries=" +
			strconv.Itoa(int(^uint(0)>>1)),
		"redis://worker:synthetic@127.0.0.1:6379/14?min_retry_backoff=-2ms",
		"redis://worker:synthetic@127.0.0.1:6379/14?max_retry_backoff=-2ms",
		"redis://worker:synthetic@127.0.0.1:6379/14?pool_size=-1",
		"redis://worker:synthetic@127.0.0.1:6379/14?min_idle_conns=-1",
		"redis://worker:synthetic@127.0.0.1:6379/14?max_idle_conns=-1",
		"redis://worker:synthetic@127.0.0.1:6379/14?max_active_conns=-1",
		"redis://worker:synthetic@127.0.0.1:6379/14?max_concurrent_dials=-1",
		"redis://worker:synthetic@127.0.0.1:6379/14?protocol=-1",
		"redis://worker:synthetic@127.0.0.1:6379/14?protocol=1",
		"redis://worker:synthetic@127.0.0.1:6379/14?protocol=4",
		"redis://worker:synthetic@127.0.0.1:70000/14",
		"redis://worker:synthetic@127.0.0.1:notaport/14",
	} {
		t.Run(redisURL, func(t *testing.T) {
			options, err := ParseWorkerRedisOptions(redisURL)
			if err == nil {
				panicked := false
				func() {
					defer func() {
						panicked = recover() != nil
					}()
					client := redis.NewClient(options)
					_ = client.Close()
				}()
				t.Fatalf(
					"unsafe Redis options accepted (constructor panic=%t)",
					panicked,
				)
			}
			for _, credential := range []string{"synthetic"} {
				if strings.Contains(err.Error(), credential) {
					t.Fatalf("Redis validation exposed credential %q: %v", credential, err)
				}
			}
		})
	}
}

func TestRegistrationRedisOptionsKeepRetrySentinelsAndValuesConstructible(
	t *testing.T,
) {
	for _, redisURL := range []string{
		"redis://worker:synthetic@127.0.0.1:6379/14?max_retries=-1",
		"redis://worker:synthetic@127.0.0.1:6379/14?max_retries=0",
		"redis://worker:synthetic@127.0.0.1:6379/14?max_retries=3",
		"redis://worker:synthetic@127.0.0.1:6379/14?min_retry_backoff=-1&max_retry_backoff=-1",
		"redis://worker:synthetic@127.0.0.1:6379/14?min_retry_backoff=0&max_retry_backoff=0",
		"redis://worker:synthetic@127.0.0.1:6379/14?min_retry_backoff=2ms&max_retry_backoff=8ms",
	} {
		t.Run(redisURL, func(t *testing.T) {
			options, err := ParseWorkerRedisOptions(redisURL)
			if err != nil {
				t.Fatalf("ParseWorkerRedisOptions: %v", err)
			}
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("validated retry options panicked: %v", recovered)
				}
			}()
			client := redis.NewClient(options)
			if err := client.Close(); err != nil {
				t.Fatalf("close unconnected Redis client: %v", err)
			}
		})
	}
}

func TestRegistrationRedisOptionsKeepSafeDefaultsConstructible(t *testing.T) {
	options, err := ParseWorkerRedisOptions(
		"redis://worker:synthetic@127.0.0.1:6379/14",
	)
	if err != nil {
		t.Fatalf("ParseWorkerRedisOptions: %v", err)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("validated default options panicked: %v", recovered)
		}
	}()
	client := redis.NewClient(options)
	if err := client.Close(); err != nil {
		t.Fatalf("close unconnected Redis client: %v", err)
	}
}

func TestRegistrationRedisOptionsRejectUnsafeConnectionLifetimeArithmetic(
	t *testing.T,
) {
	for _, redisURL := range []string{
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_idle_time=-2ns",
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_lifetime=-1ns",
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_lifetime=1s&conn_max_lifetime_jitter=-1ns",
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_lifetime=9223372036854775807ns&conn_max_lifetime_jitter=9223372036854775807ns",
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_lifetime=4611686018427387904ns&conn_max_lifetime_jitter=4611686018427387904ns",
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_lifetime=9223372036854775807ns&conn_max_lifetime_jitter=2ns",
	} {
		t.Run(redisURL, func(t *testing.T) {
			options, err := ParseWorkerRedisOptions(redisURL)
			if err == nil {
				client := redis.NewClient(options)
				_ = client.Close()
				t.Fatal("unsafe Redis connection lifetime was accepted")
			}
			if err.Error() != "worker Redis configuration is invalid" {
				t.Fatalf("connection lifetime error = %q; want generic Redis error", err)
			}
		})
	}
}

func TestRegistrationRedisOptionsKeepSafeConnectionLifetimeBounds(
	t *testing.T,
) {
	for _, redisURL := range []string{
		"redis://worker:synthetic@127.0.0.1:6379/14",
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_idle_time=-1",
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_lifetime=0s&conn_max_lifetime_jitter=0s",
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_lifetime=2ns&conn_max_lifetime_jitter=9223372036854775807ns",
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_lifetime=9223372036854775807ns&conn_max_lifetime_jitter=1ns",
		"redis://worker:synthetic@127.0.0.1:6379/14?conn_max_lifetime=4611686018427387905ns&conn_max_lifetime_jitter=4611686018427387903ns",
	} {
		t.Run(redisURL, func(t *testing.T) {
			options, err := ParseWorkerRedisOptions(redisURL)
			if err != nil {
				t.Fatalf("ParseWorkerRedisOptions: %v", err)
			}
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("validated connection lifetime panicked: %v", recovered)
				}
			}()
			client := redis.NewClient(options)
			if err := client.Close(); err != nil {
				t.Fatalf("close unconnected Redis client: %v", err)
			}
		})
	}
}

func TestRegistrationRedisConnectionLifetimeBoundsWithRealRedis(t *testing.T) {
	rawURL := strings.TrimSpace(os.Getenv("CHANNEL_OPS_GO_REDIS_TEST_URL"))
	if rawURL == "" {
		t.Skip("set CHANNEL_OPS_GO_REDIS_TEST_URL for Redis 7.4 worker integration tests")
	}
	withLifetimeQuery := func(values map[string]string) string {
		t.Helper()
		parsed, err := url.Parse(rawURL)
		if err != nil {
			t.Fatalf("parse Redis integration URL: %v", err)
		}
		query := parsed.Query()
		for key, value := range values {
			query.Set(key, value)
		}
		parsed.RawQuery = query.Encode()
		return parsed.String()
	}
	unsafeURL := withLifetimeQuery(map[string]string{
		"conn_max_lifetime":        "9223372036854775807ns",
		"conn_max_lifetime_jitter": "9223372036854775807ns",
	})
	if options, err := ParseWorkerRedisOptions(unsafeURL); err == nil {
		client := redis.NewClient(options)
		probeContext, cancelProbe := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		pingErr := client.Ping(probeContext).Err()
		cancelProbe()
		_ = client.Close()
		t.Fatalf(
			"queuedNewConn panic configuration was accepted; Ping error = %v",
			pingErr,
		)
	}

	safeQueries := []map[string]string{
		{"conn_max_idle_time": "-1"},
		{
			"conn_max_lifetime":        "9223372036854775807ns",
			"conn_max_lifetime_jitter": "1ns",
		},
		{
			"conn_max_lifetime":        "4611686018427387905ns",
			"conn_max_lifetime_jitter": "4611686018427387903ns",
		},
		{
			"conn_max_lifetime":        "2ns",
			"conn_max_lifetime_jitter": "9223372036854775807ns",
		},
	}
	for index, query := range safeQueries {
		t.Run(strconv.Itoa(index), func(t *testing.T) {
			options, err := ParseWorkerRedisOptions(withLifetimeQuery(query))
			if err != nil {
				t.Fatalf("parse safe Redis lifetime boundary: %v", err)
			}
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("safe Redis lifetime boundary panicked: %v", recovered)
				}
			}()
			client := redis.NewClient(options)
			t.Cleanup(func() { _ = client.Close() })
			probeContext, cancelProbe := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			defer cancelProbe()
			if err := client.Ping(probeContext).Err(); err != nil {
				t.Fatalf("Ping safe Redis lifetime boundary: %v", err)
			}
		})
	}
}

func writeWorkerSecret(t *testing.T, name string, value string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod secret: %v", err)
	}
	return path
}

func workerSecretStringPointer(value string) *string {
	return &value
}
