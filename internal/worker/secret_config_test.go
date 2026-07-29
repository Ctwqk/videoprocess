package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRegistrationProductionSecretsRequireBoundedMode0400Files(t *testing.T) {
	databasePath := writeWorkerSecret(t, "database-url", "postgresql://runtime:database-secret@vp-postgres/videoprocess\n", 0o400)
	tokenPath := writeWorkerSecret(t, "admission-token", "admission-secret\n", 0o400)
	env := map[string]string{
		"DEPLOY_MODE":                 "production",
		"REDIS_URL":                   "redis://go-worker:redis-secret@vp-redis/3",
		"WORKER_DATABASE_URL_FILE":    databasePath,
		"WORKER_ADMISSION_TOKEN_FILE": tokenPath,
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
		!strings.Contains(err.Error(), "DATABASE_URL") {
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
