package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Ctwqk/videoprocess/internal/storage"
	"github.com/Ctwqk/videoprocess/internal/worker"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type startupDatabaseStub struct {
	calls          *[]string
	registerErr    error
	continuityErr  error
	continuityHook func()
	continuityAge  int
	claims         worker.RegistrationClaims
	lease          worker.RegistrationLease
}

func (s *startupDatabaseStub) RegisterWorker(
	_ context.Context,
	claims worker.RegistrationClaims,
	_ string,
) (worker.RegistrationLease, error) {
	*s.calls = append(*s.calls, "register")
	s.claims = claims
	if s.registerErr != nil {
		return worker.RegistrationLease{}, s.registerErr
	}
	s.lease = worker.RegistrationLease{
		RegistrationID:   uuid.New(),
		GrantID:          uuid.New(),
		ServiceName:      claims.ServiceName,
		WorkerInstanceID: claims.WorkerInstanceID,
		WorkerSlot:       claims.WorkerSlot,
		RedisConsumerID:  claims.RedisConsumerID,
		LeaseEpoch:       1,
		LeaseSecret:      "lease-secret",
		LeaseExpiresAt:   time.Now().UTC().Add(worker.RegistrationLeaseDuration),
	}
	return s.lease, nil
}

func (s *startupDatabaseStub) HeartbeatWorker(
	_ context.Context,
	lease worker.RegistrationLease,
) (worker.RegistrationLease, error) {
	*s.calls = append(*s.calls, "heartbeat")
	return lease, nil
}

func (s *startupDatabaseStub) ReleaseWorker(
	context.Context,
	worker.RegistrationLease,
	string,
) error {
	*s.calls = append(*s.calls, "release")
	return nil
}

func (s *startupDatabaseStub) RequireWorkerRedisContinuity(
	_ context.Context,
	maxAgeSeconds int,
) error {
	*s.calls = append(*s.calls, "continuity")
	s.continuityAge = maxAgeSeconds
	if s.continuityHook != nil {
		s.continuityHook()
	}
	return s.continuityErr
}

func (s *startupDatabaseStub) CloseContext(context.Context) error {
	*s.calls = append(*s.calls, "close_database")
	return nil
}

func TestWorkerStartupRegistersAndChecksContinuityBeforeRedis(t *testing.T) {
	calls := []string{}
	database := &startupDatabaseStub{calls: &calls}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	deps := startupDependencies{
		loadSecrets: func(map[string]string) (worker.SecretConfig, error) {
			calls = append(calls, "secrets")
			return workerStartupTestSecrets(
				"redis://go-worker:redis-secret@vp-redis:6379/3",
			), nil
		},
		openDatabase: func(context.Context, string) (startupDatabase, error) {
			calls = append(calls, "open_database")
			return database, nil
		},
		openStorage: func(context.Context, worker.Config) (storage.Backend, error) {
			calls = append(calls, "open_storage")
			return nil, nil
		},
		newRedis: func(*redis.Options) *redis.Client {
			calls = append(calls, "construct_redis")
			return client
		},
		requireRedisIdentity: func(context.Context, *redis.Client, *redis.Options) error {
			calls = append(calls, "redis_identity")
			return nil
		},
		runConsumer: func(context.Context, *redis.Client, worker.Config, startupDatabase, storage.Backend, *worker.Registration) error {
			calls = append(calls, "consumer")
			return nil
		},
	}

	if err := runWorker(context.Background(), workerStartupTestEnv(), deps); err != nil {
		t.Fatalf("runWorker: %v", err)
	}
	wantPrefix := []string{
		"secrets",
		"open_database",
		"register",
		"heartbeat",
		"continuity",
		"open_storage",
		"construct_redis",
		"redis_identity",
		"consumer",
	}
	if len(calls) < len(wantPrefix) || !reflect.DeepEqual(calls[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("startup calls = %#v; want prefix %#v", calls, wantPrefix)
	}
	if worker.WorkerRedisContinuityMaxAge != 90 {
		t.Fatalf(
			"worker continuity constant = %d; want 90",
			worker.WorkerRedisContinuityMaxAge,
		)
	}
	if database.continuityAge != worker.WorkerRedisContinuityMaxAge {
		t.Fatalf(
			"continuity max age = %d; want 90",
			database.continuityAge,
		)
	}
}

func TestWorkerStartupUncertainRegistrationOrContinuityConstructsNoRedis(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		registerErr   error
		continuityErr error
	}{
		{name: "registration", registerErr: errors.New("token_invalid")},
		{name: "continuity", continuityErr: errors.New("worker_redis_continuity_unready")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			calls := []string{}
			database := &startupDatabaseStub{
				calls:         &calls,
				registerErr:   testCase.registerErr,
				continuityErr: testCase.continuityErr,
			}
			redisConstructions := 0
			deps := startupDependencies{
				loadSecrets: func(map[string]string) (worker.SecretConfig, error) {
					return workerStartupTestSecrets(
						"redis://go-worker:redis-secret@vp-redis:6379/3",
					), nil
				},
				openDatabase: func(context.Context, string) (startupDatabase, error) {
					return database, nil
				},
				openStorage: func(context.Context, worker.Config) (storage.Backend, error) {
					return nil, nil
				},
				newRedis: func(*redis.Options) *redis.Client {
					redisConstructions++
					return nil
				},
			}
			if err := runWorker(context.Background(), workerStartupTestEnv(), deps); err == nil {
				t.Fatal("runWorker succeeded despite uncertain registration state")
			}
			if redisConstructions != 0 {
				t.Fatalf("Redis constructions = %d; want 0", redisConstructions)
			}
			if !containsStartupCall(calls, "close_database") {
				t.Fatalf("startup calls = %#v; database was not closed", calls)
			}
			if testCase.continuityErr != nil &&
				!containsStartupCall(calls, "release") {
				t.Fatalf(
					"startup calls = %#v; continuity failure did not release registration",
					calls,
				)
			}
		})
	}
}

func TestWorkerStartupRegistrationLossAfterContinuityConstructsNoRedis(
	t *testing.T,
) {
	calls := []string{}
	ctx, cancel := context.WithCancelCause(context.Background())
	database := &startupDatabaseStub{
		calls: &calls,
		continuityHook: func() {
			cancel(worker.ErrRegistrationLost)
		},
	}
	redisConstructions := 0
	deps := startupDependencies{
		loadSecrets: func(map[string]string) (worker.SecretConfig, error) {
			return workerStartupTestSecrets(
				"redis://go-worker:redis-secret@vp-redis:6379/3",
			), nil
		},
		openDatabase: func(context.Context, string) (startupDatabase, error) {
			return database, nil
		},
		openStorage: func(context.Context, worker.Config) (storage.Backend, error) {
			calls = append(calls, "open_storage")
			return nil, nil
		},
		newRedis: func(*redis.Options) *redis.Client {
			redisConstructions++
			return nil
		},
	}
	err := runWorker(ctx, workerStartupTestEnv(), deps)
	if !errors.Is(err, worker.ErrRegistrationLost) {
		t.Fatalf("runWorker error = %v; want ErrRegistrationLost", err)
	}
	if redisConstructions != 0 {
		t.Fatalf("Redis constructions = %d; want 0", redisConstructions)
	}
	if !containsStartupCall(calls, "release") ||
		!containsStartupCall(calls, "close_database") {
		t.Fatalf("startup calls = %#v; want release and database close", calls)
	}
}

func TestWorkerStartupRejectsInvalidExpectedRedisIdentityBeforeConstruction(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name     string
		redisURL string
	}{
		{
			name:     "missing username",
			redisURL: "redis://vp-redis:6379/3",
		},
		{
			name:     "default username",
			redisURL: "redis://default:redis-secret@vp-redis:6379/3",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			calls := []string{}
			database := &startupDatabaseStub{calls: &calls}
			redisConstructions := 0
			redisCommands := 0
			env := workerStartupTestEnv()
			env["REDIS_URL"] = testCase.redisURL
			err := runWorker(
				context.Background(),
				env,
				startupDependencies{
					loadSecrets: func(map[string]string) (worker.SecretConfig, error) {
						return workerStartupTestSecrets(testCase.redisURL), nil
					},
					openDatabase: func(
						context.Context,
						string,
					) (startupDatabase, error) {
						return database, nil
					},
					openStorage: func(
						context.Context,
						worker.Config,
					) (storage.Backend, error) {
						return nil, nil
					},
					newRedis: func(*redis.Options) *redis.Client {
						redisConstructions++
						return redis.NewClient(
							&redis.Options{Addr: "127.0.0.1:1"},
						)
					},
					requireRedisIdentity: func(
						context.Context,
						*redis.Client,
						*redis.Options,
					) error {
						redisCommands++
						return nil
					},
				},
			)
			if err == nil ||
				!strings.Contains(err.Error(), "worker_redis_identity_unready") {
				t.Fatalf("runWorker error = %v; want local identity rejection", err)
			}
			if redisConstructions != 0 || redisCommands != 0 {
				t.Fatalf(
					"Redis constructor/commands = %d/%d; want 0/0",
					redisConstructions,
					redisCommands,
				)
			}
			if !containsStartupCall(calls, "release") ||
				!containsStartupCall(calls, "close_database") {
				t.Fatalf(
					"startup calls = %#v; want release and database close",
					calls,
				)
			}
		})
	}
}

func TestWorkerStartupRejectsUnsafeEnvironmentFallbackBeforeDatabaseOpen(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name       string
		deployMode *string
		redisURL   string
	}{
		{
			name:       "misspelled mode with local Redis",
			deployMode: stringPointer("prodution"),
			redisURL:   "redis://127.0.0.1:6379/14",
		},
		{
			name:       "explicit local mode with malformed Redis",
			deployMode: stringPointer("local"),
			redisURL:   "redis://[::1",
		},
		{
			name:       "unknown mode with malformed Redis",
			deployMode: stringPointer("sandbox"),
			redisURL:   "redis://[::1",
		},
		{
			name:       "missing mode with local Redis",
			deployMode: nil,
			redisURL:   "redis://127.0.0.1:6379/14",
		},
		{
			name:       "development mode with nonnumeric database path",
			deployMode: stringPointer("development"),
			redisURL: "redis://worker:synthetic@127.0.0.1:6379/" +
				"not-a-db",
		},
		{
			name:       "test mode with unknown Redis query option",
			deployMode: stringPointer("test"),
			redisURL: "redis://worker:synthetic@127.0.0.1:6379/14" +
				"?unknown_option=1",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			env := workerStartupTestEnv()
			if testCase.deployMode == nil {
				delete(env, "DEPLOY_MODE")
			} else {
				env["DEPLOY_MODE"] = *testCase.deployMode
			}
			env["REDIS_URL"] = testCase.redisURL
			env["DATABASE_URL"] =
				"postgresql://unsafe-environment:database-secret@localhost/videoprocess"
			env["WORKER_ADMISSION_TOKEN"] = "unsafe-admission-secret"
			delete(env, "WORKER_DATABASE_URL_FILE")
			delete(env, "WORKER_ADMISSION_TOKEN_FILE")
			databaseOpens := 0
			err := runWorker(
				context.Background(),
				env,
				startupDependencies{
					openDatabase: func(
						context.Context,
						string,
					) (startupDatabase, error) {
						databaseOpens++
						return nil, errors.New("must not open database")
					},
				},
			)
			if err == nil {
				t.Fatal("unsafe environment credential fallback was accepted")
			}
			if databaseOpens != 0 {
				t.Fatalf("database opens = %d; want 0", databaseOpens)
			}
			for _, secret := range []string{
				"database-secret",
				"unsafe-admission-secret",
			} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("startup error exposed credential %q: %v", secret, err)
				}
			}
		})
	}
}

func TestWorkerStartupRejectsMalformedProductionRedisBeforeDatabaseOpen(
	t *testing.T,
) {
	databasePath := writeStartupSecret(
		t,
		"database-url",
		"postgresql://runtime:synthetic@vp-postgres/videoprocess",
	)
	tokenPath := writeStartupSecret(
		t,
		"admission-token",
		"synthetic-admission",
	)
	env := workerStartupTestEnv()
	setStartupRedisCredential(t, env, "production", "redis://[::1")
	env["WORKER_DATABASE_URL_FILE"] = databasePath
	env["WORKER_ADMISSION_TOKEN_FILE"] = tokenPath
	databaseOpens := 0
	err := runWorker(
		context.Background(),
		env,
		startupDependencies{
			openDatabase: func(
				context.Context,
				string,
			) (startupDatabase, error) {
				databaseOpens++
				return nil, errors.New("database open must not run")
			},
		},
	)
	if err == nil {
		t.Fatal("malformed production Redis endpoint was accepted")
	}
	if databaseOpens != 0 {
		t.Fatalf("database opens = %d; want 0", databaseOpens)
	}
	for _, credential := range []string{
		"synthetic-admission",
		"postgresql://runtime:synthetic",
		databasePath,
		tokenPath,
	} {
		if strings.Contains(err.Error(), credential) {
			t.Fatalf("startup error exposed credential %q: %v", credential, err)
		}
	}
}

func TestWorkerStartupRejectsUnsafeRedisRangesInEveryModeBeforeDatabaseOpen(
	t *testing.T,
) {
	databasePath := writeStartupSecret(
		t,
		"database-url",
		"postgresql://runtime:synthetic-password@vp-postgres/videoprocess",
	)
	tokenPath := writeStartupSecret(
		t,
		"admission-token",
		"synthetic-admission-secret",
	)
	testCases := []struct {
		mode     string
		redisURL string
	}{
		{"local", "redis://worker:synthetic@127.0.0.1:6379/-1"},
		{"development", "redis://worker:synthetic@127.0.0.1:6379/14?pool_size=-1"},
		{"test", "redis://worker:synthetic@127.0.0.1:6379/14?min_idle_conns=-1"},
		{"shared", "redis://worker:synthetic@127.0.0.1:6379/14?max_idle_conns=-1"},
		{"production", "redis://worker:synthetic@127.0.0.1:6379/14?max_active_conns=-1"},
		{"local", "redis://worker:synthetic@127.0.0.1:6379/14?max_concurrent_dials=-1"},
		{"development", "redis://worker:synthetic@127.0.0.1:6379/14?protocol=4"},
		{"production", "redis://worker:synthetic@127.0.0.1:70000/14"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.mode+" "+testCase.redisURL, func(t *testing.T) {
			env := workerStartupTestEnv()
			env["DEPLOY_MODE"] = testCase.mode
			setStartupRedisCredential(
				t,
				env,
				testCase.mode,
				testCase.redisURL,
			)
			env["WORKER_DATABASE_URL_FILE"] = databasePath
			env["WORKER_ADMISSION_TOKEN_FILE"] = tokenPath
			delete(env, "DATABASE_URL")
			delete(env, "WORKER_ADMISSION_TOKEN")
			databaseOpens := 0
			err := runWorker(
				context.Background(),
				env,
				startupDependencies{
					openDatabase: func(
						context.Context,
						string,
					) (startupDatabase, error) {
						databaseOpens++
						return nil, errors.New("database open must not run")
					},
				},
			)
			if err == nil {
				t.Fatal("unsafe Redis scalar configuration was accepted")
			}
			if databaseOpens != 0 {
				t.Fatalf("database opens = %d; want 0", databaseOpens)
			}
			for _, credential := range []string{
				"synthetic-password",
				"synthetic-admission-secret",
				databasePath,
				tokenPath,
			} {
				if strings.Contains(err.Error(), credential) {
					t.Fatalf("startup error exposed credential %q: %v", credential, err)
				}
			}
		})
	}
}

func TestWorkerStartupRejectsUnsafeRedisRetriesInEveryModeBeforeDatabaseOpen(
	t *testing.T,
) {
	databasePath := writeStartupSecret(
		t,
		"database-url",
		"postgresql://runtime:synthetic-password@vp-postgres/videoprocess",
	)
	tokenPath := writeStartupSecret(
		t,
		"admission-token",
		"synthetic-admission-secret",
	)
	for _, mode := range []string{
		"local",
		"development",
		"test",
		"shared",
		"production",
	} {
		for _, redisQuery := range []string{
			"max_retries=-2",
			"min_retry_backoff=-2ms",
			"max_retry_backoff=-2ms",
		} {
			t.Run(mode+" "+redisQuery, func(t *testing.T) {
				env := workerStartupTestEnv()
				env["DEPLOY_MODE"] = mode
				setStartupRedisCredential(
					t,
					env,
					mode,
					"redis://worker:synthetic@127.0.0.1:6379/14?"+
						redisQuery,
				)
				env["WORKER_DATABASE_URL_FILE"] = databasePath
				env["WORKER_ADMISSION_TOKEN_FILE"] = tokenPath
				delete(env, "DATABASE_URL")
				delete(env, "WORKER_ADMISSION_TOKEN")
				databaseOpens := 0
				err := runWorker(
					context.Background(),
					env,
					startupDependencies{
						openDatabase: func(
							context.Context,
							string,
						) (startupDatabase, error) {
							databaseOpens++
							return nil, errors.New("database open must not run")
						},
					},
				)
				if err == nil {
					t.Fatal("unsafe Redis retry configuration was accepted")
				}
				if databaseOpens != 0 {
					t.Fatalf("database opens = %d; want 0", databaseOpens)
				}
				if err.Error() != "worker Redis configuration is invalid" {
					t.Fatalf("startup error = %q; want generic Redis error", err)
				}
			})
		}
	}
}

func TestWorkerStartupRejectsUnsafeRedisConnectionLifetimesInEveryModeBeforeDatabaseOpen(
	t *testing.T,
) {
	databasePath := writeStartupSecret(
		t,
		"database-url",
		"postgresql://runtime:synthetic-password@vp-postgres/videoprocess",
	)
	tokenPath := writeStartupSecret(
		t,
		"admission-token",
		"synthetic-admission-secret",
	)
	for _, mode := range []string{
		"local",
		"development",
		"test",
		"shared",
		"production",
	} {
		for _, redisQuery := range []string{
			"conn_max_idle_time=-2ns",
			"conn_max_lifetime=-1ns",
			"conn_max_lifetime=1s&conn_max_lifetime_jitter=-1ns",
			"conn_max_lifetime=9223372036854775807ns&conn_max_lifetime_jitter=9223372036854775807ns",
		} {
			t.Run(mode+" "+redisQuery, func(t *testing.T) {
				env := workerStartupTestEnv()
				env["DEPLOY_MODE"] = mode
				setStartupRedisCredential(
					t,
					env,
					mode,
					"redis://worker:synthetic@127.0.0.1:6379/14?"+
						redisQuery,
				)
				env["WORKER_DATABASE_URL_FILE"] = databasePath
				env["WORKER_ADMISSION_TOKEN_FILE"] = tokenPath
				delete(env, "DATABASE_URL")
				delete(env, "WORKER_ADMISSION_TOKEN")
				databaseOpens := 0
				err := runWorker(
					context.Background(),
					env,
					startupDependencies{
						openDatabase: func(
							context.Context,
							string,
						) (startupDatabase, error) {
							databaseOpens++
							return nil, errors.New("database open must not run")
						},
					},
				)
				if err == nil {
					t.Fatal("unsafe Redis connection lifetime was accepted")
				}
				if databaseOpens != 0 {
					t.Fatalf("database opens = %d; want 0", databaseOpens)
				}
				if err.Error() != "worker Redis configuration is invalid" {
					t.Fatalf("startup error = %q; want generic Redis error", err)
				}
			})
		}
	}
}

func TestWorkerStartupValidatesExactRedisACLIdentity(t *testing.T) {
	rawURL := strings.TrimSpace(os.Getenv("CHANNEL_OPS_GO_REDIS_TEST_URL"))
	if rawURL == "" {
		t.Skip("set CHANNEL_OPS_GO_REDIS_TEST_URL for Redis 7.4 ACL integration test")
	}
	adminOptions, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("parse Redis integration URL: %v", err)
	}
	admin := redis.NewClient(adminOptions)
	t.Cleanup(func() { _ = admin.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := admin.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis integration server: %v", err)
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	username := "vp-go-worker-" + suffix
	password := uuid.NewString()
	if err := admin.Do(
		ctx,
		"ACL",
		"SETUSER",
		username,
		"on",
		">"+password,
		"~*",
		"+@all",
	).Err(); err != nil {
		t.Fatalf("create Redis ACL test identity: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()
		_ = admin.Do(cleanupCtx, "ACL", "DELUSER", username).Err()
	})
	workerOptions := *adminOptions
	workerOptions.Username = username
	workerOptions.Password = password
	client := redis.NewClient(&workerOptions)
	t.Cleanup(func() { _ = client.Close() })
	if err := requireWorkerRedisIdentity(
		ctx,
		client,
		&workerOptions,
	); err != nil {
		t.Fatalf("validate exact Redis ACL identity: %v", err)
	}
	mismatchOptions := workerOptions
	mismatchOptions.Username += "-other"
	err = requireWorkerRedisIdentity(ctx, client, &mismatchOptions)
	if err == nil {
		t.Fatal("mismatched Redis ACL identity was accepted")
	}
	for _, sensitive := range []string{username, password} {
		if strings.Contains(err.Error(), sensitive) {
			t.Fatalf("Redis ACL error exposed identity material %q", sensitive)
		}
	}
	defaultOptions := workerOptions
	defaultOptions.Username = "default"
	if err := requireWorkerRedisIdentity(
		ctx,
		client,
		&defaultOptions,
	); err == nil {
		t.Fatal("default Redis ACL identity was accepted")
	}
}

func workerStartupTestEnv() map[string]string {
	return map[string]string{
		"DEPLOY_MODE":                 "production",
		"WORKER_SERVICE_NAME":         "vp-ffmpeg-go-worker-swarm",
		"WORKER_ADMISSION_GENERATION": "4",
		"WORKER_SLOT":                 "1",
		"WORKER_TYPE":                 "ffmpeg_go",
		"WORKER_HOST":                 "host127",
		"WORKER_CAPABILITIES":         "media_cpu",
		"WORKER_RELEASE_COMMIT":       "0123456789abcdef0123456789abcdef01234567",
		"VP_BUILD_COMMIT":             "0123456789abcdef0123456789abcdef01234567",
		"WORKER_IMAGE_IDENTITY":       "vp-ffmpeg-go-worker:deploy-0123456789ab",
		"WORKER_REDIS_STREAM":         "vp:tasks:ffmpeg_go",
		"WORKER_REDIS_GROUP":          "ffmpeg_go-workers",
		"STORAGE_BACKEND":             "minio",
		"MINIO_ENDPOINT":              "vp-minio:9000",
		"MINIO_BUCKET":                "videoprocess",
	}
}

func workerStartupTestSecrets(redisURL string) worker.SecretConfig {
	return worker.SecretConfig{
		DatabaseURL:    "postgresql://runtime:test@vp-postgres/videoprocess",
		AdmissionToken: "redacted",
		RedisURL:       redisURL,
		MinIOAccessKey: "synthetic-minio-access",
		MinIOSecretKey: "synthetic-minio-secret",
	}
}

func containsStartupCall(calls []string, want string) bool {
	for _, call := range calls {
		if call == want {
			return true
		}
	}
	return false
}

func stringPointer(value string) *string {
	return &value
}

func writeStartupSecret(t *testing.T, name string, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(value), 0o400); err != nil {
		t.Fatalf("write startup secret: %v", err)
	}
	if err := os.Chmod(path, 0o400); err != nil {
		t.Fatalf("chmod startup secret: %v", err)
	}
	return path
}

func setStartupRedisCredential(
	t *testing.T,
	env map[string]string,
	mode string,
	redisURL string,
) {
	t.Helper()
	switch mode {
	case "", "shared", "production":
		env["WORKER_REDIS_URL_FILE"] = writeStartupSecret(
			t,
			"redis-url",
			redisURL,
		)
		delete(env, "REDIS_URL")
	default:
		env["REDIS_URL"] = redisURL
		delete(env, "WORKER_REDIS_URL_FILE")
	}
}
