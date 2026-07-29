package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type registrationStoreStub struct {
	mu             sync.Mutex
	registerCalls  int
	heartbeatCalls int
	releaseCalls   int
	registered     RegistrationClaims
	lease          RegistrationLease
	heartbeatErr   error
}

func TestRegistrationLeaseAndHeartbeatDurationsAreExact(t *testing.T) {
	if RegistrationLeaseDuration != 180*time.Second {
		t.Fatalf(
			"registration lease duration = %s; want 180s",
			RegistrationLeaseDuration,
		)
	}
	if RegistrationHeartbeatInterval != 60*time.Second {
		t.Fatalf(
			"registration heartbeat interval = %s; want 60s",
			RegistrationHeartbeatInterval,
		)
	}
}

func (s *registrationStoreStub) RegisterWorker(
	_ context.Context,
	claims RegistrationClaims,
	_ string,
) (RegistrationLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registerCalls++
	s.registered = claims
	return s.lease, nil
}

func (s *registrationStoreStub) HeartbeatWorker(
	_ context.Context,
	lease RegistrationLease,
) (RegistrationLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeatCalls++
	if s.heartbeatErr != nil && s.heartbeatCalls > 1 {
		return RegistrationLease{}, s.heartbeatErr
	}
	lease.LeaseExpiresAt = time.Now().UTC().Add(RegistrationLeaseDuration)
	s.lease = lease
	return lease, nil
}

func (s *registrationStoreStub) ReleaseWorker(
	_ context.Context,
	_ RegistrationLease,
	_ string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCalls++
	return nil
}

func TestRegistrationFingerprintFixtureParity(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(
		"..", "..", "tests", "fixtures", "worker_registration", "fingerprints-v1.json",
	))
	if err != nil {
		t.Fatalf("read fingerprint fixture: %v", err)
	}
	var fixture struct {
		Cases []struct {
			Name      string            `json:"name"`
			Env       map[string]string `json:"env"`
			Canonical map[string]string `json:"canonical"`
			SHA256    map[string]string `json:"sha256"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode fingerprint fixture: %v", err)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("fingerprint fixture contains no cases")
	}
	for _, testCase := range fixture.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			bindings, err := BuildEndpointBindings(
				testCase.Env,
				testCase.Env["DATABASE_URL"],
			)
			if err != nil {
				t.Fatalf("BuildEndpointBindings: %v", err)
			}
			for _, dependency := range []string{"database", "redis", "storage"} {
				if bindings.Canonical[dependency] != testCase.Canonical[dependency] {
					t.Errorf("%s canonical = %q; want %q",
						dependency,
						bindings.Canonical[dependency],
						testCase.Canonical[dependency],
					)
				}
				if bindings.Fingerprints[dependency] != testCase.SHA256[dependency] {
					t.Errorf("%s fingerprint = %q; want %q",
						dependency,
						bindings.Fingerprints[dependency],
						testCase.SHA256[dependency],
					)
				}
			}
		})
	}
}

func TestRegistrationEndpointValidationFixtureParity(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(
		"..", "..", "tests", "fixtures", "worker_registration", "fingerprints-v1.json",
	))
	if err != nil {
		t.Fatalf("read fingerprint fixture: %v", err)
	}
	var fixture struct {
		ValidationCases []struct {
			Name         string                    `json:"name"`
			Accepted     bool                      `json:"accepted"`
			Bindings     map[string]map[string]any `json:"bindings"`
			BindingsJSON string                    `json:"bindings_json"`
			Replace      []any                     `json:"replace"`
			Extra        []any                     `json:"extra"`
			Canonical    map[string]string         `json:"canonical"`
			SHA256       map[string]string         `json:"sha256"`
		} `json:"endpoint_validation_cases"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode fingerprint fixture: %v", err)
	}
	if len(fixture.ValidationCases) == 0 ||
		len(fixture.ValidationCases[0].Bindings) == 0 {
		t.Fatal("endpoint validation fixture has no base bindings")
	}
	baseJSON, err := json.Marshal(fixture.ValidationCases[0].Bindings)
	if err != nil {
		t.Fatalf("marshal endpoint validation base: %v", err)
	}

	for _, testCase := range fixture.ValidationCases {
		t.Run(testCase.Name, func(t *testing.T) {
			caseJSON := []byte(testCase.BindingsJSON)
			if len(caseJSON) == 0 {
				var bindings map[string]map[string]any
				if len(testCase.Bindings) > 0 {
					bindings = testCase.Bindings
				} else if err := json.Unmarshal(baseJSON, &bindings); err != nil {
					t.Fatalf("copy endpoint validation base: %v", err)
				}
				if len(testCase.Replace) == 3 {
					dependency, dependencyOK := testCase.Replace[0].(string)
					field, fieldOK := testCase.Replace[1].(string)
					if !dependencyOK || !fieldOK {
						t.Fatalf("invalid replace fixture: %#v", testCase.Replace)
					}
					bindings[dependency][field] = testCase.Replace[2]
				}
				if len(testCase.Extra) == 3 {
					dependency, dependencyOK := testCase.Extra[0].(string)
					field, fieldOK := testCase.Extra[1].(string)
					if !dependencyOK || !fieldOK {
						t.Fatalf("invalid extra fixture: %#v", testCase.Extra)
					}
					bindings[dependency][field] = testCase.Extra[2]
				}
				caseJSON, err = json.Marshal(bindings)
				if err != nil {
					t.Fatalf("marshal endpoint validation case: %v", err)
				}
			}

			bindings, err := CanonicalEndpointBindingsJSON(caseJSON)
			if !testCase.Accepted {
				if err == nil {
					t.Fatalf("CanonicalEndpointBindingsJSON accepted %s", caseJSON)
				}
				return
			}
			if err != nil {
				t.Fatalf("CanonicalEndpointBindingsJSON: %v", err)
			}
			for _, dependency := range []string{"database", "redis", "storage"} {
				if got := bindings.Canonical[dependency]; got != testCase.Canonical[dependency] {
					t.Errorf("%s canonical = %q; want %q",
						dependency,
						got,
						testCase.Canonical[dependency],
					)
				}
				if got := bindings.Fingerprints[dependency]; got != testCase.SHA256[dependency] {
					t.Errorf("%s fingerprint = %q; want %q",
						dependency,
						got,
						testCase.SHA256[dependency],
					)
				}
			}
		})
	}
}

func TestRegistrationClaimsUseProcessInstanceInConsumerIdentity(t *testing.T) {
	instanceID := uuid.New()
	claims, err := BuildRegistrationClaims(registrationTestEnv(), "postgresql://runtime:secret@vp-postgres:5432/videoprocess", instanceID)
	if err != nil {
		t.Fatalf("BuildRegistrationClaims: %v", err)
	}
	if claims.WorkerInstanceID != instanceID {
		t.Fatalf("WorkerInstanceID = %s; want %s", claims.WorkerInstanceID, instanceID)
	}
	if want := instanceID.String(); !containsExactConsumerInstance(claims.RedisConsumerID, want) {
		t.Fatalf("RedisConsumerID = %q; want instance %q", claims.RedisConsumerID, want)
	}
}

func TestRegistrationClaimsRejectSignedPositiveIntegers(t *testing.T) {
	for _, key := range []string{
		"WORKER_ADMISSION_GENERATION",
		"WORKER_SLOT",
	} {
		t.Run(key, func(t *testing.T) {
			env := registrationTestEnv()
			env[key] = "+1"
			if _, err := BuildRegistrationClaims(
				env,
				"postgresql://runtime:secret@vp-postgres:5432/videoprocess",
				uuid.New(),
			); err == nil {
				t.Fatalf("BuildRegistrationClaims accepted signed %s", key)
			}
		})
	}
}

func TestRegistrationStartsWithHeartbeatAndLossCancelsOwnedContext(t *testing.T) {
	instanceID := uuid.New()
	claims, err := BuildRegistrationClaims(registrationTestEnv(), "postgresql://runtime:secret@vp-postgres:5432/videoprocess", instanceID)
	if err != nil {
		t.Fatalf("BuildRegistrationClaims: %v", err)
	}
	lease := RegistrationLease{
		RegistrationID:   uuid.New(),
		GrantID:          uuid.New(),
		ServiceName:      claims.ServiceName,
		WorkerInstanceID: instanceID,
		WorkerSlot:       claims.WorkerSlot,
		RedisConsumerID:  claims.RedisConsumerID,
		LeaseEpoch:       7,
		LeaseSecret:      "lease-secret",
		LeaseExpiresAt:   time.Now().UTC().Add(RegistrationLeaseDuration),
	}
	service := &registrationStoreStub{
		lease:        lease,
		heartbeatErr: errors.New("database transport detail that must be sanitized"),
	}
	registration := NewRegistration(service, claims, "admission-token")
	registration.heartbeatInterval = 5 * time.Millisecond
	registration.closeTimeout = 100 * time.Millisecond

	started, err := registration.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if started.LeaseEpoch != 7 {
		t.Fatalf("lease epoch = %d; want 7", started.LeaseEpoch)
	}
	service.mu.Lock()
	registerCalls := service.registerCalls
	heartbeatCalls := service.heartbeatCalls
	service.mu.Unlock()
	if registerCalls != 1 || heartbeatCalls != 1 {
		t.Fatalf("startup calls register=%d heartbeat=%d; want 1/1", registerCalls, heartbeatCalls)
	}

	select {
	case <-registration.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("registration loss did not cancel the owned context")
	}
	if !errors.Is(context.Cause(registration.Context()), ErrRegistrationLost) {
		t.Fatalf("registration context cause = %v; want ErrRegistrationLost", context.Cause(registration.Context()))
	}
	if err := registration.WaitLost(context.Background()); !errors.Is(err, ErrRegistrationLost) {
		t.Fatalf("WaitLost = %v; want ErrRegistrationLost", err)
	}
	if err := registration.Close(context.Background(), "shutdown"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	service.mu.Lock()
	releaseCalls := service.releaseCalls
	service.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("release calls = %d; want 1", releaseCalls)
	}
}

func TestRegistrationMarkLostPublishesStoreProvenLossAtomically(t *testing.T) {
	instanceID := uuid.New()
	claims, err := BuildRegistrationClaims(
		registrationTestEnv(),
		"postgresql://runtime:secret@vp-postgres:5432/videoprocess",
		instanceID,
	)
	if err != nil {
		t.Fatalf("BuildRegistrationClaims: %v", err)
	}
	service := &registrationStoreStub{
		lease: RegistrationLease{
			RegistrationID:   uuid.New(),
			GrantID:          uuid.New(),
			ServiceName:      claims.ServiceName,
			WorkerInstanceID: instanceID,
			WorkerSlot:       claims.WorkerSlot,
			RedisConsumerID:  claims.RedisConsumerID,
			LeaseEpoch:       8,
			LeaseSecret:      "lease-secret",
			LeaseExpiresAt: time.Now().UTC().
				Add(RegistrationLeaseDuration),
		},
	}
	registration := NewRegistration(service, claims, "admission-token")
	registration.closeTimeout = 100 * time.Millisecond
	if _, err := registration.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer registration.Close(context.Background(), "shutdown")

	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			registration.MarkLost()
		}()
	}
	wait.Wait()

	select {
	case <-registration.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("store-proven loss did not cancel registration context")
	}
	if !errors.Is(
		context.Cause(registration.Context()),
		ErrRegistrationLost,
	) {
		t.Fatalf(
			"registration context cause = %v; want ErrRegistrationLost",
			context.Cause(registration.Context()),
		)
	}
	if err := registration.WaitLost(
		context.Background(),
	); !errors.Is(err, ErrRegistrationLost) {
		t.Fatalf("WaitLost = %v; want ErrRegistrationLost", err)
	}
}

func TestRegistrationMarkLostInstallsExactCauseBeforeWaitingForGuard(
	t *testing.T,
) {
	ownedContext, cancelOwned := context.WithCancelCause(context.Background())
	registration := &Registration{
		ownedContext: ownedContext,
		cancelOwned:  cancelOwned,
		lost:         make(chan struct{}),
	}
	guardStarted := make(chan struct{})
	releaseGuard := make(chan struct{})
	_, registered := registration.registerLossGuard(func() {
		close(guardStarted)
		<-releaseGuard
	})
	if !registered {
		t.Fatal("loss guard was not registered")
	}
	markDone := make(chan struct{})
	go func() {
		registration.MarkLost()
		close(markDone)
	}()
	select {
	case <-guardStarted:
	case <-time.After(time.Second):
		close(releaseGuard)
		t.Fatal("loss guard did not start")
	}
	if !errors.Is(
		context.Cause(registration.Context()),
		ErrRegistrationLost,
	) {
		cause := context.Cause(registration.Context())
		close(releaseGuard)
		<-markDone
		t.Fatalf(
			"registration cause while guard waits = %v; want ErrRegistrationLost",
			cause,
		)
	}
	close(releaseGuard)
	select {
	case <-markDone:
	case <-time.After(time.Second):
		t.Fatal("MarkLost did not finish after guard release")
	}
}

func TestRegistrationMarkLostIsSafeForZeroValue(t *testing.T) {
	registration := &Registration{}
	registration.MarkLost()
	registration.MarkLost()
}

func registrationTestEnv() map[string]string {
	return map[string]string{
		"WORKER_SERVICE_NAME":         "vp-ffmpeg-go-worker-swarm",
		"WORKER_ADMISSION_GENERATION": "4",
		"WORKER_SLOT":                 "1",
		"WORKER_TYPE":                 "ffmpeg_go",
		"WORKER_HOST":                 "host127",
		"WORKER_CAPABILITIES":         "media_cpu",
		"WORKER_RELEASE_COMMIT":       "0123456789abcdef0123456789abcdef01234567",
		"WORKER_IMAGE_IDENTITY":       "vp-ffmpeg-go-worker:deploy-0123456789ab",
		"WORKER_REDIS_STREAM":         "vp:tasks:ffmpeg_go",
		"WORKER_REDIS_GROUP":          "ffmpeg_go-workers",
		"REDIS_URL":                   "redis://go-worker:secret@vp-redis:6379/3",
		"STORAGE_BACKEND":             "minio",
		"MINIO_ENDPOINT":              "vp-minio:9000",
		"MINIO_BUCKET":                "videoprocess",
	}
}

func containsExactConsumerInstance(consumerID string, instance string) bool {
	const separator = ":"
	if len(consumerID) <= len(instance) {
		return false
	}
	return consumerID[len(consumerID)-len(instance)-len(separator):] == separator+instance
}
