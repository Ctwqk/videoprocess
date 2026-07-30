package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	RegistrationLeaseDuration     = 180 * time.Second
	RegistrationHeartbeatInterval = 60 * time.Second
	WorkerRedisContinuityMaxAge   = 90
)

var ErrRegistrationLost = store.ErrWorkerRegistrationLost

type RegistrationClaims = store.WorkerRegistrationClaims
type RegistrationLease = store.WorkerRegistrationLease

type EndpointBindings struct {
	JSON         string
	Canonical    map[string]string
	Fingerprints map[string]string
}

type RegistrationStore interface {
	RegisterWorker(
		context.Context,
		RegistrationClaims,
		string,
	) (RegistrationLease, error)
	HeartbeatWorker(
		context.Context,
		RegistrationLease,
	) (RegistrationLease, error)
	ReleaseWorker(
		context.Context,
		RegistrationLease,
		string,
	) error
}

type Registration struct {
	service RegistrationStore
	claims  RegistrationClaims
	token   string

	mu                sync.RWMutex
	lease             RegistrationLease
	started           bool
	closed            bool
	ownedContext      context.Context
	cancelOwned       context.CancelCauseFunc
	heartbeatStop     context.CancelFunc
	heartbeatDone     chan struct{}
	lost              chan struct{}
	lostOnce          sync.Once
	lossPublished     bool
	lossGuardSequence uint64
	lossGuards        map[uint64]func()
	heartbeatInterval time.Duration
	closeTimeout      time.Duration
}

func NewRegistration(
	service RegistrationStore,
	claims RegistrationClaims,
	admissionToken string,
) *Registration {
	return &Registration{
		service:           service,
		claims:            claims,
		token:             admissionToken,
		ownedContext:      context.Background(),
		heartbeatDone:     make(chan struct{}),
		lost:              make(chan struct{}),
		heartbeatInterval: RegistrationHeartbeatInterval,
		closeTimeout:      5 * time.Second,
	}
}

func (r *Registration) Start(
	parent context.Context,
) (RegistrationLease, error) {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return RegistrationLease{}, &store.WorkerRegistrationError{
			Code: "lease_fenced",
		}
	}
	r.started = true
	r.ownedContext, r.cancelOwned = context.WithCancelCause(parent)
	heartbeatContext, heartbeatStop := context.WithCancel(r.ownedContext)
	r.heartbeatStop = heartbeatStop
	r.mu.Unlock()

	lease, err := r.service.RegisterWorker(
		parent,
		r.claims,
		r.token,
	)
	if err != nil {
		heartbeatStop()
		return RegistrationLease{}, err
	}
	r.mu.Lock()
	r.lease = lease
	r.mu.Unlock()
	renewed, err := r.heartbeat(parent)
	if err != nil {
		r.markLost()
		r.releaseBounded(lease, "startup_failed")
		heartbeatStop()
		return RegistrationLease{}, err
	}
	go r.heartbeatLoop(heartbeatContext)
	return renewed, nil
}

func (r *Registration) Context() context.Context {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ownedContext
}

func (r *Registration) Lease() RegistrationLease {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lease
}

func (r *Registration) WaitLost(ctx context.Context) error {
	select {
	case <-r.lost:
		return ErrRegistrationLost
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Registration) MarkLost() {
	r.markLost()
}

func (r *Registration) registerLossGuard(guard func()) (func(), bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lossPublished {
		return func() {}, false
	}
	if r.lossGuards == nil {
		r.lossGuards = make(map[uint64]func())
	}
	r.lossGuardSequence++
	guardID := r.lossGuardSequence
	r.lossGuards[guardID] = guard
	return func() {
		r.mu.Lock()
		delete(r.lossGuards, guardID)
		r.mu.Unlock()
	}, true
}

func (r *Registration) handoffIfActive(start func()) bool {
	if r == nil || start == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed ||
		r.lossPublished ||
		r.ownedContext == nil ||
		r.ownedContext.Err() != nil {
		return false
	}
	start()
	return true
}

func (r *Registration) Close(
	_ context.Context,
	reason string,
) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	stop := r.heartbeatStop
	started := r.started
	lease := r.lease
	r.mu.Unlock()
	if stop != nil {
		stop()
	}
	if started && stop != nil {
		select {
		case <-r.heartbeatDone:
		case <-time.After(r.closeTimeout):
		}
	}
	if lease.RegistrationID != uuid.Nil {
		r.releaseBounded(lease, reason)
	}
	return nil
}

func (r *Registration) heartbeat(
	ctx context.Context,
) (RegistrationLease, error) {
	r.mu.RLock()
	lease := r.lease
	r.mu.RUnlock()
	renewed, err := r.service.HeartbeatWorker(ctx, lease)
	if err != nil {
		return RegistrationLease{}, err
	}
	r.mu.Lock()
	r.lease = renewed
	r.mu.Unlock()
	return renewed, nil
}

func (r *Registration) heartbeatLoop(ctx context.Context) {
	defer close(r.heartbeatDone)
	interval := r.heartbeatInterval
	if interval <= 0 {
		interval = RegistrationHeartbeatInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.heartbeat(ctx); err != nil {
				r.markLost()
				return
			}
		}
	}
}

func (r *Registration) markLost() {
	r.lostOnce.Do(func() {
		r.mu.Lock()
		r.lossPublished = true
		cancel := r.cancelOwned
		guards := make([]func(), 0, len(r.lossGuards))
		for _, guard := range r.lossGuards {
			guards = append(guards, guard)
		}
		if cancel != nil {
			cancel(ErrRegistrationLost)
		}
		if r.lost != nil {
			close(r.lost)
		}
		r.mu.Unlock()
		for _, guard := range guards {
			guard()
		}
	})
}

func (r *Registration) releaseBounded(
	lease RegistrationLease,
	reason string,
) {
	timeout := r.closeTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = r.service.ReleaseWorker(ctx, lease, reason)
}

func BuildRegistrationClaims(
	env map[string]string,
	databaseURL string,
	instanceID uuid.UUID,
) (RegistrationClaims, error) {
	return BuildRegistrationClaimsWithRedis(
		env,
		databaseURL,
		env["REDIS_URL"],
		instanceID,
	)
}

func BuildRegistrationClaimsWithRedis(
	env map[string]string,
	databaseURL string,
	redisURL string,
	instanceID uuid.UUID,
) (RegistrationClaims, error) {
	if instanceID == uuid.Nil {
		return RegistrationClaims{}, &store.WorkerRegistrationError{
			Code: "claim_mismatch",
		}
	}
	serviceName, err := requiredClaim(env, "WORKER_SERVICE_NAME")
	if err != nil {
		return RegistrationClaims{}, err
	}
	workerType, err := requiredClaim(env, "WORKER_TYPE")
	if err != nil {
		return RegistrationClaims{}, err
	}
	workerType = strings.ToLower(workerType)
	workerHost, err := requiredClaim(env, "WORKER_HOST")
	if err != nil {
		return RegistrationClaims{}, err
	}
	generation, err := positiveClaimInteger(
		env,
		"WORKER_ADMISSION_GENERATION",
	)
	if err != nil {
		return RegistrationClaims{}, err
	}
	slot, err := positiveClaimInteger(env, "WORKER_SLOT")
	if err != nil {
		return RegistrationClaims{}, err
	}
	releaseCommit, err := requiredClaim(env, "WORKER_RELEASE_COMMIT")
	if err != nil {
		return RegistrationClaims{}, err
	}
	embeddedCommit := strings.TrimSpace(env["VP_BUILD_COMMIT"])
	deployMode := strings.ToLower(strings.TrimSpace(env["DEPLOY_MODE"]))
	production := deployMode == "" ||
		deployMode == "shared" ||
		deployMode == "production"
	if (production && embeddedCommit == "") ||
		(embeddedCommit != "" &&
			(!buildCommitPattern.MatchString(embeddedCommit) ||
				embeddedCommit != releaseCommit)) {
		return RegistrationClaims{}, claimMismatch()
	}
	imageIdentity, err := requiredClaim(env, "WORKER_IMAGE_IDENTITY")
	if err != nil {
		return RegistrationClaims{}, err
	}
	redisStream, err := requiredClaim(env, "WORKER_REDIS_STREAM")
	if err != nil {
		return RegistrationClaims{}, err
	}
	redisGroup, err := requiredClaim(env, "WORKER_REDIS_GROUP")
	if err != nil {
		return RegistrationClaims{}, err
	}
	rawCapabilities, err := requiredClaim(env, "WORKER_CAPABILITIES")
	if err != nil {
		return RegistrationClaims{}, err
	}
	capabilities := make([]string, 0)
	for _, capability := range strings.Split(rawCapabilities, ",") {
		capability = strings.TrimSpace(capability)
		if capability != "" {
			capabilities = append(capabilities, capability)
		}
	}
	sort.Strings(capabilities)
	if len(capabilities) == 0 {
		return RegistrationClaims{}, &store.WorkerRegistrationError{
			Code: "claim_mismatch",
		}
	}
	bindings, err := BuildEndpointBindingsWithRedis(
		env,
		databaseURL,
		redisURL,
	)
	if err != nil {
		return RegistrationClaims{}, err
	}
	return RegistrationClaims{
		ServiceName:          serviceName,
		Generation:           int64(generation),
		WorkerType:           workerType,
		WorkerHost:           workerHost,
		WorkerInstanceID:     instanceID,
		WorkerSlot:           slot,
		RedisConsumerID:      fmt.Sprintf("%s-worker@%s:%d:%s", workerType, workerHost, slot, instanceID),
		Capabilities:         capabilities,
		ReleaseCommit:        releaseCommit,
		ImageIdentity:        imageIdentity,
		RedisStream:          redisStream,
		RedisGroup:           redisGroup,
		EndpointBindingsJSON: bindings.JSON,
		DatabaseFingerprint:  bindings.Fingerprints["database"],
		RedisFingerprint:     bindings.Fingerprints["redis"],
		StorageFingerprint:   bindings.Fingerprints["storage"],
	}, nil
}

func BuildEndpointBindings(
	env map[string]string,
	databaseURL string,
) (EndpointBindings, error) {
	return BuildEndpointBindingsWithRedis(
		env,
		databaseURL,
		env["REDIS_URL"],
	)
}

func BuildEndpointBindingsWithRedis(
	env map[string]string,
	databaseURL string,
	redisURL string,
) (EndpointBindings, error) {
	database, err := databaseEndpointIdentity(databaseURL)
	if err != nil {
		return EndpointBindings{}, err
	}
	redisIdentity, err := redisEndpointIdentity(redisURL)
	if err != nil {
		return EndpointBindings{}, err
	}
	storageIdentity, err := storageEndpointIdentity(env)
	if err != nil {
		return EndpointBindings{}, err
	}
	values := map[string]any{
		"database": database,
		"redis":    redisIdentity,
		"storage":  storageIdentity,
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return EndpointBindings{}, claimMismatch()
	}
	return CanonicalEndpointBindingsJSON(encoded)
}

func CanonicalEndpointBindingsJSON(raw []byte) (EndpointBindings, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return EndpointBindings{}, claimMismatch()
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return EndpointBindings{}, claimMismatch()
	}
	root, ok := exactBindingMap(
		decoded,
		"database",
		"redis",
		"storage",
	)
	if !ok {
		return EndpointBindings{}, claimMismatch()
	}
	database, ok := validatedDatabaseBinding(root["database"])
	if !ok {
		return EndpointBindings{}, claimMismatch()
	}
	redisIdentity, ok := validatedRedisBinding(root["redis"])
	if !ok {
		return EndpointBindings{}, claimMismatch()
	}
	storageIdentity, ok := validatedStorageBinding(root["storage"])
	if !ok {
		return EndpointBindings{}, claimMismatch()
	}
	values := map[string]any{
		"database": database,
		"redis":    redisIdentity,
		"storage":  storageIdentity,
	}
	canonical := make(map[string]string, 3)
	fingerprints := make(map[string]string, 3)
	for name, value := range values {
		encoded, err := json.Marshal(value)
		if err != nil {
			return EndpointBindings{}, claimMismatch()
		}
		canonical[name] = string(encoded)
		digest := sha256.Sum256(encoded)
		fingerprints[name] = hex.EncodeToString(digest[:])
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return EndpointBindings{}, claimMismatch()
	}
	return EndpointBindings{
		JSON:         string(encoded),
		Canonical:    canonical,
		Fingerprints: fingerprints,
	}, nil
}

func validatedDatabaseBinding(value any) (map[string]any, bool) {
	binding, ok := exactBindingMap(
		value,
		"database",
		"driver",
		"host",
		"port",
	)
	if !ok || binding["driver"] != "postgresql" {
		return nil, false
	}
	database, ok := binding["database"].(string)
	if !ok || !validDependencyName(database) {
		return nil, false
	}
	host, ok := exactDependencyHost(binding["host"])
	if !ok {
		return nil, false
	}
	port, ok := exactJSONInteger(binding["port"], 1, 65535)
	if !ok {
		return nil, false
	}
	return map[string]any{
		"database": database,
		"driver":   "postgresql",
		"host":     host,
		"port":     port,
	}, true
}

func validatedRedisBinding(value any) (map[string]any, bool) {
	binding, ok := exactBindingMap(
		value,
		"database",
		"host",
		"port",
		"scheme",
	)
	if !ok {
		return nil, false
	}
	scheme, ok := binding["scheme"].(string)
	if !ok || scheme != "redis" && scheme != "rediss" {
		return nil, false
	}
	host, ok := exactDependencyHost(binding["host"])
	if !ok {
		return nil, false
	}
	port, ok := exactJSONInteger(binding["port"], 1, 65535)
	if !ok {
		return nil, false
	}
	database, ok := exactJSONInteger(
		binding["database"],
		0,
		2147483647,
	)
	if !ok {
		return nil, false
	}
	return map[string]any{
		"database": database,
		"host":     host,
		"port":     port,
		"scheme":   scheme,
	}, true
}

func validatedStorageBinding(value any) (map[string]any, bool) {
	if binding, ok := exactBindingMap(value, "backend"); ok &&
		binding["backend"] == "not_applicable" {
		return map[string]any{"backend": "not_applicable"}, true
	}
	binding, ok := exactBindingMap(
		value,
		"backend",
		"bucket",
		"host",
		"port",
	)
	if !ok || binding["backend"] != "minio" {
		return nil, false
	}
	bucket, ok := binding["bucket"].(string)
	if !ok || !validDependencyName(bucket) {
		return nil, false
	}
	host, ok := exactDependencyHost(binding["host"])
	if !ok {
		return nil, false
	}
	port, ok := exactJSONInteger(binding["port"], 1, 65535)
	if !ok {
		return nil, false
	}
	return map[string]any{
		"backend": "minio",
		"bucket":  bucket,
		"host":    host,
		"port":    port,
	}, true
}

func exactBindingMap(value any, expectedKeys ...string) (map[string]any, bool) {
	binding, ok := value.(map[string]any)
	if !ok || len(binding) != len(expectedKeys) {
		return nil, false
	}
	for _, key := range expectedKeys {
		if _, exists := binding[key]; !exists {
			return nil, false
		}
	}
	return binding, true
}

func exactDependencyHost(value any) (string, bool) {
	host, ok := value.(string)
	if !ok {
		return "", false
	}
	normalized, err := normalizedDependencyHost(host)
	if err != nil || normalized != host {
		return "", false
	}
	return host, true
}

func exactJSONInteger(value any, minimum int64, maximum int64) (int64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	rational, ok := new(big.Rat).SetString(number.String())
	if !ok || !rational.IsInt() || !rational.Num().IsInt64() {
		return 0, false
	}
	normalized := rational.Num().Int64()
	if normalized < minimum || normalized > maximum {
		return 0, false
	}
	return normalized, true
}

func databaseEndpointIdentity(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	schemeEnd := strings.Index(raw, "://")
	if schemeEnd < 1 {
		return nil, claimMismatch()
	}
	driver := strings.ToLower(strings.SplitN(raw[:schemeEnd], "+", 2)[0])
	if driver != "postgres" && driver != "postgresql" {
		return nil, claimMismatch()
	}
	config, err := pgx.ParseConfig(driver + raw[schemeEnd:])
	if err != nil {
		return nil, claimMismatch()
	}
	host, err := normalizedDependencyHost(config.Host)
	if err != nil {
		return nil, err
	}
	if config.Port == 0 || !validDependencyName(config.Database) {
		return nil, claimMismatch()
	}
	return map[string]any{
		"database": config.Database,
		"driver":   "postgresql",
		"host":     host,
		"port":     int(config.Port),
	}, nil
}

func redisEndpointIdentity(raw string) (map[string]any, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "redis" && parsed.Scheme != "rediss") {
		return nil, claimMismatch()
	}
	options, err := ParseWorkerRedisOptions(raw)
	if err != nil {
		return nil, claimMismatch()
	}
	hostValue, portValue, err := net.SplitHostPort(options.Addr)
	if err != nil {
		return nil, claimMismatch()
	}
	host, err := normalizedDependencyHost(hostValue)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portValue)
	if err != nil || port < 1 || port > 65535 ||
		options.DB < 0 || options.DB > 2147483647 {
		return nil, claimMismatch()
	}
	return map[string]any{
		"database": options.DB,
		"host":     host,
		"port":     port,
		"scheme":   parsed.Scheme,
	}, nil
}

func storageEndpointIdentity(env map[string]string) (map[string]any, error) {
	backend := strings.ToLower(strings.TrimSpace(env["STORAGE_BACKEND"]))
	if backend == "not_applicable" {
		return map[string]any{"backend": "not_applicable"}, nil
	}
	if backend != "minio" {
		return nil, claimMismatch()
	}
	rawEndpoint := strings.TrimSpace(env["MINIO_ENDPOINT"])
	if !strings.Contains(rawEndpoint, "://") {
		rawEndpoint = "http://" + rawEndpoint
	}
	parsed, err := url.Parse(rawEndpoint)
	if err != nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return nil, claimMismatch()
	}
	host, err := normalizedDependencyHost(parsed.Hostname())
	if err != nil {
		return nil, err
	}
	defaultPort := 9000
	if parsed.Scheme == "https" {
		defaultPort = 443
	}
	port, err := endpointPort(parsed, defaultPort)
	if err != nil {
		return nil, err
	}
	bucket := strings.TrimSpace(env["MINIO_BUCKET"])
	if !validDependencyName(bucket) {
		return nil, claimMismatch()
	}
	return map[string]any{
		"backend": "minio",
		"bucket":  bucket,
		"host":    host,
		"port":    port,
	}, nil
}

func endpointPort(parsed *url.URL, fallback int) (int, error) {
	raw := parsed.Port()
	if raw == "" {
		return fallback, nil
	}
	port, err := strconv.Atoi(raw)
	if err != nil || port < 1 || port > 65535 {
		return 0, claimMismatch()
	}
	return port, nil
}

var dependencyNamePattern = regexp.MustCompile(
	`^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$`,
)
var buildCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var dnsLabelPattern = regexp.MustCompile(
	`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`,
)
var numericIPv4Pattern = regexp.MustCompile(`^[0-9.]+$`)

func validDependencyName(value string) bool {
	return value != "" &&
		isASCII(value) &&
		dependencyNamePattern.MatchString(value)
}

func normalizedDependencyHost(raw string) (string, error) {
	host := strings.ToLower(
		strings.TrimSuffix(strings.Trim(raw, "[]"), "."),
	)
	if host == "" ||
		len(host) > 253 ||
		!isASCII(host) ||
		strings.Contains(host, ":") ||
		host == "localhost" ||
		host == "0.0.0.0" {
		return "", claimMismatch()
	}
	if address := net.ParseIP(host); address != nil {
		if address.To4() == nil ||
			address.String() != host ||
			address.IsLoopback() ||
			address.IsUnspecified() {
			return "", claimMismatch()
		}
		return host, nil
	}
	if numericIPv4Pattern.MatchString(host) {
		return "", claimMismatch()
	}
	for _, label := range strings.Split(host, ".") {
		if !dnsLabelPattern.MatchString(label) {
			return "", claimMismatch()
		}
	}
	return host, nil
}

func isASCII(value string) bool {
	for _, character := range value {
		if character > 127 {
			return false
		}
	}
	return true
}

func requiredClaim(
	env map[string]string,
	key string,
) (string, error) {
	value := strings.TrimSpace(env[key])
	if value == "" {
		return "", claimMismatch()
	}
	return value, nil
}

func positiveClaimInteger(
	env map[string]string,
	key string,
) (int, error) {
	value, err := requiredClaim(env, key)
	if err != nil {
		return 0, err
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return 0, claimMismatch()
		}
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, claimMismatch()
	}
	return parsed, nil
}

func claimMismatch() error {
	return &store.WorkerRegistrationError{Code: "claim_mismatch"}
}
