package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	registrationTestRelease = "0123456789abcdef0123456789abcdef01234567"
	registrationTestImage   = "vp-ffmpeg-go-worker:deploy-0123456789ab"
)

var registrationTestBindings = map[string]any{
	"database": map[string]any{
		"database": "videoprocess",
		"driver":   "postgresql",
		"host":     "vp-postgres-swarm",
		"port":     5432,
	},
	"redis": map[string]any{
		"database": 3,
		"host":     "vp-redis-swarm",
		"port":     6379,
		"scheme":   "redis",
	},
	"storage": map[string]any{
		"backend": "minio",
		"bucket":  "videoprocess",
		"host":    "vp-minio-swarm",
		"port":    9000,
	},
}

type workerPostgresFixture struct {
	ctx       context.Context
	admin     *pgxpool.Pool
	worker    *Store
	adminURL  string
	workerURL string
	role      string
	service   string
	stream    string
	group     string
	token     string
	claims    WorkerRegistrationClaims
}

type lifecycleTraceRecorder struct {
	queryStart   int
	queryEnd     int
	batchStart   int
	batchQuery   int
	batchEnd     int
	copyStart    int
	copyEnd      int
	prepareStart int
	prepareEnd   int
	acquireStart int
	acquireEnd   int
}

func (r *lifecycleTraceRecorder) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceQueryStartData,
) context.Context {
	r.queryStart++
	return ctx
}

func (r *lifecycleTraceRecorder) TraceQueryEnd(
	context.Context,
	*pgx.Conn,
	pgx.TraceQueryEndData,
) {
	r.queryEnd++
}

func (r *lifecycleTraceRecorder) TraceBatchStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceBatchStartData,
) context.Context {
	r.batchStart++
	return ctx
}

func (r *lifecycleTraceRecorder) TraceBatchQuery(
	context.Context,
	*pgx.Conn,
	pgx.TraceBatchQueryData,
) {
	r.batchQuery++
}

func (r *lifecycleTraceRecorder) TraceBatchEnd(
	context.Context,
	*pgx.Conn,
	pgx.TraceBatchEndData,
) {
	r.batchEnd++
}

func (r *lifecycleTraceRecorder) TraceCopyFromStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceCopyFromStartData,
) context.Context {
	r.copyStart++
	return ctx
}

func (r *lifecycleTraceRecorder) TraceCopyFromEnd(
	context.Context,
	*pgx.Conn,
	pgx.TraceCopyFromEndData,
) {
	r.copyEnd++
}

func (r *lifecycleTraceRecorder) TracePrepareStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TracePrepareStartData,
) context.Context {
	r.prepareStart++
	return ctx
}

func (r *lifecycleTraceRecorder) TracePrepareEnd(
	context.Context,
	*pgx.Conn,
	pgx.TracePrepareEndData,
) {
	r.prepareEnd++
}

func (r *lifecycleTraceRecorder) TraceAcquireStart(
	ctx context.Context,
	_ *pgxpool.Pool,
	_ pgxpool.TraceAcquireStartData,
) context.Context {
	r.acquireStart++
	return ctx
}

func (r *lifecycleTraceRecorder) TraceAcquireEnd(
	context.Context,
	*pgxpool.Pool,
	pgxpool.TraceAcquireEndData,
) {
	r.acquireEnd++
}

func TestRegistrationPostgres16TokenClaimsLeaseAndTakeover(t *testing.T) {
	fixture := newWorkerPostgresFixture(t)

	if _, err := fixture.worker.RegisterWorker(
		fixture.ctx,
		fixture.claims,
		"wrong-token",
	); workerRegistrationCode(err) != "token_invalid" {
		t.Fatalf("wrong token error = %v; want token_invalid", err)
	}
	mismatched := fixture.claims
	mismatched.WorkerHost = "host150"
	if _, err := fixture.worker.RegisterWorker(
		fixture.ctx,
		mismatched,
		fixture.token,
	); workerRegistrationCode(err) != "claim_mismatch" {
		t.Fatalf("wrong claim error = %v; want claim_mismatch", err)
	}

	firstClaims := fixture.claims
	firstClaims.WorkerInstanceID = uuid.New()
	firstClaims.RedisConsumerID = "ffmpeg_go-worker@host127:1:" + firstClaims.WorkerInstanceID.String()
	secondClaims := fixture.claims
	secondClaims.WorkerInstanceID = uuid.New()
	secondClaims.WorkerSlot = 2
	secondClaims.RedisConsumerID = "ffmpeg_go-worker@host127:2:" + secondClaims.WorkerInstanceID.String()

	var wait sync.WaitGroup
	wait.Add(2)
	leases := make(chan WorkerRegistrationLease, 2)
	errs := make(chan error, 2)
	for _, claims := range []WorkerRegistrationClaims{firstClaims, secondClaims} {
		claims := claims
		go func() {
			defer wait.Done()
			lease, err := fixture.worker.RegisterWorker(
				fixture.ctx,
				claims,
				fixture.token,
			)
			if err != nil {
				errs <- err
				return
			}
			leases <- lease
		}()
	}
	wait.Wait()
	close(leases)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent register: %v", err)
	}
	gotLeases := make([]WorkerRegistrationLease, 0, 2)
	for lease := range leases {
		gotLeases = append(gotLeases, lease)
	}
	if len(gotLeases) != 2 {
		t.Fatalf("registered leases = %d; want 2", len(gotLeases))
	}
	if gotLeases[0].LeaseEpoch == gotLeases[1].LeaseEpoch {
		t.Fatalf("concurrent epochs are equal: %d", gotLeases[0].LeaseEpoch)
	}
	active := gotLeases[0]
	stale := gotLeases[1]
	if active.LeaseEpoch < stale.LeaseEpoch {
		active, stale = stale, active
	}
	if active.LeaseEpoch != stale.LeaseEpoch+1 {
		t.Fatalf("epochs active/stale = %d/%d; want adjacent", active.LeaseEpoch, stale.LeaseEpoch)
	}

	databaseNow := time.Time{}
	if err := fixture.admin.QueryRow(
		fixture.ctx,
		"SELECT clock_timestamp()",
	).Scan(&databaseNow); err != nil {
		t.Fatalf("database clock: %v", err)
	}
	remaining := active.LeaseExpiresAt.Sub(databaseNow)
	if remaining < 175*time.Second || remaining > 181*time.Second {
		t.Fatalf("registration lease remaining = %s; want approximately 180s", remaining)
	}
	if _, err := fixture.worker.HeartbeatWorker(
		fixture.ctx,
		stale,
	); !errors.Is(err, ErrWorkerRegistrationLost) {
		t.Fatalf("stale heartbeat error = %v; want ErrWorkerRegistrationLost", err)
	}
	renewed, err := fixture.worker.HeartbeatWorker(fixture.ctx, active)
	if err != nil {
		t.Fatalf("active heartbeat: %v", err)
	}
	if !renewed.LeaseExpiresAt.After(active.LeaseExpiresAt) {
		t.Fatalf("heartbeat expiry %s did not advance from %s", renewed.LeaseExpiresAt, active.LeaseExpiresAt)
	}

	if err := fixture.worker.ReleaseWorker(
		fixture.ctx,
		renewed,
		"test_shutdown",
	); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := fixture.worker.RequireWorkerLease(
		fixture.ctx,
		renewed.RegistrationID,
		renewed.LeaseEpoch,
	); !errors.Is(err, ErrWorkerRegistrationLost) {
		t.Fatalf("require released lease error = %v; want ErrWorkerRegistrationLost", err)
	}

	expiringClaims := fixture.claims
	expiringClaims.WorkerInstanceID = uuid.New()
	expiringClaims.RedisConsumerID = "ffmpeg_go-worker@host127:3:" + expiringClaims.WorkerInstanceID.String()
	expiring, err := fixture.worker.RegisterWorker(
		fixture.ctx,
		expiringClaims,
		fixture.token,
	)
	if err != nil {
		t.Fatalf("register expiring lease: %v", err)
	}
	if _, err := fixture.admin.Exec(
		fixture.ctx,
		`UPDATE public.worker_registrations
		 SET registered_at = clock_timestamp() - interval '3 seconds',
		     heartbeat_at = clock_timestamp() - interval '2 seconds',
		     lease_expires_at = clock_timestamp() - interval '1 second'
		 WHERE id = $1`,
		expiring.RegistrationID,
	); err != nil {
		t.Fatalf("expire registration: %v", err)
	}
	if _, err := fixture.worker.HeartbeatWorker(
		fixture.ctx,
		expiring,
	); !errors.Is(err, ErrWorkerRegistrationExpired) {
		t.Fatalf("expired heartbeat error = %v; want ErrWorkerRegistrationExpired", err)
	}
}

func TestRegistrationPostgres16WorkerFenceOrdersCallbackBeforeTakeover(
	t *testing.T,
) {
	fixture := newWorkerPostgresFixture(t)
	lease := fixture.register(t)
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	fenceDone := make(chan error, 1)
	go func() {
		fenceDone <- fixture.worker.WithWorkerRegistrationFence(
			fixture.ctx,
			lease,
			func(context.Context) error {
				close(callbackStarted)
				<-releaseCallback
				return nil
			},
		)
	}()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		close(releaseCallback)
		t.Fatal("registration-fenced callback did not start")
	}

	takeoverClaims := fixture.claims
	takeoverClaims.WorkerInstanceID = uuid.New()
	takeoverClaims.RedisConsumerID = fmt.Sprintf(
		"ffmpeg_go-worker@host127:%d:%s",
		takeoverClaims.WorkerSlot,
		takeoverClaims.WorkerInstanceID,
	)
	takeoverDone := make(chan error, 1)
	go func() {
		_, err := fixture.worker.RegisterWorker(
			fixture.ctx,
			takeoverClaims,
			fixture.token,
		)
		takeoverDone <- err
	}()
	select {
	case err := <-takeoverDone:
		close(releaseCallback)
		t.Fatalf("takeover completed before fenced callback: %v", err)
	case <-time.After(75 * time.Millisecond):
	}
	close(releaseCallback)
	if err := <-fenceDone; err != nil {
		t.Fatalf("registration-fenced callback: %v", err)
	}
	if err := <-takeoverDone; err != nil {
		t.Fatalf("takeover after fenced callback: %v", err)
	}

	staleCallbackCalls := 0
	err := fixture.worker.WithWorkerRegistrationFence(
		fixture.ctx,
		lease,
		func(context.Context) error {
			staleCallbackCalls++
			return nil
		},
	)
	if !errors.Is(err, ErrWorkerRegistrationLost) {
		t.Fatalf("stale fenced callback error = %v; want registration lost", err)
	}
	if staleCallbackCalls != 0 {
		t.Fatalf("stale fenced callback calls = %d; want 0", staleCallbackCalls)
	}
}

func TestRegistrationErrorsExposeOnlyStableCodes(t *testing.T) {
	for _, code := range []string{
		"artifact_mismatch",
		"event_emission_mismatch",
		"task_delivery_attestation_missing",
		"lease_margin_insufficient",
		"production_task_authority_changed",
	} {
		err := sanitizeWorkerRegistrationError(&pgconn.PgError{
			Message: code,
			Detail:  "database-secret-that-must-not-escape",
			Hint:    "admission-token-that-must-not-escape",
		})
		if workerRegistrationCode(err) != code {
			t.Errorf("sanitized code = %q; want %q", workerRegistrationCode(err), code)
		}
		for _, secret := range []string{
			"database-secret-that-must-not-escape",
			"admission-token-that-must-not-escape",
		} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("sanitized %s error exposed %q", code, secret)
			}
		}
	}
	unknown := sanitizeWorkerRegistrationError(
		errors.New("transport failed for postgresql://user:secret@database"),
	)
	if workerRegistrationCode(unknown) != "lease_fenced" ||
		strings.Contains(unknown.Error(), "secret") {
		t.Fatalf("unknown sanitized error = %v; want lease_fenced", unknown)
	}
}

func TestRegistrationPostgres16ContinuityGateUsesDatabaseClock(t *testing.T) {
	fixture := newWorkerPostgresFixture(t)
	var previous []byte
	err := fixture.admin.QueryRow(
		fixture.ctx,
		`SELECT to_jsonb(status)
		 FROM public.worker_redis_continuity_status AS status
		 WHERE status.singleton`,
	).Scan(&previous)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("snapshot Redis continuity status: %v", err)
	}
	if _, err := fixture.admin.Exec(
		fixture.ctx,
		"DELETE FROM public.worker_redis_continuity_status",
	); err != nil {
		t.Fatalf("clear Redis continuity status: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()
		if _, cleanupErr := fixture.admin.Exec(
			cleanupCtx,
			"DELETE FROM public.worker_redis_continuity_status",
		); cleanupErr != nil {
			t.Errorf("clear test Redis continuity status: %v", cleanupErr)
			return
		}
		if len(previous) > 0 {
			if _, cleanupErr := fixture.admin.Exec(
				cleanupCtx,
				`INSERT INTO public.worker_redis_continuity_status
				 SELECT (
					jsonb_populate_record(
						NULL::public.worker_redis_continuity_status,
						$1::jsonb
					)
				 ).*`,
				string(previous),
			); cleanupErr != nil {
				t.Errorf("restore Redis continuity status: %v", cleanupErr)
			}
		}
	})
	startedAt := time.Now().UTC()
	if _, err := fixture.admin.Exec(
		fixture.ctx,
		`INSERT INTO public.worker_redis_continuity_status (
			singleton, run_id, state, reason_code, redis_run_id,
			expected_count, checked_count, started_at, lease_expires_at,
			finished_at
		 ) VALUES (
			TRUE, $1, 'ready', 'ready', 'go-integration-run', 0, 0,
			$2::timestamptz,
			$2::timestamptz + interval '300 seconds',
			clock_timestamp()
		 )`,
		uuid.New(),
		startedAt,
	); err != nil {
		t.Fatalf("insert ready Redis continuity status: %v", err)
	}

	if err := fixture.worker.RequireWorkerRedisContinuity(
		fixture.ctx,
		0,
	); workerRegistrationCode(err) !=
		"worker_redis_continuity_request_invalid" {
		t.Fatalf("invalid continuity request error = %v", err)
	}
	if err := fixture.worker.RequireWorkerRedisContinuity(
		fixture.ctx,
		90,
	); err != nil {
		t.Fatalf("fresh continuity gate: %v", err)
	}
	if _, err := fixture.admin.Exec(
		fixture.ctx,
		`UPDATE public.worker_redis_continuity_status
		 SET finished_at = clock_timestamp() - interval '91 seconds'
		 WHERE singleton`,
	); err != nil {
		t.Fatalf("age Redis continuity status: %v", err)
	}
	if err := fixture.worker.RequireWorkerRedisContinuity(
		fixture.ctx,
		90,
	); workerRegistrationCode(err) !=
		"worker_redis_continuity_stale" {
		t.Fatalf("stale continuity error = %v", err)
	}
}

func TestRegistrationPostgres16CloseContextOwnsHeldConnectionLifecycle(
	t *testing.T,
) {
	fixture := newWorkerPostgresFixture(t)
	connection, err := fixture.worker.Pool.Acquire(fixture.ctx)
	if err != nil {
		t.Fatalf("acquire worker pool connection: %v", err)
	}
	closeContext, cancelClose := context.WithTimeout(
		context.Background(),
		75*time.Millisecond,
	)
	defer cancelClose()
	started := time.Now()
	err = fixture.worker.CloseContext(closeContext)
	elapsed := time.Since(started)
	pingContext, cancelPing := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	pingErr := fixture.worker.Ping(pingContext)
	cancelPing()
	stack := make([]byte, 1<<20)
	stack = stack[:runtime.Stack(stack, true)]
	connection.Release()
	finalCloseContext, cancelFinalClose := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancelFinalClose()
	finalCloseErr := fixture.worker.CloseContext(finalCloseContext)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext error = %v; want stable deadline exceeded", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("CloseContext returned after %s; want bounded close", elapsed)
	}
	if pingErr != nil {
		t.Fatalf("pool was closed by timed-out CloseContext: %v", pingErr)
	}
	if strings.Contains(
		string(stack),
		"github.com/jackc/pgx/v5/pgxpool.(*Pool).Close",
	) {
		t.Fatal("timed-out CloseContext left an unowned pgxpool.Close goroutine")
	}
	if finalCloseErr != nil {
		t.Fatalf("CloseContext after release: %v", finalCloseErr)
	}
}

func TestRegistrationPostgres16CloseContextGatesRacingDirectAcquire(
	t *testing.T,
) {
	fixture := newWorkerPostgresFixture(t)
	held, err := fixture.worker.Pool.Acquire(fixture.ctx)
	if err != nil {
		t.Fatalf("acquire existing worker connection: %v", err)
	}
	defer held.Release()
	closeContext, cancelClose := context.WithTimeout(
		context.Background(),
		125*time.Millisecond,
	)
	defer cancelClose()
	closeDone := make(chan error, 1)
	closeStarted := time.Now()
	go func() {
		closeDone <- fixture.worker.CloseContext(closeContext)
	}()
	time.Sleep(25 * time.Millisecond)

	acquireContext, cancelAcquire := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	racing, acquireErr := fixture.worker.Pool.Acquire(acquireContext)
	cancelAcquire()
	if racing != nil {
		defer racing.Release()
	}
	if acquireErr == nil {
		t.Fatal("direct Pool.Acquire passed while CloseContext gate was closed")
	}
	select {
	case closeErr := <-closeDone:
		if !errors.Is(closeErr, context.DeadlineExceeded) {
			t.Fatalf(
				"CloseContext error = %v; want stable deadline exceeded",
				closeErr,
			)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("CloseContext exceeded its deadline with a racing Acquire")
	}
	if elapsed := time.Since(closeStarted); elapsed > 500*time.Millisecond {
		t.Fatalf("CloseContext returned after %s; want bounded close", elapsed)
	}

	pingContext, cancelPing := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	pingErr := fixture.worker.Ping(pingContext)
	cancelPing()
	if pingErr != nil {
		t.Fatalf("deadline path did not reopen acquisition gate: %v", pingErr)
	}
	held.Release()
	finalCloseContext, cancelFinalClose := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancelFinalClose()
	if err := fixture.worker.CloseContext(finalCloseContext); err != nil {
		t.Fatalf("CloseContext after held release: %v", err)
	}
}

func TestRegistrationPostgres16CloseContextWaitsForAcquireBeforePrepare(
	t *testing.T,
) {
	var armed atomic.Bool
	pingStarted := make(chan struct{})
	releasePing := make(chan struct{})
	worker := newPoolLifecycleTestStore(
		t,
		func(config *pgxpool.Config) {
			config.MaxConns = 1
			config.ShouldPing = func(
				context.Context,
				pgxpool.ShouldPingParams,
			) bool {
				if armed.CompareAndSwap(true, false) {
					close(pingStarted)
					<-releasePing
				}
				return false
			}
		},
	)
	armed.Store(true)
	acquireDone := make(chan error, 1)
	go func() {
		connection, err := worker.Pool.Acquire(context.Background())
		if connection != nil {
			connection.Release()
		}
		acquireDone <- err
	}()
	select {
	case <-pingStarted:
	case <-time.After(time.Second):
		close(releasePing)
		t.Fatal("Acquire did not stall before PrepareConn")
	}

	closeContext, cancelClose := context.WithTimeout(
		context.Background(),
		75*time.Millisecond,
	)
	defer cancelClose()
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- worker.CloseContext(closeContext)
	}()
	select {
	case closeErr := <-closeDone:
		if !errors.Is(closeErr, context.DeadlineExceeded) {
			close(releasePing)
			<-acquireDone
			t.Fatalf(
				"CloseContext error = %v; want deadline while pre-Prepare Acquire is admitted",
				closeErr,
			)
		}
	case <-time.After(300 * time.Millisecond):
		close(releasePing)
		acquireErr := <-acquireDone
		closeErr := <-closeDone
		t.Fatalf(
			"CloseContext crossed deadline before PrepareConn: close=%v acquire=%v",
			closeErr,
			acquireErr,
		)
	}
	close(releasePing)
	if err := <-acquireDone; err != nil {
		t.Fatalf("admitted Acquire after deadline reopen: %v", err)
	}
	finalCloseContext, cancelFinalClose := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancelFinalClose()
	if err := worker.CloseContext(finalCloseContext); err != nil {
		t.Fatalf("CloseContext after pre-Prepare Acquire ended: %v", err)
	}
}

func TestRegistrationPostgres16CloseContextAllowsClosedHijackToConverge(
	t *testing.T,
) {
	worker := newPoolLifecycleTestStore(t, nil)
	connection, err := worker.Pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire connection to hijack: %v", err)
	}
	raw := connection.Hijack()
	rawCloseContext, cancelRawClose := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	if err := raw.Close(rawCloseContext); err != nil {
		cancelRawClose()
		t.Fatalf("close hijacked connection: %v", err)
	}
	cancelRawClose()

	closeContext, cancelClose := context.WithTimeout(
		context.Background(),
		150*time.Millisecond,
	)
	defer cancelClose()
	if err := worker.CloseContext(closeContext); err != nil {
		t.Fatalf("CloseContext after raw hijacked close: %v", err)
	}
}

func TestRegistrationPostgres16ExternalStoreCloseFailsWithoutMutation(
	t *testing.T,
) {
	testURL := strings.TrimSpace(os.Getenv("CHANNEL_OPS_GO_POSTGRES_TEST_URL"))
	if testURL == "" {
		t.Skip("set CHANNEL_OPS_GO_POSTGRES_TEST_URL for PostgreSQL 16 worker integration tests")
	}
	config, err := pgxpool.ParseConfig(testURL)
	if err != nil {
		t.Fatalf("parse external pool config: %v", err)
	}
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open external pool: %v", err)
	}
	t.Cleanup(pool.Close)
	connection, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire external connection: %v", err)
	}
	var backendPID uint32
	if err := connection.QueryRow(
		context.Background(),
		"SELECT pg_backend_pid()",
	).Scan(&backendPID); err != nil {
		connection.Release()
		t.Fatalf("read external backend PID: %v", err)
	}
	external := &Store{Pool: pool}
	closeContext, cancelClose := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	closeStarted := time.Now()
	closeErr := external.CloseContext(closeContext)
	elapsed := time.Since(closeStarted)
	cancelClose()
	if !errors.Is(closeErr, ErrStorePoolNotOwned) {
		connection.Release()
		t.Fatalf("external CloseContext error = %v; want stable not-owned error", closeErr)
	}
	if elapsed > 25*time.Millisecond {
		connection.Release()
		t.Fatalf("external CloseContext elapsed = %s; want immediate failure", elapsed)
	}
	var heldValue int
	if err := connection.QueryRow(
		context.Background(),
		"SELECT 1",
	).Scan(&heldValue); err != nil || heldValue != 1 {
		connection.Release()
		t.Fatalf("external held connection mutated: value=%d err=%v", heldValue, err)
	}
	connection.Release()

	reused, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("reacquire external connection: %v", err)
	}
	defer reused.Release()
	var reusedPID uint32
	if err := reused.QueryRow(
		context.Background(),
		"SELECT pg_backend_pid()",
	).Scan(&reusedPID); err != nil {
		t.Fatalf("read reused external backend PID: %v", err)
	}
	if reusedPID != backendPID {
		t.Fatalf(
			"external backend PID changed across CloseContext = %d/%d; want no reset",
			backendPID,
			reusedPID,
		)
	}
}

func TestRegistrationPostgres16CloseContextSerializesConcurrentOwners(
	t *testing.T,
) {
	worker := newPoolLifecycleTestStore(t, nil)
	held, err := worker.Pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire concurrent-close holder: %v", err)
	}
	closeContext, cancelClose := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancelClose()
	closeResults := make(chan error, 2)
	for range 2 {
		go func() {
			closeResults <- worker.CloseContext(closeContext)
		}()
	}
	time.Sleep(25 * time.Millisecond)
	held.Release()
	for index := 0; index < 2; index++ {
		select {
		case err := <-closeResults:
			if err != nil {
				t.Fatalf("concurrent CloseContext %d: %v", index, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("concurrent CloseContext %d did not converge", index)
		}
	}
}

func TestRegistrationPoolGateComposesExistingTracerInterfaces(t *testing.T) {
	config, err := pgxpool.ParseConfig(
		"postgresql://worker:synthetic@127.0.0.1:5432/videoprocess",
	)
	if err != nil {
		t.Fatalf("parse tracer pool config: %v", err)
	}
	recorder := &lifecycleTraceRecorder{}
	config.ConnConfig.Tracer = recorder
	gate := newPoolAcquisitionGate()
	gate.install(config)
	ctx := context.Background()

	queryTracer := config.ConnConfig.Tracer
	queryContext := queryTracer.TraceQueryStart(
		ctx,
		nil,
		pgx.TraceQueryStartData{},
	)
	queryTracer.TraceQueryEnd(queryContext, nil, pgx.TraceQueryEndData{})
	batchTracer, ok := config.ConnConfig.Tracer.(pgx.BatchTracer)
	if !ok {
		t.Fatal("composed tracer dropped BatchTracer")
	}
	batchContext := batchTracer.TraceBatchStart(
		ctx,
		nil,
		pgx.TraceBatchStartData{},
	)
	batchTracer.TraceBatchQuery(batchContext, nil, pgx.TraceBatchQueryData{})
	batchTracer.TraceBatchEnd(batchContext, nil, pgx.TraceBatchEndData{})
	copyTracer, ok := config.ConnConfig.Tracer.(pgx.CopyFromTracer)
	if !ok {
		t.Fatal("composed tracer dropped CopyFromTracer")
	}
	copyContext := copyTracer.TraceCopyFromStart(
		ctx,
		nil,
		pgx.TraceCopyFromStartData{},
	)
	copyTracer.TraceCopyFromEnd(copyContext, nil, pgx.TraceCopyFromEndData{})
	prepareTracer, ok := config.ConnConfig.Tracer.(pgx.PrepareTracer)
	if !ok {
		t.Fatal("composed tracer dropped PrepareTracer")
	}
	prepareContext := prepareTracer.TracePrepareStart(
		ctx,
		nil,
		pgx.TracePrepareStartData{},
	)
	prepareTracer.TracePrepareEnd(
		prepareContext,
		nil,
		pgx.TracePrepareEndData{},
	)
	acquireTracer, ok := config.ConnConfig.Tracer.(pgxpool.AcquireTracer)
	if !ok {
		t.Fatal("composed tracer dropped AcquireTracer")
	}
	acquireContext := acquireTracer.TraceAcquireStart(
		ctx,
		nil,
		pgxpool.TraceAcquireStartData{},
	)
	acquireTracer.TraceAcquireEnd(
		acquireContext,
		nil,
		pgxpool.TraceAcquireEndData{},
	)
	if *recorder != (lifecycleTraceRecorder{
		queryStart:   1,
		queryEnd:     1,
		batchStart:   1,
		batchQuery:   1,
		batchEnd:     1,
		copyStart:    1,
		copyEnd:      1,
		prepareStart: 1,
		prepareEnd:   1,
		acquireStart: 1,
		acquireEnd:   1,
	}) {
		t.Fatalf("existing tracer calls = %#v; want every interface once", recorder)
	}
}

func TestRegistrationPostgres16PoolGatePreservesAcquireHooks(t *testing.T) {
	for _, usePrepare := range []bool{false, true} {
		name := "before-acquire"
		if usePrepare {
			name = "prepare-precedence"
		}
		t.Run(name, func(t *testing.T) {
			var prepareCalls atomic.Int32
			var beforeAcquireCalls atomic.Int32
			var afterReleaseCalls atomic.Int32
			var beforeCloseCalls atomic.Int32
			worker := newPoolLifecycleTestStore(
				t,
				func(config *pgxpool.Config) {
					config.MaxConns = 1
					config.BeforeAcquire = func(
						context.Context,
						*pgx.Conn,
					) bool {
						beforeAcquireCalls.Add(1)
						return true
					}
					if usePrepare {
						config.PrepareConn = func(
							context.Context,
							*pgx.Conn,
						) (bool, error) {
							prepareCalls.Add(1)
							return true, nil
						}
					}
					config.AfterRelease = func(*pgx.Conn) bool {
						afterReleaseCalls.Add(1)
						return true
					}
					config.BeforeClose = func(*pgx.Conn) {
						beforeCloseCalls.Add(1)
					}
				},
			)
			prepareBefore := prepareCalls.Load()
			acquireBefore := beforeAcquireCalls.Load()
			releaseBefore := afterReleaseCalls.Load()
			closeBefore := beforeCloseCalls.Load()

			connection, err := worker.Pool.Acquire(context.Background())
			if err != nil {
				t.Fatalf("acquire hook-preservation connection: %v", err)
			}
			connection.Release()
			releaseDeadline := time.Now().Add(time.Second)
			for afterReleaseCalls.Load()-releaseBefore == 0 &&
				time.Now().Before(releaseDeadline) {
				time.Sleep(time.Millisecond)
			}
			if got := afterReleaseCalls.Load() - releaseBefore; got != 1 {
				t.Fatalf("AfterRelease calls = %d; want 1", got)
			}
			if usePrepare {
				if got := prepareCalls.Load() - prepareBefore; got != 1 {
					t.Fatalf("PrepareConn calls = %d; want 1", got)
				}
				if got := beforeAcquireCalls.Load() - acquireBefore; got != 0 {
					t.Fatalf("BeforeAcquire calls with PrepareConn = %d; want 0", got)
				}
			} else if got := beforeAcquireCalls.Load() - acquireBefore; got != 1 {
				t.Fatalf("BeforeAcquire calls = %d; want 1", got)
			}
			closeContext, cancelClose := context.WithTimeout(
				context.Background(),
				time.Second,
			)
			defer cancelClose()
			if err := worker.CloseContext(closeContext); err != nil {
				t.Fatalf("close hook-preservation store: %v", err)
			}
			if got := beforeCloseCalls.Load() - closeBefore; got != 1 {
				t.Fatalf("BeforeClose calls = %d; want 1", got)
			}
		})
	}
}

func newPoolLifecycleTestStore(
	t *testing.T,
	configure func(*pgxpool.Config),
) *Store {
	t.Helper()
	testURL := strings.TrimSpace(os.Getenv("CHANNEL_OPS_GO_POSTGRES_TEST_URL"))
	if testURL == "" {
		t.Skip("set CHANNEL_OPS_GO_POSTGRES_TEST_URL for PostgreSQL 16 worker integration tests")
	}
	config, err := pgxpool.ParseConfig(testURL)
	if err != nil {
		t.Fatalf("parse pool lifecycle test config: %v", err)
	}
	if configure != nil {
		configure(config)
	}
	gate := newPoolAcquisitionGate()
	gate.install(config)
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open pool lifecycle test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	pingContext, cancelPing := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancelPing()
	if err := pool.Ping(pingContext); err != nil {
		t.Fatalf("ping pool lifecycle test pool: %v", err)
	}
	return &Store{Pool: pool, closeGate: gate}
}

func newWorkerPostgresFixture(t *testing.T) *workerPostgresFixture {
	t.Helper()
	testURL := strings.TrimSpace(os.Getenv("CHANNEL_OPS_GO_POSTGRES_TEST_URL"))
	if testURL == "" {
		t.Skip("set CHANNEL_OPS_GO_POSTGRES_TEST_URL for PostgreSQL 16 worker integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, testURL)
	if err != nil {
		t.Fatalf("open PostgreSQL integration admin: %v", err)
	}
	if err := admin.Ping(ctx); err != nil {
		admin.Close()
		t.Fatalf("ping PostgreSQL integration admin: %v", err)
	}
	var serverVersionText string
	if err := admin.QueryRow(ctx, "SHOW server_version_num").Scan(&serverVersionText); err != nil {
		admin.Close()
		t.Fatalf("read PostgreSQL version: %v", err)
	}
	serverVersion, err := strconv.Atoi(serverVersionText)
	if err != nil {
		admin.Close()
		t.Fatalf("parse PostgreSQL version %q: %v", serverVersionText, err)
	}
	if serverVersion < 160000 {
		admin.Close()
		t.Fatalf("PostgreSQL version = %d; require 160000 or newer", serverVersion)
	}
	var migration string
	if err := admin.QueryRow(ctx, "SELECT version_num FROM public.alembic_version").Scan(&migration); err != nil {
		admin.Close()
		t.Fatalf("read migration version: %v", err)
	}
	if migration != "034_worker_registrations" {
		admin.Close()
		t.Fatalf("migration version = %q; want 034_worker_registrations", migration)
	}

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	role := "vp_go_worker_" + suffix
	service := "vp-go-worker-" + suffix
	stream := "vp:test:tasks:" + suffix
	group := "vp-test-workers-" + suffix
	password := strings.ReplaceAll(uuid.NewString(), "-", "")
	token := "admission-" + suffix
	roleIdentifier := fmt.Sprintf("%q", role)
	if _, err := admin.Exec(
		ctx,
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", roleIdentifier, password),
	); err != nil {
		admin.Close()
		t.Fatalf("create worker role: %v", err)
	}

	signatures := []string{
		"vp_worker_register(text,bigint,text,text,uuid,integer,text,jsonb,text,text,text,text,jsonb,text,text,text,text,text)",
		"vp_worker_heartbeat(uuid,text,uuid,bigint,text)",
		"vp_worker_release(uuid,text,uuid,bigint,text,text)",
		"vp_require_worker_lease(uuid,bigint)",
		"vp_require_worker_redis_continuity(integer)",
		"vp_claim_worker_node(uuid,bigint,text,uuid,uuid,text,text,text,text,uuid)",
		"vp_require_worker_node_claim(uuid,bigint,text,timestamp with time zone,uuid,uuid)",
		"vp_persist_worker_artifact(uuid,bigint,text,timestamp with time zone,uuid,uuid,text,text,bigint,text,text,jsonb)",
		"vp_prepare_worker_event_emission(uuid,bigint,text,timestamp with time zone,uuid,uuid,uuid,text,text,text,jsonb,text)",
		"vp_mark_worker_event_emitted(uuid,uuid,bigint,text)",
		"vp_list_worker_prepared_event_emissions(uuid,bigint,integer)",
		"vp_load_worker_prepared_event_emission(uuid,uuid,bigint)",
		"vp_authorize_worker_task_ack(uuid,uuid,bigint,text,timestamp with time zone)",
		"vp_require_worker_task_ack_receipt(uuid,bigint,text,timestamp with time zone,text,text,text,text,uuid)",
		"vp_acknowledge_worker_task_delivery(uuid,uuid,bigint,text,timestamp with time zone,text,text,text,text,uuid)",
	}
	grantErr := func() error {
		grantTx, err := admin.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = grantTx.Rollback(ctx) }()
		if _, err := grantTx.Exec(
			ctx,
			"SELECT pg_advisory_xact_lock(8675309, 4)",
		); err != nil {
			return err
		}
		for _, signature := range signatures {
			if _, err := grantTx.Exec(
				ctx,
				fmt.Sprintf(
					"GRANT EXECUTE ON FUNCTION public.%s TO %s",
					signature,
					roleIdentifier,
				),
			); err != nil {
				return fmt.Errorf("grant %s: %w", signature, err)
			}
		}
		return grantTx.Commit(ctx)
	}()
	if grantErr != nil {
		dropWorkerRole(t, admin, roleIdentifier)
		admin.Close()
		t.Fatalf("grant worker functions: %v", grantErr)
	}

	bindingsJSON, err := json.Marshal(registrationTestBindings)
	if err != nil {
		t.Fatalf("marshal endpoint bindings: %v", err)
	}
	var databaseFingerprint string
	var redisFingerprint string
	var storageFingerprint string
	if err := admin.QueryRow(
		ctx,
		`SELECT database_fingerprint, redis_fingerprint, storage_fingerprint
		 FROM public.vp_worker_endpoint_fingerprints($1::jsonb)`,
		string(bindingsJSON),
	).Scan(&databaseFingerprint, &redisFingerprint, &storageFingerprint); err != nil {
		dropWorkerRole(t, admin, roleIdentifier)
		admin.Close()
		t.Fatalf("fingerprint endpoint bindings: %v", err)
	}
	if _, err := admin.Exec(
		ctx,
		`INSERT INTO public.worker_admission_grants (
			service_name, generation, worker_type, worker_host,
			capabilities_json, release_commit, image_identity,
			database_principal, redis_stream, redis_group,
			endpoint_bindings_json, token_sha256, state, issued_at,
			issued_by, activated_at
		) VALUES (
			$1, 1, 'ffmpeg_go', 'host127', '["media_cpu"]'::jsonb,
			$2, $3, $4, $5, $6,
			$7::jsonb, $8, 'active', clock_timestamp(), 'go-test',
			clock_timestamp()
		)`,
		service,
		registrationTestRelease,
		registrationTestImage,
		role,
		stream,
		group,
		string(bindingsJSON),
		testSHA256(token),
	); err != nil {
		dropWorkerRole(t, admin, roleIdentifier)
		admin.Close()
		t.Fatalf("insert worker admission grant: %v", err)
	}

	workerURL := databaseURLWithCredentials(t, testURL, role, password)
	worker, err := Open(ctx, workerURL)
	if err != nil {
		_, _ = admin.Exec(ctx, "DELETE FROM public.worker_admission_grants WHERE service_name = $1", service)
		dropWorkerRole(t, admin, roleIdentifier)
		admin.Close()
		t.Fatalf("open worker PostgreSQL store: %v", err)
	}
	fixture := &workerPostgresFixture{
		ctx:       ctx,
		admin:     admin,
		worker:    worker,
		adminURL:  testURL,
		workerURL: workerURL,
		role:      role,
		service:   service,
		stream:    stream,
		group:     group,
		token:     token,
		claims: WorkerRegistrationClaims{
			ServiceName:          service,
			Generation:           1,
			WorkerType:           "ffmpeg_go",
			WorkerHost:           "host127",
			WorkerInstanceID:     uuid.New(),
			WorkerSlot:           1,
			RedisConsumerID:      "ffmpeg_go-worker@host127:1:" + uuid.NewString(),
			Capabilities:         []string{"media_cpu"},
			ReleaseCommit:        registrationTestRelease,
			ImageIdentity:        registrationTestImage,
			RedisStream:          stream,
			RedisGroup:           group,
			EndpointBindingsJSON: string(bindingsJSON),
			DatabaseFingerprint:  databaseFingerprint,
			RedisFingerprint:     redisFingerprint,
			StorageFingerprint:   storageFingerprint,
		},
	}
	fixture.claims.RedisConsumerID = "ffmpeg_go-worker@host127:1:" + fixture.claims.WorkerInstanceID.String()
	t.Cleanup(func() {
		worker.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(
			cleanupCtx,
			"DELETE FROM public.worker_registrations WHERE service_name = $1",
			service,
		)
		_, _ = admin.Exec(
			cleanupCtx,
			"DELETE FROM public.worker_admission_grants WHERE service_name = $1",
			service,
		)
		dropWorkerRole(t, admin, roleIdentifier)
		admin.Close()
	})
	return fixture
}

func (f *workerPostgresFixture) register(t *testing.T) WorkerRegistrationLease {
	t.Helper()
	claims := f.claims
	claims.WorkerInstanceID = uuid.New()
	claims.RedisConsumerID = fmt.Sprintf(
		"ffmpeg_go-worker@host127:%d:%s",
		claims.WorkerSlot,
		claims.WorkerInstanceID,
	)
	lease, err := f.worker.RegisterWorker(f.ctx, claims, f.token)
	if err != nil {
		t.Fatalf("register worker fixture: %v", err)
	}
	return lease
}

func workerRegistrationCode(err error) string {
	var registrationError *WorkerRegistrationError
	if errors.As(err, &registrationError) {
		return registrationError.Code
	}
	return ""
}

func testSHA256(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func databaseURLWithCredentials(
	t *testing.T,
	rawURL string,
	username string,
	password string,
) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse PostgreSQL test URL: %v", err)
	}
	parsed.User = url.UserPassword(username, password)
	return parsed.String()
}

func dropWorkerRole(t *testing.T, admin *pgxpool.Pool, roleIdentifier string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dropTx, err := admin.Begin(ctx)
	if err != nil {
		t.Errorf("begin worker role cleanup: %v", err)
		return
	}
	defer func() { _ = dropTx.Rollback(ctx) }()
	if _, err := dropTx.Exec(
		ctx,
		"SELECT pg_advisory_xact_lock(8675309, 4)",
	); err != nil {
		t.Errorf("lock worker role cleanup: %v", err)
		return
	}
	if _, err := dropTx.Exec(
		ctx,
		fmt.Sprintf("DROP OWNED BY %s", roleIdentifier),
	); err != nil {
		t.Errorf("drop worker role ownership: %v", err)
		return
	}
	if _, err := dropTx.Exec(
		ctx,
		fmt.Sprintf("DROP ROLE IF EXISTS %s", roleIdentifier),
	); err != nil {
		t.Errorf("drop worker role: %v", err)
		return
	}
	if err := dropTx.Commit(ctx); err != nil {
		t.Errorf("commit worker role cleanup: %v", err)
	}
}
