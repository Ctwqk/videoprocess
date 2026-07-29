package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrWorkerRegistrationLost    = errors.New("worker registration lost")
	ErrWorkerRegistrationExpired = errors.New("worker registration expired")
)

type WorkerRegistrationError struct {
	Code string
}

func (e *WorkerRegistrationError) Error() string {
	if e == nil || e.Code == "" {
		return "worker_registration_unavailable"
	}
	return e.Code
}

func (e *WorkerRegistrationError) Is(target error) bool {
	switch target {
	case ErrWorkerRegistrationLost:
		return e != nil && (e.Code == "lease_fenced" || e.Code == "lease_expired")
	case ErrWorkerRegistrationExpired:
		return e != nil && e.Code == "lease_expired"
	default:
		return false
	}
}

type WorkerRegistrationClaims struct {
	ServiceName          string
	Generation           int64
	WorkerType           string
	WorkerHost           string
	WorkerInstanceID     uuid.UUID
	WorkerSlot           int
	RedisConsumerID      string
	Capabilities         []string
	ReleaseCommit        string
	ImageIdentity        string
	RedisStream          string
	RedisGroup           string
	EndpointBindingsJSON string
	DatabaseFingerprint  string
	RedisFingerprint     string
	StorageFingerprint   string
}

type WorkerRegistrationLease struct {
	RegistrationID   uuid.UUID
	GrantID          uuid.UUID
	ServiceName      string
	WorkerInstanceID uuid.UUID
	WorkerSlot       int
	RedisConsumerID  string
	LeaseEpoch       int64
	LeaseSecret      string
	LeaseExpiresAt   time.Time
}

func (s *Store) RegisterWorker(
	ctx context.Context,
	claims WorkerRegistrationClaims,
	admissionToken string,
) (WorkerRegistrationLease, error) {
	var empty WorkerRegistrationLease
	if s == nil || s.Pool == nil {
		return empty, &WorkerRegistrationError{Code: "lease_fenced"}
	}
	tokenHash, err := boundedSecretSHA256(admissionToken, "token_invalid")
	if err != nil {
		return empty, err
	}
	leaseSecret, err := newLeaseSecret()
	if err != nil {
		return empty, &WorkerRegistrationError{Code: "lease_fenced"}
	}
	leaseSecretHash, err := boundedSecretSHA256(
		leaseSecret,
		"claim_mismatch",
	)
	if err != nil {
		return empty, err
	}
	capabilitiesJSON, err := json.Marshal(claims.Capabilities)
	if err != nil {
		return empty, &WorkerRegistrationError{Code: "claim_mismatch"}
	}

	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	var registrationText string
	var grantText string
	var epoch int64
	var expiresAt time.Time
	err = tx.QueryRow(
		ctx,
		`
		SELECT registration_id::text, grant_id::text, lease_epoch,
		       lease_expires_at
		FROM public.vp_worker_register(
			$1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10,
			$11, $12, $13::jsonb, $14, $15, $16, $17, $18
		)
		`,
		claims.ServiceName,
		claims.Generation,
		claims.WorkerType,
		claims.WorkerHost,
		claims.WorkerInstanceID,
		claims.WorkerSlot,
		claims.RedisConsumerID,
		string(capabilitiesJSON),
		claims.ReleaseCommit,
		claims.ImageIdentity,
		claims.RedisStream,
		claims.RedisGroup,
		claims.EndpointBindingsJSON,
		claims.DatabaseFingerprint,
		claims.RedisFingerprint,
		claims.StorageFingerprint,
		tokenHash,
		leaseSecretHash,
	).Scan(
		&registrationText,
		&grantText,
		&epoch,
		&expiresAt,
	)
	if err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	registrationID, err := uuid.Parse(registrationText)
	if err != nil {
		return empty, &WorkerRegistrationError{Code: "lease_fenced"}
	}
	grantID, err := uuid.Parse(grantText)
	if err != nil {
		return empty, &WorkerRegistrationError{Code: "lease_fenced"}
	}
	if err := tx.Commit(ctx); err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	return WorkerRegistrationLease{
		RegistrationID:   registrationID,
		GrantID:          grantID,
		ServiceName:      claims.ServiceName,
		WorkerInstanceID: claims.WorkerInstanceID,
		WorkerSlot:       claims.WorkerSlot,
		RedisConsumerID:  claims.RedisConsumerID,
		LeaseEpoch:       epoch,
		LeaseSecret:      leaseSecret,
		LeaseExpiresAt:   expiresAt.UTC(),
	}, nil
}

func (s *Store) HeartbeatWorker(
	ctx context.Context,
	lease WorkerRegistrationLease,
) (WorkerRegistrationLease, error) {
	var empty WorkerRegistrationLease
	secretHash, err := boundedSecretSHA256(
		lease.LeaseSecret,
		"lease_fenced",
	)
	if err != nil {
		return empty, err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	var expiresAt time.Time
	if err := tx.QueryRow(
		ctx,
		`SELECT public.vp_worker_heartbeat($1, $2, $3, $4, $5)`,
		lease.RegistrationID,
		lease.ServiceName,
		lease.WorkerInstanceID,
		lease.LeaseEpoch,
		secretHash,
	).Scan(&expiresAt); err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	lease.LeaseExpiresAt = expiresAt.UTC()
	return lease, nil
}

func (s *Store) ReleaseWorker(
	ctx context.Context,
	lease WorkerRegistrationLease,
	reason string,
) error {
	normalizedReason := strings.TrimSpace(reason)
	if normalizedReason == "" || normalizedReason != reason || len(reason) > 255 {
		return &WorkerRegistrationError{Code: "claim_mismatch"}
	}
	secretHash, err := boundedSecretSHA256(
		lease.LeaseSecret,
		"lease_fenced",
	)
	if err != nil {
		return err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	var released bool
	if err := tx.QueryRow(
		ctx,
		`SELECT public.vp_worker_release($1, $2, $3, $4, $5, $6)`,
		lease.RegistrationID,
		lease.ServiceName,
		lease.WorkerInstanceID,
		lease.LeaseEpoch,
		secretHash,
		reason,
	).Scan(&released); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	if !released {
		return &WorkerRegistrationError{Code: "lease_fenced"}
	}
	if err := tx.Commit(ctx); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	return nil
}

func (s *Store) RequireWorkerLease(
	ctx context.Context,
	registrationID uuid.UUID,
	leaseEpoch int64,
) error {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	if _, err := tx.Exec(
		ctx,
		`SELECT public.vp_require_worker_lease($1, $2)`,
		registrationID,
		leaseEpoch,
	); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	return nil
}

func (s *Store) RequireWorkerRedisContinuity(
	ctx context.Context,
	maxAgeSeconds int,
) error {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	if _, err := tx.Exec(
		ctx,
		`SELECT public.vp_require_worker_redis_continuity($1)`,
		maxAgeSeconds,
	); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	return nil
}

func boundedSecretSHA256(value string, code string) (string, error) {
	if value == "" || len([]byte(value)) > 4096 {
		return "", &WorkerRegistrationError{Code: code}
	}
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:]), nil
}

func newLeaseSecret() (string, error) {
	material := make([]byte, 32)
	if _, err := rand.Read(material); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(material), nil
}

func rollbackWorkerTransaction(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func sanitizeWorkerRegistrationError(err error) error {
	if err == nil {
		return nil
	}
	var registrationError *WorkerRegistrationError
	if errors.As(err, &registrationError) {
		return registrationError
	}
	message := err.Error()
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		message = postgresError.Message
	}
	for _, code := range []string{
		"database_principal_privileged",
		"database_principal_mismatch",
		"grant_missing",
		"grant_disabled",
		"token_invalid",
		"claim_mismatch",
		"lease_expired",
		"lease_fenced",
		"lease_margin_insufficient",
		"worker_redis_continuity_request_invalid",
		"worker_redis_continuity_missing",
		"worker_redis_continuity_running",
		"worker_redis_continuity_error",
		"worker_redis_continuity_stale",
		"worker_redis_continuity_result_invalid",
		"worker_redis_continuity_run_expired",
		"worker_redis_continuity_run_mismatch",
		"task_dispatch_mismatch",
		"task_delivery_attestation_mismatch",
		"task_delivery_attestation_missing",
		"node_claim_mismatch",
		"job_authority_changed",
		"channel_authority_changed",
		"production_task_authority_changed",
		"schedule_authority_changed",
		"artifact_mismatch",
		"event_emission_missing",
		"event_emission_mismatch",
		"event_emission_unresolved",
		"event_receipt_missing",
		"task_ack_authority_missing",
	} {
		if strings.Contains(message, code) {
			return &WorkerRegistrationError{Code: code}
		}
	}
	return &WorkerRegistrationError{Code: "lease_fenced"}
}
