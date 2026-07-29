package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type WorkerTaskDeliveryProof struct {
	RedisStream   string
	ConsumerGroup string
	MessageID     string
	PayloadSHA256 string
	DispatchKey   uuid.UUID
}

type WorkerNodeClaim struct {
	RegistrationID  uuid.UUID
	LeaseEpoch      int64
	WorkerID        string
	WorkerStartedAt time.Time
	JobID           uuid.UUID
	NodeExecutionID uuid.UUID
	AttestationID   uuid.UUID
	Delivery        WorkerTaskDeliveryProof
}

type WorkerEventPublisher func(
	ctx context.Context,
	stream string,
	marker string,
	values map[string]string,
) (string, error)

type WorkerTaskAcknowledger func(
	ctx context.Context,
	stream string,
	group string,
	messageID string,
) (int64, error)

type PreparedWorkerEvent struct {
	ID                  uuid.UUID
	SourceAttestationID uuid.UUID
	Claim               WorkerNodeClaim
	RedisStream         string
	ConsumerGroup       string
	PayloadSHA256       string
	Values              map[string]string
	EventType           string
}

type WorkerTaskDispatchRow struct {
	ID                  uuid.UUID
	OriginReceiptID     *uuid.UUID
	DispatchKey         uuid.UUID
	JobID               uuid.UUID
	NodeExecutionID     uuid.UUID
	RedisStream         string
	ConsumerGroup       string
	PayloadSHA256       string
	Payload             map[string]string
	DeliveryState       string
	RedisMessageID      *string
	ResolutionState     string
	DeliveryAttemptedAt *time.Time
	DeliveryError       *string
	DeliveredAt         *time.Time
	AcknowledgedAt      *time.Time
	CancelledAt         *time.Time
	CreatedAt           time.Time
}

func CanonicalRedisPayloadSHA256(
	values map[string]string,
) (string, error) {
	encoded, err := canonicalRedisPayloadJSON(values)
	if err != nil {
		return "", errors.New("Redis payload is invalid")
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func CompletedWorkerEventValues(
	claim WorkerNodeClaim,
	proof WorkerTaskDeliveryProof,
	artifactID string,
) map[string]string {
	values := baseWorkerEventValues(claim, proof)
	values["event"] = "node_completed"
	values["output_artifact_id"] = strings.TrimSpace(artifactID)
	return values
}

func FailedWorkerEventValues(
	claim WorkerNodeClaim,
	proof WorkerTaskDeliveryProof,
	errorMessage string,
) map[string]string {
	values := baseWorkerEventValues(claim, proof)
	values["event"] = "node_failed"
	values["error"] = boundedUTF8(errorMessage, 2000)
	return values
}

func baseWorkerEventValues(
	claim WorkerNodeClaim,
	proof WorkerTaskDeliveryProof,
) map[string]string {
	return map[string]string{
		"job_id":                 claim.JobID.String(),
		"node_execution_id":      claim.NodeExecutionID.String(),
		"worker_id":              claim.WorkerID,
		"started_at":             claim.WorkerStartedAt.UTC().Format(time.RFC3339Nano),
		"worker_registration_id": claim.RegistrationID.String(),
		"worker_lease_epoch":     strconv.FormatInt(claim.LeaseEpoch, 10),
		"task_stream":            proof.RedisStream,
		"task_group":             proof.ConsumerGroup,
		"task_message_id":        proof.MessageID,
		"task_payload_sha256":    proof.PayloadSHA256,
		"task_dispatch_key":      proof.DispatchKey.String(),
	}
}

func (s *Store) PrepareWorkerEvent(
	ctx context.Context,
	claim WorkerNodeClaim,
	redisStream string,
	consumerGroup string,
	values map[string]string,
) (uuid.UUID, error) {
	if err := validateWorkerNodeClaim(claim); err != nil {
		return uuid.Nil, err
	}
	if err := validateWorkerEventValues(claim, values); err != nil {
		return uuid.Nil, err
	}
	if strings.TrimSpace(redisStream) == "" ||
		strings.TrimSpace(consumerGroup) == "" {
		return uuid.Nil, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	payloadHash, err := CanonicalRedisPayloadSHA256(values)
	if err != nil {
		return uuid.Nil, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	payloadJSON, err := canonicalRedisPayloadJSON(values)
	if err != nil {
		return uuid.Nil, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return uuid.Nil, sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	var emissionText string
	if err := tx.QueryRow(
		ctx,
		`
		SELECT public.vp_prepare_worker_event_emission(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11::jsonb, $12
		)::text
		`,
		claim.RegistrationID,
		claim.LeaseEpoch,
		claim.WorkerID,
		claim.WorkerStartedAt,
		claim.JobID,
		claim.NodeExecutionID,
		claim.AttestationID,
		redisStream,
		consumerGroup,
		payloadHash,
		string(payloadJSON),
		values["event"],
	).Scan(&emissionText); err != nil {
		return uuid.Nil, sanitizeWorkerRegistrationError(err)
	}
	emissionID, err := uuid.Parse(emissionText)
	if err != nil {
		return uuid.Nil, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, sanitizeWorkerRegistrationError(err)
	}
	return emissionID, nil
}

func (s *Store) PublishPreparedWorkerEvent(
	ctx context.Context,
	lease WorkerRegistrationLease,
	emissionID uuid.UUID,
	publish WorkerEventPublisher,
) (WorkerNodeClaim, error) {
	var empty WorkerNodeClaim
	if publish == nil || emissionID == uuid.Nil {
		return empty, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	emission, err := loadPreparedWorkerEvent(
		ctx,
		tx,
		emissionID,
		lease,
	)
	if err != nil {
		return empty, err
	}
	marker := "vp:worker-event-emission:" + emission.ID.String()
	messageID, err := runWorkerCallback(ctx, func() (string, error) {
		return publish(
			ctx,
			emission.RedisStream,
			marker,
			copyStringMap(emission.Values),
		)
	})
	if err != nil {
		if ctx.Err() != nil {
			return empty, context.Cause(ctx)
		}
		return empty, errors.New("worker event publication failed")
	}
	if strings.TrimSpace(messageID) == "" ||
		messageID != strings.TrimSpace(messageID) {
		return empty, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	if _, err := tx.Exec(
		ctx,
		`SELECT public.vp_mark_worker_event_emitted($1, $2, $3, $4)`,
		emission.ID,
		lease.RegistrationID,
		lease.LeaseEpoch,
		messageID,
	); err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	return emission.Claim, nil
}

func (s *Store) ListPreparedWorkerEventIDs(
	ctx context.Context,
	lease WorkerRegistrationLease,
	limit int,
) ([]uuid.UUID, error) {
	if limit < 1 || limit > 100 {
		return nil, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	rows, err := tx.Query(
		ctx,
		`
		SELECT emission_id::text
		FROM public.vp_list_worker_prepared_event_emissions($1, $2, $3)
		`,
		lease.RegistrationID,
		lease.LeaseEpoch,
		limit,
	)
	if err != nil {
		return nil, sanitizeWorkerRegistrationError(err)
	}
	defer rows.Close()
	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, sanitizeWorkerRegistrationError(err)
		}
		id, err := uuid.Parse(raw)
		if err != nil {
			return nil, &WorkerRegistrationError{
				Code: "event_emission_mismatch",
			}
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, sanitizeWorkerRegistrationError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, sanitizeWorkerRegistrationError(err)
	}
	return ids, nil
}

func (s *Store) AcknowledgeWorkerTask(
	ctx context.Context,
	claim WorkerNodeClaim,
	acknowledge WorkerTaskAcknowledger,
) error {
	if acknowledge == nil {
		return &WorkerRegistrationError{
			Code: "task_ack_authority_missing",
		}
	}
	if err := validateWorkerNodeClaim(claim); err != nil {
		return err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	if _, err := tx.Exec(
		ctx,
		`SELECT public.vp_authorize_worker_task_ack($1, $2, $3, $4, $5)`,
		claim.AttestationID,
		claim.RegistrationID,
		claim.LeaseEpoch,
		claim.WorkerID,
		claim.WorkerStartedAt,
	); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	if err := requireWorkerTaskAckReceipt(
		ctx,
		tx,
		claim,
	); err != nil {
		return err
	}
	result, err := runWorkerCallback(ctx, func() (int64, error) {
		return acknowledge(
			ctx,
			claim.Delivery.RedisStream,
			claim.Delivery.ConsumerGroup,
			claim.Delivery.MessageID,
		)
	})
	if err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return errors.New("worker task acknowledgement failed")
	}
	if result != 0 && result != 1 {
		return &WorkerRegistrationError{
			Code: "task_ack_authority_missing",
		}
	}
	if err := acknowledgeWorkerTaskDelivery(
		ctx,
		tx,
		claim,
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	return nil
}

func (s *Store) AcknowledgeWorkerTaskFromReceipt(
	ctx context.Context,
	claim WorkerNodeClaim,
	acknowledge WorkerTaskAcknowledger,
) error {
	if acknowledge == nil {
		return &WorkerRegistrationError{
			Code: "event_receipt_missing",
		}
	}
	if err := validateWorkerNodeClaim(claim); err != nil {
		return err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	if err := requireWorkerTaskAckReceipt(ctx, tx, claim); err != nil {
		return err
	}
	result, err := runWorkerCallback(ctx, func() (int64, error) {
		return acknowledge(
			ctx,
			claim.Delivery.RedisStream,
			claim.Delivery.ConsumerGroup,
			claim.Delivery.MessageID,
		)
	})
	if err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return errors.New("worker task acknowledgement failed")
	}
	if result != 0 && result != 1 {
		return &WorkerRegistrationError{
			Code: "task_ack_authority_missing",
		}
	}
	if err := acknowledgeWorkerTaskDelivery(
		ctx,
		tx,
		claim,
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	return nil
}

func (s *Store) LoadWorkerTaskDispatch(
	ctx context.Context,
	dispatchKey uuid.UUID,
) (WorkerTaskDispatchRow, error) {
	return scanWorkerTaskDispatch(s.Pool.QueryRow(
		ctx,
		workerTaskDispatchSelect+`
		WHERE dispatch_key = $1
		`,
		dispatchKey,
	))
}

func (s *Store) LoadInitialWorkerTaskDispatch(
	ctx context.Context,
	nodeExecutionID uuid.UUID,
) (WorkerTaskDispatchRow, error) {
	return scanWorkerTaskDispatch(s.Pool.QueryRow(
		ctx,
		workerTaskDispatchSelect+`
		WHERE node_execution_id = $1
		  AND origin_receipt_id IS NULL
		ORDER BY created_at DESC, id DESC
		LIMIT 1
		`,
		nodeExecutionID,
	))
}

func (s *Store) LoadDownstreamWorkerTaskDispatch(
	ctx context.Context,
	originReceiptID uuid.UUID,
	nodeExecutionID uuid.UUID,
) (WorkerTaskDispatchRow, error) {
	return scanWorkerTaskDispatch(s.Pool.QueryRow(
		ctx,
		workerTaskDispatchSelect+`
		WHERE origin_receipt_id = $1
		  AND node_execution_id = $2
		`,
		originReceiptID,
		nodeExecutionID,
	))
}

func (s *Store) LoadRetryWorkerTaskDispatch(
	ctx context.Context,
	dispatchID uuid.UUID,
) (WorkerTaskDispatchRow, error) {
	return scanWorkerTaskDispatch(s.Pool.QueryRow(
		ctx,
		workerTaskDispatchSelect+`
		WHERE id = $1
		  AND delivery_state IN ('attempting', 'uncertain')
		`,
		dispatchID,
	))
}

const workerTaskDispatchSelect = `
		SELECT id::text, origin_receipt_id::text, dispatch_key::text,
		       job_id::text, node_execution_id::text, redis_stream,
		       consumer_group, payload_sha256, payload_json, delivery_state,
		       delivery_attempted_at, delivery_error, redis_message_id,
		       resolution_state, acknowledged_at, cancelled_at, created_at,
		       delivered_at
		FROM public.worker_task_dispatches
`

type workerTaskDispatchScanner interface {
	Scan(...any) error
}

func scanWorkerTaskDispatch(
	scanner workerTaskDispatchScanner,
) (WorkerTaskDispatchRow, error) {
	var row WorkerTaskDispatchRow
	var idText string
	var originText *string
	var dispatchText string
	var jobText string
	var nodeText string
	var payloadJSON []byte
	err := scanner.Scan(
		&idText,
		&originText,
		&dispatchText,
		&jobText,
		&nodeText,
		&row.RedisStream,
		&row.ConsumerGroup,
		&row.PayloadSHA256,
		&payloadJSON,
		&row.DeliveryState,
		&row.DeliveryAttemptedAt,
		&row.DeliveryError,
		&row.RedisMessageID,
		&row.ResolutionState,
		&row.AcknowledgedAt,
		&row.CancelledAt,
		&row.CreatedAt,
		&row.DeliveredAt,
	)
	if err != nil {
		return WorkerTaskDispatchRow{}, err
	}
	rawIDs := []string{idText, dispatchText, jobText, nodeText}
	targetIDs := []*uuid.UUID{
		&row.ID,
		&row.DispatchKey,
		&row.JobID,
		&row.NodeExecutionID,
	}
	for index, raw := range rawIDs {
		parsed, err := uuid.Parse(raw)
		if err != nil {
			return WorkerTaskDispatchRow{}, err
		}
		*targetIDs[index] = parsed
	}
	if originText != nil {
		originID, err := uuid.Parse(*originText)
		if err != nil {
			return WorkerTaskDispatchRow{}, err
		}
		row.OriginReceiptID = &originID
	}
	if err := json.Unmarshal(payloadJSON, &row.Payload); err != nil {
		return WorkerTaskDispatchRow{}, err
	}
	return row, nil
}

func loadPreparedWorkerEvent(
	ctx context.Context,
	tx pgx.Tx,
	emissionID uuid.UUID,
	lease WorkerRegistrationLease,
) (PreparedWorkerEvent, error) {
	var emissionText string
	var attestationText string
	var jobText string
	var nodeText string
	var payloadJSON []byte
	var emission PreparedWorkerEvent
	err := tx.QueryRow(
		ctx,
		`
		SELECT emission_id::text, source_task_attestation_id::text,
		       job_id::text, node_execution_id::text, worker_id,
		       worker_started_at, redis_stream, consumer_group,
		       payload_sha256, payload_json, event_type
		FROM public.vp_load_worker_prepared_event_emission($1, $2, $3)
		`,
		emissionID,
		lease.RegistrationID,
		lease.LeaseEpoch,
	).Scan(
		&emissionText,
		&attestationText,
		&jobText,
		&nodeText,
		&emission.Claim.WorkerID,
		&emission.Claim.WorkerStartedAt,
		&emission.RedisStream,
		&emission.ConsumerGroup,
		&emission.PayloadSHA256,
		&payloadJSON,
		&emission.EventType,
	)
	if err != nil {
		return PreparedWorkerEvent{}, sanitizeWorkerRegistrationError(err)
	}
	emission.ID, err = uuid.Parse(emissionText)
	if err != nil {
		return PreparedWorkerEvent{}, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	emission.SourceAttestationID, err = uuid.Parse(attestationText)
	if err != nil {
		return PreparedWorkerEvent{}, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	emission.Claim.JobID, err = uuid.Parse(jobText)
	if err != nil {
		return PreparedWorkerEvent{}, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	emission.Claim.NodeExecutionID, err = uuid.Parse(nodeText)
	if err != nil {
		return PreparedWorkerEvent{}, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	emission.Claim.RegistrationID = lease.RegistrationID
	emission.Claim.LeaseEpoch = lease.LeaseEpoch
	emission.Claim.AttestationID = emission.SourceAttestationID
	if err := json.Unmarshal(payloadJSON, &emission.Values); err != nil {
		return PreparedWorkerEvent{}, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	payloadHash, err := CanonicalRedisPayloadSHA256(emission.Values)
	if err != nil ||
		payloadHash != emission.PayloadSHA256 ||
		emission.Values["event"] != emission.EventType {
		return PreparedWorkerEvent{}, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	proof, err := deliveryProofFromEventValues(emission.Values)
	if err != nil {
		return PreparedWorkerEvent{}, err
	}
	emission.Claim.Delivery = proof
	if err := validateWorkerEventValues(
		emission.Claim,
		emission.Values,
	); err != nil {
		return PreparedWorkerEvent{}, err
	}
	return emission, nil
}

func requireWorkerTaskAckReceipt(
	ctx context.Context,
	tx pgx.Tx,
	claim WorkerNodeClaim,
) error {
	proof := claim.Delivery
	if _, err := tx.Exec(
		ctx,
		`
		SELECT public.vp_require_worker_task_ack_receipt(
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
		`,
		claim.RegistrationID,
		claim.LeaseEpoch,
		claim.WorkerID,
		claim.WorkerStartedAt,
		proof.RedisStream,
		proof.ConsumerGroup,
		proof.MessageID,
		proof.PayloadSHA256,
		proof.DispatchKey,
	); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	return nil
}

func acknowledgeWorkerTaskDelivery(
	ctx context.Context,
	tx pgx.Tx,
	claim WorkerNodeClaim,
) error {
	proof := claim.Delivery
	if _, err := tx.Exec(
		ctx,
		`
		SELECT public.vp_acknowledge_worker_task_delivery(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
		`,
		claim.AttestationID,
		claim.RegistrationID,
		claim.LeaseEpoch,
		claim.WorkerID,
		claim.WorkerStartedAt,
		proof.RedisStream,
		proof.ConsumerGroup,
		proof.MessageID,
		proof.PayloadSHA256,
		proof.DispatchKey,
	); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	return nil
}

func validateWorkerNodeClaim(claim WorkerNodeClaim) error {
	if claim.RegistrationID == uuid.Nil ||
		claim.LeaseEpoch <= 0 ||
		strings.TrimSpace(claim.WorkerID) == "" ||
		claim.WorkerStartedAt.IsZero() ||
		claim.JobID == uuid.Nil ||
		claim.NodeExecutionID == uuid.Nil ||
		claim.AttestationID == uuid.Nil {
		return &WorkerRegistrationError{Code: "claim_mismatch"}
	}
	return validateWorkerTaskDeliveryProof(claim.Delivery)
}

func validateWorkerTaskDeliveryProof(
	proof WorkerTaskDeliveryProof,
) error {
	if strings.TrimSpace(proof.RedisStream) == "" ||
		strings.TrimSpace(proof.ConsumerGroup) == "" ||
		strings.TrimSpace(proof.MessageID) == "" ||
		len(proof.PayloadSHA256) != 64 ||
		proof.DispatchKey == uuid.Nil {
		return &WorkerRegistrationError{Code: "task_dispatch_mismatch"}
	}
	for _, character := range proof.PayloadSHA256 {
		if character < '0' ||
			character > '9' && character < 'a' ||
			character > 'f' {
			return &WorkerRegistrationError{
				Code: "task_dispatch_mismatch",
			}
		}
	}
	return nil
}

func validateWorkerEventValues(
	claim WorkerNodeClaim,
	values map[string]string,
) error {
	if values == nil || len(values) != 13 {
		return &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	proof := claim.Delivery
	expected := map[string]string{
		"job_id":                 claim.JobID.String(),
		"node_execution_id":      claim.NodeExecutionID.String(),
		"worker_id":              claim.WorkerID,
		"started_at":             claim.WorkerStartedAt.UTC().Format(time.RFC3339Nano),
		"worker_registration_id": claim.RegistrationID.String(),
		"worker_lease_epoch":     strconv.FormatInt(claim.LeaseEpoch, 10),
		"task_stream":            proof.RedisStream,
		"task_group":             proof.ConsumerGroup,
		"task_message_id":        proof.MessageID,
		"task_payload_sha256":    proof.PayloadSHA256,
		"task_dispatch_key":      proof.DispatchKey.String(),
	}
	for key, want := range expected {
		if values[key] != want {
			return &WorkerRegistrationError{
				Code: "event_emission_mismatch",
			}
		}
	}
	switch values["event"] {
	case "node_completed":
		if strings.TrimSpace(values["output_artifact_id"]) == "" ||
			values["error"] != "" {
			return &WorkerRegistrationError{
				Code: "event_emission_mismatch",
			}
		}
	case "node_failed":
		if strings.TrimSpace(values["error"]) == "" ||
			values["output_artifact_id"] != "" {
			return &WorkerRegistrationError{
				Code: "event_emission_mismatch",
			}
		}
	default:
		return &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	return nil
}

func deliveryProofFromEventValues(
	values map[string]string,
) (WorkerTaskDeliveryProof, error) {
	dispatchKey, err := uuid.Parse(values["task_dispatch_key"])
	if err != nil {
		return WorkerTaskDeliveryProof{}, &WorkerRegistrationError{
			Code: "event_emission_mismatch",
		}
	}
	proof := WorkerTaskDeliveryProof{
		RedisStream:   values["task_stream"],
		ConsumerGroup: values["task_group"],
		MessageID:     values["task_message_id"],
		PayloadSHA256: values["task_payload_sha256"],
		DispatchKey:   dispatchKey,
	}
	if err := validateWorkerTaskDeliveryProof(proof); err != nil {
		return WorkerTaskDeliveryProof{}, err
	}
	return proof, nil
}

func boundedUTF8(value string, maximumBytes int) string {
	if len(value) <= maximumBytes {
		return value
	}
	for maximumBytes > 0 &&
		!utf8.ValidString(value[:maximumBytes]) {
		maximumBytes--
	}
	return value[:maximumBytes]
}

func copyStringMap(values map[string]string) map[string]string {
	copied := make(map[string]string, len(values))
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		copied[key] = values[key]
	}
	return copied
}

func canonicalRedisPayloadJSON(values map[string]string) ([]byte, error) {
	if values == nil {
		return nil, errors.New("Redis payload is invalid")
	}
	keys := make([]string, 0, len(values))
	for key, value := range values {
		if !utf8.ValidString(key) || !utf8.ValidString(value) {
			return nil, errors.New("Redis payload is invalid")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	builder.WriteByte('{')
	for index, key := range keys {
		if index > 0 {
			builder.WriteByte(',')
		}
		writePythonJSONString(&builder, key)
		builder.WriteByte(':')
		writePythonJSONString(&builder, values[key])
	}
	builder.WriteByte('}')
	return []byte(builder.String()), nil
}

func writePythonJSONString(builder *strings.Builder, value string) {
	builder.WriteByte('"')
	for _, character := range value {
		switch character {
		case '"', '\\':
			builder.WriteByte('\\')
			builder.WriteRune(character)
		case '\b':
			builder.WriteString(`\b`)
		case '\f':
			builder.WriteString(`\f`)
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		default:
			switch {
			case character < 0x20:
				fmt.Fprintf(builder, `\u%04x`, character)
			case character <= 0x7f:
				builder.WriteRune(character)
			case character <= 0xffff:
				fmt.Fprintf(builder, `\u%04x`, character)
			default:
				scalar := character - 0x10000
				high := 0xd800 + scalar>>10
				low := 0xdc00 + scalar&0x3ff
				fmt.Fprintf(builder, `\u%04x\u%04x`, high, low)
			}
		}
	}
	builder.WriteByte('"')
}
