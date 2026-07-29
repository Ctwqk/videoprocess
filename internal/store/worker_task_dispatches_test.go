package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const idempotentWorkerEventXAddScript = `
local existing = redis.call('GET', KEYS[2])
if existing then
    return existing
end
local message_id = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 1))
redis.call('SET', KEYS[2], message_id)
return message_id
`

type registeredTaskFixture struct {
	pg          *workerPostgresFixture
	redis       *redis.Client
	lease       WorkerRegistrationLease
	pipelineID  uuid.UUID
	jobID       uuid.UUID
	nodeID      uuid.UUID
	dispatchKey uuid.UUID
	messageID   string
	payload     map[string]string
	proof       WorkerTaskDeliveryProof
	eventStream string
}

func TestRegistrationPostgres16ClaimEventAndAckAreExactAndReplayable(t *testing.T) {
	pgFixture := newWorkerPostgresFixture(t)
	redisClient := newWorkerRedisIntegrationClient(t)
	task := newRegisteredTaskFixture(t, pgFixture, redisClient)

	for name, mutate := range map[string]func(WorkerTaskDeliveryProof) WorkerTaskDeliveryProof{
		"stream": func(proof WorkerTaskDeliveryProof) WorkerTaskDeliveryProof {
			proof.RedisStream += ":wrong"
			return proof
		},
		"group": func(proof WorkerTaskDeliveryProof) WorkerTaskDeliveryProof {
			proof.ConsumerGroup += "-wrong"
			return proof
		},
		"message": func(proof WorkerTaskDeliveryProof) WorkerTaskDeliveryProof {
			proof.MessageID += "-wrong"
			return proof
		},
		"hash": func(proof WorkerTaskDeliveryProof) WorkerTaskDeliveryProof {
			proof.PayloadSHA256 = strings.Repeat("0", 64)
			return proof
		},
		"dispatch": func(proof WorkerTaskDeliveryProof) WorkerTaskDeliveryProof {
			proof.DispatchKey = uuid.New()
			return proof
		},
	} {
		t.Run("reject_"+name, func(t *testing.T) {
			if _, err := pgFixture.worker.ClaimWorkerNode(
				task.pg.ctx,
				task.lease,
				task.jobID,
				task.nodeID,
				mutate(task.proof),
			); err == nil {
				t.Fatalf("claim with arbitrary %s succeeded", name)
			}
			var status string
			if err := task.pg.admin.QueryRow(
				task.pg.ctx,
				"SELECT status::text FROM public.node_executions WHERE id = $1",
				task.nodeID,
			).Scan(&status); err != nil {
				t.Fatalf("read rejected claim state: %v", err)
			}
			if status != "QUEUED" {
				t.Fatalf("node status after rejected claim = %q; want QUEUED", status)
			}
		})
	}

	claim, err := pgFixture.worker.ClaimWorkerNode(
		task.pg.ctx,
		task.lease,
		task.jobID,
		task.nodeID,
		task.proof,
	)
	if err != nil {
		t.Fatalf("ClaimWorkerNode: %v", err)
	}
	if claim.AttestationID == uuid.Nil || claim.WorkerStartedAt.IsZero() {
		t.Fatalf("claim = %#v", claim)
	}
	var attestedRegistration uuid.UUID
	var attestedEpoch int64
	var attestedDispatch uuid.UUID
	if err := task.pg.admin.QueryRow(
		task.pg.ctx,
		`SELECT worker_registration_id, worker_lease_epoch, dispatch_key
		 FROM public.worker_task_delivery_attestations
		 WHERE id = $1`,
		claim.AttestationID,
	).Scan(&attestedRegistration, &attestedEpoch, &attestedDispatch); err != nil {
		t.Fatalf("read delivery attestation: %v", err)
	}
	if attestedRegistration != task.lease.RegistrationID ||
		attestedEpoch != task.lease.LeaseEpoch ||
		attestedDispatch != task.dispatchKey {
		t.Fatalf("attestation registration/epoch/dispatch = %s/%d/%s",
			attestedRegistration,
			attestedEpoch,
			attestedDispatch,
		)
	}

	eventValues := CompletedWorkerEventValues(
		claim,
		task.proof,
		"00000000-0000-0000-0000-000000000777",
	)
	emissionID, err := pgFixture.worker.PrepareWorkerEvent(
		task.pg.ctx,
		claim,
		task.eventStream,
		"orchestrator",
		eventValues,
	)
	if err != nil {
		t.Fatalf("PrepareWorkerEvent: %v", err)
	}
	t.Cleanup(func() {
		_ = redisClient.Del(
			context.Background(),
			"vp:worker-event-emission:"+emissionID.String(),
		).Err()
	})

	publish := realEventPublisher(redisClient)
	injected := true
	_, err = pgFixture.worker.PublishPreparedWorkerEvent(
		task.pg.ctx,
		task.lease,
		emissionID,
		func(
			ctx context.Context,
			stream string,
			marker string,
			values map[string]string,
		) (string, error) {
			messageID, publishErr := publish(ctx, stream, marker, values)
			if publishErr != nil {
				return "", publishErr
			}
			if injected {
				injected = false
				return "", errors.New("injected database-side failure after Redis success")
			}
			return messageID, nil
		},
	)
	if err == nil {
		t.Fatal("first event publication did not expose injected post-Redis failure")
	}
	if _, err := pgFixture.worker.PublishPreparedWorkerEvent(
		task.pg.ctx,
		task.lease,
		emissionID,
		publish,
	); err != nil {
		t.Fatalf("replay prepared event publication: %v", err)
	}
	events, err := redisClient.XRange(
		task.pg.ctx,
		task.eventStream,
		"-",
		"+",
	).Result()
	if err != nil {
		t.Fatalf("read worker events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event entries after replay = %d; want exactly 1", len(events))
	}
	for field, want := range map[string]string{
		"worker_registration_id": task.lease.RegistrationID.String(),
		"worker_lease_epoch":     strconv.FormatInt(task.lease.LeaseEpoch, 10),
		"task_stream":            task.proof.RedisStream,
		"task_group":             task.proof.ConsumerGroup,
		"task_message_id":        task.proof.MessageID,
		"task_payload_sha256":    task.proof.PayloadSHA256,
		"task_dispatch_key":      task.proof.DispatchKey.String(),
	} {
		if got := fmt.Sprint(events[0].Values[field]); got != want {
			t.Errorf("event %s = %q; want %q", field, got, want)
		}
	}

	ackInjected := true
	ack := func(
		ctx context.Context,
		stream string,
		group string,
		messageID string,
	) (int64, error) {
		result, ackErr := redisClient.XAck(ctx, stream, group, messageID).Result()
		if ackErr != nil {
			return 0, ackErr
		}
		if ackInjected {
			ackInjected = false
			return 0, errors.New("injected database-side failure after Redis ACK")
		}
		return result, nil
	}
	if err := pgFixture.worker.AcknowledgeWorkerTask(
		task.pg.ctx,
		claim,
		ack,
	); err == nil {
		t.Fatal("first task acknowledgement did not expose injected post-Redis failure")
	}
	if err := pgFixture.worker.AcknowledgeWorkerTask(
		task.pg.ctx,
		claim,
		ack,
	); err != nil {
		t.Fatalf("replay task acknowledgement: %v", err)
	}
	pending, err := redisClient.XPending(
		task.pg.ctx,
		task.proof.RedisStream,
		task.proof.ConsumerGroup,
	).Result()
	if err != nil {
		t.Fatalf("read task pending state: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending count after replayed acknowledgement = %d; want 0", pending.Count)
	}
	var ackState string
	var resolutionState string
	if err := task.pg.admin.QueryRow(
		task.pg.ctx,
		`SELECT attestation.ack_state, dispatch.resolution_state
		 FROM public.worker_task_delivery_attestations AS attestation
		 JOIN public.worker_task_dispatches AS dispatch
		   ON dispatch.dispatch_key = attestation.dispatch_key
		 WHERE attestation.id = $1`,
		claim.AttestationID,
	).Scan(&ackState, &resolutionState); err != nil {
		t.Fatalf("read durable task acknowledgement: %v", err)
	}
	if ackState != "acknowledged" || resolutionState != "acknowledged" {
		t.Fatalf("durable ack states = %q/%q; want acknowledged/acknowledged", ackState, resolutionState)
	}
}

func TestRegistrationLostEpochWithoutProofDoesNotInvokeFinalRedisWrite(t *testing.T) {
	pgFixture := newWorkerPostgresFixture(t)
	redisClient := newWorkerRedisIntegrationClient(t)
	task := newRegisteredTaskFixture(t, pgFixture, redisClient)
	claim, err := pgFixture.worker.ClaimWorkerNode(
		task.pg.ctx,
		task.lease,
		task.jobID,
		task.nodeID,
		task.proof,
	)
	if err != nil {
		t.Fatalf("ClaimWorkerNode: %v", err)
	}
	emissionID, err := pgFixture.worker.PrepareWorkerEvent(
		task.pg.ctx,
		claim,
		task.eventStream,
		"orchestrator",
		FailedWorkerEventValues(
			claim,
			task.proof,
			"integration failure",
		),
	)
	if err != nil {
		t.Fatalf("PrepareWorkerEvent: %v", err)
	}

	takeoverClaims := pgFixture.claims
	takeoverClaims.WorkerInstanceID = uuid.New()
	takeoverClaims.WorkerSlot = 2
	takeoverClaims.RedisConsumerID = "ffmpeg_go-worker@host127:2:" + takeoverClaims.WorkerInstanceID.String()
	if _, err := pgFixture.worker.RegisterWorker(
		task.pg.ctx,
		takeoverClaims,
		pgFixture.token,
	); err != nil {
		t.Fatalf("register takeover: %v", err)
	}

	eventWrites := 0
	if _, err := pgFixture.worker.PublishPreparedWorkerEvent(
		task.pg.ctx,
		task.lease,
		emissionID,
		func(
			context.Context,
			string,
			string,
			map[string]string,
		) (string, error) {
			eventWrites++
			return "1-0", nil
		},
	); err == nil {
		t.Fatal("lost epoch event publication succeeded")
	}
	if eventWrites != 0 {
		t.Fatalf("event writes after unproven epoch loss = %d; want 0", eventWrites)
	}

	artifactWrites := 0
	if _, err := pgFixture.worker.PersistWorkerArtifact(
		task.pg.ctx,
		claim,
		CreateArtifactInput{
			JobID:           task.jobID.String(),
			NodeExecutionID: task.nodeID.String(),
			Kind:            "INTERMEDIATE",
			Filename:        "lost-epoch.mp4",
			MimeType:        "video/mp4",
			FileSize:        1,
			StorageBackend:  "minio",
			StoragePath:     "staging/test/lost-epoch.mp4",
			MediaInfo:       map[string]any{},
		},
		func(context.Context) error {
			artifactWrites++
			return nil
		},
	); err == nil {
		t.Fatal("lost epoch artifact persistence succeeded")
	}
	if artifactWrites != 0 {
		t.Fatalf("artifact writes after unproven epoch loss = %d; want 0", artifactWrites)
	}

	ackWrites := 0
	err = pgFixture.worker.AcknowledgeWorkerTask(
		task.pg.ctx,
		claim,
		func(context.Context, string, string, string) (int64, error) {
			ackWrites++
			return 1, nil
		},
	)
	if err == nil {
		t.Fatal("lost epoch task acknowledgement succeeded")
	}
	if ackWrites != 0 {
		t.Fatalf("ACK writes after unproven epoch loss = %d; want 0", ackWrites)
	}
}

func TestRegistrationPostgres16AppliedReceiptRecoversTaskAcknowledgement(
	t *testing.T,
) {
	pgFixture := newWorkerPostgresFixture(t)
	redisClient := newWorkerRedisIntegrationClient(t)
	task := newRegisteredTaskFixture(t, pgFixture, redisClient)
	claim, err := pgFixture.worker.ClaimWorkerNode(
		task.pg.ctx,
		task.lease,
		task.jobID,
		task.nodeID,
		task.proof,
	)
	if err != nil {
		t.Fatalf("ClaimWorkerNode: %v", err)
	}
	eventValues := CompletedWorkerEventValues(
		claim,
		task.proof,
		uuid.NewString(),
	)
	eventJSON, err := canonicalRedisPayloadJSON(eventValues)
	if err != nil {
		t.Fatalf("canonical receipt payload: %v", err)
	}
	eventHash, err := CanonicalRedisPayloadSHA256(eventValues)
	if err != nil {
		t.Fatalf("hash receipt payload: %v", err)
	}
	if _, err := task.pg.admin.Exec(
		task.pg.ctx,
		`INSERT INTO public.registered_worker_event_receipts (
			source_task_attestation_id, redis_stream, consumer_group,
			message_id, payload_sha256, payload_json, event_type, job_id,
			node_execution_id, worker_registration_id, worker_lease_epoch,
			worker_id, worker_started_at, source_task_stream,
			source_task_group, source_task_message_id, application_state,
			applied_at
		 ) VALUES (
			$1, $2, 'orchestrator', $3, $4, $5::jsonb, 'node_completed',
			$6, $7, $8, $9, $10, $11, $12, $13, $14, 'applied',
			clock_timestamp()
		 )`,
		claim.AttestationID,
		task.eventStream,
		"receipt-"+task.messageID,
		eventHash,
		string(eventJSON),
		task.jobID,
		task.nodeID,
		claim.RegistrationID,
		claim.LeaseEpoch,
		claim.WorkerID,
		claim.WorkerStartedAt,
		task.proof.RedisStream,
		task.proof.ConsumerGroup,
		task.proof.MessageID,
	); err != nil {
		t.Fatalf("insert applied receipt: %v", err)
	}
	if err := task.pg.worker.AcknowledgeWorkerTaskFromReceipt(
		task.pg.ctx,
		claim,
		func(
			ctx context.Context,
			stream string,
			group string,
			messageID string,
		) (int64, error) {
			return redisClient.XAck(ctx, stream, group, messageID).Result()
		},
	); err != nil {
		t.Fatalf("recover acknowledgement from applied receipt: %v", err)
	}
	pending, err := redisClient.XPending(
		task.pg.ctx,
		task.proof.RedisStream,
		task.proof.ConsumerGroup,
	).Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending after receipt recovery = %#v, err=%v", pending, err)
	}
	var attestationState string
	var dispatchState string
	if err := task.pg.admin.QueryRow(
		task.pg.ctx,
		`SELECT attestation.ack_state, dispatch.resolution_state
		 FROM public.worker_task_delivery_attestations AS attestation
		 JOIN public.worker_task_dispatches AS dispatch
		   ON dispatch.dispatch_key = attestation.dispatch_key
		 WHERE attestation.id = $1`,
		claim.AttestationID,
	).Scan(&attestationState, &dispatchState); err != nil {
		t.Fatalf("read receipt-recovered acknowledgement: %v", err)
	}
	if attestationState != "acknowledged" ||
		dispatchState != "acknowledged" {
		t.Fatalf(
			"receipt-recovered states = %q/%q; want acknowledged/acknowledged",
			attestationState,
			dispatchState,
		)
	}
}

func TestRegistrationPostgres16RedisWritesHoldSharedRegistrationFence(
	t *testing.T,
) {
	t.Run("event_xadd", func(t *testing.T) {
		pgFixture := newWorkerPostgresFixture(t)
		redisClient := newWorkerRedisIntegrationClient(t)
		task := newRegisteredTaskFixture(t, pgFixture, redisClient)
		claim, err := pgFixture.worker.ClaimWorkerNode(
			task.pg.ctx,
			task.lease,
			task.jobID,
			task.nodeID,
			task.proof,
		)
		if err != nil {
			t.Fatalf("ClaimWorkerNode: %v", err)
		}
		emissionID, err := pgFixture.worker.PrepareWorkerEvent(
			task.pg.ctx,
			claim,
			task.eventStream,
			"orchestrator",
			CompletedWorkerEventValues(
				claim,
				task.proof,
				uuid.NewString(),
			),
		)
		if err != nil {
			t.Fatalf("PrepareWorkerEvent: %v", err)
		}
		marker := "vp:worker-event-emission:" + emissionID.String()
		t.Cleanup(func() {
			_ = redisClient.Del(context.Background(), marker).Err()
		})
		writeStarted := make(chan struct{})
		finishWrite := make(chan struct{})
		publishDone := make(chan error, 1)
		publish := realEventPublisher(redisClient)
		go func() {
			_, publishErr := pgFixture.worker.PublishPreparedWorkerEvent(
				context.Background(),
				task.lease,
				emissionID,
				func(
					ctx context.Context,
					stream string,
					marker string,
					values map[string]string,
				) (string, error) {
					messageID, writeErr := publish(
						ctx,
						stream,
						marker,
						values,
					)
					if writeErr != nil {
						return "", writeErr
					}
					close(writeStarted)
					<-finishWrite
					return messageID, nil
				},
			)
			publishDone <- publishErr
		}()
		waitForTestSignal(t, writeStarted, "event XADD callback")
		takeoverDone := startWorkerTakeover(
			pgFixture,
			2,
		)
		assertTakeoverBlocked(t, takeoverDone, "event XADD")
		close(finishWrite)
		waitForTestResult(t, publishDone, "event publication")
		waitForTestResult(t, takeoverDone, "event takeover")
	})

	t.Run("task_xack", func(t *testing.T) {
		pgFixture := newWorkerPostgresFixture(t)
		redisClient := newWorkerRedisIntegrationClient(t)
		task := newRegisteredTaskFixture(t, pgFixture, redisClient)
		claim, err := pgFixture.worker.ClaimWorkerNode(
			task.pg.ctx,
			task.lease,
			task.jobID,
			task.nodeID,
			task.proof,
		)
		if err != nil {
			t.Fatalf("ClaimWorkerNode: %v", err)
		}
		emissionID, err := pgFixture.worker.PrepareWorkerEvent(
			task.pg.ctx,
			claim,
			task.eventStream,
			"orchestrator",
			CompletedWorkerEventValues(
				claim,
				task.proof,
				uuid.NewString(),
			),
		)
		if err != nil {
			t.Fatalf("PrepareWorkerEvent: %v", err)
		}
		marker := "vp:worker-event-emission:" + emissionID.String()
		t.Cleanup(func() {
			_ = redisClient.Del(context.Background(), marker).Err()
		})
		if _, err := pgFixture.worker.PublishPreparedWorkerEvent(
			task.pg.ctx,
			task.lease,
			emissionID,
			realEventPublisher(redisClient),
		); err != nil {
			t.Fatalf("publish event before XACK fence test: %v", err)
		}

		writeStarted := make(chan struct{})
		finishWrite := make(chan struct{})
		ackDone := make(chan error, 1)
		go func() {
			ackDone <- pgFixture.worker.AcknowledgeWorkerTask(
				context.Background(),
				claim,
				func(
					ctx context.Context,
					stream string,
					group string,
					messageID string,
				) (int64, error) {
					result, writeErr := redisClient.XAck(
						ctx,
						stream,
						group,
						messageID,
					).Result()
					if writeErr != nil {
						return 0, writeErr
					}
					close(writeStarted)
					<-finishWrite
					return result, nil
				},
			)
		}()
		waitForTestSignal(t, writeStarted, "task XACK callback")
		takeoverDone := startWorkerTakeover(
			pgFixture,
			2,
		)
		assertTakeoverBlocked(t, takeoverDone, "task XACK")
		close(finishWrite)
		waitForTestResult(t, ackDone, "task acknowledgement")
		waitForTestResult(t, takeoverDone, "task takeover")
	})
}

func TestRegistrationPostgres16ArtifactRechecksDatabaseLeaseAfterRemoteSave(
	t *testing.T,
) {
	pgFixture := newWorkerPostgresFixture(t)
	redisClient := newWorkerRedisIntegrationClient(t)
	task := newRegisteredTaskFixture(t, pgFixture, redisClient)
	claim, err := pgFixture.worker.ClaimWorkerNode(
		task.pg.ctx,
		task.lease,
		task.jobID,
		task.nodeID,
		task.proof,
	)
	if err != nil {
		t.Fatalf("ClaimWorkerNode: %v", err)
	}
	saveCalls := 0
	_, err = pgFixture.worker.PersistWorkerArtifact(
		task.pg.ctx,
		claim,
		CreateArtifactInput{
			JobID:           task.jobID.String(),
			NodeExecutionID: task.nodeID.String(),
			Kind:            "INTERMEDIATE",
			Filename:        "trim.mp4",
			MimeType:        "video/mp4",
			FileSize:        5,
			StorageBackend:  "minio",
			StoragePath:     "staging/test/trim.mp4",
			MediaInfo:       map[string]any{"duration": 1},
		},
		func(context.Context) error {
			saveCalls++
			_, updateErr := pgFixture.admin.Exec(
				task.pg.ctx,
				`UPDATE public.worker_registrations
				 SET registered_at = clock_timestamp() - interval '3 seconds',
				     heartbeat_at = clock_timestamp() - interval '2 seconds',
				     lease_expires_at = clock_timestamp() - interval '1 second'
				 WHERE id = $1`,
				task.lease.RegistrationID,
			)
			return updateErr
		},
	)
	if err == nil {
		t.Fatal("artifact pointer persisted after lease expired during remote save")
	}
	if saveCalls != 1 {
		t.Fatalf("remote save calls = %d; want 1", saveCalls)
	}
	var count int
	if err := task.pg.admin.QueryRow(
		task.pg.ctx,
		`SELECT count(*)
		 FROM public.artifacts
		 WHERE job_id = $1 AND storage_path = 'staging/test/trim.mp4'`,
		task.jobID,
	).Scan(&count); err != nil {
		t.Fatalf("count rejected artifact pointers: %v", err)
	}
	if count != 0 {
		t.Fatalf("artifact pointers after second lease check = %d; want 0", count)
	}
}

func TestRegistrationPostgres16ArtifactSaveAndPointerHoldSharedFence(
	t *testing.T,
) {
	pgFixture := newWorkerPostgresFixture(t)
	redisClient := newWorkerRedisIntegrationClient(t)
	task := newRegisteredTaskFixture(t, pgFixture, redisClient)
	claim, err := pgFixture.worker.ClaimWorkerNode(
		task.pg.ctx,
		task.lease,
		task.jobID,
		task.nodeID,
		task.proof,
	)
	if err != nil {
		t.Fatalf("ClaimWorkerNode: %v", err)
	}

	saveStarted := make(chan struct{})
	finishSave := make(chan struct{})
	persistDone := make(chan error, 1)
	go func() {
		_, persistErr := pgFixture.worker.PersistWorkerArtifact(
			context.Background(),
			claim,
			CreateArtifactInput{
				JobID:           task.jobID.String(),
				NodeExecutionID: task.nodeID.String(),
				Kind:            "INTERMEDIATE",
				Filename:        "trim.mp4",
				MimeType:        "video/mp4",
				FileSize:        5,
				StorageBackend:  "minio",
				StoragePath:     "staging/test/fenced-trim.mp4",
				MediaInfo:       map[string]any{"duration": 1},
			},
			func(context.Context) error {
				close(saveStarted)
				<-finishSave
				return nil
			},
		)
		persistDone <- persistErr
	}()
	select {
	case <-saveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("artifact save callback did not start")
	}

	takeoverClaims := pgFixture.claims
	takeoverClaims.WorkerInstanceID = uuid.New()
	takeoverClaims.WorkerSlot = 2
	takeoverClaims.RedisConsumerID = "ffmpeg_go-worker@host127:2:" +
		takeoverClaims.WorkerInstanceID.String()
	takeoverDone := make(chan error, 1)
	go func() {
		_, takeoverErr := pgFixture.worker.RegisterWorker(
			context.Background(),
			takeoverClaims,
			pgFixture.token,
		)
		takeoverDone <- takeoverErr
	}()
	select {
	case err := <-takeoverDone:
		t.Fatalf("takeover crossed active artifact fence: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(finishSave)
	select {
	case err := <-persistDone:
		if err != nil {
			t.Fatalf("persist fenced artifact: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("artifact persistence did not finish")
	}
	select {
	case err := <-takeoverDone:
		if err != nil {
			t.Fatalf("takeover after artifact transaction: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("takeover remained blocked after artifact transaction")
	}
	var count int
	if err := task.pg.admin.QueryRow(
		task.pg.ctx,
		`SELECT count(*)
		 FROM public.artifacts
		 WHERE job_id = $1 AND storage_path = 'staging/test/fenced-trim.mp4'`,
		task.jobID,
	).Scan(&count); err != nil {
		t.Fatalf("count fenced artifact pointer: %v", err)
	}
	if count != 1 {
		t.Fatalf("fenced artifact pointers = %d; want 1", count)
	}
}

func TestRegistrationPostgres16CancelledCallbacksReleaseSharedFence(
	t *testing.T,
) {
	for _, operation := range []string{
		"event XADD",
		"task XACK",
		"artifact save",
	} {
		t.Run(operation, func(t *testing.T) {
			pgFixture := newWorkerPostgresFixture(t)
			redisClient := newWorkerRedisIntegrationClient(t)
			task := newRegisteredTaskFixture(t, pgFixture, redisClient)
			claim, err := pgFixture.worker.ClaimWorkerNode(
				task.pg.ctx,
				task.lease,
				task.jobID,
				task.nodeID,
				task.proof,
			)
			if err != nil {
				t.Fatalf("ClaimWorkerNode: %v", err)
			}
			var emissionID uuid.UUID
			if operation != "artifact save" {
				emissionID, err = pgFixture.worker.PrepareWorkerEvent(
					task.pg.ctx,
					claim,
					task.eventStream,
					"orchestrator",
					CompletedWorkerEventValues(
						claim,
						task.proof,
						uuid.NewString(),
					),
				)
				if err != nil {
					t.Fatalf("PrepareWorkerEvent: %v", err)
				}
			}
			if operation == "task XACK" {
				marker := "vp:worker-event-emission:" + emissionID.String()
				t.Cleanup(func() {
					_ = redisClient.Del(
						context.Background(),
						marker,
					).Err()
				})
				if _, err := pgFixture.worker.PublishPreparedWorkerEvent(
					task.pg.ctx,
					task.lease,
					emissionID,
					realEventPublisher(redisClient),
				); err != nil {
					t.Fatalf("publish event before bounded XACK: %v", err)
				}
			}

			callbackStarted := make(chan struct{})
			releaseCallback := make(chan struct{})
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() { close(releaseCallback) })
			}
			t.Cleanup(release)
			operationContext, cancelOperation := context.WithTimeout(
				context.Background(),
				300*time.Millisecond,
			)
			defer cancelOperation()
			operationDone := make(chan error, 1)
			go func() {
				switch operation {
				case "event XADD":
					_, operationErr := pgFixture.worker.
						PublishPreparedWorkerEvent(
							operationContext,
							task.lease,
							emissionID,
							func(
								context.Context,
								string,
								string,
								map[string]string,
							) (string, error) {
								close(callbackStarted)
								<-releaseCallback
								return "1-0", nil
							},
						)
					operationDone <- operationErr
				case "task XACK":
					operationDone <- pgFixture.worker.
						AcknowledgeWorkerTask(
							operationContext,
							claim,
							func(
								context.Context,
								string,
								string,
								string,
							) (int64, error) {
								close(callbackStarted)
								<-releaseCallback
								return 1, nil
							},
						)
				case "artifact save":
					_, operationErr := pgFixture.worker.
						PersistWorkerArtifact(
							operationContext,
							claim,
							CreateArtifactInput{
								JobID:           task.jobID.String(),
								NodeExecutionID: task.nodeID.String(),
								Kind:            "INTERMEDIATE",
								Filename:        "bounded.mp4",
								MimeType:        "video/mp4",
								FileSize:        32 << 20,
								StorageBackend:  "minio",
								StoragePath: "staging/test/" +
									uuid.NewString() + ".mp4",
								MediaInfo: map[string]any{},
							},
							func(context.Context) error {
								close(callbackStarted)
								<-releaseCallback
								return nil
							},
						)
					operationDone <- operationErr
				}
			}()
			waitForTestSignal(t, callbackStarted, operation+" callback")
			takeoverDone := startWorkerTakeover(pgFixture, 2)
			assertTakeoverBlocked(t, takeoverDone, operation)

			select {
			case operationErr := <-operationDone:
				if operationErr == nil {
					release()
					t.Fatalf("%s succeeded after callback deadline", operation)
				}
			case <-time.After(time.Second):
				release()
				select {
				case <-operationDone:
				case <-time.After(5 * time.Second):
				}
				select {
				case <-takeoverDone:
				case <-time.After(5 * time.Second):
				}
				t.Fatalf("%s ignored callback deadline", operation)
			}
			select {
			case takeoverErr := <-takeoverDone:
				if takeoverErr != nil {
					release()
					t.Fatalf(
						"%s takeover after rollback: %v",
						operation,
						takeoverErr,
					)
				}
			case <-time.After(time.Second):
				release()
				t.Fatalf(
					"%s shared fence remained held after bounded return",
					operation,
				)
			}
			release()
		})
	}
}

func TestRegistrationPostgres16ReadsInitialDownstreamAndRetryDispatches(
	t *testing.T,
) {
	pgFixture := newWorkerPostgresFixture(t)
	redisClient := newWorkerRedisIntegrationClient(t)
	task := newRegisteredTaskFixture(t, pgFixture, redisClient)
	claim, err := pgFixture.worker.ClaimWorkerNode(
		task.pg.ctx,
		task.lease,
		task.jobID,
		task.nodeID,
		task.proof,
	)
	if err != nil {
		t.Fatalf("ClaimWorkerNode: %v", err)
	}
	eventValues := CompletedWorkerEventValues(
		claim,
		task.proof,
		uuid.NewString(),
	)
	eventJSON, err := json.Marshal(eventValues)
	if err != nil {
		t.Fatalf("marshal event receipt payload: %v", err)
	}
	eventHash, err := CanonicalRedisPayloadSHA256(eventValues)
	if err != nil {
		t.Fatalf("hash event receipt payload: %v", err)
	}
	receiptID := uuid.New()
	if _, err := task.pg.admin.Exec(
		task.pg.ctx,
		`INSERT INTO public.registered_worker_event_receipts (
			id, source_task_attestation_id, redis_stream, consumer_group,
			message_id, payload_sha256, payload_json, event_type, job_id,
			node_execution_id, worker_registration_id, worker_lease_epoch,
			worker_id, worker_started_at, source_task_stream,
			source_task_group, source_task_message_id, application_state,
			applied_at
		 ) VALUES (
			$1, $2, $3, 'orchestrator', $4, $5, $6::jsonb,
			'node_completed', $7, $8, $9, $10, $11, $12, $13, $14, $15,
			'applied', clock_timestamp()
		 )`,
		receiptID,
		claim.AttestationID,
		task.eventStream,
		"event-"+task.messageID,
		eventHash,
		string(eventJSON),
		task.jobID,
		task.nodeID,
		claim.RegistrationID,
		claim.LeaseEpoch,
		claim.WorkerID,
		claim.WorkerStartedAt,
		task.proof.RedisStream,
		task.proof.ConsumerGroup,
		task.proof.MessageID,
	); err != nil {
		t.Fatalf("insert downstream origin receipt: %v", err)
	}

	downstreamNodeID := uuid.New()
	retryNodeID := uuid.New()
	for _, node := range []struct {
		id   uuid.UUID
		name string
	}{
		{id: downstreamNodeID, name: "downstream-1"},
		{id: retryNodeID, name: "retry-1"},
	} {
		if _, err := task.pg.admin.Exec(
			task.pg.ctx,
			`INSERT INTO public.node_executions (
				id, job_id, node_id, node_type, status
			 ) VALUES ($1, $2, $3, 'trim', 'QUEUED')`,
			node.id,
			task.jobID,
			node.name,
		); err != nil {
			t.Fatalf("insert %s node: %v", node.name, err)
		}
	}
	downstreamKey := uuid.New()
	downstreamPayload := map[string]string{
		"job_id":            task.jobID.String(),
		"node_execution_id": downstreamNodeID.String(),
		"node_id":           "downstream-1",
		"node_type":         "trim",
		"dispatch_key":      downstreamKey.String(),
	}
	downstreamHash, err := CanonicalRedisPayloadSHA256(downstreamPayload)
	if err != nil {
		t.Fatalf("hash downstream dispatch: %v", err)
	}
	downstreamJSON, _ := json.Marshal(downstreamPayload)
	if _, err := task.pg.admin.Exec(
		task.pg.ctx,
		`INSERT INTO public.worker_task_dispatches (
			origin_receipt_id, dispatch_key, job_id, node_execution_id,
			redis_stream, consumer_group, payload_sha256, payload_json
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)`,
		receiptID,
		downstreamKey,
		task.jobID,
		downstreamNodeID,
		task.proof.RedisStream,
		task.proof.ConsumerGroup,
		downstreamHash,
		string(downstreamJSON),
	); err != nil {
		t.Fatalf("insert downstream dispatch: %v", err)
	}

	retryID := uuid.New()
	retryKey := uuid.New()
	retryPayload := map[string]string{
		"job_id":            task.jobID.String(),
		"node_execution_id": retryNodeID.String(),
		"node_id":           "retry-1",
		"node_type":         "trim",
		"dispatch_key":      retryKey.String(),
	}
	retryHash, err := CanonicalRedisPayloadSHA256(retryPayload)
	if err != nil {
		t.Fatalf("hash retry dispatch: %v", err)
	}
	retryJSON, _ := json.Marshal(retryPayload)
	if _, err := task.pg.admin.Exec(
		task.pg.ctx,
		`INSERT INTO public.worker_task_dispatches (
			id, dispatch_key, job_id, node_execution_id, redis_stream,
			consumer_group, payload_sha256, payload_json, delivery_state,
			delivery_attempted_at, delivery_error
		 ) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8::jsonb, 'uncertain',
			clock_timestamp(), 'connection_reset'
		 )`,
		retryID,
		retryKey,
		task.jobID,
		retryNodeID,
		task.proof.RedisStream,
		task.proof.ConsumerGroup,
		retryHash,
		string(retryJSON),
	); err != nil {
		t.Fatalf("insert retry dispatch: %v", err)
	}

	adminStore := &Store{Pool: task.pg.admin}
	initial, err := adminStore.LoadInitialWorkerTaskDispatch(
		task.pg.ctx,
		task.nodeID,
	)
	if err != nil {
		t.Fatalf("load initial dispatch: %v", err)
	}
	downstream, err := adminStore.LoadDownstreamWorkerTaskDispatch(
		task.pg.ctx,
		receiptID,
		downstreamNodeID,
	)
	if err != nil {
		t.Fatalf("load downstream dispatch: %v", err)
	}
	retry, err := adminStore.LoadRetryWorkerTaskDispatch(
		task.pg.ctx,
		retryID,
	)
	if err != nil {
		t.Fatalf("load retry dispatch: %v", err)
	}
	if initial.DispatchKey != task.dispatchKey ||
		initial.RedisMessageID == nil ||
		*initial.RedisMessageID != task.messageID ||
		!reflect.DeepEqual(initial.Payload, task.payload) {
		t.Fatalf("initial dispatch = %#v", initial)
	}
	if downstream.OriginReceiptID == nil ||
		*downstream.OriginReceiptID != receiptID ||
		downstream.DispatchKey != downstreamKey ||
		downstream.PayloadSHA256 != downstreamHash ||
		!reflect.DeepEqual(downstream.Payload, downstreamPayload) {
		t.Fatalf("downstream dispatch = %#v", downstream)
	}
	if retry.ID != retryID ||
		retry.DispatchKey != retryKey ||
		retry.DeliveryState != "uncertain" ||
		retry.DeliveryAttemptedAt == nil ||
		retry.DeliveryError == nil ||
		*retry.DeliveryError != "connection_reset" ||
		!reflect.DeepEqual(retry.Payload, retryPayload) {
		t.Fatalf("retry dispatch = %#v", retry)
	}
}

func TestRegistrationCanonicalRedisPayloadHashIsOrderIndependent(t *testing.T) {
	first := map[string]string{
		"dispatch_key": "00000000-0000-0000-0000-000000000001",
		"job_id":       "00000000-0000-0000-0000-000000000002",
		"node_id":      "trim-1",
	}
	second := map[string]string{
		"node_id":      "trim-1",
		"job_id":       "00000000-0000-0000-0000-000000000002",
		"dispatch_key": "00000000-0000-0000-0000-000000000001",
	}
	firstHash, err := CanonicalRedisPayloadSHA256(first)
	if err != nil {
		t.Fatalf("CanonicalRedisPayloadSHA256(first): %v", err)
	}
	secondHash, err := CanonicalRedisPayloadSHA256(second)
	if err != nil {
		t.Fatalf("CanonicalRedisPayloadSHA256(second): %v", err)
	}
	if firstHash != secondHash {
		t.Fatalf("canonical hashes differ: %s != %s", firstHash, secondHash)
	}
}

func TestRegistrationCanonicalRedisPayloadHashMatchesPythonEscaping(
	t *testing.T,
) {
	hash, err := CanonicalRedisPayloadSHA256(map[string]string{
		"amp":     "<&>",
		"control": "line\n\u0001",
		"unicode": "é😀",
	})
	if err != nil {
		t.Fatalf("CanonicalRedisPayloadSHA256: %v", err)
	}
	const want = "f17f42e7c6be4e3aa8c94ec753dd411d5278a443aaba059a954788ae7a1f3df0"
	if hash != want {
		t.Fatalf("canonical payload hash = %s; want Python %s", hash, want)
	}
}

func newWorkerRedisIntegrationClient(t *testing.T) *redis.Client {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv("CHANNEL_OPS_GO_REDIS_TEST_URL"))
	if rawURL == "" {
		t.Skip("set CHANNEL_OPS_GO_REDIS_TEST_URL for Redis 7.4 worker integration tests")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("parse Redis integration URL: %v", err)
	}
	client := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Fatalf("ping Redis integration server: %v", err)
	}
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		client.Close()
		t.Fatalf("read Redis integration version: %v", err)
	}
	version := ""
	for _, line := range strings.Split(info, "\n") {
		if value, ok := strings.CutPrefix(
			strings.TrimSpace(line),
			"redis_version:",
		); ok {
			version = value
			break
		}
	}
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		client.Close()
		t.Fatalf("invalid Redis integration version %q", version)
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	if majorErr != nil || minorErr != nil ||
		major < 7 || major == 7 && minor < 4 {
		client.Close()
		t.Fatalf("Redis version = %q; require 7.4 or newer", version)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client
}

func newRegisteredTaskFixture(
	t *testing.T,
	pgFixture *workerPostgresFixture,
	redisClient *redis.Client,
) *registeredTaskFixture {
	t.Helper()
	lease := pgFixture.register(t)
	pipelineID := uuid.New()
	jobID := uuid.New()
	nodeID := uuid.New()
	dispatchKey := uuid.New()
	eventStream := "vp:test:events:" + strings.TrimPrefix(pgFixture.service, "vp-go-worker-")

	var previousState string
	var previousGuarded *uuid.UUID
	insertedSchedule, err := pgFixture.admin.Exec(
		pgFixture.ctx,
		`INSERT INTO public.runtime_schedules (
			service_name, state, guarded_job_id, updated_at, updated_by
		 ) VALUES (
			'videoprocess', 'OPEN', NULL, clock_timestamp(),
			'go-worker-registration-test'
		 )
		 ON CONFLICT (service_name) DO NOTHING`,
	)
	if err != nil {
		t.Fatalf("ensure runtime schedule: %v", err)
	}
	scheduleCreated := insertedSchedule.RowsAffected() == 1
	if !scheduleCreated {
		if err := pgFixture.admin.QueryRow(
			pgFixture.ctx,
			`SELECT state, guarded_job_id
		 FROM public.runtime_schedules
		 WHERE service_name = 'videoprocess'`,
		).Scan(&previousState, &previousGuarded); err != nil {
			t.Fatalf("read runtime schedule: %v", err)
		}
	}
	if _, err := pgFixture.admin.Exec(
		pgFixture.ctx,
		`UPDATE public.runtime_schedules
		 SET state = 'OPEN', guarded_job_id = NULL, updated_at = clock_timestamp(),
		     updated_by = 'go-worker-registration-test'
		 WHERE service_name = 'videoprocess'`,
	); err != nil {
		t.Fatalf("open runtime schedule: %v", err)
	}
	if _, err := pgFixture.admin.Exec(
		pgFixture.ctx,
		`INSERT INTO public.pipelines (
			id, name, description, definition, is_template, template_tags
		 ) VALUES ($1, $2, '', '{}'::json, FALSE, '{}')`,
		pipelineID,
		"go worker registration "+pipelineID.String(),
	); err != nil {
		t.Fatalf("insert task pipeline: %v", err)
	}
	if _, err := pgFixture.admin.Exec(
		pgFixture.ctx,
		`INSERT INTO public.jobs (
			id, pipeline_id, pipeline_snapshot, status, orchestrator_owner
		 ) VALUES ($1, $2, '{}'::json, 'RUNNING', 'python')`,
		jobID,
		pipelineID,
	); err != nil {
		t.Fatalf("insert task job: %v", err)
	}
	if _, err := pgFixture.admin.Exec(
		pgFixture.ctx,
		`INSERT INTO public.node_executions (
			id, job_id, node_id, node_type, status
		 ) VALUES ($1, $2, 'trim-1', 'trim', 'QUEUED')`,
		nodeID,
		jobID,
	); err != nil {
		t.Fatalf("insert task node: %v", err)
	}

	if err := redisClient.XGroupCreateMkStream(
		pgFixture.ctx,
		pgFixture.stream,
		pgFixture.group,
		"0",
	).Err(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		t.Fatalf("create task group: %v", err)
	}
	payload := map[string]string{
		"job_id":               jobID.String(),
		"node_execution_id":    nodeID.String(),
		"node_id":              "trim-1",
		"node_type":            "trim",
		"config":               "{}",
		"input_artifacts":      "{}",
		"preferred_hosts":      `["host127"]`,
		"affinity_enqueued_at": strconv.FormatInt(time.Now().UTC().Unix(), 10),
		"affinity_bounces":     "0",
		"event_stream":         eventStream,
		"orchestrator_owner":   "python",
		"dispatch_key":         dispatchKey.String(),
	}
	messageID, err := redisClient.XAdd(
		pgFixture.ctx,
		&redis.XAddArgs{Stream: pgFixture.stream, Values: stringMapToAny(payload)},
	).Result()
	if err != nil {
		t.Fatalf("add task message: %v", err)
	}
	if _, err := redisClient.XReadGroup(
		pgFixture.ctx,
		&redis.XReadGroupArgs{
			Group:    pgFixture.group,
			Consumer: "non-preferred@host150",
			Streams:  []string{pgFixture.stream, ">"},
			Count:    1,
		},
	).Result(); err != nil {
		t.Fatalf("deliver task into PEL: %v", err)
	}
	payloadHash, err := CanonicalRedisPayloadSHA256(payload)
	if err != nil {
		t.Fatalf("hash task payload: %v", err)
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal task payload: %v", err)
	}
	if _, err := pgFixture.admin.Exec(
		pgFixture.ctx,
		`INSERT INTO public.worker_task_dispatches (
			dispatch_key, job_id, node_execution_id, redis_stream,
			consumer_group, payload_sha256, payload_json, delivery_state,
			delivery_attempted_at, redis_message_id, delivered_at
		 ) VALUES (
			$1, $2, $3, $4, $5, $6, $7::jsonb, 'delivered',
			clock_timestamp(), $8, clock_timestamp()
		 )`,
		dispatchKey,
		jobID,
		nodeID,
		pgFixture.stream,
		pgFixture.group,
		payloadHash,
		string(payloadJSON),
		messageID,
	); err != nil {
		t.Fatalf("insert durable task dispatch: %v", err)
	}
	fixture := &registeredTaskFixture{
		pg:          pgFixture,
		redis:       redisClient,
		lease:       lease,
		pipelineID:  pipelineID,
		jobID:       jobID,
		nodeID:      nodeID,
		dispatchKey: dispatchKey,
		messageID:   messageID,
		payload:     payload,
		proof: WorkerTaskDeliveryProof{
			RedisStream:   pgFixture.stream,
			ConsumerGroup: pgFixture.group,
			MessageID:     messageID,
			PayloadSHA256: payloadHash,
			DispatchKey:   dispatchKey,
		},
		eventStream: eventStream,
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pgFixture.admin.Exec(ctx, "DELETE FROM public.registered_worker_event_deliveries WHERE source_task_attestation_id IN (SELECT id FROM public.worker_task_delivery_attestations WHERE job_id = $1)", jobID)
		_, _ = pgFixture.admin.Exec(ctx, "DELETE FROM public.worker_task_dispatches WHERE job_id = $1 AND origin_receipt_id IS NOT NULL", jobID)
		_, _ = pgFixture.admin.Exec(ctx, "DELETE FROM public.registered_worker_event_receipts WHERE job_id = $1", jobID)
		_, _ = pgFixture.admin.Exec(ctx, "DELETE FROM public.worker_event_emissions WHERE job_id = $1", jobID)
		_, _ = pgFixture.admin.Exec(ctx, "DELETE FROM public.worker_task_delivery_attestations WHERE job_id = $1", jobID)
		_, _ = pgFixture.admin.Exec(ctx, "DELETE FROM public.worker_task_dispatches WHERE job_id = $1", jobID)
		_, _ = pgFixture.admin.Exec(ctx, "DELETE FROM public.artifacts WHERE job_id = $1", jobID)
		_, _ = pgFixture.admin.Exec(ctx, "DELETE FROM public.node_executions WHERE job_id = $1", jobID)
		_, _ = pgFixture.admin.Exec(ctx, "DELETE FROM public.jobs WHERE id = $1", jobID)
		_, _ = pgFixture.admin.Exec(ctx, "DELETE FROM public.pipelines WHERE id = $1", pipelineID)
		if scheduleCreated {
			_, _ = pgFixture.admin.Exec(
				ctx,
				`DELETE FROM public.runtime_schedules
				 WHERE service_name = 'videoprocess'`,
			)
		} else {
			_, _ = pgFixture.admin.Exec(
				ctx,
				`UPDATE public.runtime_schedules
				 SET state = $1, guarded_job_id = $2,
				     updated_at = clock_timestamp(),
				     updated_by = 'go-worker-registration-test-cleanup'
				 WHERE service_name = 'videoprocess'`,
				previousState,
				previousGuarded,
			)
		}
		_ = redisClient.Del(
			ctx,
			pgFixture.stream,
			eventStream,
		).Err()
	})
	return fixture
}

func realEventPublisher(client *redis.Client) WorkerEventPublisher {
	return func(
		ctx context.Context,
		stream string,
		marker string,
		values map[string]string,
	) (string, error) {
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		args := make([]any, 0, len(values)*2)
		for _, key := range keys {
			args = append(args, key, values[key])
		}
		result, err := client.Eval(
			ctx,
			idempotentWorkerEventXAddScript,
			[]string{stream, marker},
			args...,
		).Result()
		if err != nil {
			return "", err
		}
		messageID, ok := result.(string)
		if !ok || strings.TrimSpace(messageID) == "" {
			return "", fmt.Errorf("invalid Redis event message id %T", result)
		}
		return messageID, nil
	}
}

func stringMapToAny(values map[string]string) map[string]any {
	result := make(map[string]any, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func startWorkerTakeover(
	fixture *workerPostgresFixture,
	slot int,
) <-chan error {
	done := make(chan error, 1)
	claims := fixture.claims
	claims.WorkerInstanceID = uuid.New()
	claims.WorkerSlot = slot
	claims.RedisConsumerID = fmt.Sprintf(
		"ffmpeg_go-worker@host127:%d:%s",
		slot,
		claims.WorkerInstanceID,
	)
	go func() {
		_, err := fixture.worker.RegisterWorker(
			context.Background(),
			claims,
			fixture.token,
		)
		done <- err
	}()
	return done
}

func assertTakeoverBlocked(
	t *testing.T,
	done <-chan error,
	operation string,
) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("takeover crossed active %s fence: %v", operation, err)
	case <-time.After(150 * time.Millisecond):
	}
}

func waitForTestSignal(
	t *testing.T,
	signal <-chan struct{},
	operation string,
) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not start", operation)
	}
}

func waitForTestResult(
	t *testing.T,
	result <-chan error,
	operation string,
) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("%s failed: %v", operation, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not finish", operation)
	}
}

func TestRegistrationEventProofFieldsAreImmutableCopies(t *testing.T) {
	proof := WorkerTaskDeliveryProof{
		RedisStream:   "vp:tasks:test",
		ConsumerGroup: "test-workers",
		MessageID:     "1-0",
		PayloadSHA256: strings.Repeat("a", 64),
		DispatchKey:   uuid.MustParse("00000000-0000-0000-0000-000000000001"),
	}
	claim := WorkerNodeClaim{
		RegistrationID:  uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		LeaseEpoch:      4,
		WorkerID:        "worker@test",
		WorkerStartedAt: time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
		JobID:           uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		NodeExecutionID: uuid.MustParse("00000000-0000-0000-0000-000000000004"),
	}
	completed := CompletedWorkerEventValues(
		claim,
		proof,
		"00000000-0000-0000-0000-000000000005",
	)
	failed := FailedWorkerEventValues(claim, proof, "ffmpeg failed")
	for _, event := range []map[string]string{completed, failed} {
		got := WorkerTaskDeliveryProof{
			RedisStream:   event["task_stream"],
			ConsumerGroup: event["task_group"],
			MessageID:     event["task_message_id"],
			PayloadSHA256: event["task_payload_sha256"],
			DispatchKey:   uuid.MustParse(event["task_dispatch_key"]),
		}
		if !reflect.DeepEqual(got, proof) {
			t.Fatalf("event proof = %#v; want %#v", got, proof)
		}
	}
	claim.AttestationID = uuid.MustParse(
		"00000000-0000-0000-0000-000000000006",
	)
	claim.Delivery = proof
	completed["unexpected"] = "not-authorized"
	if err := validateWorkerEventValues(claim, completed); err == nil {
		t.Fatal("completed event accepted an extra field")
	}
}
