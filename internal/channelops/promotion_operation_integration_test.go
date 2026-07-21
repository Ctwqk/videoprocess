package channelops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type durablePromotionYouTube struct {
	fakeYouTube
	scheduleCalls   atomic.Int32
	statusCalls     atomic.Int32
	scheduleErr     error
	statusErr       error
	observedPrivacy string
	observedStatus  string
	mu              sync.Mutex
	attemptKeys     []string
}

type failIfCalledPromotionPDS struct {
	calls atomic.Int32
}

func (f *failIfCalledPromotionPDS) Decide(
	ctx context.Context,
	req PDSDecisionRequest,
) (PDSDecision, error) {
	f.calls.Add(1)
	return PDSDecision{}, errors.New("PDS must not run after promotion submission may have occurred")
}

func (f *durablePromotionYouTube) SchedulePublish(
	ctx context.Context,
	videoID string,
	scheduledAt time.Time,
	privacy string,
	idempotencyKey string,
) error {
	f.scheduleCalls.Add(1)
	f.mu.Lock()
	f.attemptKeys = append(f.attemptKeys, idempotencyKey)
	f.mu.Unlock()
	return f.scheduleErr
}

func (f *durablePromotionYouTube) PublicationStatus(
	ctx context.Context,
	videoID string,
) (YouTubePublicationStatus, error) {
	f.statusCalls.Add(1)
	if f.statusErr != nil {
		return YouTubePublicationStatus{}, f.statusErr
	}
	privacy := f.observedPrivacy
	if privacy == "" {
		privacy = "unlisted"
	}
	publishStatus := f.observedStatus
	if publishStatus == "" {
		publishStatus = "scheduled"
	}
	return YouTubePublicationStatus{
		VideoID:       videoID,
		PublishStatus: publishStatus,
		Privacy:       privacy,
		Permalink:     "https://youtu.be/" + videoID,
		Raw:           map[string]any{"status": "scheduled", "privacy": privacy},
	}, nil
}

func (f *durablePromotionYouTube) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.attemptKeys...)
}

func TestPromotionExplicitPublicTargetIsRejectedBeforeExternalCalls(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
	promote.PayloadJSON["target_visibility"] = "public"

	recorder := &externalCallRecorder{}
	handler := baseHandler
	handler.PDS = &recordingPDS{recorder: recorder}
	handler.YouTube = &recordingYouTube{recorder: recorder}
	err := handler.Handle(ctx, promote)
	if !errors.Is(err, ErrPromotionOperationConflict) {
		t.Fatalf("public promotion error = %v, want operation conflict", err)
	}
	if got := recorder.total(); got != 0 {
		t.Fatalf("external calls = %d, want zero", got)
	}
	var operations int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*) FROM publication_promotion_operations
	`).Scan(&operations); err != nil {
		t.Fatalf("count promotion operations: %v", err)
	}
	if operations != 0 {
		t.Fatalf("promotion operations = %d, want zero", operations)
	}
}

func TestStorePromotionRejectsPublicVisibility(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, handler, QueuePromotePublication)
	publicationID := firstString(promote.PayloadJSON, "publication_id")

	err := fixture.Store.PromotePublication(
		ctx,
		publicationID,
		"public",
		fixture.Store.Now(),
		PDSDecision{Verdict: "allow", DecisionID: "allow"},
		promote.ID,
		time.Hour,
	)
	if !errors.Is(err, ErrPromotionOperationConflict) {
		t.Fatalf("store public promotion error = %v, want operation conflict", err)
	}
	publication, err := fixture.Store.GetPublication(ctx, publicationID)
	if err != nil {
		t.Fatalf("GetPublication: %v", err)
	}
	if publication.PublishStatus != "uploaded" || publication.CurrentPrivacy != "private" {
		t.Fatalf("public promotion mutated publication = %#v", publication)
	}
}

func TestPromotionConfirmationMismatchDoesNotAdvanceOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, handler, QueuePromotePublication)

	var preparation promotionPreparation
	if err := fixture.Store.WithQueueExecutionFence(ctx, promote, func(fencedStore *Store) error {
		fencedHandler := handler.withStore(fencedStore)
		prepared, err := fencedHandler.preparePromotion(ctx, promote)
		preparation = prepared
		return err
	}); err != nil {
		t.Fatalf("prepare promotion: %v", err)
	}
	operation, shouldSubmit, err := fixture.Store.BeginPromotionSubmission(ctx, preparation.Operation.ID)
	if err != nil || !shouldSubmit {
		t.Fatalf("begin submission = %#v, %v, want submission authority", operation, err)
	}
	_, err = fixture.Store.ConfirmPromotionOperation(
		ctx,
		operation.ID,
		YouTubePublicationStatus{Privacy: "private", PublishStatus: "scheduled"},
		map[string]any{"mismatch": true},
	)
	if !errors.Is(err, ErrPromotionOperationConflict) {
		t.Fatalf("mismatched confirmation error = %v, want operation conflict", err)
	}
	stored, err := fixture.Store.GetPromotionOperationForPublication(ctx, operation.PublicationID)
	if err != nil {
		t.Fatalf("GetPromotionOperationForPublication: %v", err)
	}
	if stored == nil || stored.Status != PromotionSubmitting || stored.ObservedPrivacy != nil {
		t.Fatalf("operation advanced after mismatched confirmation = %#v", stored)
	}
}

func TestPromotionRetryReconcilesBeforeCallingPDSAgain(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)

	youtube := &durablePromotionYouTube{
		scheduleErr: errors.New("manager response was lost"),
		statusErr:   errors.New("manager status temporarily unavailable"),
	}
	handler := baseHandler
	handler.YouTube = youtube
	if err := handler.Handle(ctx, promote); !errors.Is(err, ErrPromotionOutcomeUncertain) {
		t.Fatalf("initial promotion error = %v, want uncertain", err)
	}

	youtube.statusErr = nil
	youtube.observedPrivacy = "unlisted"
	pds := &failIfCalledPromotionPDS{}
	handler.PDS = pds
	if err := handler.Handle(ctx, promote); err != nil {
		t.Fatalf("status-first promotion retry: %v", err)
	}
	if got := pds.calls.Load(); got != 0 {
		t.Fatalf("PDS retry calls = %d, want zero", got)
	}
	if got := youtube.scheduleCalls.Load(); got != 1 {
		t.Fatalf("manager schedule calls = %d, want one", got)
	}
	if got := youtube.statusCalls.Load(); got != 2 {
		t.Fatalf("manager status calls = %d, want two", got)
	}
}

func TestPromotionManagerSuccessFinalizeFailureReplaysWithoutDuplicateSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
	publicationID := firstString(promote.PayloadJSON, "publication_id")

	failureSuffix := strings.ReplaceAll(testUUID(t, "promotion finalize failure"), "-", "")
	functionName := "fail_promotion_finalize_" + failureSuffix
	triggerName := "trg_fail_promotion_finalize_" + failureSuffix
	if _, err := fixture.Store.Pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.id = '%s'::uuid AND NEW.publish_status = 'scheduled' THEN
				RAISE EXCEPTION 'forced promotion finalize failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER %s BEFORE UPDATE ON publication_records
		FOR EACH ROW EXECUTE FUNCTION %s();
	`, functionName, publicationID, triggerName, functionName)); err != nil {
		t.Fatalf("create finalize failure trigger: %v", err)
	}
	dropFailureTrigger := func() {
		_, _ = fixture.Store.Pool.Exec(ctx, fmt.Sprintf(
			"DROP TRIGGER IF EXISTS %s ON publication_records; DROP FUNCTION IF EXISTS %s()",
			triggerName,
			functionName,
		))
	}
	defer dropFailureTrigger()

	youtube := &durablePromotionYouTube{}
	handler := baseHandler
	handler.YouTube = youtube
	firstErr := handler.Handle(ctx, promote)
	if firstErr == nil || !strings.Contains(firstErr.Error(), "forced promotion finalize failure") {
		t.Fatalf("first promotion error = %v, want forced finalize failure", firstErr)
	}

	var operationStatus string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status
		FROM publication_promotion_operations
		WHERE publication_id = $1::uuid
	`, publicationID).Scan(&operationStatus); err != nil {
		t.Fatalf("read confirmed promotion operation: %v", err)
	}
	if operationStatus != "confirmed" {
		t.Fatalf("operation status after finalize failure = %s, want confirmed", operationStatus)
	}

	dropFailureTrigger()
	if err := handler.Handle(ctx, promote); err != nil {
		t.Fatalf("retry promotion: %v", err)
	}
	if got := youtube.scheduleCalls.Load(); got != 1 {
		t.Fatalf("manager schedule calls = %d, want 1", got)
	}
	keys := youtube.keys()
	if len(keys) != 1 || !strings.HasPrefix(keys[0], "channelops-promotion:") {
		t.Fatalf("manager attempt keys = %#v", keys)
	}

	publication, err := fixture.Store.GetPublication(ctx, publicationID)
	if err != nil {
		t.Fatalf("GetPublication: %v", err)
	}
	if publication.PublishStatus != "scheduled" || publication.CurrentPrivacy != "unlisted" {
		t.Fatalf("finalized publication = %#v", publication)
	}
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status
		FROM publication_promotion_operations
		WHERE publication_id = $1::uuid
	`, publicationID).Scan(&operationStatus); err != nil {
		t.Fatalf("read finalized promotion operation: %v", err)
	}
	if operationStatus != "finalized" {
		t.Fatalf("operation status after retry = %s, want finalized", operationStatus)
	}
}

func TestPromotionResponseLossReconcilesMatchingPrivacyWithoutDuplicateSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
	publicationID := firstString(promote.PayloadJSON, "publication_id")

	youtube := &durablePromotionYouTube{
		scheduleErr:     errors.New("connection reset after manager accepted request"),
		observedPrivacy: "unlisted",
	}
	handler := baseHandler
	handler.YouTube = youtube
	if err := handler.Handle(ctx, promote); err != nil {
		t.Fatalf("response-loss promotion: %v", err)
	}
	if err := handler.Handle(ctx, promote); err != nil {
		t.Fatalf("exact promotion replay: %v", err)
	}
	if got := youtube.scheduleCalls.Load(); got != 1 {
		t.Fatalf("manager schedule calls = %d, want 1", got)
	}
	if got := youtube.statusCalls.Load(); got != 1 {
		t.Fatalf("manager status calls = %d, want 1", got)
	}

	publication, err := fixture.Store.GetPublication(ctx, publicationID)
	if err != nil {
		t.Fatalf("GetPublication: %v", err)
	}
	if publication.PublishStatus != "scheduled" || publication.CurrentPrivacy != "unlisted" {
		t.Fatalf("reconciled publication = %#v", publication)
	}
	var operationStatus string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status
		FROM publication_promotion_operations
		WHERE publication_id = $1::uuid
	`, publicationID).Scan(&operationStatus); err != nil {
		t.Fatalf("read promotion operation: %v", err)
	}
	if operationStatus != PromotionFinalized {
		t.Fatalf("operation status = %s, want %s", operationStatus, PromotionFinalized)
	}
}

func TestPromotionUncertainStatusNeverBlindlyResubmits(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, scenario := range []struct {
		name            string
		observedPrivacy string
		observedStatus  string
		statusErr       error
	}{
		{name: "contradictory", observedPrivacy: "private"},
		{name: "unavailable", statusErr: errors.New("manager status unavailable")},
		{name: "matching privacy with removed status", observedPrivacy: "unlisted", observedStatus: "removed"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
			promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
			publicationID := firstString(promote.PayloadJSON, "publication_id")

			youtube := &durablePromotionYouTube{
				scheduleErr:     errors.New("manager response was lost"),
				observedPrivacy: scenario.observedPrivacy,
				observedStatus:  scenario.observedStatus,
				statusErr:       scenario.statusErr,
			}
			handler := baseHandler
			handler.YouTube = youtube
			for attempt := 0; attempt < 2; attempt++ {
				err := handler.Handle(ctx, promote)
				if !errors.Is(err, ErrPromotionOutcomeUncertain) {
					t.Fatalf("promotion attempt %d error = %v, want uncertain", attempt+1, err)
				}
			}
			if got := youtube.scheduleCalls.Load(); got != 1 {
				t.Fatalf("manager schedule calls = %d, want 1", got)
			}
			if got := youtube.statusCalls.Load(); got != 2 {
				t.Fatalf("manager status calls = %d, want 2", got)
			}

			var operationStatus string
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status
				FROM publication_promotion_operations
				WHERE publication_id = $1::uuid
			`, publicationID).Scan(&operationStatus); err != nil {
				t.Fatalf("read uncertain promotion operation: %v", err)
			}
			if operationStatus != PromotionUncertain {
				t.Fatalf("operation status = %s, want %s", operationStatus, PromotionUncertain)
			}
			task, err := fixture.Store.GetProductionTask(ctx, taskIDForQueueItem(t, ctx, fixture, promote))
			if err != nil {
				t.Fatalf("GetProductionTask: %v", err)
			}
			if task.State != TaskHeld || task.BlockedByGuard == nil || *task.BlockedByGuard != "promotion_outcome_uncertain" {
				t.Fatalf("uncertain task = %#v", task)
			}
		})
	}
}

func TestQuarantineBetweenPromotionPhasesRetainsConfirmedEvidence(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	var operations []*cancellableTestOperation
	registerBoundedFixtureCleanup(t, fixture, &operations)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
	publicationID := firstString(promote.PayloadJSON, "publication_id")

	releaseYouTube := make(chan struct{})
	youtube := &blockingPromotionYouTube{
		started: make(chan struct{}, 1),
		release: releaseYouTube,
	}
	handler := baseHandler
	handler.YouTube = youtube
	handleOperation := startCancellableTestOperation(t, func() { close(releaseYouTube) }, func(operationCtx context.Context) error {
		return handler.Handle(operationCtx, promote)
	})
	operations = append(operations, handleOperation)
	select {
	case <-youtube.started:
	case <-time.After(5 * time.Second):
		t.Fatal("promotion did not reach blocking YouTube client")
	}

	quarantineCtx, cancelQuarantine := context.WithTimeout(ctx, 2*time.Second)
	defer cancelQuarantine()
	quarantineTx, err := fixture.Store.Pool.Begin(quarantineCtx)
	if err != nil {
		t.Fatalf("begin quarantine: %v", err)
	}
	registerBoundedRollback(t, quarantineTx)
	if err := applyFenceTestQuarantine(quarantineCtx, quarantineTx, fixture.ChannelID); err != nil {
		t.Fatalf("apply quarantine while manager call is active: %v", err)
	}
	if err := quarantineTx.Commit(quarantineCtx); err != nil {
		t.Fatalf("commit quarantine while manager call is active: %v", err)
	}

	handleOperation.releaseBlocker()
	handleErr, waitErr := handleOperation.waitOrCancelAndDrain(5*time.Second, testOperationCleanupTimeout)
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	if !errors.Is(handleErr, ErrChannelExecutionBlocked) {
		t.Fatalf("promotion after quarantine error = %v, want channel blocked", handleErr)
	}
	if got := youtube.calls.Load(); got != 1 {
		t.Fatalf("manager schedule calls = %d, want 1", got)
	}

	var operationStatus string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status
		FROM publication_promotion_operations
		WHERE publication_id = $1::uuid
	`, publicationID).Scan(&operationStatus); err != nil {
		t.Fatalf("read promotion evidence: %v", err)
	}
	if operationStatus != PromotionConfirmed {
		t.Fatalf("operation status = %s, want %s", operationStatus, PromotionConfirmed)
	}
	publication, err := fixture.Store.GetPublication(ctx, publicationID)
	if err != nil {
		t.Fatalf("GetPublication: %v", err)
	}
	if publication.PublishStatus != "uploaded" {
		t.Fatalf("publication status = %s, want uploaded", publication.PublishStatus)
	}
	if retryErr := handler.Handle(ctx, promote); !errors.Is(retryErr, ErrChannelExecutionBlocked) {
		t.Fatalf("retry after quarantine error = %v, want channel blocked", retryErr)
	}
	if got := youtube.calls.Load(); got != 1 {
		t.Fatalf("manager schedule calls after retry = %d, want 1", got)
	}
	requireQuarantinedPromotion(t, ctx, fixture, promote, 0)
}
