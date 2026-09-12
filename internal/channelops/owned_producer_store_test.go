package channelops

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type ownedProducerFenceProbe struct {
	pgx.Tx
	queries   []string
	protected bool
	stop      error
}

func markTaskPlanningWithNativeTestFence(ctx context.Context, store *Store, task ProductionTaskRow, planID string, payload map[string]any, approval AutoFlowApprovalObservation, parent string) error {
	return store.WithChannelExecutionFence(ctx, task.ChannelProfileID, func(fenced *Store) error {
		return fenced.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, planID, payload, approval, parent)
	})
}

type ownedReconcileCommitProbe struct {
	pgx.Tx
	owned bool
	lost  bool
	steps []string
	stop  error
}

func (p *ownedReconcileCommitProbe) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	return ownedB2FenceRow(func(dest ...any) error {
		if strings.Contains(query, "SELECT EXISTS") {
			p.steps = append(p.steps, "typed_binding")
			*dest[0].(*bool) = p.owned
			return nil
		}
		if strings.Contains(query, "FROM runtime_schedules") {
			p.steps = append(p.steps, "schedule_lock")
			*dest[0].(*string) = "CLOSED"
			return nil
		}
		p.steps = append(p.steps, "completion_helper")
		return p.stop
	})
}
func (p *ownedReconcileCommitProbe) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	if strings.Contains(query, "UPDATE publication_records") {
		p.steps = append(p.steps, "real_publication_result")
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	if !strings.Contains(query, "UPDATE channel_ops_queue_items") || !strings.Contains(query, "locked_by = NULL") || !strings.Contains(query, "locked_at = NULL") {
		return pgconn.CommandTag{}, errors.New("unexpected queue mutation")
	}
	p.steps = append(p.steps, "persisted_queue_success")
	if p.lost {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func TestOwnedReconcileHookUsesRealQueueTransitionBeforeCompletion(t *testing.T) {
	for _, variant := range []string{"owned", "ordinary", "lost"} {
		t.Run(variant, func(t *testing.T) {
			probe := &ownedReconcileCommitProbe{owned: variant != "ordinary", lost: variant == "lost", stop: errors.New("bounded completion entry")}
			channel, owner, at := ownedTestID(2), "native-runner", time.Date(2026, 9, 12, 8, 0, 0, 0, time.UTC)
			store := &Store{executionDB: probe, executionChannelID: &channel, Now: func() time.Time { return at }}
			task := ProductionTaskRow{ID: ownedTestID(600), ChannelProfileID: channel}
			pub := PublicationRow{ID: ownedTestID(800), ProductionTaskID: task.ID}
			item := QueueItemRow{ID: ownedTestID(900), Kind: QueueReconcilePublication, Status: QueueStatusRunning, LockedBy: &owner, LockedAt: &at, AttemptCount: 1}
			err := store.finishOwnedReconcile(context.Background(), item, pub, task, YouTubePublicationStatus{PublishStatus: "scheduled", Privacy: "unlisted"})
			want := "typed_binding,schedule_lock,real_publication_result,persisted_queue_success,completion_helper"
			if variant == "ordinary" {
				want = "typed_binding,real_publication_result"
				if err != nil {
					t.Fatal(err)
				}
			} else if variant == "lost" {
				want = "typed_binding,schedule_lock,real_publication_result,persisted_queue_success"
				if !errors.Is(err, ErrQueueLeaseLost) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, probe.stop) {
				t.Fatal(err)
			}
			if strings.Join(probe.steps, ",") != want {
				t.Fatalf("steps %v, want %s", probe.steps, want)
			}
			if item.Status != QueueStatusRunning || item.LockedBy == nil || item.LockedAt == nil {
				t.Fatal("forged in-memory terminal queue")
			}
		})
	}
}

func (p *ownedProducerFenceProbe) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	p.queries = append(p.queries, query)
	return ownedB2FenceRow(func(dest ...any) error {
		switch {
		case strings.Contains(query, "FROM runtime_schedules"):
			*dest[0].(*string) = "OPEN"
			return nil
		case strings.Contains(query, "SELECT EXISTS"):
			*dest[0].(*bool) = p.protected
			return nil
		default:
			return p.stop
		}
	})
}

func TestOwnedProducerStoreRequiresFenceAndScheduleFirst(t *testing.T) {
	task := ProductionTaskRow{ID: ownedTestID(600), ChannelProfileID: ownedTestID(2)}
	if _, err := (&Store{}).lockOwnedProducer(context.Background(), task, ""); !errors.Is(err, errOwnedInventory) {
		t.Fatalf("unfenced producer: %v", err)
	}
	for _, protected := range []bool{false, true} {
		probe := &ownedProducerFenceProbe{protected: protected, stop: errors.New("bounded native read stop")}
		store := &Store{executionDB: probe, executionChannelID: &task.ChannelProfileID}
		authority, err := store.lockOwnedProducer(context.Background(), task, "")
		if protected && !errors.Is(err, probe.stop) || !protected && (err != nil || authority != nil) {
			t.Fatalf("protected %t: %+v %v", protected, authority, err)
		}
		if len(probe.queries) < 2 || !strings.Contains(probe.queries[0], "FROM runtime_schedules") || !strings.Contains(probe.queries[0], "FOR UPDATE") || !strings.Contains(probe.queries[1], "approved_at IS NOT NULL") {
			t.Fatalf("not schedule first: %v", probe.queries)
		}
	}
}
