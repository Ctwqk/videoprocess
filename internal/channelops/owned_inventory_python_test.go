package channelops

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOwnedPythonReleasedLeaderRejectsBeforeDBValidation(t *testing.T) {
	state := &leaderState{}
	store := &Store{leadership: state}
	authority := LeaderAuthority{ServiceName: leaderServiceName, HolderID: "python-go-contender", Epoch: 1}
	state.publish(authority)
	fenced := store.withExecutionDB(nil, nil)
	// Release clears this shared state; its actual SQL/unlock remains parent-PG tested.
	state.clear(authority)
	configured, current := fenced.leadership.snapshot()
	if !configured || current != nil {
		t.Fatal("released authority remains published")
	}
	// A nil DB proves refusal happens before any stale-epoch SQL validation.
	err := fenced.assertLeaderAuthority(context.Background(), nil, true)
	if !errors.Is(err, ErrLeaderAuthorityUnavailable) {
		t.Fatalf("released leader error = %v", err)
	}
}

// Only the parent-run Python scratch fixture supplies these exact identities.
// No fixture data, approval, Go production path, or leader checks are replaced.
func TestOwnedPythonContenderBridge(t *testing.T) {
	channel, directory := os.Getenv("OWNED_PYTHON_CONTENDER_CHANNEL"), os.Getenv("OWNED_PYTHON_CONTENDER_DIR")
	if testing.Short() || channel == "" || directory == "" {
		t.Skip("parent-only Python/Go contender fixture")
	}
	if !uuidPattern.MatchString(channel) || !filepath.IsAbs(directory) {
		t.Fatal("invalid contender identity")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatal("private contender directory required")
	}
	dsn, err := ownedDisposableURL(os.Getenv("OWNED_INVENTORY_DISPOSABLE_TEST_URL"), os.Getenv("OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM"))
	if err != nil {
		t.Fatal("explicit scratch designation required")
	}
	if os.Getenv("OWNED_PYTHON_CONTENDER_MODE") == "seed_retirement" {
		newOwnedPGFixtureWithHistory(t, func(store *Store, now time.Time) map[string]any {
			return ownedB2PGSeedRetirement(t, store, now)
		})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	store, err := OpenStore(ctx, dsn)
	if err != nil {
		t.Fatal("scratch store unavailable")
	}
	defer store.Close()
	var now time.Time
	if err := store.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	lease := acquireLeaderTestLease(t, ctx, store, "python-go-contender", now)
	released := false
	defer func() {
		if !released {
			if err := ownedReleaseLeaderAtDBTime(ctx, store.Pool.QueryRow(ctx, `SELECT clock_timestamp()`), lease.Release); err != nil {
				t.Error("scratch leader release failed")
			}
		}
	}()
	_, err = store.Enqueue(ctx, EnqueueOptions{Kind: QueueAgentTick, IdempotencyKey: "go-contender:" + channel,
		Payload: map[string]any{"channel_id": channel}, ChannelProfileID: &channel, Priority: 100, RunAfter: now})
	if err != nil {
		t.Fatal(err)
	}
	item, err := store.ClaimNextForChannelAndKinds(ctx, "go-contender", channel, []string{QueueAgentTick})
	if err != nil || item == nil {
		t.Fatal("scratch queue claim failed")
	}
	h := HandlerService{Store: store, PDS: ownedTestPDS(func(ctx context.Context, request PDSDecisionRequest) (PDSDecision, error) {
		if err := os.WriteFile(filepath.Join(directory, "ready"), []byte(request.Context["candidate_id"].(string)), 0600); err != nil {
			return PDSDecision{}, err
		}
		for {
			if _, err := os.Stat(filepath.Join(directory, "release")); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return PDSDecision{}, ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
		if os.Getenv("OWNED_PYTHON_CONTENDER_MODE") == "leader_loss" {
			if err := ownedReleaseLeaderAtDBTime(ctx, store.Pool.QueryRow(ctx, `SELECT clock_timestamp()`), lease.Release); err != nil {
				return PDSDecision{}, err
			}
			released = true
		}
		return ownedProducerRealDecision(), nil
	})}
	err = h.HandleAgentTick(ctx, *item)
	if os.Getenv("OWNED_PYTHON_CONTENDER_MODE") == "leader_loss" {
		if !released || !errors.Is(err, ErrLeaderAuthorityUnavailable) {
			t.Fatalf("actual lost Go leader was not refused: %v", err)
		}
		authority := lease.Authority()
		var releaseRecorded bool
		if err := store.Pool.QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM channelops_leader_epochs WHERE service_name=$1 AND holder_id=$2 AND epoch=$3
			AND released_at IS NOT NULL AND released_at>=heartbeat_at)`,
			authority.ServiceName, authority.HolderID, authority.Epoch).Scan(&releaseRecorded); err != nil || !releaseRecorded {
			t.Fatal("actual leader release was not durably recorded")
		}
		var goAudits int
		if err := store.Pool.QueryRow(ctx, `SELECT count(*) FROM agent_tick_audits
			WHERE channel_profile_id=$1::uuid AND decision_summary_json->>'handler_version'='go'`, channel).Scan(&goAudits); err != nil || goAudits != 0 {
			t.Fatal("released Go contender produced an audit/effect")
		}
	} else if err != nil {
		t.Fatal(err)
	}
}
