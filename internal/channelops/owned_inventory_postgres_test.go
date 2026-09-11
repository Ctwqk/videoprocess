package channelops

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Parent-run contracts only. Never infer a database from DATABASE_URL or LoadConfig.
func ownedDisposableURL(raw, confirmation string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() != "127.0.0.1" || u.Port() == "" || !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "vp_owned_inventory_test_") || strings.TrimPrefix(u.Path, "/") != confirmation {
		return "", errOwnedInventory
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", errOwnedInventory
	}
	return raw, nil
}

func TestOwnedDisposableURLNeverUsesAmbientDatabase(t *testing.T) {
	valid := "postgresql://fixture:fixture@127.0.0.1:54329/vp_owned_inventory_test_go"
	if _, err := ownedDisposableURL(valid, "vp_owned_inventory_test_go"); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"", "postgres://127.0.0.1/prod", "postgres://db:5432/vp_owned_inventory_test_go", valid + "?host=other", strings.Replace(valid, "127.0.0.1", "150", 1)} {
		if _, err := ownedDisposableURL(raw, "vp_owned_inventory_test_go"); err == nil {
			t.Fatal("unsafe disposable DSN accepted")
		}
	}
	if _, err := ownedDisposableURL(valid, ""); err == nil {
		t.Fatal("missing confirmation accepted")
	}
}

type ownedPGFixture struct {
	store   *Store
	channel ChannelProfileRow
	data    ownedInventoryData
	lease   *LeaderLease
}

func ownedReleaseLeaderAtDBTime(ctx context.Context, row pgx.Row, release func(context.Context, time.Time) error) error {
	var observed time.Time
	if err := row.Scan(&observed); err != nil {
		return err
	}
	return release(ctx, observed.UTC())
}

type ownedClockRow struct {
	at  time.Time
	err error
}

func (row ownedClockRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(dest) != 1 {
		return errors.New("unexpected fixture clock columns")
	}
	value, ok := dest[0].(*time.Time)
	if !ok {
		return errors.New("unexpected fixture clock type")
	}
	*value = row.at
	return nil
}

func TestOwnedFixtureLeaderReleaseUsesDatabaseObservation(t *testing.T) {
	local := time.Date(2026, 9, 11, 1, 53, 5, 804823000, time.UTC)
	for _, skew := range []time.Duration{91 * time.Millisecond, -91 * time.Millisecond} {
		t.Run(skew.String(), func(t *testing.T) {
			acquired := local.Add(skew)
			observed := acquired.Add(time.Millisecond)
			err := ownedReleaseLeaderAtDBTime(context.Background(), ownedClockRow{at: observed}, func(_ context.Context, released time.Time) error {
				if released.Before(acquired) {
					return errors.New("ck_channelops_leader_release_order")
				}
				if !released.Equal(observed) {
					return errors.New("release did not use the database observation")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOwnedFixtureLeaderReleaseDoesNotFallbackOnClockError(t *testing.T) {
	failure := errors.New("offline clock read failed")
	called := false
	err := ownedReleaseLeaderAtDBTime(context.Background(), ownedClockRow{err: failure}, func(context.Context, time.Time) error { called = true; return nil })
	if !errors.Is(err, failure) || called {
		t.Fatal("clock error caused a fabricated release timestamp")
	}
}

func TestOwnedFixtureLeaderReleasePreservesReleaseError(t *testing.T) {
	failure := errors.New("offline release failed")
	err := ownedReleaseLeaderAtDBTime(context.Background(), ownedClockRow{at: time.Date(2026, 9, 11, 1, 53, 6, 0, time.UTC)}, func(context.Context, time.Time) error { return failure })
	if !errors.Is(err, failure) {
		t.Fatal("release error was hidden")
	}
}

func ownedNewUUID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal("fixture entropy unavailable")
	}
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func newOwnedPGFixture(t *testing.T) *ownedPGFixture {
	t.Helper()
	if testing.Short() || os.Getenv("OWNED_INVENTORY_DISPOSABLE_TEST_URL") == "" {
		t.Skip("explicit disposable inventory PostgreSQL required")
	}
	dsn, err := ownedDisposableURL(os.Getenv("OWNED_INVENTORY_DISPOSABLE_TEST_URL"), os.Getenv("OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM"))
	if err != nil {
		t.Fatal("invalid explicit disposable database designation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := OpenStore(ctx, dsn)
	if err != nil {
		t.Fatal("disposable database unavailable")
	}
	t.Cleanup(store.Close)
	var revision string
	if err := store.Pool.QueryRow(ctx, `SELECT version_num FROM alembic_version`).Scan(&revision); err != nil || revision != "037_owned_seed_inventory" {
		t.Fatal("disposable database must be migrated to 037")
	}
	var now time.Time
	if err := store.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal("fixture clock unavailable")
	}
	channel, data, _ := ownedTestFixture(t)
	replacements := map[string]string{}
	for _, id := range []int{1, 2, 3, 4, 5, 101, 102, 103, 104, 105, 106, 107, 201, 202, 203, 204, 205, 206, 207, 301, 302, 303, 304, 305, 306, 307} {
		replacements[ownedTestID(id)] = ownedNewUUID(t)
	}
	var random [11]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal("fixture entropy unavailable")
	}
	replacements["UCaaaaaaaaaaaaaaaaaaaaaa"] = "UC" + hex.EncodeToString(random[:])
	var replace func(any) any
	replace = func(v any) any {
		switch v := v.(type) {
		case string:
			for old, next := range replacements {
				v = strings.ReplaceAll(v, old, next)
			}
			return v
		case map[string]any:
			for k, item := range v {
				v[k] = replace(item)
			}
			return v
		case []any:
			for i, item := range v {
				v[i] = replace(item)
			}
			return v
		default:
			return v
		}
	}
	replace(data.Inventory)
	replace(data.Bindings)
	for _, input := range data.Items {
		replace(input.Item)
		replace(input.Seed)
		replace(input.Asset)
	}
	channel.ID = replacements[channel.ID]
	inventoryID := ownedString(data.Inventory["id"])
	channel.OwnedSeedInventoryID = &inventoryID
	data.AccountIDs = []string{ownedString(data.Inventory["target_account_id"])}
	starts := now.UTC().Add(-time.Hour)
	data.Inventory["starts_at"], data.Inventory["expires_at"], data.Inventory["approved_at"] = ownedISO(starts), ownedISO(starts.Add(168*time.Hour)), ownedISO(starts)
	for _, kind := range []string{"channel", "account", "lane", "format"} {
		row := ownedMap(data.Bindings[kind])
		row["created_at"], row["updated_at"] = ownedISO(now), ownedISO(now)
	}
	ownedMap(data.Bindings["channel"])["tick_interval_minutes"] = 1
	ownedMap(data.Bindings["account"])["account_label"] = "offline fixture"
	for _, input := range data.Items {
		input.Seed["created_at"], input.Seed["updated_at"] = ownedISO(now), ownedISO(now)
		input.Asset["filename"], input.Asset["original_name"], input.Asset["uploaded_by"] = "fixture.mp4", "fixture.mp4", "offline-test"
		binding, err := ownedFields(input.Seed, "id channel_profile_id topic_lane_id target_account_id prompt title_seed source_policy source_platforms_json material_library_ids_json constraints_json")
		if err != nil {
			t.Fatal(err)
		}
		input.Item["seed_sha256"] = ownedTestHash(t, binding)
		descriptor, err := ownedAssetDescriptor(input.Asset)
		if err != nil {
			t.Fatal(err)
		}
		input.Item["storage_descriptor_json"] = descriptor
	}
	manifest := ownedMap(data.Inventory["manifest_json"])
	manifest["starts_at"], manifest["expires_at"] = data.Inventory["starts_at"], data.Inventory["expires_at"]
	config, err := ownedConfiguration(data.Bindings)
	if err != nil {
		t.Fatal(err)
	}
	manifest["configuration_sha256"] = ownedTestHash(t, config)
	for i, value := range ownedArray(manifest["entries"]) {
		entry := ownedMap(value)
		entry["seed_sha256"] = data.Items[i].Item["seed_sha256"]
		entry["storage_descriptor"] = data.Items[i].Item["storage_descriptor_json"]
	}
	data.Inventory["manifest_sha256"] = ownedTestHash(t, manifest)
	data.Inventory["client_request_id"], data.Inventory["request_sha256"], data.Inventory["created_by"] = ownedNewUUID(t), strings.Repeat("a", 64), "offline-test"
	insert := func(table string, row map[string]any) {
		t.Helper()
		keys := make([]string, 0, len(row))
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal("fixture JSON invalid")
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM json_populate_record(NULL::%s,$1::json) r", table, strings.Join(keys, ","), strings.Join(keys, ","), table)
		if _, err := store.Pool.Exec(ctx, query, raw); err != nil {
			t.Fatalf("fixture insert failed: %s", table)
		}
	}
	insert("channel_profiles", ownedMap(data.Bindings["channel"]))
	insert("topic_lanes", ownedMap(data.Bindings["lane"]))
	insert("lane_format_matrix", ownedMap(data.Bindings["format"]))
	insert("publishing_accounts", ownedMap(data.Bindings["account"]))
	for _, input := range data.Items {
		insert("assets", input.Asset)
		insert("manual_seeds", input.Seed)
	}
	insert("owned_seed_inventories", data.Inventory)
	for _, input := range data.Items {
		insert("owned_seed_inventory_items", input.Item)
	}
	if _, err := store.Pool.Exec(ctx, `UPDATE channel_profiles SET owned_seed_inventory_id=$2::uuid WHERE id=$1::uuid`, channel.ID, inventoryID); err != nil {
		t.Fatal("fixture pointer failed")
	}
	if _, err := store.Pool.Exec(ctx, `INSERT INTO runtime_schedules(service_name,state,updated_by) VALUES('videoprocess','OPEN','offline-test') ON CONFLICT(service_name) DO UPDATE SET state='OPEN',guarded_job_id=NULL`); err != nil {
		t.Fatal("fixture runtime failed")
	}
	f := &ownedPGFixture{store: store, channel: channel, data: data}
	f.lease = acquireLeaderTestLease(t, ctx, store, "owned-inventory-test-"+channel.ID, now)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Immutable history is retained; the parent drops this disposable database.
		if _, err := store.Pool.Exec(cleanup, `UPDATE channel_profiles SET intake_paused_at=clock_timestamp(),enabled=FALSE WHERE id=$1::uuid`, channel.ID); err != nil {
			t.Error("fixture channel cleanup failed")
		}
		if _, err := store.Pool.Exec(cleanup, `UPDATE production_tasks SET state='failed' WHERE channel_profile_id=$1::uuid`, channel.ID); err != nil {
			t.Error("fixture task cleanup failed")
		}
		if _, err := store.Pool.Exec(cleanup, `UPDATE channel_ops_queue_items SET status='failed',locked_at=NULL,locked_by=NULL WHERE channel_profile_id=$1::uuid AND status IN ('queued','running')`, channel.ID); err != nil {
			t.Error("fixture queue cleanup failed")
		}
		err := ownedReleaseLeaderAtDBTime(cleanup, store.Pool.QueryRow(cleanup, `SELECT clock_timestamp()`), f.lease.Release)
		if err != nil && !errors.Is(err, ErrLeaderAuthorityLost) {
			t.Error("fixture leader release failed")
		}
		// A failed clock read must not strand the dedicated lease connection or fabricate released_at.
		f.lease.mu.Lock()
		defer f.lease.mu.Unlock()
		f.lease.state.clear(f.lease.authority)
		if err := f.lease.releaseConnectionLocked(cleanup); err != nil {
			t.Error("fixture leader connection cleanup failed")
		}
	})
	return f
}

func (f *ownedPGFixture) assertCounts(t *testing.T, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for name, sql := range map[string]string{
		"task":           `SELECT count(*) FROM production_tasks WHERE channel_profile_id=$1::uuid`,
		"reservation":    `SELECT count(*) FROM owned_seed_inventory_items WHERE inventory_id=(SELECT owned_seed_inventory_id FROM channel_profiles WHERE id=$1::uuid) AND state='reserved'`,
		"exhausted_seed": `SELECT count(*) FROM manual_seeds WHERE channel_profile_id=$1::uuid AND status='exhausted'`,
		"plan_queue":     `SELECT count(*) FROM channel_ops_queue_items WHERE channel_profile_id=$1::uuid AND kind='plan_task'`,
		"attached_audit": `SELECT count(*) FROM decision_audit_entries WHERE channel_profile_id=$1::uuid AND created_task_id IS NOT NULL`,
	} {
		var got int
		if err := f.store.Pool.QueryRow(ctx, sql, f.channel.ID).Scan(&got); err != nil {
			t.Fatalf("read fixture count failed: %s", name)
		}
		if got != want {
			t.Fatalf("%s count=%d want=%d", name, got, want)
		}
	}
}

func TestOwnedPGAtomicContendersAndCommittedResultLossReplay(t *testing.T) {
	f := newOwnedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	pds := ownedTestPDS(func(ctx context.Context, _ PDSDecisionRequest) (PDSDecision, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return PDSDecision{}, ctx.Err()
		}
		return PDSDecision{Verdict: "allow", DecisionID: "offline"}, nil
	})
	results := make(chan error, 2)
	for _, bucket := range []string{"first", "different-bucket"} {
		go func(bucket string) { results <- f.store.RunTick(ctx, f.channel.ID, bucket, HandlerService{PDS: pds}) }(bucket)
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("contender did not reach external PDS")
		}
	}
	// Two callers passed prepare, but no database locks remain during PDS.
	releaseOnce.Do(func() { close(release) })
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("contender failed: %T", err)
		}
	}
	f.assertCounts(t, 1)
	noPDS := ownedTestPDS(func(context.Context, PDSDecisionRequest) (PDSDecision, error) {
		t.Error("replay called PDS")
		return PDSDecision{}, nil
	})
	// Model a caller that lost the committed return value, then restarted in a new bucket.
	if err := f.store.RunTick(ctx, f.channel.ID, "restarted", HandlerService{PDS: noPDS}); err != nil {
		t.Fatalf("replay failed: %T", err)
	}
	f.assertCounts(t, 1)
	var mode string
	var snapshot []byte
	if err := f.store.Pool.QueryRow(ctx, `SELECT approval_mode,channel_config_snapshot_json FROM production_tasks WHERE channel_profile_id=$1::uuid`, f.channel.ID).Scan(&mode, &snapshot); err != nil {
		t.Fatal("task snapshot unavailable")
	}
	v, err := ownedDecode(snapshot)
	if err != nil || mode != ApprovalAgent || ownedMap(ownedMap(v)["owned_inventory"])["inventory_id"] != *f.channel.OwnedSeedInventoryID {
		t.Fatal("task typed authority missing")
	}
}

func TestOwnedPGQueueAndLeaderLossDuringPDSCannotConsume(t *testing.T) {
	for _, mode := range []string{"queue", "leader"} {
		t.Run(mode, func(t *testing.T) {
			f := newOwnedPGFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			channelID := f.channel.ID
			if _, err := f.store.Enqueue(ctx, EnqueueOptions{Kind: QueueAgentTick, IdempotencyKey: "agent_tick:owned-test:" + channelID, Payload: map[string]any{"channel_id": channelID, "bucket": "lease"}, ChannelProfileID: &channelID}); err != nil {
				t.Fatal("fixture tick enqueue failed")
			}
			item, err := f.store.ClaimNextForKinds(ctx, handlerWorkerID(f.lease.Authority()), []string{QueueAgentTick})
			if err != nil || item == nil {
				t.Fatal("fixture tick claim failed")
			}
			h := HandlerService{Store: f.store, PDS: ownedTestPDS(func(ctx context.Context, _ PDSDecisionRequest) (PDSDecision, error) {
				if mode == "queue" {
					_, err := f.store.Pool.Exec(ctx, `UPDATE channel_ops_queue_items SET locked_by='replacement' WHERE id=$1::uuid`, item.ID)
					if err != nil {
						return PDSDecision{}, err
					}
				} else {
					if err := ownedReleaseLeaderAtDBTime(ctx, f.store.Pool.QueryRow(ctx, `SELECT clock_timestamp()`), f.lease.Release); err != nil {
						t.Error("fixture intentional leader release failed")
						return PDSDecision{}, err
					}
				}
				return PDSDecision{Verdict: "allow", DecisionID: "offline"}, nil
			})}
			err = h.HandleAgentTick(ctx, *item)
			if mode == "queue" && !errors.Is(err, ErrQueueLeaseLost) {
				t.Fatalf("queue loss not fenced: %T", err)
			}
			if mode == "leader" && !errors.Is(err, ErrLeaderAuthorityUnavailable) && !errors.Is(err, ErrLeaderAuthorityLost) {
				t.Fatalf("leader loss not fenced: %T", err)
			}
			f.assertCounts(t, 0)
		})
	}
}

func TestOwnedPGEmptyPlatformAliasDeniedBeforePDS(t *testing.T) {
	f := newOwnedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Model a preexisting legacy alias, including a disabled one, using this disposable database only.
	if _, err := f.store.Pool.Exec(ctx, `INSERT INTO publishing_accounts(id,channel_profile_id,platform,account_label,platform_account_id,credential_ref,platform_specific_config_json,default_privacy,external_asset_auto_publish,enabled,created_at,updated_at)
		SELECT gen_random_uuid(),channel_profile_id,'','legacy alias',platform_account_id,credential_ref,'{}'::json,'private',FALSE,FALSE,now(),now() FROM publishing_accounts WHERE id=$1::uuid`, f.data.AccountIDs[0]); err != nil {
		t.Fatal("legacy alias fixture failed")
	}
	h := HandlerService{PDS: ownedTestPDS(func(context.Context, PDSDecisionRequest) (PDSDecision, error) {
		t.Error("alias reached policy call")
		return PDSDecision{}, nil
	})}
	if err := f.store.RunTick(ctx, f.channel.ID, "alias", h); err != nil {
		t.Fatalf("alias hold failed: %T", err)
	}
	f.assertCounts(t, 0)
	var state string
	if err := f.store.Pool.QueryRow(ctx, `SELECT state FROM owned_seed_inventories WHERE id=$1::uuid`, *f.channel.OwnedSeedInventoryID).Scan(&state); err != nil || state != "held" {
		t.Fatal("alias did not close intake")
	}
}

func TestOwnedPGPolicyFailureAndMutationHoldWithoutReplacement(t *testing.T) {
	for _, mode := range []string{"deny", "error", "seed_mutation", "asset_mutation", "runtime_closed"} {
		t.Run(mode, func(t *testing.T) {
			f := newOwnedPGFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var calls atomic.Int32
			pds := ownedTestPDS(func(ctx context.Context, _ PDSDecisionRequest) (PDSDecision, error) {
				calls.Add(1)
				switch mode {
				case "deny":
					return PDSDecision{Verdict: "block"}, nil
				case "error":
					return PDSDecision{}, errors.New("offline failure")
				case "seed_mutation":
					_, err := f.store.Pool.Exec(ctx, `UPDATE manual_seeds SET prompt='changed during PDS' WHERE id=$1::uuid`, f.data.Items[0].Seed["id"])
					if err != nil {
						return PDSDecision{}, err
					}
				case "asset_mutation":
					_, err := f.store.Pool.Exec(ctx, `UPDATE assets SET file_size=file_size+1 WHERE id=$1::uuid`, f.data.Items[0].Asset["id"])
					if err != nil {
						return PDSDecision{}, err
					}
				case "runtime_closed":
					_, err := f.store.Pool.Exec(ctx, `UPDATE runtime_schedules SET state='CLOSED' WHERE service_name='videoprocess'`)
					if err != nil {
						return PDSDecision{}, err
					}
				}
				return PDSDecision{Verdict: "allow", DecisionID: "offline"}, nil
			})
			if err := f.store.RunTick(ctx, f.channel.ID, "first", HandlerService{PDS: pds}); err != nil {
				t.Fatalf("fresh finalizer failed: %T", err)
			}
			f.assertCounts(t, 0)
			var state string
			var paused bool
			if err := f.store.Pool.QueryRow(ctx, `SELECT i.state,c.intake_paused_at IS NOT NULL FROM channel_profiles c JOIN owned_seed_inventories i ON i.id=c.owned_seed_inventory_id WHERE c.id=$1::uuid`, f.channel.ID).Scan(&state, &paused); err != nil {
				t.Fatal("hold state unavailable")
			}
			if mode != "runtime_closed" && (state != "held" || !paused) {
				t.Fatal("failed input/policy did not close intake")
			}
			_ = f.store.RunTick(ctx, f.channel.ID, "next", HandlerService{PDS: pds})
			if calls.Load() != 1 {
				t.Fatal("failure selected a replacement")
			}
		})
	}
}

func TestOwnedPGRollbackIncludesReservationTaskSeedAuditAndQueue(t *testing.T) {
	f := newOwnedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sentinel := errors.New("offline rollback after complete finalizer")
	err := f.store.withChannelExecutionFence(ctx, f.channel.ID, true, func(s *Store) error {
		p, err := s.prepareTick(ctx, f.channel.ID, "rollback", agentTickOptions{})
		if err != nil {
			return err
		}
		if len(p.Candidates) != 1 {
			return errOwnedInventory
		}
		candidate := p.Candidates[0]
		candidate.PDSDecisionJSON = map[string]any{"verdict": "allow"}
		if err := s.finalizeTick(ctx, p, []TickCandidate{candidate}, nil); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("rollback failed before tested boundary: %T", err)
	}
	f.assertCounts(t, 0)
	if err := f.store.RunTick(ctx, f.channel.ID, "retry", HandlerService{PDS: fakePDS{decision: PDSDecision{Verdict: "allow"}}}); err != nil {
		t.Fatalf("retry failed: %T", err)
	}
	f.assertCounts(t, 1)
}
