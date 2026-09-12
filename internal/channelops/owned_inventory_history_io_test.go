package channelops

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

type ownedHistoryReadStub struct {
	query string
	calls int
	rows  []byte
	at    time.Time
	err   error
}

func (s *ownedHistoryReadStub) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	s.query = query
	s.calls++
	return s
}

func (s *ownedHistoryReadStub) Scan(dest ...any) error {
	if s.err != nil {
		return s.err
	}
	*dest[0].(*time.Time) = s.at
	*dest[1].(*[]byte) = append([]byte(nil), s.rows...)
	return nil
}

func TestOwnedHistoryDBLoaderGolden(t *testing.T) {
	for _, name := range historyGoldenNames {
		t.Run(name, func(t *testing.T) {
			f := historyGolden(t, name)
			db := &ownedHistoryReadStub{rows: historyJSON(t, f["rows"]), at: historyAt(t, f["observed_at"])}
			snapshot, err := loadOwnedHistorySnapshot(context.Background(), db, f["platform_channel_id"].(string))
			if err != nil {
				t.Fatal(err)
			}
			if db.calls != 1 || snapshot.snapshotSHA256() != f["snapshot_sha256"] || snapshot.redisJSON != "[]" {
				t.Fatal("loader lost one-statement snapshot identity or imported Redis authority")
			}
			// Supplied observations are separate from the DB-only reader, as in A1.
			withRedis, err := newOwnedHistorySnapshot([]byte(snapshot.rowsJSON), snapshot.platformChannelID, snapshot.observedAt, historyJSON(t, f["redis_observations"]))
			if err != nil {
				t.Fatal(err)
			}
			got, err := ownedDecode(mustHistoryAssessmentJSON(t, assessOwnedHistorySnapshot(withRedis, historyAt(t, f["now"]))))
			if err != nil || !ownedEqual(got, f["expected"]) {
				t.Fatal("DB projection changed the frozen assessment", err)
			}
		})
	}
}

func TestOwnedHistoryDBLoaderCompleteBoundedProjection(t *testing.T) {
	f := historyGolden(t, "direct")
	db := &ownedHistoryReadStub{rows: historyJSON(t, f["rows"]), at: historyAt(t, f["observed_at"])}
	if _, err := loadOwnedHistorySnapshot(context.Background(), db, f["platform_channel_id"].(string)); err != nil {
		t.Fatal(err)
	}
	query := strings.ToLower(db.query)
	if strings.Count(query, "limit 8193") != 27 || !strings.Contains(query, "clock_timestamp()") {
		t.Fatal("missing complete bounded MVCC enumeration")
	}
	for _, table := range strings.Fields(historyTableNames) {
		if strings.Count(query, "from public."+table+" ") != 1 {
			t.Errorf("missing or repeated table %s", table)
		}
	}
	for _, forbidden := range []string{" join ", " where ", " for update", " for share", "lease_secret_sha256", "token_sha256", "select *"} {
		if strings.Contains(query, forbidden) {
			t.Errorf("filtered, locked, or secret-bearing projection: %s", forbidden)
		}
	}
}

func TestOwnedHistoryDBLoaderRefusesIncompleteAndOverflow(t *testing.T) {
	for _, variant := range []string{"query_error", "missing_set", "overflow", "oversize", "invalid_json"} {
		t.Run(variant, func(t *testing.T) {
			f := historyGolden(t, "direct")
			rows := historyTestRows(f)
			if variant == "missing_set" {
				delete(rows, "worker_admission_grants")
			}
			if variant == "overflow" {
				values := []any{}
				for i := 0; i < ownedHistoryMaxSnapshotRows+1; i++ {
					values = append(values, map[string]any{"id": historyTestUID(i)})
				}
				rows["youtube_upload_operations"] = values
			}
			db := &ownedHistoryReadStub{rows: historyJSON(t, rows), at: historyAt(t, f["observed_at"])}
			if variant == "query_error" {
				db.err = errors.New("secret-bearing database error")
			}
			if variant == "oversize" {
				db.rows = []byte(strings.Repeat(" ", ownedHistoryMaxBytes+1))
			}
			if variant == "invalid_json" {
				db.rows = []byte("{")
			}
			snapshot, err := loadOwnedHistorySnapshot(context.Background(), db, f["platform_channel_id"].(string))
			if err == nil || !strings.HasPrefix(err.Error(), "owned_history_") || strings.Contains(err.Error(), "secret-bearing") || snapshot.rowsJSON != "" {
				t.Fatalf("failed read exposed partial authority: %v", err)
			}
		})
	}
}

func TestOwnedHistoryDBLoaderKeepsNewBlankOrphanOperation(t *testing.T) {
	f := historyGolden(t, "direct")
	rows := historyTestRows(f)
	rows["youtube_upload_operations"] = append(rows["youtube_upload_operations"].([]any), map[string]any{"id": historyTestUID(999), "production_task_id": nil})
	db := &ownedHistoryReadStub{rows: historyJSON(t, rows), at: historyAt(t, f["observed_at"])}
	snapshot, err := loadOwnedHistorySnapshot(context.Background(), db, f["platform_channel_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	got := assessOwnedHistorySnapshot(snapshot, historyAt(t, f["now"]))
	if got.BlockReason == nil || *got.BlockReason != "owned_history_orphan" {
		t.Fatal("orphan was hidden from global classification", got.BlockReason)
	}
}

type ownedHistoryRedisStub struct {
	commands [][]any
	markers  map[string]any
	pending  map[string][]any
	who      string
	err      error
	closeErr error
	closed   bool
}

func (s *ownedHistoryRedisStub) Do(ctx context.Context, args ...any) *redis.Cmd {
	s.commands = append(s.commands, append([]any(nil), args...))
	cmd := redis.NewCmd(ctx, args...)
	if s.err != nil {
		cmd.SetErr(s.err)
		return cmd
	}
	switch args[0] {
	case "ACL":
		cmd.SetVal(s.who)
	case "GET":
		value := s.markers[args[1].(string)]
		if value == nil {
			cmd.SetErr(redis.Nil)
		} else {
			cmd.SetVal(value)
		}
	case "XPENDING":
		values := s.pending[args[1].(string)+"/"+args[3].(string)]
		if values == nil {
			values = []any{}
		}
		cmd.SetVal(values)
	default:
		cmd.SetErr(errors.New("unexpected non-read command"))
	}
	return cmd
}

func (s *ownedHistoryRedisStub) Close() error { s.closed = true; return s.closeErr }

func ownedHistoryRedisFixture(t *testing.T, f map[string]any) *ownedHistoryRedisStub {
	t.Helper()
	s := &ownedHistoryRedisStub{who: "fixture-history-reader", markers: map[string]any{}, pending: map[string][]any{}}
	for _, raw := range f["redis_observations"].([]any) {
		obs := raw.(map[string]any)
		if obs["kind"] == "task" {
			s.markers["vp:worker-task-dispatch:"+obs["dispatch_key"].(string)] = obs["marker_message_id"]
		}
	}
	return s
}

func TestOwnedHistoryRedisRequestUsesCurrentApprovedGraph(t *testing.T) {
	for _, name := range []string{"direct", "history_only", "retired_unassigned"} {
		t.Run(name, func(t *testing.T) {
			f := historyGolden(t, name)
			snapshot := historySnapshot(t, f)
			req, err := ownedHistoryRedisRequest(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if name != "retired_unassigned" {
				if req != nil {
					t.Fatal("ordinary history requested Redis")
				}
				return
			}
			if req == nil || len(req.sources()) != len(f["redis_observations"].([]any)) {
				t.Fatal("retirement omitted native sources")
			}
			delete(historyTestFirst(f, "worker_task_dispatches"), "payload_json")
			if _, err := ownedHistoryRedisRequest(historySnapshot(t, f)); err == nil {
				t.Fatal("changed current graph was replaced by stored certificate")
			}
		})
	}
}

func TestOwnedHistoryRedisObservationIsAuthenticatedReadOnly(t *testing.T) {
	for _, variant := range []string{"good", "wrong_identity", "read_error", "close_error", "marker_drift", "pending"} {
		t.Run(variant, func(t *testing.T) {
			f := historyGolden(t, "retired_unassigned")
			snapshot := historySnapshot(t, f)
			req, err := ownedHistoryRedisRequest(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			client := ownedHistoryRedisFixture(t, f)
			switch variant {
			case "wrong_identity":
				client.who = "other"
			case "read_error":
				client.err = errors.New("secret-bearing Redis error")
			case "close_error":
				client.closeErr = errors.New("secret-bearing close error")
			case "marker_drift":
				for key := range client.markers {
					client.markers[key] = "9999-0"
				}
			case "pending":
				for _, source := range req.sources() {
					if source.message != nil {
						client.pending[source.stream+"/"+*source.message] = []any{[]any{*source.message, "native-consumer", int64(0), int64(1)}}
					}
				}
			}
			factory := func(options *redis.Options) ownedHistoryRedisClient {
				if options.Username != "fixture-history-reader" || options.MaxRetries != -1 || options.PoolSize != 1 || !options.ContextTimeoutEnabled {
					t.Fatal("unbounded or unbound Redis configuration")
				}
				return client
			}
			evidence, err := observeOwnedHistoryRedis(context.Background(), req, "redis://fixture-history-reader:fixture-secret@127.0.0.1:55464/15", factory)
			if variant == "wrong_identity" || variant == "read_error" || variant == "close_error" {
				if err == nil || strings.Contains(err.Error(), "secret-bearing") || evidence != nil {
					t.Fatal("unverified read produced evidence", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				fresh, err := ownedHistoryWithObservations(snapshot, evidence)
				if err != nil {
					t.Fatal(err)
				}
				got := assessOwnedHistorySnapshot(fresh, historyAt(t, f["now"]))
				if variant == "good" {
					decoded, _ := ownedDecode(mustHistoryAssessmentJSON(t, got))
					if !ownedEqual(decoded, f["expected"]) {
						t.Fatal("native observation changed full assessment")
					}
				} else if got.BlockReason == nil || *got.BlockReason != "owned_history_retired_redis_changed" {
					t.Fatal("Redis drift accepted", got.BlockReason)
				}
			}
			if !client.closed || len(client.commands) == 0 || client.commands[0][0] != "ACL" {
				t.Fatal("missing authentication/owned cleanup")
			}
			if variant == "wrong_identity" && len(client.commands) != 1 {
				t.Fatal("read before authenticating configured native identity")
			}
			for _, args := range client.commands {
				if args[0] != "ACL" && args[0] != "GET" && args[0] != "XPENDING" {
					t.Fatal("non-read Redis command", args[0])
				}
			}
		})
	}
}

func TestOwnedHistoryRedisObservationRejectsUnboundConfig(t *testing.T) {
	f := historyGolden(t, "retired_unassigned")
	req, err := ownedHistoryRedisRequest(historySnapshot(t, f))
	if err != nil {
		t.Fatal(err)
	}
	for _, url := range []string{"", "redis://127.0.0.1:55464/15", "redis://default:secret@127.0.0.1:55464/15", "redis://reader@127.0.0.1:55464/15", "redis://reader:secret@127.0.0.1:55464/15?read_timeout=0"} {
		if _, err := observeOwnedHistoryRedis(context.Background(), req, url, func(*redis.Options) ownedHistoryRedisClient {
			t.Fatal("invalid config constructed a client")
			return nil
		}); err == nil {
			t.Fatal("accepted unbound Redis config")
		}
	}
}

func TestOwnedHistoryTickObservationReentersAfterExternalRead(t *testing.T) {
	for _, variant := range []string{"fresh", "wait_expired", "read_failure", "cancelled"} {
		t.Run(variant, func(t *testing.T) {
			f := historyGolden(t, "retired_unassigned")
			snapshot := historySnapshot(t, f)
			client := ownedHistoryRedisFixture(t, f)
			if variant == "read_failure" {
				client.err = errors.New("read failed")
			}
			locked, reads, phases := false, 0, 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := HandlerService{Config: Config{OwnedHistoryRedisURL: "redis://fixture-history-reader:fixture-secret@127.0.0.1:55464/15"}, ownedHistoryRedisFactory: func(*redis.Options) ownedHistoryRedisClient {
				if locked {
					t.Fatal("Redis client constructed under row locks")
				}
				reads++
				return client
			}}
			err := h.withOwnedHistoryObservation(ctx, func(evidence *ownedHistoryRedisEvidence) error {
				locked = true
				defer func() { locked = false }()
				phases++
				current := snapshot
				if variant == "wait_expired" && phases > 1 {
					current.observedAt = snapshot.observedAt.Add(time.Duration(phases-1) * 61 * time.Second)
				}
				if variant == "cancelled" {
					cancel()
				}
				_, err := ownedHistoryWithObservations(current, evidence)
				return err
			})
			switch variant {
			case "fresh":
				if err != nil || reads != 1 || phases != 2 {
					t.Fatalf("phase counts %d/%d, error %v", reads, phases, err)
				}
			case "wait_expired":
				if !errors.Is(err, ErrHandlerSnapshotStale) || reads != 2 || phases != 3 {
					t.Fatalf("stale evidence reused or refresh unbounded: %d/%d %v", reads, phases, err)
				}
			case "read_failure":
				if err == nil || err.Error() != "owned_history_redis_identity" || reads != 1 || phases != 2 {
					t.Fatalf("read failure did not reach fresh DB phase: %d/%d %v", reads, phases, err)
				}
			case "cancelled":
				if !errors.Is(err, context.Canceled) || reads != 0 || phases != 1 {
					t.Fatalf("cancelled controller continued: %d/%d %v", reads, phases, err)
				}
			}
		})
	}
}
