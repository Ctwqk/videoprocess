package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type snapshotDB struct {
	pgx.Tx
	policy        []byte
	writes        int
	queries       int
	fail          string
	zero          string
	sealed        bool
	complete      bool
	pending       bool
	tasks         int
	attached      int
	snapshotFacts [][]byte
	statements    []string
	taskArgs      [][]any
	tickSummary   []byte
	conflictTick  bool
	exactReplay   bool
}

func (d *snapshotDB) Exec(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
	d.writes++
	d.statements = append(d.statements, q)
	if d.complete {
		return pgconn.CommandTag{}, errors.New("write after completion")
	}
	if d.fail != "" && strings.Contains(q, d.fail) {
		return pgconn.CommandTag{}, errors.New("injected write failure")
	}
	if strings.Contains(q, "INSERT INTO decision_policy_versions") && d.policy == nil {
		d.policy = append([]byte(nil), args[0].([]byte)...)
	}
	if strings.Contains(q, "SET created_task_id") {
		if d.sealed {
			return pgconn.CommandTag{}, errors.New("immutable decision task update")
		}
		d.attached++
	}
	if strings.Contains(q, "feature_snapshot_id =") {
		d.sealed = true
	}
	if strings.Contains(q, "snapshot_complete") {
		d.complete = true
	}
	if d.zero != "" && strings.Contains(q, d.zero) {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (d *snapshotDB) QueryRow(_ context.Context, q string, args ...any) pgx.Row {
	d.queries++
	d.statements = append(d.statements, q)
	return ownedB2FenceRow(func(dest ...any) error {
		if d.fail != "" && strings.Contains(q, d.fail) {
			return errors.New("injected query failure")
		}
		switch {
		case strings.Contains(q, "FROM decision_policy_versions"):
			*dest[0].(*string) = "policy-id"
			*dest[1].(*[]byte) = d.policy
		case strings.Contains(q, "INSERT INTO candidate_feature_snapshots"):
			d.snapshotFacts = append(d.snapshotFacts, append([]byte(nil), args[2].([]byte)...))
			*dest[0].(*string) = fmt.Sprintf("snapshot-%d", d.queries)
		case strings.Contains(q, "INSERT INTO agent_tick_audits"):
			if d.conflictTick {
				return pgx.ErrNoRows
			}
			d.pending = len(args) > 13 && args[13] == "snapshot_pending"
			d.tickSummary = append([]byte(nil), args[9].([]byte)...)
			*dest[0].(*string) = "tick-id"
		case strings.Contains(q, "INSERT INTO decision_audit_entries"):
			*dest[0].(*string) = fmt.Sprintf("decision-%d", d.queries)
		case strings.Contains(q, "INSERT INTO production_tasks"):
			if !d.pending || d.complete || d.sealed {
				return errors.New("task outside mutable pending audit")
			}
			d.tasks++
			d.taskArgs = append(d.taskArgs, args)
			*dest[0].(*string) = "00000000-0000-0000-0000-000000000099"
		case strings.Contains(q, "INSERT INTO channel_ops_queue_items"):
			*dest[0].(*string) = "queue-id"
		case strings.Contains(q, "FROM agent_tick_audits a WHERE"):
			if string(args[10].([]byte)) == "null" {
				return errors.New("replay lost generated summary")
			}
			*dest[0].(*string) = "tick-id"
			*dest[1].(*bool) = d.exactReplay
		default:
			return fmt.Errorf("unexpected snapshot query: %s", q)
		}
		return nil
	})
}

type snapshotRows struct {
	pgx.Rows
	values [][]any
	index  int
}

func (r *snapshotRows) Close()                 {}
func (r *snapshotRows) Err() error             { return nil }
func (r *snapshotRows) Next() bool             { r.index++; return r.index <= len(r.values) }
func (r *snapshotRows) Scan(dest ...any) error { return snapshotScan(r.values[r.index-1], dest) }
func snapshotScan(values, dest []any) error {
	if len(values) != len(dest) {
		return fmt.Errorf("scan arity %d != %d", len(values), len(dest))
	}
	for i, v := range values {
		if v != nil {
			reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(v))
		}
	}
	return nil
}

type normalSnapshotDB struct {
	snapshotDB
	dryRun, empty, deny bool
	at                  time.Time
}

func (d *normalSnapshotDB) QueryRow(ctx context.Context, q string, args ...any) pgx.Row {
	if strings.Contains(q, "FROM channel_profiles") {
		return ownedB2FenceRow(func(dest ...any) error {
			return snapshotScan([]any{
				ownedTestID(1), true, d.dryRun, nil, nil, 60, 1, []byte(`{}`), []byte(`{}`), []byte(`{}`), "9:16", d.at, d.at, nil,
			}, dest)
		})
	}
	if strings.Contains(q, "SELECT COUNT(*)") {
		return ownedB2FenceRow(func(dest ...any) error { *dest[0].(*int64) = 0; return nil })
	}
	return d.snapshotDB.QueryRow(ctx, q, args...)
}
func (d *normalSnapshotDB) Query(_ context.Context, q string, args ...any) (pgx.Rows, error) {
	r := &snapshotRows{}
	switch {
	case strings.Contains(q, "FROM topic_lanes"):
		if !d.empty {
			r.values = [][]any{{ownedTestID(2), ownedTestID(1), "Lane", "Description", []byte(`[]`), true, nil, 1.0, 1, 0, 1, d.at}}
		}
	case strings.Contains(q, "FROM publishing_accounts"):
		if !d.deny {
			r.values = [][]any{{ownedTestID(3), ownedTestID(1), "youtube", "Account", "platform", true, nil, "unlisted", false, d.at}}
		}
	case strings.Contains(q, "FROM manual_seeds"):
		if !d.empty {
			r.values = [][]any{{ownedTestID(4), ownedTestID(1), nil, nil, "original prompt", "original title", "original_only", []byte(`[]`), []byte(`[]`), []byte(`{}`), "active", d.at}}
		}
	case strings.Contains(q, "discovery_signals"), strings.Contains(q, "lane_format_matrix"):
	default:
		return nil, fmt.Errorf("unexpected input query: %s", q)
	}
	return r, nil
}

func TestRunTickPersistsPolicySnapshots(t *testing.T) {
	for _, name := range []string{"success", "dry-run", "all-rejected", "empty"} {
		t.Run(name, func(t *testing.T) {
			db := &normalSnapshotDB{at: time.Unix(123, 0), dryRun: name == "dry-run", deny: name == "all-rejected", empty: name == "empty"}
			s := &Store{executionDB: db, Now: func() time.Time { return db.at }, buildCommitSHA: snapshotTestCommit}
			p, err := s.prepareTick(context.Background(), ownedTestID(1), "bucket", agentTickOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.finalizeTick(context.Background(), p, p.Candidates, nil); err != nil {
				t.Fatal(err)
			}
			wantTasks, wantFacts := 0, 1
			if name == "success" {
				wantTasks = 1
			}
			if name == "empty" {
				wantFacts = 0
			}
			if !db.pending || !db.complete || db.tasks != wantTasks || len(db.snapshotFacts) != wantFacts || db.attached != wantTasks {
				t.Fatalf("%s: pending=%t complete=%t tasks=%d facts=%d links=%d", name, db.pending, db.complete, db.tasks, len(db.snapshotFacts), db.attached)
			}
			if !strings.Contains(db.statements[len(db.statements)-1], "snapshot_complete") {
				t.Fatal("completion not last")
			}
			if wantTasks == 1 {
				args := db.taskArgs[0]
				if args[7] != "original title" || args[8] != "original prompt" || args[14] != ApprovalHuman || args[15] != TaskSelected {
					t.Fatalf("changed task payload: %v", args)
				}
			}
		})
	}
}

func TestRunTickRejectsDevelopmentIdentity(t *testing.T) {
	db := &normalSnapshotDB{at: time.Unix(123, 0)}
	s := &Store{executionDB: db, Now: func() time.Time { return db.at }, buildCommitSHA: "development"}
	if _, err := s.prepareTick(context.Background(), ownedTestID(1), "bucket", agentTickOptions{}); err == nil || db.writes != 0 {
		t.Fatal("development identity reached persistence")
	}
}

func TestRunTickSnapshotWriteFailuresRemainIncomplete(t *testing.T) {
	for _, failure := range []string{"INSERT INTO decision_policy_versions", "INSERT INTO agent_tick_audits", "INSERT INTO candidate_feature_snapshots", "INSERT INTO decision_audit_entries", "INSERT INTO production_tasks", "SET created_task_id", "INSERT INTO channel_ops_queue_items", "feature_snapshot_id =", "snapshot_complete"} {
		t.Run(failure, func(t *testing.T) {
			db := &normalSnapshotDB{at: time.Unix(123, 0)}
			db.fail = failure
			s := &Store{executionDB: db, Now: func() time.Time { return db.at }, buildCommitSHA: snapshotTestCommit}
			p, err := s.prepareTick(context.Background(), ownedTestID(1), "bucket", agentTickOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.finalizeTick(context.Background(), p, p.Candidates, nil); err == nil {
				t.Fatal("snapshot write failure swallowed")
			}
			if db.complete {
				t.Fatal("failed transaction marked complete")
			}
		})
	}
}

func snapshotTestFacts(t *testing.T) (PolicyVersion, SnapshotSet) {
	t.Helper()
	policy, err := BuildBaselinePolicy(ChannelProfileRow{ID: "channel", ConfigVersion: 1}, snapshotTestCommit)
	if err != nil {
		t.Fatal(err)
	}
	set, err := BuildCandidateSnapshots(policy, []TickCandidate{{CandidateID: "accepted"}, {CandidateID: "rejected", Rejected: true}}, time.Unix(123, 0))
	if err != nil {
		t.Fatal(err)
	}
	return policy, set
}

func TestResolvePolicyVersion(t *testing.T) {
	policy, _ := snapshotTestFacts(t)
	db := &snapshotDB{}
	id, err := ResolvePolicyVersion(context.Background(), db, policy)
	if err != nil || id != "policy-id" {
		t.Fatalf("resolve: %s %v", id, err)
	}
	again, err := ResolvePolicyVersion(context.Background(), db, policy)
	if err != nil || again != id {
		t.Fatalf("reuse: %s %v", again, err)
	}
	var conflicting PolicyVersion
	if err := json.Unmarshal(db.policy, &conflicting); err != nil {
		t.Fatal(err)
	}
	conflicting.FormulaJSON["selection"] = "changed"
	db.policy, _ = json.Marshal(conflicting)
	if _, err := ResolvePolicyVersion(context.Background(), db, policy); err == nil {
		t.Fatal("same key/version with conflicting content accepted")
	}
}

func TestResolvePolicyVersionRejectsInvalidIdentityBeforeWriting(t *testing.T) {
	policy, _ := snapshotTestFacts(t)
	for _, change := range []func(*PolicyVersion){func(p *PolicyVersion) { p.CodeCommitSHA = "development" }, func(p *PolicyVersion) { p.ConfigHash = "wrong" }} {
		p := policy
		change(&p)
		db := &snapshotDB{}
		if _, err := ResolvePolicyVersion(context.Background(), db, p); err == nil || db.writes != 0 {
			t.Fatal("invalid policy reached writer")
		}
	}
}

func TestInsertCandidateSnapshots(t *testing.T) {
	_, set := snapshotTestFacts(t)
	db := &snapshotDB{}
	ids, err := InsertCandidateSnapshots(context.Background(), db, "tick", "policy", set)
	if err != nil || len(ids) != 2 || ids["accepted"] == "" || ids["rejected"] == "" || ids["accepted"] == ids["rejected"] {
		t.Fatalf("links: %v %v", ids, err)
	}
}

func TestInsertCandidateSnapshotsRejectsCorruptFactsBeforeWriting(t *testing.T) {
	for _, name := range []string{"duplicate", "hash", "asof", "set", "policy"} {
		t.Run(name, func(t *testing.T) {
			_, set := snapshotTestFacts(t)
			switch name {
			case "duplicate":
				set.Candidates = append(set.Candidates, set.Candidates[0])
			case "hash":
				set.Candidates[0].FeatureHash = "wrong"
			case "asof":
				set.Candidates[0].FeatureAsOf = time.Unix(999, 0)
			case "set":
				set.CandidateSetHash = "wrong"
			case "policy":
				set.Candidates[0].PolicyVersion = "wrong"
			}
			db := &snapshotDB{}
			if _, err := InsertCandidateSnapshots(context.Background(), db, "tick", "policy", set); err == nil || db.queries != 0 {
				t.Fatal("corrupt facts reached database")
			}
		})
	}
}

func TestPreparedSnapshotsPreserveOriginalFactsAndAsOf(t *testing.T) {
	s := &Store{buildCommitSHA: snapshotTestCommit}
	candidate := TickCandidate{CandidateID: "one", Prompt: "original"}
	p, err := s.captureTickFacts(tickPreparation{Channel: ChannelProfileRow{ID: "channel"}, Now: time.Unix(123, 0), Candidates: []TickCandidate{candidate}})
	if err != nil {
		t.Fatal(err)
	}
	p.Candidates[0].Prompt = "mutated after preparation"
	candidate.Prompt = "current mutable config"
	candidate.Rejected = true
	candidate.RejectionGuard = "owned_inventory_inputs_changed"
	set, err := p.decisionSnapshots(TickResult{Rejected: []TickCandidate{candidate}})
	if err != nil {
		t.Fatal(err)
	}
	if set.FeatureAsOf != time.Unix(123, 0).UTC() || set.Candidates[0].RawFeaturesJSON["prompt"] != "original" || set.Candidates[0].Decision != "rejected" {
		t.Fatalf("lost preparation facts: %+v", set)
	}
	if _, err := p.decisionSnapshots(TickResult{}); err == nil {
		t.Fatal("missing candidate accepted")
	}
}

func TestPreparedSnapshotsUseOnePostgresPrecisionAsOf(t *testing.T) {
	s := &Store{buildCommitSHA: snapshotTestCommit}
	at := time.Unix(123, 123456789)
	p, err := s.captureTickFacts(tickPreparation{Channel: ChannelProfileRow{ID: "channel"}, Now: at, Candidates: []TickCandidate{{CandidateID: "one"}}})
	if err != nil {
		t.Fatal(err)
	}
	set, err := p.decisionSnapshots(TickResult{Accepted: p.Candidates})
	if err != nil {
		t.Fatal(err)
	}
	if !p.Now.Equal(at) || set.FeatureAsOf.Nanosecond() != 123456000 || !set.Candidates[0].FeatureAsOf.Equal(set.FeatureAsOf) {
		t.Fatalf("inconsistent persisted precision or changed task clock: %v %v", p.Now, set.FeatureAsOf)
	}
}

func TestPreparedSnapshotsRejectReboundPolicy(t *testing.T) {
	s := &Store{buildCommitSHA: snapshotTestCommit}
	p, err := s.captureTickFacts(tickPreparation{Channel: ChannelProfileRow{ID: "original"}, Now: time.Unix(123, 0), Candidates: []TickCandidate{{CandidateID: "one"}}})
	if err != nil {
		t.Fatal(err)
	}
	p.Policy, err = BuildBaselinePolicy(ChannelProfileRow{ID: "changed"}, snapshotTestCommit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.decisionSnapshots(TickResult{Accepted: p.Candidates}); err == nil {
		t.Fatal("original facts rebound to changed policy")
	}
}

func TestSnapshotAuditRetainsExactDecisionFacts(t *testing.T) {
	db := &snapshotDB{}
	s := &Store{executionDB: db, Now: func() time.Time { return time.Unix(123, 0) }, buildCommitSHA: snapshotTestCommit}
	candidate := TickCandidate{CandidateID: "held", Rejected: true, PDSRequestJSON: map[string]any{"action": "candidate_accept", "source": "original"}}
	p, err := s.captureTickFacts(tickPreparation{Channel: ChannelProfileRow{ID: "channel"}, ChannelID: "channel", Now: s.Now(), Candidates: []TickCandidate{candidate}})
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.beginSnapshotAudit(context.Background(), p, TickResult{Rejected: p.Candidates}, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	var summary struct {
		Decisions map[string]CandidateDecisionFacts `json:"snapshot_decisions"`
	}
	if err := json.Unmarshal(db.tickSummary, &summary); err != nil {
		t.Fatal(err)
	}
	decision, ok := summary.Decisions["held"]
	if !ok || decision.PDSRequestJSON["source"] != "original" {
		t.Fatal("hashed PDS request evidence was not persisted")
	}
	persisted := w.snapshots.Candidates[0]
	persisted.CandidateDecisionFacts = decision
	hash, err := snapshotDecisionHash(persisted)
	if err != nil || hash != w.snapshots.Candidates[0].DecisionHash {
		t.Fatal("persisted decision cannot reproduce decision hash")
	}
}

func TestSnapshotAuditExactReplayAndConflict(t *testing.T) {
	for _, exact := range []bool{true, false} {
		t.Run(fmt.Sprint(exact), func(t *testing.T) {
			db := &snapshotDB{conflictTick: true, exactReplay: exact}
			s := &Store{executionDB: db, Now: func() time.Time { return time.Unix(123, 0) }, buildCommitSHA: snapshotTestCommit}
			p, err := s.captureTickFacts(tickPreparation{Channel: ChannelProfileRow{ID: "channel"}, ChannelID: "channel", Now: s.Now(), Candidates: []TickCandidate{{CandidateID: "one"}}})
			if err != nil {
				t.Fatal(err)
			}
			w, err := s.beginSnapshotAudit(context.Background(), p, TickResult{Accepted: p.Candidates}, nil)
			if exact && (err != nil || !w.replay) {
				t.Fatalf("exact replay: %v %v", w, err)
			}
			if !exact && err == nil {
				t.Fatal("conflicting replay accepted")
			}
			if len(db.snapshotFacts) != 0 || db.tasks != 0 || db.sealed || db.complete {
				t.Fatal("retry wrote new facts or tasks")
			}
		})
	}
}

func TestSnapshotCompletionRejectsMissingLinks(t *testing.T) {
	_, set := snapshotTestFacts(t)
	for _, name := range []string{"missing-snapshot", "missing-decision", "zero-update", "cardinality"} {
		t.Run(name, func(t *testing.T) {
			db := &snapshotDB{}
			w := snapshotAuditWrite{tickID: "tick", policyID: "policy", snapshots: set, snapshotIDs: map[string]string{"accepted": "s1", "rejected": "s2"}, decisionIDs: map[string]string{"accepted": "d1", "rejected": "d2"}}
			switch name {
			case "missing-snapshot":
				delete(w.snapshotIDs, "rejected")
			case "missing-decision":
				delete(w.decisionIDs, "accepted")
			case "zero-update":
				db.zero = "feature_snapshot_id ="
			case "cardinality":
				db.zero = "snapshot_complete"
			}
			if err := w.complete(context.Background(), db); err == nil {
				t.Fatal("incomplete snapshot accepted")
			}
		})
	}
}
