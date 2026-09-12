package channelops

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const snapshotTestCommit = "baf94ba1c72f51bda65de7b61e74f849fa87330c"

func snapshotTestPolicy(t *testing.T) PolicyVersion {
	t.Helper()
	policy, err := BuildBaselinePolicy(ChannelProfileRow{ID: "channel", ConfigVersion: 3, DefaultAspectRatio: "9:16"}, snapshotTestCommit)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func snapshotTestCandidates() []TickCandidate {
	return []TickCandidate{
		{CandidateID: "z", Source: SourceManualSeed, SourceKind: SourceManualSeed,
			Seed: &ManualSeedRow{ID: "seed"}, Lane: &TopicLaneRow{ID: "lane", Weight: 2},
			LaneFormat: &LaneFormatRow{ID: "format", Weight: 3}, Account: &PublishingAccountRow{ID: "account", DefaultPrivacy: "unlisted"},
			ConstraintsJSON:     map[string]any{"nested": map[string]any{"b": 2, "a": 1}},
			SourcePlatformsJSON: []string{"youtube"}, MaterialLibraryIDsJSON: []string{"material"},
			ScoreJSON:        map[string]any{"lane_weight": 2, "format_weight": 3},
			GuardResultsJSON: []map[string]any{{"guard": "privacy", "verdict": "allow"}}},
		{CandidateID: "a", Source: SourceTrendYT, DiscoverySignal: &DiscoverySignalRow{ID: "signal", TrendScore: .7, NoveltyScore: .2},
			Rejected: true, RejectionGuard: "account_unavailable", RejectionReason: "No account"},
	}
}

func snapshotTestBuild(t *testing.T, policy PolicyVersion, candidates []TickCandidate, asOf time.Time) SnapshotSet {
	t.Helper()
	set, err := BuildCandidateSnapshots(policy, candidates, asOf)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestCanonicalPolicyHashMapOrderAndNumbers(t *testing.T) {
	left := map[string]any{"z": []any{map[string]any{"b": 2, "a": 1}}, "a": json.Number("9007199254740993")}
	right := map[string]any{}
	right["a"] = int64(9007199254740993)
	right["z"] = []any{map[string]int{"a": 1, "b": 2}}
	a, err := canonicalPolicyHash(left)
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicalPolicyHash(right)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("map construction changed hash: %s != %s", a, b)
	}
	right["a"] = int64(9007199254740992)
	b, err = canonicalPolicyHash(right)
	if err != nil || a == b {
		t.Fatalf("lost integer precision: %s, %v", b, err)
	}
}

func TestBuildBaselinePolicyIdentityAndSemanticChanges(t *testing.T) {
	channel := ChannelProfileRow{ID: "channel", ConfigVersion: 3, DefaultAspectRatio: "9:16", RiskPolicyJSON: map[string]any{"nested": map[string]any{"b": 2, "a": 1}}}
	base, err := BuildBaselinePolicy(channel, snapshotTestCommit)
	if err != nil {
		t.Fatal(err)
	}
	if base.PolicyKey != "channelops-baseline" || base.FeatureSchemaVersion != "channelops-candidate-v1" || base.CodeCommitSHA != snapshotTestCommit || base.TemplateRegistryVersion != "legacy-unversioned" || base.PromptBundleVersion != "legacy-unversioned" {
		t.Fatalf("wrong immutable identities: %#v", base)
	}
	channel.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	channel.UpdatedAt = channel.CreatedAt
	channel.RiskPolicyJSON = map[string]any{"nested": map[string]any{"a": 1, "b": 2}}
	same, err := BuildBaselinePolicy(channel, snapshotTestCommit)
	if err != nil || same.ConfigHash != base.ConfigHash || same.Version != base.Version {
		t.Fatalf("nonsemantic change: %#v, %v", same, err)
	}
	for name, mutate := range map[string]func(*ChannelProfileRow){
		"risk":     func(c *ChannelProfileRow) { c.RiskPolicyJSON = map[string]any{"public": true} },
		"cadence":  func(c *ChannelProfileRow) { c.CadencePolicyJSON = map[string]any{"max_posts_per_day": 2} },
		"mix":      func(c *ChannelProfileRow) { c.ContentMixPolicyJSON = map[string]any{"manual_seed": .5} },
		"aspect":   func(c *ChannelProfileRow) { c.DefaultAspectRatio = "16:9" },
		"interval": func(c *ChannelProfileRow) { c.TickIntervalMinutes = 30 },
		"owned":    func(c *ChannelProfileRow) { c.OwnedInventoryActive = true },
	} {
		t.Run(name, func(t *testing.T) {
			changed := channel
			mutate(&changed)
			got, err := BuildBaselinePolicy(changed, snapshotTestCommit)
			if err != nil || got.ConfigHash == base.ConfigHash || got.Version == base.Version {
				t.Fatalf("semantic change ignored: %#v, %v", got, err)
			}
		})
	}
	changedCommit, err := BuildBaselinePolicy(channel, strings.Repeat("a", 40))
	if err != nil || changedCommit.ConfigHash == base.ConfigHash {
		t.Fatalf("code identity ignored: %v", err)
	}
	for _, commit := range []string{"", "development", "unknown", "baf94ba", strings.Repeat("a", 39), strings.Repeat("a", 41), strings.Repeat("g", 40), " " + snapshotTestCommit} {
		t.Run(commit, func(t *testing.T) {
			got, err := BuildBaselinePolicy(channel, commit)
			if err == nil || !reflect.DeepEqual(got, PolicyVersion{}) {
				t.Fatalf("invalid identity produced facts: %#v, %v", got, err)
			}
		})
	}
}

func TestBuildCandidateSnapshotsCompleteOrderedAndMissing(t *testing.T) {
	asOf := time.Date(2026, 9, 12, 10, 11, 12, 123, time.FixedZone("local", -7*3600))
	candidates := snapshotTestCandidates()
	set := snapshotTestBuild(t, snapshotTestPolicy(t), candidates, asOf)
	if len(set.Candidates) != 2 || set.Candidates[0].CandidateID != "z" || set.Candidates[1].CandidateID != "a" {
		t.Fatalf("changed candidate sequence: %#v", set)
	}
	if set.Candidates[0].Decision != "accepted" || set.Candidates[1].Decision != "rejected" {
		t.Fatalf("lost decisions: %#v", set.Candidates)
	}
	for _, snapshot := range set.Candidates {
		if !snapshot.FeatureAsOf.Equal(asOf) || snapshot.FeatureAsOf != set.FeatureAsOf || snapshot.CandidateSetHash != set.CandidateSetHash {
			t.Fatalf("inconsistent tick facts: %#v", snapshot)
		}
		for _, field := range []string{"baseline_score", "final_score", "rank", "exposure", "reward", "eligibility", "normalized_features", "cadence", "content_mix", "material_supply", "production_reliability", "cost_estimate", "risk_estimate"} {
			if !snapshot.MissingFeatureMaskJSON[field] {
				t.Errorf("fabricated %s", field)
			}
		}
		if snapshot.BaselineScore != nil || snapshot.FinalScore != nil || snapshot.Rank != nil || snapshot.NormalizedFeaturesJSON != nil || snapshot.CostEstimateJSON != nil {
			t.Fatal("fabricated missing values")
		}
	}
	if set.Candidates[0].MissingFeatureMaskJSON["lane_id"] || set.Candidates[0].MissingFeatureMaskJSON["source_record_id"] {
		t.Fatal("known identity marked missing")
	}
	for _, field := range []string{"lane_id", "format_id", "account_id"} {
		if !set.Candidates[1].MissingFeatureMaskJSON[field] {
			t.Errorf("missing %s not marked", field)
		}
	}
	missing := snapshotTestBuild(t, snapshotTestPolicy(t), []TickCandidate{{CandidateID: "missing", Lane: &TopicLaneRow{}, LaneFormat: &LaneFormatRow{}, Account: &PublishingAccountRow{}, Seed: &ManualSeedRow{}}}, asOf).Candidates[0]
	for _, field := range []string{"lane_id", "format_id", "account_id", "source_record_id"} {
		if !missing.MissingFeatureMaskJSON[field] {
			t.Errorf("empty %s not marked", field)
		}
	}
	reversed := snapshotTestBuild(t, snapshotTestPolicy(t), []TickCandidate{candidates[1], candidates[0]}, asOf)
	if reversed.CandidateSetHash != set.CandidateSetHash || reversed.Candidates[1].FeatureHash != set.Candidates[0].FeatureHash || reversed.Candidates[1].DecisionHash != set.Candidates[0].DecisionHash {
		t.Fatal("input order changed hashes")
	}
}

func TestBuildCandidateSnapshotsHashBoundaries(t *testing.T) {
	policy := snapshotTestPolicy(t)
	asOf := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	candidates := snapshotTestCandidates()
	base := snapshotTestBuild(t, policy, candidates, asOf)
	candidates[0].Lane.CreatedAt = asOf
	candidates[0].Lane.PausedUntil = &asOf
	candidates[0].Seed.CreatedAt = asOf
	candidates[0].LaneFormat.CreatedAt = asOf
	candidates[0].Account.CreatedAt = asOf
	candidates[1].DiscoverySignal.ObservedAt = asOf
	candidates[1].DiscoverySignal.RawJSON = map[string]any{"metrics": map[string]any{"views": 999}}
	candidates[0].PDSDecisionJSON = map[string]any{"verdict": "block", "evaluated_at": asOf}
	candidates[0].PDSRequestJSON = map[string]any{"context": "later"}
	candidates[0].LearningContextJSON = map[string]any{"metrics": map[string]any{"views": 100}}
	candidates[0].ScoreJSON["reward"] = .9
	candidates[0].ScoreJSON["observed_at"] = asOf
	later := snapshotTestBuild(t, policy, candidates, asOf.Add(time.Hour))
	if later.CandidateSetHash != base.CandidateSetHash {
		t.Fatal("later facts changed set identity")
	}
	for i := range base.Candidates {
		if later.Candidates[i].FeatureHash != base.Candidates[i].FeatureHash {
			t.Fatalf("later facts leaked into features at %d", i)
		}
	}
	if later.Candidates[0].DecisionHash == base.Candidates[0].DecisionHash {
		t.Fatal("decision evidence ignored")
	}
	candidates[0].Rejected = true
	candidates[0].RejectionGuard = "pds"
	rejected := snapshotTestBuild(t, policy, candidates, asOf)
	if rejected.Candidates[0].FeatureHash != later.Candidates[0].FeatureHash || rejected.Candidates[0].DecisionHash == later.Candidates[0].DecisionHash {
		t.Fatal("decision/feature boundary lost")
	}
	candidates[0].Lane.Weight++
	changed := snapshotTestBuild(t, policy, candidates, asOf)
	if changed.Candidates[0].FeatureHash == rejected.Candidates[0].FeatureHash || changed.CandidateSetHash != rejected.CandidateSetHash {
		t.Fatal("feature/identity boundary lost")
	}
	candidates[0].Seed.ID = "another-seed"
	changed = snapshotTestBuild(t, policy, candidates, asOf)
	if changed.CandidateSetHash == rejected.CandidateSetHash {
		t.Fatal("source identity ignored")
	}
}

func TestBuildCandidateSnapshotsDetachedInputs(t *testing.T) {
	channel := ChannelProfileRow{RiskPolicyJSON: map[string]any{"nested": map[string]any{"allowed": true}}}
	policy, err := BuildBaselinePolicy(channel, snapshotTestCommit)
	if err != nil {
		t.Fatal(err)
	}
	beforePolicy, _ := json.Marshal(policy)
	channel.RiskPolicyJSON["nested"].(map[string]any)["allowed"] = false
	afterPolicy, _ := json.Marshal(policy)
	if string(beforePolicy) != string(afterPolicy) {
		t.Fatal("policy aliases channel input")
	}
	candidates := snapshotTestCandidates()
	before, _ := json.Marshal(candidates)
	set := snapshotTestBuild(t, policy, candidates, time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC))
	after, _ := json.Marshal(candidates)
	if string(before) != string(after) {
		t.Fatal("builder mutated inputs")
	}
	if !reflect.DeepEqual(candidates, snapshotTestCandidates()) {
		t.Fatal("builder changed input types or values")
	}
	snapshotBefore, _ := json.Marshal(set)
	candidates[0].ConstraintsJSON["nested"].(map[string]any)["a"] = 99
	candidates[0].SourcePlatformsJSON[0] = "changed"
	candidates[0].Lane.ID = "changed"
	candidates[0].GuardResultsJSON[0]["verdict"] = "changed"
	snapshotAfter, _ := json.Marshal(set)
	if string(snapshotBefore) != string(snapshotAfter) {
		t.Fatal("snapshots alias candidate inputs")
	}
}

func TestBuildCandidateSnapshotsExactReplay(t *testing.T) {
	// Independently calculated from the fixture's canonical JSON with Ruby's
	// JSON and Digest::SHA256, not from the Go builders or their hash helper.
	policy, err := BuildBaselinePolicy(ChannelProfileRow{}, snapshotTestCommit)
	if err != nil {
		t.Fatal(err)
	}
	asOf := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	set := snapshotTestBuild(t, policy, []TickCandidate{{CandidateID: "fixture", Source: SourceManualSeed}}, asOf)
	for name, pair := range map[string][2]string{
		"config":        {policy.ConfigHash, "e689027ab29b8967c2951a355e2d78e7e7661257aaf3ce1036781d144fb5068e"},
		"candidate_set": {set.CandidateSetHash, "efa3b13900cb7b4301a8d51d16c439427a1f5738ea9520545719650115a1b89d"},
		"feature":       {set.Candidates[0].FeatureHash, "4259932a097c4017c7a6284149c734116f666fc5c880e0fbac4ca10b72cf4feb"},
		"decision":      {set.Candidates[0].DecisionHash, "2d3fb69502dece7ab3d0458ee9820c9be57ee0ba9cc63c94318106b644cf7c1c"},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s hash = %s, want %s", name, pair[0], pair[1])
		}
	}
}

func TestBuildCandidateSnapshotsCanonicalEvidenceAndOwnedReferences(t *testing.T) {
	policy := snapshotTestPolicy(t)
	asOf := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	candidate := TickCandidate{CandidateID: "owned", Source: SourceManualSeed,
		owned:           &ownedCandidateAuthority{InventoryID: "inventory", ItemID: "item", ManifestSHA: "manifest", AssetID: "asset"},
		ConstraintsJSON: map[string]any{"nested": map[string]any{"z": 2, "a": 1}},
		PDSDecisionJSON: map[string]any{"metadata": map[string]any{"z": 2, "a": 1}},
	}
	first := snapshotTestBuild(t, policy, []TickCandidate{candidate}, asOf)
	if first.Candidates[0].SourceRecordRefsJSON["owned_item_id"] != "item" || first.Candidates[0].MissingFeatureMaskJSON["source_record_id"] {
		t.Fatal("owned source identity lost")
	}
	candidate.ConstraintsJSON = map[string]any{"nested": map[string]int{"a": 1, "z": 2}}
	candidate.PDSDecisionJSON = map[string]any{"metadata": map[string]int{"a": 1, "z": 2}}
	replay := snapshotTestBuild(t, policy, []TickCandidate{candidate}, asOf.Add(time.Hour))
	if first.Candidates[0].FeatureHash != replay.Candidates[0].FeatureHash || first.Candidates[0].DecisionHash != replay.Candidates[0].DecisionHash {
		t.Fatal("map insertion order or asOf changed replay hashes")
	}
	candidate.owned.ItemID = "another-item"
	changed := snapshotTestBuild(t, policy, []TickCandidate{candidate}, asOf)
	if changed.CandidateSetHash == first.CandidateSetHash {
		t.Fatal("owned source change not bound to candidate set")
	}
}

func TestBuildCandidateSnapshotsConcurrentReplay(t *testing.T) {
	policy, candidates := snapshotTestPolicy(t), snapshotTestCandidates()
	asOf := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	want := snapshotTestBuild(t, policy, candidates, asOf)
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			got, err := BuildCandidateSnapshots(policy, candidates, asOf)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Errorf("concurrent replay differs: %v", err)
			}
		}()
	}
	workers.Wait()
}

func TestBuildCandidateSnapshotsRefusesInvalidInput(t *testing.T) {
	policy := snapshotTestPolicy(t)
	asOf := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	for _, candidates := range [][]TickCandidate{{{CandidateID: "same"}, {CandidateID: "same"}}, {{CandidateID: ""}}, {{CandidateID: " "}}} {
		got, err := BuildCandidateSnapshots(policy, candidates, asOf)
		if err == nil || !reflect.DeepEqual(got, SnapshotSet{}) {
			t.Fatalf("invalid candidates produced partial facts: %#v, %v", got, err)
		}
	}
	for _, bad := range []any{math.NaN(), math.Inf(1), math.Inf(-1), json.Number("NaN"), json.Number(""), json.Number("01"), json.Number("1.2.3")} {
		channel := ChannelProfileRow{RiskPolicyJSON: map[string]any{"nested": []any{map[string]any{"bad": bad}}}}
		gotPolicy, err := BuildBaselinePolicy(channel, snapshotTestCommit)
		if err == nil || !reflect.DeepEqual(gotPolicy, PolicyVersion{}) {
			t.Fatalf("invalid number produced policy: %#v, %v", gotPolicy, err)
		}
		for _, field := range []string{"constraints", "score", "pds", "learning", "guard"} {
			candidates := snapshotTestCandidates()
			switch field {
			case "constraints":
				candidates[1].ConstraintsJSON = map[string]any{"bad": bad}
			case "score":
				candidates[1].ScoreJSON = map[string]any{"bad": bad}
			case "pds":
				candidates[1].PDSDecisionJSON = map[string]any{"bad": bad}
			case "learning":
				candidates[1].LearningContextJSON = map[string]any{"bad": bad}
			case "guard":
				candidates[1].GuardResultsJSON = []map[string]any{{"bad": bad}}
			}
			got, err := BuildCandidateSnapshots(policy, candidates, asOf)
			if err == nil || !reflect.DeepEqual(got, SnapshotSet{}) {
				t.Fatalf("%s invalid number produced partial facts: %#v, %v", field, got, err)
			}
		}
	}
	for _, invalid := range []PolicyVersion{{}, func() PolicyVersion { p := policy; p.ConfigHash = "forged"; return p }()} {
		if got, err := BuildCandidateSnapshots(invalid, nil, asOf); err == nil || !reflect.DeepEqual(got, SnapshotSet{}) {
			t.Fatalf("invalid policy accepted: %#v, %v", got, err)
		}
	}
	if got, err := BuildCandidateSnapshots(policy, nil, time.Time{}); err == nil || !reflect.DeepEqual(got, SnapshotSet{}) {
		t.Fatalf("missing asOf accepted: %#v, %v", got, err)
	}
	empty := snapshotTestBuild(t, policy, nil, asOf)
	if empty.Candidates == nil || len(empty.Candidates) != 0 || len(empty.CandidateSetHash) != 64 {
		t.Fatalf("empty set not explicit: %#v", empty)
	}
}
