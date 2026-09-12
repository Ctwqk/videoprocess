package channelops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ResolvePolicyVersion must use the caller's fenced transaction. Conflicts are
// read and verified, never updated: policy rows are immutable from insertion.
func ResolvePolicyVersion(ctx context.Context, db dbExecutor, policy PolicyVersion) (string, error) {
	if err := validateSnapshotCommit(policy.CodeCommitSHA); err != nil {
		return "", err
	}
	hash, err := canonicalPolicyHash(policy.PolicyDefinition)
	if err != nil {
		return "", err
	}
	if policy.ConfigHash != hash || policy.Version != "sha256:"+hash || policy.PolicyKey != "channelops-baseline" || policy.FeatureSchemaVersion != CandidateFeatureSchemaVersion {
		return "", errors.New("invalid policy identity or hash")
	}
	raw, err := canonicalSnapshotJSON(policy)
	if err != nil {
		return "", err
	}
	_, err = db.Exec(ctx, `
  INSERT INTO decision_policy_versions (
   id,policy_key,version,status,feature_schema_version,reward_version,formula_json,
   hard_guard_config_json,portfolio_config_json,exploration_config_json,code_commit_sha,
   template_registry_version,prompt_bundle_version,config_hash,created_by,change_reason)
  SELECT gen_random_uuid(),p.policy_key,p.version,p.status,p.feature_schema_version,p.reward_version,p.formula_json,
   p.hard_guard_config_json,p.portfolio_config_json,p.exploration_config_json,p.code_commit_sha,
   p.template_registry_version,p.prompt_bundle_version,p.config_hash,p.created_by,p.change_reason
  FROM json_populate_record(NULL::decision_policy_versions,$1::json) p
  ON CONFLICT (policy_key,version) DO NOTHING`, raw)
	if err != nil {
		return "", err
	}
	var id string
	var storedJSON []byte
	if err := db.QueryRow(ctx, `SELECT id::text,row_to_json(p) FROM decision_policy_versions p WHERE policy_key=$1 AND version=$2`, policy.PolicyKey, policy.Version).Scan(&id, &storedJSON); err != nil {
		return "", err
	}
	var stored PolicyVersion
	decoder := json.NewDecoder(bytes.NewReader(storedJSON))
	decoder.UseNumber()
	if err := decoder.Decode(&stored); err != nil {
		return "", err
	}
	actual, err := canonicalSnapshotJSON(stored)
	if err != nil {
		return "", err
	}
	if id == "" || !bytes.Equal(actual, raw) {
		return "", errors.New("immutable policy version content conflict")
	}
	return id, nil
}

func snapshotDecisionHash(snapshot CandidateSnapshot) (string, error) {
	return canonicalPolicyHash(struct {
		PolicyVersion    string `json:"policy_version"`
		CandidateSetHash string `json:"candidate_set_hash"`
		FeatureHash      string `json:"feature_hash"`
		CandidateDecisionFacts
	}{snapshot.PolicyVersion, snapshot.CandidateSetHash, snapshot.FeatureHash, snapshot.CandidateDecisionFacts})
}

func validateSnapshotSet(set SnapshotSet) error {
	if set.FeatureAsOf.IsZero() || set.PolicyVersion == "" {
		return errors.New("missing snapshot identity")
	}
	seen := map[string]bool{}
	identities := make([]CandidateSnapshotIdentity, 0, len(set.Candidates))
	for _, snapshot := range set.Candidates {
		if strings.TrimSpace(snapshot.CandidateID) == "" || seen[snapshot.CandidateID] {
			return errors.New("duplicate or missing candidate identity")
		}
		seen[snapshot.CandidateID] = true
		if snapshot.PolicyVersion != set.PolicyVersion || !snapshot.FeatureAsOf.Equal(set.FeatureAsOf) || snapshot.CandidateSetHash != set.CandidateSetHash || snapshot.FeatureSchemaVersion != CandidateFeatureSchemaVersion {
			return errors.New("snapshot set identity mismatch")
		}
		hash, err := canonicalPolicyHash(snapshot.CandidateFeatureFacts)
		if err != nil {
			return err
		}
		if hash != snapshot.FeatureHash {
			return errors.New("snapshot feature hash mismatch")
		}
		hash, err = snapshotDecisionHash(snapshot)
		if err != nil {
			return err
		}
		if hash != snapshot.DecisionHash || (snapshot.Decision != "accepted" && snapshot.Decision != "rejected") {
			return errors.New("snapshot decision hash mismatch")
		}
		identities = append(identities, snapshot.CandidateSnapshotIdentity)
	}
	sort.Slice(identities, func(i, j int) bool { return identities[i].CandidateID < identities[j].CandidateID })
	hash, err := canonicalPolicyHash(identities)
	if err != nil {
		return err
	}
	if hash != set.CandidateSetHash {
		return errors.New("candidate set hash mismatch")
	}
	return nil
}

func InsertCandidateSnapshots(ctx context.Context, db dbExecutor, tickAuditID, policyID string, snapshots SnapshotSet) (map[string]string, error) {
	if tickAuditID == "" || policyID == "" {
		return nil, errors.New("missing snapshot parent")
	}
	if err := validateSnapshotSet(snapshots); err != nil {
		return nil, err
	}
	ids := make(map[string]string, len(snapshots.Candidates))
	for _, snapshot := range snapshots.Candidates {
		raw, err := canonicalSnapshotJSON(struct {
			CandidateSnapshot
			CandidateSource string  `json:"candidate_source"`
			TopicLaneID     *string `json:"topic_lane_id"`
			LaneFormatID    *string `json:"lane_format_id"`
			TargetAccountID *string `json:"target_account_id"`
		}{snapshot, snapshot.Source, snapshot.LaneID, snapshot.FormatID, snapshot.AccountID})
		if err != nil {
			return nil, err
		}
		var id string
		err = db.QueryRow(ctx, `
   INSERT INTO candidate_feature_snapshots (
    id,tick_audit_id,policy_version_id,candidate_id,candidate_source,source_kind,
    topic_lane_id,lane_format_id,target_account_id,feature_schema_version,feature_as_of,
    raw_features_json,normalized_features_json,missing_feature_mask_json,cadence_snapshot_json,
    content_mix_snapshot_json,material_supply_json,production_reliability_json,learning_references_json,
    source_record_refs_json,cost_estimate_json,risk_estimate_json,candidate_set_hash,feature_hash)
   SELECT gen_random_uuid(),$1::uuid,$2::uuid,p.candidate_id,p.candidate_source,p.source_kind,
    p.topic_lane_id,p.lane_format_id,p.target_account_id,p.feature_schema_version,p.feature_as_of,
    p.raw_features_json,p.normalized_features_json,p.missing_feature_mask_json,p.cadence_snapshot_json,
    p.content_mix_snapshot_json,p.material_supply_json,p.production_reliability_json,p.learning_references_json,
    p.source_record_refs_json,p.cost_estimate_json,p.risk_estimate_json,p.candidate_set_hash,p.feature_hash
   FROM json_populate_record(NULL::candidate_feature_snapshots,$3::json) p
   RETURNING id::text`, tickAuditID, policyID, raw).Scan(&id)
		if err != nil {
			return nil, err
		}
		if id == "" {
			return nil, errors.New("missing inserted snapshot ID")
		}
		for _, existing := range ids {
			if existing == id {
				return nil, errors.New("duplicate snapshot ID")
			}
		}
		ids[snapshot.CandidateID] = id
	}
	return ids, nil
}

func (s *Store) captureTickFacts(p tickPreparation) (tickPreparation, error) {
	policy, err := BuildBaselinePolicy(p.Channel, s.buildCommitSHA)
	if err != nil {
		return tickPreparation{}, err
	}
	// JSON timestamps and pgx binary timestamps must carry the same PG precision.
	features, err := BuildCandidateSnapshots(policy, p.Candidates, p.Now.Truncate(time.Microsecond))
	if err != nil {
		return tickPreparation{}, err
	}
	p.Policy, p.Features = policy, features
	return p, nil
}

// Decisions may change at revalidation; their feature facts must still describe
// the original preparation, not a newly loaded channel or mutated candidate.
func (p tickPreparation) decisionSnapshots(result TickResult) (SnapshotSet, error) {
	if p.Features.PolicyVersion != p.Policy.Version || !p.Features.FeatureAsOf.Equal(p.Now.Truncate(time.Microsecond)) {
		return SnapshotSet{}, errors.New("snapshot preparation policy or asOf mismatch")
	}
	candidates := append(append([]TickCandidate(nil), result.Accepted...), result.Rejected...)
	final, err := BuildCandidateSnapshots(p.Policy, candidates, p.Features.FeatureAsOf)
	if err != nil {
		return SnapshotSet{}, err
	}
	if err := validateSnapshotSet(p.Features); err != nil {
		return SnapshotSet{}, err
	}
	if len(final.Candidates) != len(p.Features.Candidates) {
		return SnapshotSet{}, errors.New("decision candidate cardinality mismatch")
	}
	original := map[string]CandidateSnapshot{}
	for _, snapshot := range p.Features.Candidates {
		original[snapshot.CandidateID] = snapshot
	}
	final.CandidateSetHash = p.Features.CandidateSetHash
	for i := range final.Candidates {
		snapshot := &final.Candidates[i]
		source, ok := original[snapshot.CandidateID]
		if !ok {
			return SnapshotSet{}, errors.New("decision candidate missing preparation facts")
		}
		snapshot.CandidateFeatureFacts = source.CandidateFeatureFacts
		snapshot.FeatureHash = source.FeatureHash
		snapshot.CandidateSetHash = final.CandidateSetHash
		snapshot.DecisionHash, err = snapshotDecisionHash(*snapshot)
		if err != nil {
			return SnapshotSet{}, err
		}
	}
	return final, nil
}

type snapshotAuditWrite struct {
	tickID, policyID         string
	snapshots                SnapshotSet
	snapshotIDs, decisionIDs map[string]string
	expectedTasks            int
	replay                   bool
}

func (s *Store) beginSnapshotAudit(ctx context.Context, p tickPreparation, result TickResult, summary map[string]any) (snapshotAuditWrite, error) {
	summary = jsonObject(summary)
	set, err := p.decisionSnapshots(result)
	if err != nil {
		return snapshotAuditWrite{}, err
	}
	policyID, err := ResolvePolicyVersion(ctx, s.db(), p.Policy)
	if err != nil {
		return snapshotAuditWrite{}, err
	}
	w := snapshotAuditWrite{policyID: policyID, snapshots: set, expectedTasks: result.TasksToCreate()}
	w.tickID, err = s.insertTickAuditWithSnapshots(ctx, s.db(), p.ChannelID, p.Bucket, result, summary, &w)
	if errors.Is(err, pgx.ErrNoRows) {
		// An exact retry is a no-op. A new asOf or changed decision is a conflict,
		// including legacy history: never reconstruct old facts from current inputs.
		var exact bool
		raw, jsonErr := canonicalSnapshotJSON(set.Candidates)
		if jsonErr != nil {
			return w, jsonErr
		}
		summaryJSON, jsonErr := canonicalSnapshotJSON(summary)
		if jsonErr != nil {
			return w, jsonErr
		}
		guardsJSON, jsonErr := canonicalSnapshotJSON(candidateGuardSummaries(result.Rejected))
		if jsonErr != nil {
			return w, jsonErr
		}
		err = s.db().QueryRow(ctx, `
   SELECT a.id::text, a.replay_status='snapshot_complete' AND a.policy_version_id=$3::uuid
    AND a.candidate_set_hash=$4 AND a.feature_as_of=$5 AND a.dry_run=$6
    AND a.tasks_selected=$7 AND a.tasks_rejected=$8 AND a.candidates_scored=$9
    AND a.decision_summary_json::jsonb=$11::jsonb AND a.guards_triggered_json::jsonb=$12::jsonb
    AND (SELECT count(*) FROM candidate_feature_snapshots f WHERE f.tick_audit_id=a.id)=$9
    AND (SELECT count(*) FROM decision_audit_entries d WHERE d.tick_audit_id=a.id)=$9
    AND (SELECT count(*) FROM decision_audit_entries d WHERE d.tick_audit_id=a.id AND d.created_task_id IS NOT NULL)=$7
    AND (SELECT count(*) FROM jsonb_to_recordset($10::jsonb) x(candidate_id text,feature_hash text,decision_hash text)
      JOIN candidate_feature_snapshots f ON f.tick_audit_id=a.id AND f.candidate_id=x.candidate_id AND f.feature_hash=x.feature_hash
      JOIN decision_audit_entries d ON d.tick_audit_id=a.id AND d.candidate_id=x.candidate_id
       AND d.feature_snapshot_id=f.id AND d.policy_version_id=$3::uuid AND d.decision_hash=x.decision_hash
      WHERE f.policy_version_id=$3::uuid AND f.candidate_set_hash=$4 AND f.feature_as_of=$5
       AND (d.created_task_id IS NOT NULL)=(d.selected AND NOT a.dry_run)
       AND (d.created_task_id IS NULL OR EXISTS(SELECT 1 FROM production_tasks t
         WHERE t.id=d.created_task_id AND t.channel_profile_id=a.channel_profile_id AND t.rationale_json->>'candidate_id'=d.candidate_id)))=$9
   FROM agent_tick_audits a WHERE a.channel_profile_id=$1::uuid AND a.tick_id=$2`, p.ChannelID, fmt.Sprintf("tick:%s:%s", p.ChannelID, p.Bucket), policyID, set.CandidateSetHash, set.FeatureAsOf, result.DryRun, result.TasksToCreate(), len(result.Rejected), len(set.Candidates), raw, summaryJSON, guardsJSON).Scan(&w.tickID, &exact)
		if err != nil {
			return w, err
		}
		if !exact {
			return w, errors.New("immutable tick snapshot conflict")
		}
		w.replay = true
		return w, nil
	}
	if err != nil {
		return w, err
	}
	w.snapshotIDs, err = InsertCandidateSnapshots(ctx, s.db(), w.tickID, policyID, set)
	if err != nil {
		return w, err
	}
	w.decisionIDs, err = s.insertDecisionAuditEntries(ctx, s.db(), w.tickID, p.ChannelID, result)
	return w, err
}

func (w snapshotAuditWrite) complete(ctx context.Context, db dbExecutor) error {
	if w.replay {
		return nil
	}
	if err := validateSnapshotSet(w.snapshots); err != nil {
		return err
	}
	if len(w.snapshotIDs) != len(w.snapshots.Candidates) || len(w.decisionIDs) != len(w.snapshots.Candidates) {
		return errors.New("snapshot link cardinality mismatch")
	}
	for _, snapshot := range w.snapshots.Candidates {
		snapshotID, decisionID := w.snapshotIDs[snapshot.CandidateID], w.decisionIDs[snapshot.CandidateID]
		if snapshotID == "" || decisionID == "" {
			return errors.New("missing candidate snapshot or decision link")
		}
		tag, err := db.Exec(ctx, `UPDATE decision_audit_entries
   SET feature_snapshot_id = $2::uuid,policy_version_id=$3::uuid,candidate_set_hash=$4,
    decision_hash=$5,decision=$6,baseline_score=$7,final_score=$8,rank=$9
   WHERE id=$1::uuid AND tick_audit_id=$10::uuid AND candidate_id=$11 AND feature_snapshot_id IS NULL`,
			decisionID, snapshotID, w.policyID, w.snapshots.CandidateSetHash, snapshot.DecisionHash, snapshot.Decision, snapshot.BaselineScore, snapshot.FinalScore, snapshot.Rank, w.tickID, snapshot.CandidateID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return errors.New("decision snapshot link not sealed")
		}
	}
	tag, err := db.Exec(ctx, `UPDATE agent_tick_audits a SET replay_status='snapshot_complete'
  WHERE a.id=$1::uuid AND a.replay_status='snapshot_pending' AND a.policy_version_id=$2::uuid
   AND a.candidate_set_hash=$3 AND a.feature_as_of=$4 AND a.candidates_scored=$5
   AND (SELECT count(*) FROM candidate_feature_snapshots f WHERE f.tick_audit_id=a.id)=$5
   AND (SELECT count(*) FROM decision_audit_entries d WHERE d.tick_audit_id=a.id)=$5
   AND (SELECT count(DISTINCT d.feature_snapshot_id) FROM decision_audit_entries d
     JOIN candidate_feature_snapshots f ON f.id=d.feature_snapshot_id AND f.tick_audit_id=d.tick_audit_id AND f.candidate_id=d.candidate_id
     WHERE d.tick_audit_id=a.id AND d.policy_version_id=$2::uuid AND f.policy_version_id=$2::uuid
      AND d.candidate_set_hash=$3 AND f.candidate_set_hash=$3 AND f.feature_as_of=$4
      AND d.decision_hash IS NOT NULL AND d.selected=(d.decision='accepted')
      AND (d.created_task_id IS NOT NULL)=(d.selected AND NOT a.dry_run)
      AND (d.created_task_id IS NULL OR EXISTS(SELECT 1 FROM production_tasks t
        WHERE t.id=d.created_task_id AND t.channel_profile_id=a.channel_profile_id AND t.rationale_json->>'candidate_id'=d.candidate_id)))=$5
   AND (SELECT count(*) FROM decision_audit_entries d WHERE d.tick_audit_id=a.id AND d.created_task_id IS NOT NULL)=$6`,
		w.tickID, w.policyID, w.snapshots.CandidateSetHash, w.snapshots.FeatureAsOf, len(w.snapshots.Candidates), w.expectedTasks)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("snapshot completion cardinality mismatch")
	}
	return nil
}
