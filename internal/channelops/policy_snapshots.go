package channelops

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

const (
	CandidateFeatureSchemaVersion = "channelops-candidate-v1"
	LegacyTemplateRegistryVersion = "legacy-unversioned"
	LegacyPromptBundleVersion     = "legacy-unversioned"
	LegacyRewardVersion           = "legacy-unversioned"
)

// BuildCommitSHA is reserved for exact binary identity injection in Task2b.
// Pure builders never read it; callers must supply an exact commit explicitly.
var BuildCommitSHA = "development"

// PolicyDefinition contains only semantic, hash-covered policy content.
type PolicyDefinition struct {
	PolicyKey               string         `json:"policy_key"`
	FeatureSchemaVersion    string         `json:"feature_schema_version"`
	RewardVersion           string         `json:"reward_version"`
	FormulaJSON             map[string]any `json:"formula_json"`
	HardGuardConfigJSON     map[string]any `json:"hard_guard_config_json"`
	PortfolioConfigJSON     map[string]any `json:"portfolio_config_json"`
	ExplorationConfigJSON   map[string]any `json:"exploration_config_json"`
	CodeCommitSHA           string         `json:"code_commit_sha"`
	TemplateRegistryVersion string         `json:"template_registry_version"`
	PromptBundleVersion     string         `json:"prompt_bundle_version"`
}

// PolicyVersion is an in-memory fact, not a persisted row or an activation.
type PolicyVersion struct {
	PolicyDefinition
	Version      string `json:"version"`
	ConfigHash   string `json:"config_hash"`
	Status       string `json:"status"`
	CreatedBy    string `json:"created_by"`
	ChangeReason string `json:"change_reason"`
}

type CandidateSnapshotIdentity struct {
	CandidateID          string            `json:"candidate_id"`
	Source               string            `json:"source"`
	SourceKind           string            `json:"source_kind"`
	LaneID               *string           `json:"lane_id"`
	FormatID             *string           `json:"format_id"`
	AccountID            *string           `json:"account_id"`
	SourceRecordRefsJSON map[string]string `json:"source_record_refs_json"`
}

// CandidateFeatureFacts deliberately excludes clock values and decision outputs.
// Nil summaries mean unavailable, not a computed zero or an eligibility claim.
type CandidateFeatureFacts struct {
	CandidateSnapshotIdentity
	FeatureSchemaVersion      string          `json:"feature_schema_version"`
	RawFeaturesJSON           map[string]any  `json:"raw_features_json"`
	NormalizedFeaturesJSON    map[string]any  `json:"normalized_features_json"`
	MissingFeatureMaskJSON    map[string]bool `json:"missing_feature_mask_json"`
	CadenceSnapshotJSON       map[string]any  `json:"cadence_snapshot_json"`
	ContentMixSnapshotJSON    map[string]any  `json:"content_mix_snapshot_json"`
	MaterialSupplyJSON        map[string]any  `json:"material_supply_json"`
	ProductionReliabilityJSON map[string]any  `json:"production_reliability_json"`
	LearningReferencesJSON    map[string]any  `json:"learning_references_json"`
	CostEstimateJSON          map[string]any  `json:"cost_estimate_json"`
	RiskEstimateJSON          map[string]any  `json:"risk_estimate_json"`
}

type CandidateDecisionFacts struct {
	Decision            string           `json:"decision"`
	RejectionGuard      string           `json:"rejection_guard"`
	RejectionReason     string           `json:"rejection_reason"`
	ScoreJSON           map[string]any   `json:"score_json"`
	GuardResultsJSON    []map[string]any `json:"guard_results_json"`
	PDSDecisionJSON     map[string]any   `json:"pds_decision_json"`
	PDSRequestJSON      map[string]any   `json:"pds_request_json"`
	LearningContextJSON map[string]any   `json:"learning_context_json"`
	BaselineScore       *float64         `json:"baseline_score"`
	FinalScore          *float64         `json:"final_score"`
	Rank                *int             `json:"rank"`
}

type CandidateSnapshot struct {
	CandidateFeatureFacts
	CandidateDecisionFacts
	PolicyVersion    string    `json:"policy_version"`
	FeatureAsOf      time.Time `json:"feature_as_of"`
	CandidateSetHash string    `json:"candidate_set_hash"`
	FeatureHash      string    `json:"feature_hash"`
	DecisionHash     string    `json:"decision_hash"`
}

type SnapshotSet struct {
	PolicyVersion    string              `json:"policy_version"`
	FeatureAsOf      time.Time           `json:"feature_as_of"`
	CandidateSetHash string              `json:"candidate_set_hash"`
	Candidates       []CandidateSnapshot `json:"candidates"`
}

// BuildBaselinePolicy refuses unverifiable code identities even for local calls.
// Status is draft: building facts does not validate or activate a live policy.
func BuildBaselinePolicy(channel ChannelProfileRow, codeCommitSHA string) (PolicyVersion, error) {
	if err := validateSnapshotCommit(codeCommitSHA); err != nil {
		return PolicyVersion{}, err
	}
	definition := PolicyDefinition{
		PolicyKey: "channelops-baseline", FeatureSchemaVersion: CandidateFeatureSchemaVersion,
		RewardVersion: LegacyRewardVersion, CodeCommitSHA: codeCommitSHA,
		TemplateRegistryVersion: LegacyTemplateRegistryVersion, PromptBundleVersion: LegacyPromptBundleVersion,
		FormulaJSON: map[string]any{
			"selection": "existing-candidate-order", "aggregate_score": nil, "learning_influence": false,
		},
		HardGuardConfigJSON: map[string]any{
			"risk_policy_json":      snapshotObject(channel.RiskPolicyJSON),
			"cadence_policy_json":   snapshotObject(channel.CadencePolicyJSON),
			"tick_interval_minutes": channel.TickIntervalMinutes,
		},
		PortfolioConfigJSON: map[string]any{
			"channel_profile_id": channel.ID, "config_version": channel.ConfigVersion,
			"content_mix_policy_json": snapshotObject(channel.ContentMixPolicyJSON),
			"default_aspect_ratio":    channel.DefaultAspectRatio,
			"owned_seed_inventory_id": channel.OwnedSeedInventoryID,
			"owned_inventory_active":  channel.OwnedInventoryActive,
		},
		ExplorationConfigJSON: map[string]any{"enabled": false},
	}
	// A JSON round trip both detaches nested inputs and preserves exact JSON numbers.
	if err := cloneSnapshotJSON(definition, &definition); err != nil {
		return PolicyVersion{}, fmt.Errorf("policy content: %w", err)
	}
	hash, err := canonicalPolicyHash(definition)
	if err != nil {
		return PolicyVersion{}, err
	}
	return PolicyVersion{
		PolicyDefinition: definition, Version: "sha256:" + hash, ConfigHash: hash,
		Status: "draft", CreatedBy: "channelops-baseline-builder", ChangeReason: "passive-policy-snapshot",
	}, nil
}

// BuildCandidateSnapshots returns either all facts or a zero set plus an error.
// It never selects candidates, creates tasks, or mutates caller-owned inputs.
func BuildCandidateSnapshots(policy PolicyVersion, candidates []TickCandidate, asOf time.Time) (SnapshotSet, error) {
	if err := validateSnapshotCommit(policy.CodeCommitSHA); err != nil {
		return SnapshotSet{}, err
	}
	hash, err := canonicalPolicyHash(policy.PolicyDefinition)
	if err != nil {
		return SnapshotSet{}, fmt.Errorf("policy content: %w", err)
	}
	if policy.PolicyKey != "channelops-baseline" || policy.FeatureSchemaVersion != CandidateFeatureSchemaVersion || policy.ConfigHash != hash || policy.Version != "sha256:"+hash {
		return SnapshotSet{}, errors.New("policy identity or content hash mismatch")
	}
	if asOf.IsZero() {
		return SnapshotSet{}, errors.New("feature asOf is required")
	}
	asOf = asOf.UTC().Round(0)
	if _, err := json.Marshal(asOf); err != nil {
		return SnapshotSet{}, fmt.Errorf("feature asOf: %w", err)
	}
	set := SnapshotSet{PolicyVersion: policy.Version, FeatureAsOf: asOf, Candidates: make([]CandidateSnapshot, 0, len(candidates))}
	identities := make([]CandidateSnapshotIdentity, 0, len(candidates))
	seen := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.CandidateID) == "" || seen[candidate.CandidateID] {
			return SnapshotSet{}, errors.New("candidate IDs must be nonempty and unique")
		}
		seen[candidate.CandidateID] = true
		// Validate all supplied JSON, including evidence excluded from feature hashes.
		if _, err := canonicalSnapshotJSON(candidate); err != nil {
			return SnapshotSet{}, fmt.Errorf("candidate %q: %w", candidate.CandidateID, err)
		}
		features := buildCandidateFeatureFacts(candidate)
		decision := "accepted"
		if candidate.Rejected {
			decision = "rejected"
		}
		snapshot := CandidateSnapshot{
			CandidateFeatureFacts: features,
			CandidateDecisionFacts: CandidateDecisionFacts{
				Decision: decision, RejectionGuard: candidate.RejectionGuard, RejectionReason: candidate.RejectionReason,
				ScoreJSON: snapshotObject(candidate.ScoreJSON), GuardResultsJSON: candidateGuardResultsJSON(candidate),
				PDSDecisionJSON: snapshotObject(candidate.PDSDecisionJSON), PDSRequestJSON: snapshotObject(candidate.PDSRequestJSON),
				LearningContextJSON: snapshotObject(candidate.LearningContextJSON),
			},
			PolicyVersion: policy.Version, FeatureAsOf: asOf,
		}
		if err := cloneSnapshotJSON(snapshot, &snapshot); err != nil {
			return SnapshotSet{}, fmt.Errorf("candidate %q facts: %w", candidate.CandidateID, err)
		}
		snapshot.FeatureHash, err = canonicalPolicyHash(snapshot.CandidateFeatureFacts)
		if err != nil {
			return SnapshotSet{}, err
		}
		identities = append(identities, snapshot.CandidateSnapshotIdentity)
		set.Candidates = append(set.Candidates, snapshot)
	}
	// Sort only private identities. Returned candidates retain the caller's order.
	sort.Slice(identities, func(i, j int) bool { return identities[i].CandidateID < identities[j].CandidateID })
	set.CandidateSetHash, err = canonicalPolicyHash(identities)
	if err != nil {
		return SnapshotSet{}, err
	}
	for i := range set.Candidates {
		snapshot := &set.Candidates[i]
		snapshot.CandidateSetHash = set.CandidateSetHash
		snapshot.DecisionHash, err = canonicalPolicyHash(struct {
			PolicyVersion    string `json:"policy_version"`
			CandidateSetHash string `json:"candidate_set_hash"`
			FeatureHash      string `json:"feature_hash"`
			CandidateDecisionFacts
		}{policy.Version, set.CandidateSetHash, snapshot.FeatureHash, snapshot.CandidateDecisionFacts})
		if err != nil {
			return SnapshotSet{}, err
		}
	}
	return set, nil
}

func buildCandidateFeatureFacts(candidate TickCandidate) CandidateFeatureFacts {
	identity := CandidateSnapshotIdentity{
		CandidateID: candidate.CandidateID, Source: candidate.Source, SourceKind: candidateSource(candidate),
		SourceRecordRefsJSON: map[string]string{},
	}
	raw := map[string]any{
		"prompt": candidate.Prompt, "title_seed": candidate.TitleSeed,
		"source_platforms_json": candidate.SourcePlatformsJSON, "material_library_ids_json": candidate.MaterialLibraryIDsJSON,
		"constraints_json": snapshotObject(candidate.ConstraintsJSON), "manual_material_override": candidate.ManualMaterialOverride,
	}
	if lane := candidate.Lane; lane != nil {
		identity.LaneID = snapshotID(lane.ID)
		raw["lane"] = map[string]any{
			"name": lane.Name, "description": lane.Description, "keywords_json": lane.KeywordsJSON,
			"enabled": lane.Enabled, "weight": lane.Weight, "max_posts_per_day": lane.MaxPostsPerDay,
			"cooldown_after_post_min": lane.CooldownAfterPostMin, "max_consecutive_streak": lane.MaxConsecutiveStreak,
		}
	}
	if format := candidate.LaneFormat; format != nil {
		identity.FormatID = snapshotID(format.ID)
		raw["format"] = map[string]any{
			"format_key": format.FormatKey, "enabled": format.Enabled, "weight": format.Weight,
			"target_duration_sec": format.TargetDurationSec, "default_publish_visibility": format.DefaultPublishVisibility,
			"template_pool_json": format.TemplatePoolJSON, "source_platforms_json": format.SourcePlatformsJSON,
		}
	}
	if account := candidate.Account; account != nil {
		identity.AccountID = snapshotID(account.ID)
		raw["account"] = map[string]any{
			"platform": account.Platform, "enabled": account.Enabled,
			"default_privacy": account.DefaultPrivacy, "external_auto_publish": account.ExternalAutoPublish,
		}
	}
	if seed := candidate.Seed; seed != nil {
		if snapshotID(seed.ID) != nil {
			identity.SourceRecordRefsJSON["manual_seed_id"] = seed.ID
		}
		raw["source_policy"] = seed.SourcePolicy
	}
	if signal := candidate.DiscoverySignal; signal != nil {
		if snapshotID(signal.ID) != nil {
			identity.SourceRecordRefsJSON["discovery_signal_id"] = signal.ID
		}
		if snapshotID(signal.SourceExternalID) != nil {
			identity.SourceRecordRefsJSON["source_external_id"] = signal.SourceExternalID
		}
		raw["discovery"] = map[string]any{"trend_score": signal.TrendScore, "novelty_score": signal.NoveltyScore, "keywords_json": signal.KeywordsJSON}
	}
	if owned := candidate.owned; owned != nil {
		for key, value := range map[string]string{"owned_inventory_id": owned.InventoryID, "owned_item_id": owned.ItemID, "asset_id": owned.AssetID, "manifest_sha": owned.ManifestSHA, "content_sha": owned.ContentSHA, "seed_sha": owned.SeedSHA} {
			if snapshotID(value) != nil {
				identity.SourceRecordRefsJSON[key] = value
			}
		}
	}
	// Only pre-decision score components belong to v1 features. Arbitrary score,
	// learning, PDS, and metrics payloads remain decision evidence, never features.
	components := map[string]any{}
	for _, key := range []string{"lane_weight", "format_weight", "trend_score", "novelty_score"} {
		if value, ok := candidate.ScoreJSON[key]; ok {
			components[key] = value
		}
	}
	raw["score_components"] = components
	missing := map[string]bool{
		"lane_id": identity.LaneID == nil, "format_id": identity.FormatID == nil, "account_id": identity.AccountID == nil,
		"source_record_id": identity.SourceRecordRefsJSON["manual_seed_id"] == "" && identity.SourceRecordRefsJSON["discovery_signal_id"] == "" && identity.SourceRecordRefsJSON["owned_item_id"] == "",
		"source":           strings.TrimSpace(candidate.Source) == "", "source_platforms": candidate.SourcePlatformsJSON == nil,
		"material_library_ids": candidate.MaterialLibraryIDsJSON == nil,
	}
	for _, key := range []string{"baseline_score", "final_score", "rank", "exposure", "reward", "eligibility", "normalized_features", "cadence", "content_mix", "material_supply", "production_reliability", "learning_references", "cost_estimate", "risk_estimate"} {
		missing[key] = true
	}
	return CandidateFeatureFacts{CandidateSnapshotIdentity: identity, FeatureSchemaVersion: CandidateFeatureSchemaVersion, RawFeaturesJSON: raw, MissingFeatureMaskJSON: missing}
}

func snapshotID(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}

func snapshotObject(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}

func validateSnapshotCommit(value string) error {
	if len(value) != 40 {
		return errors.New("policy requires an exact 40-character hexadecimal commit SHA")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return errors.New("policy requires an exact 40-character hexadecimal commit SHA")
	}
	return nil
}

func canonicalPolicyHash(value any) (string, error) {
	data, err := canonicalSnapshotJSON(value)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

func canonicalSnapshotJSON(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	// encoding/json treats the invalid empty json.Number as zero. Refuse it
	// explicitly, after Marshal has rejected cycles and other invalid values.
	if err := rejectEmptySnapshotNumbers(reflect.ValueOf(value), 0); err != nil {
		return nil, err
	}
	var normalized any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}

func cloneSnapshotJSON[T any](value T, target *T) error {
	data, err := canonicalSnapshotJSON(value)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var detached T
	if err := decoder.Decode(&detached); err != nil {
		return err
	}
	*target = detached
	return nil
}

func rejectEmptySnapshotNumbers(value reflect.Value, depth int) error {
	if !value.IsValid() {
		return nil
	}
	if depth > 1000 {
		return errors.New("snapshot JSON nesting exceeds limit")
	}
	if value.Type() == reflect.TypeFor[json.Number]() && value.String() == "" {
		return errors.New("empty JSON number")
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !value.IsNil() {
			return rejectEmptySnapshotNumbers(value.Elem(), depth+1)
		}
	case reflect.Map:
		iter := value.MapRange()
		for iter.Next() {
			if err := rejectEmptySnapshotNumbers(iter.Value(), depth+1); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := rejectEmptySnapshotNumbers(value.Index(i), depth+1); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if field.IsExported() && field.Tag.Get("json") != "-" {
				if err := rejectEmptySnapshotNumbers(value.Field(i), depth+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
