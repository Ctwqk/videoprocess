package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func (s *Store) LoadTickInputs(ctx context.Context, channelID string, now time.Time) (ChannelProfileRow, []TopicLaneRow, []PublishingAccountRow, []ManualSeedRow, []DiscoverySignalRow, map[string][]LaneFormatRow, error) {
	channel, err := s.GetChannelProfile(ctx, channelID)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, nil, err
	}
	lanes, err := s.ListActiveLanes(ctx, channelID, now)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, nil, err
	}
	accounts, err := s.ListActiveAccounts(ctx, channelID, now)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, nil, err
	}
	seeds, err := s.ListActiveManualSeeds(ctx, channelID)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, nil, err
	}
	signals, err := s.ListActiveDiscoverySignals(ctx, channelID, now)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, nil, err
	}
	formats, err := s.ListLaneFormats(ctx, lanes)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, nil, err
	}
	return channel, lanes, accounts, seeds, signals, formats, nil
}

func (s *Store) GetChannelProfile(ctx context.Context, channelID string) (ChannelProfileRow, error) {
	var row ChannelProfileRow
	var riskJSON, cadenceJSON, contentMixJSON []byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, enabled, dry_run, halted_at, tick_interval_minutes, config_version,
	       risk_policy_json, cadence_policy_json, content_mix_policy_json,
	       default_aspect_ratio, created_at, updated_at
	FROM channel_profiles
	WHERE id = $1::uuid
	`, channelID).Scan(
		&row.ID,
		&row.Enabled,
		&row.DryRun,
		&row.HaltedAt,
		&row.TickIntervalMinutes,
		&row.ConfigVersion,
		&riskJSON,
		&cadenceJSON,
		&contentMixJSON,
		&row.DefaultAspectRatio,
		&row.CreatedAt,
		&row.UpdatedAt,
	)
	if err != nil {
		return ChannelProfileRow{}, err
	}
	if err := unmarshalJSONObject(riskJSON, &row.RiskPolicyJSON); err != nil {
		return ChannelProfileRow{}, fmt.Errorf("scan channel risk_policy_json: %w", err)
	}
	if err := unmarshalJSONObject(cadenceJSON, &row.CadencePolicyJSON); err != nil {
		return ChannelProfileRow{}, fmt.Errorf("scan channel cadence_policy_json: %w", err)
	}
	if err := unmarshalJSONObject(contentMixJSON, &row.ContentMixPolicyJSON); err != nil {
		return ChannelProfileRow{}, fmt.Errorf("scan channel content_mix_policy_json: %w", err)
	}
	return row, nil
}

func (s *Store) ListActiveLanes(ctx context.Context, channelID string, now time.Time) ([]TopicLaneRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, channel_profile_id, name, description, keywords_json, enabled, paused_until,
		       weight, max_posts_per_day, cooldown_after_post_minutes, max_consecutive_streak,
		       created_at
	FROM topic_lanes
	WHERE channel_profile_id = $1::uuid
		  AND enabled = TRUE
		  AND (paused_until IS NULL OR paused_until <= $2)
		ORDER BY weight DESC, created_at ASC
	`, channelID, now.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []TopicLaneRow{}
	for rows.Next() {
		var row TopicLaneRow
		var keywordsJSON []byte
		if err := rows.Scan(
			&row.ID,
			&row.ChannelProfileID,
			&row.Name,
			&row.Description,
			&keywordsJSON,
			&row.Enabled,
			&row.PausedUntil,
			&row.Weight,
			&row.MaxPostsPerDay,
			&row.CooldownAfterPostMin,
			&row.MaxConsecutiveStreak,
			&row.CreatedAt,
		); err != nil {
			return nil, err
		}
		if err := unmarshalJSONStringSlice(keywordsJSON, &row.KeywordsJSON); err != nil {
			return nil, fmt.Errorf("scan topic_lanes.keywords_json: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) ListActiveAccounts(ctx context.Context, channelID string, now time.Time) ([]PublishingAccountRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, channel_profile_id, platform, account_label, platform_account_id,
		       enabled, paused_until, default_privacy, external_asset_auto_publish,
		       created_at
	FROM publishing_accounts
	WHERE channel_profile_id = $1::uuid
		  AND enabled = TRUE
		  AND (paused_until IS NULL OR paused_until <= $2)
		ORDER BY created_at ASC
	`, channelID, now.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []PublishingAccountRow{}
	for rows.Next() {
		var row PublishingAccountRow
		if err := rows.Scan(
			&row.ID,
			&row.ChannelProfileID,
			&row.Platform,
			&row.AccountLabel,
			&row.PlatformAccountID,
			&row.Enabled,
			&row.PausedUntil,
			&row.DefaultPrivacy,
			&row.ExternalAutoPublish,
			&row.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) ListActiveManualSeeds(ctx context.Context, channelID string) ([]ManualSeedRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, channel_profile_id, topic_lane_id, target_account_id, prompt, title_seed,
		       source_policy, source_platforms_json, material_library_ids_json,
		       constraints_json, status, created_at
	FROM manual_seeds
	WHERE channel_profile_id = $1::uuid
		  AND status = 'active'
		ORDER BY created_at ASC
	`, channelID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []ManualSeedRow{}
	for rows.Next() {
		var row ManualSeedRow
		var sourcePlatformsJSON, materialLibraryIDsJSON, constraintsJSON []byte
		if err := rows.Scan(
			&row.ID,
			&row.ChannelProfileID,
			&row.TopicLaneID,
			&row.TargetAccountID,
			&row.Prompt,
			&row.TitleSeed,
			&row.SourcePolicy,
			&sourcePlatformsJSON,
			&materialLibraryIDsJSON,
			&constraintsJSON,
			&row.Status,
			&row.CreatedAt,
		); err != nil {
			return nil, err
		}
		if err := unmarshalJSONStringSlice(sourcePlatformsJSON, &row.SourcePlatformsJSON); err != nil {
			return nil, fmt.Errorf("scan manual_seeds.source_platforms_json: %w", err)
		}
		if err := unmarshalJSONStringSlice(materialLibraryIDsJSON, &row.MaterialLibraryIDsJSON); err != nil {
			return nil, fmt.Errorf("scan manual_seeds.material_library_ids_json: %w", err)
		}
		if err := unmarshalJSONObject(constraintsJSON, &row.ConstraintsJSON); err != nil {
			return nil, fmt.Errorf("scan manual_seeds.constraints_json: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) ListActiveDiscoverySignals(ctx context.Context, channelID string, now time.Time) ([]DiscoverySignalRow, error) {
	rows, err := s.Pool.Query(ctx, `
		WITH ranked_signals AS (
			SELECT id, channel_profile_id, topic_lane_id, source, source_url, source_external_id,
			       title, summary, keywords_json, trend_score, novelty_score, raw_json, status,
			       expires_at, observed_at, created_at,
			       row_number() OVER (
			           PARTITION BY topic_lane_id
			           ORDER BY trend_score DESC, observed_at DESC, created_at ASC
			       ) AS lane_rank
			FROM discovery_signals
			WHERE channel_profile_id = $1::uuid
			  AND source = 'youtube_search'
			  AND status = 'active'
			  AND (expires_at IS NULL OR expires_at > $2::timestamptz)
		)
		SELECT id, channel_profile_id, topic_lane_id, source, source_url, source_external_id,
		       title, summary, keywords_json, trend_score, novelty_score, raw_json, status,
		       expires_at, observed_at, created_at
		FROM ranked_signals
		WHERE lane_rank <= 50
		ORDER BY trend_score DESC, observed_at DESC, created_at ASC
		LIMIT 250
	`, channelID, now.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []DiscoverySignalRow{}
	for rows.Next() {
		var row DiscoverySignalRow
		var keywordsJSON, rawJSON []byte
		if err := rows.Scan(
			&row.ID,
			&row.ChannelProfileID,
			&row.TopicLaneID,
			&row.Source,
			&row.SourceURL,
			&row.SourceExternalID,
			&row.Title,
			&row.Summary,
			&keywordsJSON,
			&row.TrendScore,
			&row.NoveltyScore,
			&rawJSON,
			&row.Status,
			&row.ExpiresAt,
			&row.ObservedAt,
			&row.CreatedAt,
		); err != nil {
			return nil, err
		}
		if err := unmarshalJSONStringSlice(keywordsJSON, &row.KeywordsJSON); err != nil {
			return nil, fmt.Errorf("scan discovery_signals.keywords_json: %w", err)
		}
		if err := unmarshalJSONObject(rawJSON, &row.RawJSON); err != nil {
			return nil, fmt.Errorf("scan discovery_signals.raw_json: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) ListLaneFormats(ctx context.Context, lanes []TopicLaneRow) (map[string][]LaneFormatRow, error) {
	result := map[string][]LaneFormatRow{}
	laneIDs := make([]string, 0, len(lanes))
	for _, lane := range lanes {
		result[lane.ID] = []LaneFormatRow{}
		laneIDs = append(laneIDs, lane.ID)
	}
	if len(laneIDs) == 0 {
		return result, nil
	}

	rows, err := s.Pool.Query(ctx, `
		SELECT id, topic_lane_id, format_key, enabled, weight, target_duration_sec,
		       default_publish_visibility, template_pool_json, source_platforms_json,
		       created_at
	FROM lane_format_matrix
	WHERE topic_lane_id::text = ANY($1::text[])
		  AND enabled = TRUE
		ORDER BY weight DESC, created_at ASC
	`, laneIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var row LaneFormatRow
		var templatePoolJSON, sourcePlatformsJSON []byte
		if err := rows.Scan(
			&row.ID,
			&row.TopicLaneID,
			&row.FormatKey,
			&row.Enabled,
			&row.Weight,
			&row.TargetDurationSec,
			&row.DefaultPublishVisibility,
			&templatePoolJSON,
			&sourcePlatformsJSON,
			&row.CreatedAt,
		); err != nil {
			return nil, err
		}
		if err := unmarshalJSONStringSlice(templatePoolJSON, &row.TemplatePoolJSON); err != nil {
			return nil, fmt.Errorf("scan lane_format_matrix.template_pool_json: %w", err)
		}
		if err := unmarshalJSONStringSlice(sourcePlatformsJSON, &row.SourcePlatformsJSON); err != nil {
			return nil, fmt.Errorf("scan lane_format_matrix.source_platforms_json: %w", err)
		}
		result[row.TopicLaneID] = append(result[row.TopicLaneID], row)
	}
	return result, rows.Err()
}

func (s *Store) InsertTickAudit(ctx context.Context, channelID string, bucket string, result TickResult, summary map[string]any) (string, error) {
	return s.insertTickAudit(ctx, s.Pool, channelID, bucket, result, summary)
}

func (s *Store) insertTickAudit(ctx context.Context, db dbExecutor, channelID string, bucket string, result TickResult, summary map[string]any) (string, error) {
	now := s.Now().UTC()
	tickID := fmt.Sprintf("tick:%s:%s", channelID, bucket)
	decisionSummary := jsonObject(summary)
	decisionSummary["accepted_candidates"] = candidateAuditSummaries(result.Accepted)
	decisionSummary["rejected_candidates"] = candidateAuditSummaries(result.Rejected)
	decisionSummary["tasks_to_create"] = result.TasksToCreate()

	summaryJSON, err := json.Marshal(decisionSummary)
	if err != nil {
		return "", err
	}
	guardsJSON, err := json.Marshal(candidateGuardSummaries(result.Rejected))
	if err != nil {
		return "", err
	}

	var id string
	err = db.QueryRow(ctx, `
		INSERT INTO agent_tick_audits (
			id, channel_profile_id, tick_id, started_at, finished_at, dry_run,
			ideas_discovered, candidates_scored, tasks_selected, tasks_rejected,
			guards_triggered_json, decision_summary_json, error_message
		)
		VALUES (gen_random_uuid(), $1::uuid, $2, $3, $4, $5, $6, $6, $7, $8, $9::json, $10::json, NULL)
		ON CONFLICT (channel_profile_id, tick_id) DO UPDATE
		SET finished_at = EXCLUDED.finished_at,
		    dry_run = EXCLUDED.dry_run,
		    ideas_discovered = EXCLUDED.ideas_discovered,
		    candidates_scored = EXCLUDED.candidates_scored,
		    tasks_selected = EXCLUDED.tasks_selected,
		    tasks_rejected = EXCLUDED.tasks_rejected,
		    guards_triggered_json = EXCLUDED.guards_triggered_json,
		    decision_summary_json = EXCLUDED.decision_summary_json,
		    error_message = NULL
		RETURNING id
	`, channelID, tickID, now, now, result.DryRun, len(result.Accepted)+len(result.Rejected),
		result.TasksToCreate(), len(result.Rejected), guardsJSON, summaryJSON).Scan(&id)
	return id, err
}

func (s *Store) InsertDecisionAuditEntries(ctx context.Context, tickAuditID string, channelID string, result TickResult) (map[string]string, error) {
	return s.insertDecisionAuditEntries(ctx, s.Pool, tickAuditID, channelID, result)
}

func (s *Store) insertDecisionAuditEntries(ctx context.Context, db dbExecutor, tickAuditID string, channelID string, result TickResult) (map[string]string, error) {
	ids := map[string]string{}
	candidates := make([]TickCandidate, 0, len(result.Accepted)+len(result.Rejected))
	candidates = append(candidates, result.Accepted...)
	candidates = append(candidates, result.Rejected...)
	for _, candidate := range candidates {
		scoreJSON, err := json.Marshal(candidateScoreJSON(candidate))
		if err != nil {
			return nil, err
		}
		guardsJSON, err := json.Marshal(candidateGuardResultsJSON(candidate))
		if err != nil {
			return nil, err
		}
		learningJSON, err := json.Marshal(jsonObject(candidate.LearningContextJSON))
		if err != nil {
			return nil, err
		}
		pdsJSON, err := json.Marshal(jsonObject(candidate.PDSDecisionJSON))
		if err != nil {
			return nil, err
		}
		var id string
		err = db.QueryRow(ctx, `
			INSERT INTO decision_audit_entries (
				id, tick_audit_id, channel_profile_id, candidate_id, candidate_source,
				topic_lane_id, lane_format_id, target_account_id, score_json, guard_results_json,
				pds_decision_json, learning_context_json, selected, rejection_reason, created_at
			)
			VALUES (
				gen_random_uuid(), $1::uuid, $2::uuid, $3, $4, $5::uuid, $6::uuid, $7::uuid,
				$8::json, $9::json, $10::json, $11::json, $12, $13, $14::timestamptz
			)
			RETURNING id
		`, tickAuditID, channelID, candidate.CandidateID, candidateSource(candidate),
			candidateLaneID(candidate), candidateFormatID(candidate), candidateAccountUUID(candidate),
			scoreJSON, guardsJSON, pdsJSON, learningJSON, !candidate.Rejected,
			candidateRejectionReason(candidate), s.Now().UTC()).Scan(&id)
		if err != nil {
			return nil, err
		}
		ids[candidate.CandidateID] = id
	}
	return ids, nil
}

func (s *Store) AttachDecisionAuditTask(ctx context.Context, auditID string, taskID string) error {
	return s.attachDecisionAuditTask(ctx, s.Pool, auditID, taskID)
}

func (s *Store) attachDecisionAuditTask(ctx context.Context, db dbExecutor, auditID string, taskID string) error {
	tag, err := db.Exec(ctx, `
		UPDATE decision_audit_entries
		SET created_task_id = $2::uuid
		WHERE id = $1::uuid
	`, auditID, taskID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("decision audit entry %s not found", auditID)
	}
	return nil
}

func (s *Store) InsertProductionTask(ctx context.Context, channel ChannelProfileRow, candidate TickCandidate, now time.Time) (string, error) {
	return s.insertProductionTask(ctx, s.Pool, channel, candidate, now)
}

func (s *Store) insertProductionTask(ctx context.Context, db dbExecutor, channel ChannelProfileRow, candidate TickCandidate, now time.Time) (string, error) {
	if candidate.Account == nil {
		return "", errors.New("production task candidate has no target account")
	}
	laneID := candidateLaneID(candidate)
	formatID := candidateFormatID(candidate)
	manualSeedID := candidateManualSeedID(candidate)
	discoverySignalID := candidateDiscoverySignalID(candidate)
	approvalMode := ApprovalAgent
	if candidate.SourceKind == SourceManualSeed {
		approvalMode = ApprovalHuman
	}
	rationale := map[string]any{
		"candidate_id": candidate.CandidateID,
		"source_kind":  candidate.SourceKind,
	}
	if candidate.DiscoverySignal != nil {
		rationale["discovery_signal_id"] = candidate.DiscoverySignal.ID
	}
	scoreBreakdown := map[string]any{
		"source":      candidate.Source,
		"source_kind": candidate.SourceKind,
	}
	snapshot := channelConfigSnapshot(channel, candidate)
	transition := []map[string]any{Transition("seeded", TaskSelected, "agent_tick", now)}

	rationaleJSON, err := json.Marshal(rationale)
	if err != nil {
		return "", err
	}
	scoreJSON, err := json.Marshal(scoreBreakdown)
	if err != nil {
		return "", err
	}
	sourcePlatformsJSON, err := json.Marshal(stringSlice(candidate.SourcePlatformsJSON))
	if err != nil {
		return "", err
	}
	materialLibraryIDsJSON, err := json.Marshal(stringSlice(candidate.MaterialLibraryIDsJSON))
	if err != nil {
		return "", err
	}
	snapshotJSON, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	transitionJSON, err := json.Marshal(transition)
	if err != nil {
		return "", err
	}

	var id string
	err = db.QueryRow(ctx, `
		INSERT INTO production_tasks (
			id, channel_profile_id, topic_lane_id, lane_format_id, target_account_id,
			manual_seed_id, discovery_signal_id, source, title_seed, prompt, rationale_json,
			score_breakdown_json, portfolio_bucket, source_platforms_json,
			material_library_ids_json, uses_external_assets, approval_mode,
			agent_approval_evidence_json, priority, state, state_updated_at, retry_count,
			channel_config_version_snapshot, channel_config_snapshot_json,
			transition_history_json, created_at, updated_at
		)
		VALUES (
			gen_random_uuid(), $1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid, $6::uuid, $7, $8, $9,
			$10::json, $11::json, 'explore', $12::json, $13::json,
			$14, $15, '{}'::json, 0.0, $16, $17::timestamptz, 0, $18, $19::json,
			$20::json, $17::timestamp, $17::timestamp
		)
		RETURNING id
	`, channel.ID, laneID, formatID, candidate.Account.ID, manualSeedID, discoverySignalID, candidate.Source,
		candidate.TitleSeed, candidate.Prompt, rationaleJSON, scoreJSON, sourcePlatformsJSON,
		materialLibraryIDsJSON, usesExternalAssets(candidate), approvalMode, TaskSelected, now.UTC(),
		channel.ConfigVersion, snapshotJSON, transitionJSON).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) MarkDiscoverySignalConverted(ctx context.Context, signalID string, taskID string) error {
	return s.markDiscoverySignalConverted(ctx, s.Pool, signalID, taskID)
}

func (s *Store) markDiscoverySignalConverted(ctx context.Context, db dbExecutor, signalID string, taskID string) error {
	tag, err := db.Exec(ctx, `
		UPDATE discovery_signals
		SET status = 'converted',
		    converted_task_id = $2::uuid,
		    updated_at = $3::timestamp
		WHERE id = $1::uuid
	`, signalID, taskID, s.Now().UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("discovery signal %s not found", signalID)
	}
	return nil
}

func unmarshalJSONObject(raw []byte, dest *map[string]any) error {
	if len(raw) == 0 || string(raw) == "null" {
		*dest = map[string]any{}
		return nil
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		return err
	}
	if *dest == nil {
		*dest = map[string]any{}
	}
	return nil
}

func unmarshalJSONStringSlice(raw []byte, dest *[]string) error {
	if len(raw) == 0 || string(raw) == "null" {
		*dest = []string{}
		return nil
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		return err
	}
	if *dest == nil {
		*dest = []string{}
	}
	return nil
}

func candidateAuditSummaries(candidates []TickCandidate) []map[string]any {
	result := make([]map[string]any, 0, len(candidates))
	for _, candidate := range candidates {
		result = append(result, map[string]any{
			"candidate_id": candidate.CandidateID,
			"source":       candidate.Source,
			"source_kind":  candidate.SourceKind,
			"lane_id":      valueOrEmpty(candidateLaneID(candidate)),
			"format_id":    valueOrEmpty(candidateFormatID(candidate)),
			"account_id":   candidateAccountID(candidate),
			"guard":        candidate.RejectionGuard,
			"reason":       candidate.RejectionReason,
		})
	}
	return result
}

func candidateGuardSummaries(candidates []TickCandidate) []map[string]any {
	result := []map[string]any{}
	for _, candidate := range candidates {
		if candidate.RejectionGuard == "" {
			continue
		}
		result = append(result, map[string]any{
			"guard":        candidate.RejectionGuard,
			"reason":       candidate.RejectionReason,
			"candidate_id": candidate.CandidateID,
			"lane_id":      valueOrEmpty(candidateLaneID(candidate)),
			"format_id":    valueOrEmpty(candidateFormatID(candidate)),
			"account_id":   candidateAccountID(candidate),
		})
	}
	return result
}

func channelConfigSnapshot(channel ChannelProfileRow, candidate TickCandidate) map[string]any {
	snapshot := map[string]any{
		"channel": map[string]any{
			"id":                      channel.ID,
			"dry_run":                 channel.DryRun,
			"default_aspect_ratio":    channel.DefaultAspectRatio,
			"risk_policy_json":        jsonObject(channel.RiskPolicyJSON),
			"cadence_policy_json":     jsonObject(channel.CadencePolicyJSON),
			"content_mix_policy_json": jsonObject(channel.ContentMixPolicyJSON),
		},
		"risk_policy_json":    jsonObject(channel.RiskPolicyJSON),
		"cadence_policy_json": jsonObject(channel.CadencePolicyJSON),
	}
	if candidate.Account != nil {
		snapshot["account"] = map[string]any{
			"id":                          candidate.Account.ID,
			"platform":                    candidate.Account.Platform,
			"default_privacy":             candidate.Account.DefaultPrivacy,
			"external_asset_auto_publish": candidate.Account.ExternalAutoPublish,
		}
	}
	if candidate.Lane != nil {
		snapshot["lane"] = map[string]any{
			"id":            candidate.Lane.ID,
			"name":          candidate.Lane.Name,
			"description":   candidate.Lane.Description,
			"keywords_json": stringSlice(candidate.Lane.KeywordsJSON),
		}
	}
	if candidate.LaneFormat != nil {
		snapshot["lane_format"] = map[string]any{
			"id":                         candidate.LaneFormat.ID,
			"format_key":                 candidate.LaneFormat.FormatKey,
			"default_publish_visibility": candidate.LaneFormat.DefaultPublishVisibility,
			"target_duration_sec":        candidate.LaneFormat.TargetDurationSec,
			"template_pool_json":         stringSlice(candidate.LaneFormat.TemplatePoolJSON),
			"source_platforms_json":      stringSlice(candidate.LaneFormat.SourcePlatformsJSON),
		}
	}
	if candidate.Seed != nil {
		snapshot["manual_seed"] = map[string]any{
			"id":               candidate.Seed.ID,
			"source_policy":    candidate.Seed.SourcePolicy,
			"constraints_json": jsonObject(candidate.Seed.ConstraintsJSON),
		}
	}
	if candidate.DiscoverySignal != nil {
		snapshot["discovery_signal"] = map[string]any{
			"id":                 candidate.DiscoverySignal.ID,
			"source":             candidate.DiscoverySignal.Source,
			"source_external_id": candidate.DiscoverySignal.SourceExternalID,
			"title":              candidate.DiscoverySignal.Title,
			"trend_score":        candidate.DiscoverySignal.TrendScore,
		}
	}
	return snapshot
}

func candidateLaneID(candidate TickCandidate) *string {
	if candidate.Lane == nil {
		return nil
	}
	value := candidate.Lane.ID
	return &value
}

func candidateFormatID(candidate TickCandidate) *string {
	if candidate.LaneFormat == nil {
		return nil
	}
	value := candidate.LaneFormat.ID
	return &value
}

func candidateManualSeedID(candidate TickCandidate) *string {
	if candidate.Seed == nil {
		return nil
	}
	value := candidate.Seed.ID
	return &value
}

func candidateDiscoverySignalID(candidate TickCandidate) *string {
	if candidate.DiscoverySignal == nil {
		return nil
	}
	value := candidate.DiscoverySignal.ID
	return &value
}

func candidateAccountUUID(candidate TickCandidate) *string {
	if candidate.Account == nil {
		return nil
	}
	value := candidate.Account.ID
	return &value
}

func candidateAccountID(candidate TickCandidate) string {
	if candidate.Account == nil {
		return ""
	}
	return candidate.Account.ID
}

func candidateSource(candidate TickCandidate) string {
	if candidate.SourceKind != "" {
		return candidate.SourceKind
	}
	if candidate.Source != "" {
		return candidate.Source
	}
	return "unknown"
}

func candidateScoreJSON(candidate TickCandidate) map[string]any {
	score := jsonObject(candidate.ScoreJSON)
	if _, ok := score["source"]; !ok && candidate.Source != "" {
		score["source"] = candidate.Source
	}
	if _, ok := score["source_kind"]; !ok {
		score["source_kind"] = candidateSource(candidate)
	}
	return score
}

func candidateGuardResultsJSON(candidate TickCandidate) []map[string]any {
	if len(candidate.GuardResultsJSON) > 0 {
		return candidate.GuardResultsJSON
	}
	if candidate.Rejected {
		return []map[string]any{{
			"guard":   candidate.RejectionGuard,
			"verdict": "reject",
			"reason":  candidate.RejectionReason,
		}}
	}
	return []map[string]any{}
}

func candidateRejectionReason(candidate TickCandidate) *string {
	if !candidate.Rejected || candidate.RejectionReason == "" {
		return nil
	}
	value := candidate.RejectionReason
	return &value
}

func usesExternalAssets(candidate TickCandidate) bool {
	return len(candidate.SourcePlatformsJSON) > 0 || len(candidate.MaterialLibraryIDsJSON) > 0
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
