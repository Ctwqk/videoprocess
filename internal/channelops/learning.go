package channelops

import (
	"context"
	"math"
	"strconv"
)

type LearningStateInput struct {
	ChannelID     string
	DimensionType string
	DimensionKey  string
	WindowDays    int
	SampleCount   int
	AvgReward     float64
}

func LearningRecommendation(sampleCount int, avgReward float64) map[string]any {
	action := "insufficient_data"
	if sampleCount >= 10 {
		action = "observe"
		if avgReward >= 0.65 {
			action = "promote_more"
		}
		if avgReward < 0.25 {
			action = "cool_down"
		}
	}
	return map[string]any{"action": action, "sample_count": sampleCount, "avg_reward": avgReward}
}

func learningRecomputeWindows(value any) []int {
	windows := []int{}
	appendWindow := func(raw any) {
		window := intFromAny(raw)
		if window > 0 {
			windows = append(windows, window)
		}
	}
	switch typed := value.(type) {
	case nil:
	case []int:
		for _, item := range typed {
			appendWindow(item)
		}
	case []any:
		for _, item := range typed {
			appendWindow(item)
		}
	default:
		appendWindow(typed)
	}
	if len(windows) == 0 {
		return []int{7, 30}
	}
	return windows
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case float32:
		return int(typed)
	case string:
		parsed, err := strconv.Atoi(typed)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

func (s *Store) RecomputeLearningState(ctx context.Context, channelID string, windowDays int) error {
	return s.RecomputeLearningStateForSources(ctx, channelID, windowDays)
}

func (s *Store) RecomputeLearningStateForSources(ctx context.Context, channelID string, windowDays int) error {
	if err := requireUUID("channel_id", channelID); err != nil {
		return err
	}
	if windowDays <= 0 {
		windowDays = 7
	}
	now := s.Now().UTC()
	since := now.AddDate(0, 0, -windowDays)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	rows, err := tx.Query(ctx, `
		SELECT COALESCE(NULLIF(t.source, ''), 'unknown') AS source,
		       COUNT(*)::int AS sample_count,
		       AVG(f.reward_score)::float8 AS avg_reward
		FROM production_tasks t
		JOIN publication_records p ON p.production_task_id = t.id
		JOIN feedback_snapshots f ON f.publication_id = p.id
		WHERE t.channel_profile_id = $1::uuid
		  AND f.collected_at >= $2::timestamptz
		  AND f.metrics_completeness_score >= 0.4
		  AND f.reward_score IS NOT NULL
		GROUP BY COALESCE(NULLIF(t.source, ''), 'unknown')
	`, channelID, since)
	if err != nil {
		return err
	}
	inputs := []LearningStateInput{}
	for rows.Next() {
		var input LearningStateInput
		input.ChannelID = channelID
		input.DimensionType = "source"
		input.WindowDays = windowDays
		if err := rows.Scan(&input.DimensionKey, &input.SampleCount, &input.AvgReward); err != nil {
			rows.Close()
			return err
		}
		inputs = append(inputs, input)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	if _, err := tx.Exec(ctx, `
		DELETE FROM learning_states
		WHERE channel_profile_id = $1::uuid
		  AND dimension_type = 'source'
		  AND window_days = $2
	`, channelID, windowDays); err != nil {
		return err
	}
	for _, input := range inputs {
		confidence := math.Min(float64(input.SampleCount)/20.0, 1.0)
		if _, err := tx.Exec(ctx, `
			INSERT INTO learning_states (
				id, channel_profile_id, dimension_type, dimension_key, window_days,
				sample_count, avg_reward, confidence, recommendation_json,
				last_computed_at, created_at, updated_at
			)
			VALUES (
				gen_random_uuid(), $1::uuid, $2, $3, $4,
				$5, $6, $7, $8::json, $9::timestamptz, $9::timestamp, $9::timestamp
			)
		`, input.ChannelID, input.DimensionType, input.DimensionKey, input.WindowDays,
			input.SampleCount, input.AvgReward, confidence, mustJSON(LearningRecommendation(input.SampleCount, input.AvgReward)), now); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}
