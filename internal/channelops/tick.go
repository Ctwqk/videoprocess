package channelops

import (
	"fmt"
	"strings"
)

type TickCandidate struct {
	CandidateID            string
	Source                 string
	SourceKind             string
	Seed                   *ManualSeedRow
	Lane                   *TopicLaneRow
	LaneFormat             *LaneFormatRow
	Account                *PublishingAccountRow
	Prompt                 string
	TitleSeed              string
	SourcePlatformsJSON    []string
	MaterialLibraryIDsJSON []string
	ConstraintsJSON        map[string]any
	ManualMaterialOverride bool
	ScoreJSON              map[string]any
	GuardResultsJSON       []map[string]any
	LearningContextJSON    map[string]any
	DiscoverySignal        *DiscoverySignalRow
	Rejected               bool
	RejectionGuard         string
	RejectionReason        string
}

type TickResult struct {
	DryRun   bool
	Accepted []TickCandidate
	Rejected []TickCandidate
}

func (r TickResult) TasksToCreate() int {
	if r.DryRun {
		return 0
	}
	return len(r.Accepted)
}

func BuildTickCandidates(
	channel ChannelProfileRow,
	lanes []TopicLaneRow,
	accounts []PublishingAccountRow,
	seeds []ManualSeedRow,
	signals []DiscoverySignalRow,
	laneFormats map[string][]LaneFormatRow,
	bucket string,
) []TickCandidate {
	candidates := []TickCandidate{}
	activeLanes := enabledLanes(lanes)
	activeAccounts := enabledAccounts(accounts)
	laneByID := map[string]TopicLaneRow{}
	for _, lane := range activeLanes {
		laneByID[lane.ID] = lane
	}

	var fallbackLane *TopicLaneRow
	if len(activeLanes) > 0 {
		laneCopy := activeLanes[0]
		fallbackLane = &laneCopy
	}

	selectedByLane := map[string]int{}
	accountIndex := 0
	for _, seed := range seeds {
		lane, laneRejection := resolveSeedLane(seed, fallbackLane, laneByID)
		laneFormat := firstEnabledLaneFormat(lane, laneFormats)
		account, accountRejection := resolveSeedAccount(seed, activeAccounts)
		candidate := CandidateFromManualSeed(seed, lane, laneFormat, account, bucket)
		switch {
		case laneRejection != "":
			rejectCandidate(&candidate, "lane_unavailable", laneRejection)
		case accountRejection != "":
			rejectCandidate(&candidate, "account_unavailable", accountRejection)
		}
		candidates = append(candidates, candidate)
		if !candidate.Rejected && lane != nil {
			selectedByLane[lane.ID]++
		}
	}

	for _, signal := range signals {
		lane, laneRejection := resolveSignalLane(signal, fallbackLane, laneByID)
		laneFormat := firstEnabledLaneFormat(lane, laneFormats)
		var account *PublishingAccountRow
		if len(activeAccounts) > 0 {
			accountCopy := activeAccounts[accountIndex%len(activeAccounts)]
			account = &accountCopy
			accountIndex++
		}
		candidate := CandidateFromDiscoverySignal(channel, signal, lane, laneFormat, account, bucket)
		switch {
		case laneRejection != "":
			rejectCandidate(&candidate, "lane_unavailable", laneRejection)
		case account == nil:
			rejectCandidate(&candidate, "account_unavailable", "No active publishing account is available.")
		}
		candidates = append(candidates, candidate)
		if !candidate.Rejected && lane != nil {
			selectedByLane[lane.ID]++
		}
	}

	generatedLaneFormats := map[string]bool{}
	for _, lane := range activeLanes {
		laneBudget := positiveInt(lane.MaxPostsPerDay, 1)
		remaining := laneBudget - selectedByLane[lane.ID]
		if remaining <= 0 {
			continue
		}
		generated := 0
		for _, format := range laneFormats[lane.ID] {
			if !format.Enabled || generated >= remaining {
				continue
			}
			laneFormatKey := lane.ID + ":" + format.ID
			if generatedLaneFormats[laneFormatKey] {
				continue
			}
			generatedLaneFormats[laneFormatKey] = true

			var account *PublishingAccountRow
			if len(activeAccounts) > 0 {
				accountCopy := activeAccounts[accountIndex%len(activeAccounts)]
				account = &accountCopy
				accountIndex++
			}
			laneCopy := lane
			formatCopy := format
			candidate := TickCandidate{
				CandidateID:         candidateID(SourceLaneSeed, lane.ID, format.ID, bucket, ""),
				Source:              SourceLaneSeed,
				SourceKind:          SourceLaneSeed,
				Lane:                &laneCopy,
				LaneFormat:          &formatCopy,
				Account:             account,
				Prompt:              LanePrompt(channel, lane, format),
				TitleSeed:           lane.Name,
				SourcePlatformsJSON: stringSlice(format.SourcePlatformsJSON),
				ConstraintsJSON: map[string]any{
					"template_pool_json": stringSlice(format.TemplatePoolJSON),
				},
				ScoreJSON: map[string]any{
					"source":        SourceLaneSeed,
					"source_kind":   SourceLaneSeed,
					"lane_weight":   lane.Weight,
					"format_key":    format.FormatKey,
					"format_weight": format.Weight,
				},
				LearningContextJSON: map[string]any{},
			}
			if account == nil {
				rejectCandidate(&candidate, "account_unavailable", "No active publishing account is available.")
			}
			candidates = append(candidates, candidate)
			generated++
		}
	}
	return candidates
}

func CandidateFromDiscoverySignal(
	channel ChannelProfileRow,
	signal DiscoverySignalRow,
	lane *TopicLaneRow,
	laneFormat *LaneFormatRow,
	account *PublishingAccountRow,
	bucket string,
) TickCandidate {
	laneID := ""
	if lane != nil {
		laneID = lane.ID
	} else if signal.TopicLaneID != nil {
		laneID = *signal.TopicLaneID
	}
	formatID := ""
	sourcePlatforms := []string{"youtube"}
	if laneFormat != nil {
		formatID = laneFormat.ID
		if len(laneFormat.SourcePlatformsJSON) > 0 {
			sourcePlatforms = stringSlice(laneFormat.SourcePlatformsJSON)
		}
	}
	signalCopy := signal
	return TickCandidate{
		CandidateID:            candidateID(SourceTrendYT, laneID, formatID, bucket, signal.ID),
		Source:                 SourceTrendYT,
		SourceKind:             SourceTrendYT,
		DiscoverySignal:        &signalCopy,
		Lane:                   lane,
		LaneFormat:             laneFormat,
		Account:                account,
		Prompt:                 DiscoveryPrompt(channel, signal, lane, laneFormat),
		TitleSeed:              signal.Title,
		SourcePlatformsJSON:    sourcePlatforms,
		ManualMaterialOverride: false,
		ScoreJSON: map[string]any{
			"source":              SourceTrendYT,
			"source_kind":         SourceTrendYT,
			"discovery_signal_id": signal.ID,
			"trend_score":         signal.TrendScore,
			"novelty_score":       signal.NoveltyScore,
		},
		LearningContextJSON: map[string]any{},
	}
}

func CandidateFromManualSeed(seed ManualSeedRow, lane *TopicLaneRow, laneFormat *LaneFormatRow, account *PublishingAccountRow, bucket string) TickCandidate {
	sourceKind := SourceManualSeed
	if seed.SourcePolicy == SourceTrendYT {
		sourceKind = SourceTrendYT
	}
	laneID := ""
	if lane != nil {
		laneID = lane.ID
	} else if seed.TopicLaneID != nil {
		laneID = *seed.TopicLaneID
	}
	formatID := ""
	if laneFormat != nil {
		formatID = laneFormat.ID
	}
	sourcePlatforms := stringSlice(seed.SourcePlatformsJSON)
	if len(sourcePlatforms) == 0 && laneFormat != nil {
		sourcePlatforms = stringSlice(laneFormat.SourcePlatformsJSON)
	}
	return TickCandidate{
		CandidateID:            candidateID(SourceManualSeed, laneID, formatID, bucket, seed.ID),
		Source:                 SourceManualSeed,
		SourceKind:             sourceKind,
		Seed:                   &seed,
		Lane:                   lane,
		LaneFormat:             laneFormat,
		Account:                account,
		Prompt:                 seed.Prompt,
		TitleSeed:              seed.TitleSeed,
		SourcePlatformsJSON:    sourcePlatforms,
		MaterialLibraryIDsJSON: stringSlice(seed.MaterialLibraryIDsJSON),
		ConstraintsJSON:        jsonObject(seed.ConstraintsJSON),
		ManualMaterialOverride: sourceKind == SourceManualSeed,
		ScoreJSON: map[string]any{
			"source":                   SourceManualSeed,
			"source_kind":              sourceKind,
			"source_policy":            seed.SourcePolicy,
			"manual_material_override": sourceKind == SourceManualSeed,
		},
		LearningContextJSON: map[string]any{},
	}
}

func LanePrompt(channel ChannelProfileRow, lane TopicLaneRow, format LaneFormatRow) string {
	duration := positiveInt(format.TargetDurationSec, 30)
	aspectRatio := strings.TrimSpace(channel.DefaultAspectRatio)
	if aspectRatio == "" {
		aspectRatio = "9:16"
	}
	return fmt.Sprintf(
		"Create a %s video for the %q topic. Theme: %s. Keywords: %v. Target duration: %ds. Aspect ratio: %s.",
		format.FormatKey,
		lane.Name,
		lane.Description,
		lane.KeywordsJSON,
		duration,
		aspectRatio,
	)
}

func DiscoveryPrompt(channel ChannelProfileRow, signal DiscoverySignalRow, lane *TopicLaneRow, format *LaneFormatRow) string {
	laneName := "general"
	laneDescription := ""
	keywords := signal.KeywordsJSON
	if lane != nil {
		laneName = lane.Name
		laneDescription = lane.Description
		if len(keywords) == 0 {
			keywords = lane.KeywordsJSON
		}
	}
	formatKey := "short"
	duration := 30
	if format != nil {
		formatKey = format.FormatKey
		duration = positiveInt(format.TargetDurationSec, 30)
	}
	aspectRatio := strings.TrimSpace(channel.DefaultAspectRatio)
	if aspectRatio == "" {
		aspectRatio = "9:16"
	}
	return fmt.Sprintf(
		"Create a %s video for the %q topic based on this YouTube trend. Trend title: %s. Summary: %s. Theme: %s. Keywords: %v. Target duration: %ds. Aspect ratio: %s.",
		formatKey,
		laneName,
		signal.Title,
		signal.Summary,
		laneDescription,
		keywords,
		duration,
		aspectRatio,
	)
}

func acceptedRejected(candidates []TickCandidate) ([]TickCandidate, []TickCandidate) {
	accepted := []TickCandidate{}
	rejected := []TickCandidate{}
	for _, candidate := range candidates {
		if candidate.Rejected && len(candidate.GuardResultsJSON) == 0 {
			rejectCandidate(&candidate, candidate.RejectionGuard, candidate.RejectionReason)
		}
		if candidate.Rejected {
			rejected = append(rejected, candidate)
		} else {
			accepted = append(accepted, candidate)
		}
	}
	return accepted, rejected
}

func rejectCandidate(candidate *TickCandidate, guard string, reason string) {
	candidate.Rejected = true
	candidate.RejectionGuard = guard
	candidate.RejectionReason = reason
	candidate.GuardResultsJSON = []map[string]any{{
		"guard":   guard,
		"verdict": "reject",
		"reason":  reason,
	}}
}

func candidateID(source string, laneID string, formatID string, bucket string, seedID string) string {
	if laneID == "" {
		laneID = "unassigned"
	}
	if formatID == "" {
		formatID = "none"
	}
	if seedID != "" {
		return fmt.Sprintf("%s:%s:lane:%s:format:%s:%s", source, seedID, laneID, formatID, bucket)
	}
	return fmt.Sprintf("%s:lane:%s:format:%s:%s", source, laneID, formatID, bucket)
}

func resolveSignalLane(signal DiscoverySignalRow, fallback *TopicLaneRow, laneByID map[string]TopicLaneRow) (*TopicLaneRow, string) {
	if signal.TopicLaneID == nil {
		if fallback == nil {
			return nil, "No active topic lane is available."
		}
		laneCopy := *fallback
		return &laneCopy, ""
	}
	lane, ok := laneByID[*signal.TopicLaneID]
	if !ok {
		return nil, fmt.Sprintf("Discovery signal topic lane %s is disabled, paused, or unavailable.", *signal.TopicLaneID)
	}
	laneCopy := lane
	return &laneCopy, ""
}

func enabledLanes(lanes []TopicLaneRow) []TopicLaneRow {
	result := []TopicLaneRow{}
	for _, lane := range lanes {
		if lane.Enabled {
			result = append(result, lane)
		}
	}
	return result
}

func enabledAccounts(accounts []PublishingAccountRow) []PublishingAccountRow {
	result := []PublishingAccountRow{}
	for _, account := range accounts {
		if account.Enabled {
			result = append(result, account)
		}
	}
	return result
}

func resolveSeedLane(seed ManualSeedRow, fallback *TopicLaneRow, laneByID map[string]TopicLaneRow) (*TopicLaneRow, string) {
	if seed.TopicLaneID == nil {
		if fallback == nil {
			return nil, "No active topic lane is available."
		}
		laneCopy := *fallback
		return &laneCopy, ""
	}
	lane, ok := laneByID[*seed.TopicLaneID]
	if !ok {
		return nil, fmt.Sprintf("Target topic lane %s is disabled, paused, or unavailable.", *seed.TopicLaneID)
	}
	laneCopy := lane
	return &laneCopy, ""
}

func resolveSeedAccount(seed ManualSeedRow, accounts []PublishingAccountRow) (*PublishingAccountRow, string) {
	if seed.TargetAccountID != nil {
		for _, account := range accounts {
			if account.ID == *seed.TargetAccountID {
				accountCopy := account
				return &accountCopy, ""
			}
		}
		return nil, fmt.Sprintf("Target publishing account %s is disabled, paused, or unavailable.", *seed.TargetAccountID)
	}
	if len(accounts) == 0 {
		return nil, "No active publishing account is available."
	}
	accountCopy := accounts[0]
	return &accountCopy, ""
}

func firstEnabledLaneFormat(lane *TopicLaneRow, laneFormats map[string][]LaneFormatRow) *LaneFormatRow {
	if lane == nil {
		return nil
	}
	for _, format := range laneFormats[lane.ID] {
		if format.Enabled {
			formatCopy := format
			return &formatCopy
		}
	}
	return nil
}

func positiveInt(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
