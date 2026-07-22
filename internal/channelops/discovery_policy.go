package channelops

import (
	"encoding/json"
	"fmt"
	"strconv"
)

const discoverySourceYouTubeSearch = "youtube_search"

type DiscoveryPolicy struct {
	Enabled            bool
	IntervalMinutes    int
	MaxQueriesPerRun   int
	MaxResultsPerQuery int
	MinViewCount       int
	RegionCode         string
}

var defaultDiscoveryPolicy = DiscoveryPolicy{
	Enabled:            false,
	IntervalMinutes:    360,
	MaxQueriesPerRun:   3,
	MaxResultsPerQuery: 10,
	MinViewCount:       1000,
	RegionCode:         "US",
}

// DiscoveryPolicyFromContentMix mirrors the Python policy parser for JSON-decoded content mixes.
func DiscoveryPolicyFromContentMix(contentMix map[string]any) (DiscoveryPolicy, error) {
	if contentMix == nil {
		return DiscoveryPolicy{}, fmt.Errorf("content mix policy must be an object")
	}

	nested := map[string]any{}
	if raw, exists := contentMix["youtube_discovery"]; exists {
		var ok bool
		nested, ok = raw.(map[string]any)
		if !ok || nested == nil {
			return DiscoveryPolicy{}, fmt.Errorf("youtube discovery policy must be an object")
		}
	}

	enabled, err := discoveryBool(nested, "enabled", defaultDiscoveryPolicy.Enabled)
	if err != nil {
		return DiscoveryPolicy{}, err
	}
	interval, err := discoveryInt(nested, "interval_minutes", defaultDiscoveryPolicy.IntervalMinutes, 60, 1440)
	if err != nil {
		return DiscoveryPolicy{}, err
	}
	queries, err := discoveryInt(nested, "max_queries_per_run", defaultDiscoveryPolicy.MaxQueriesPerRun, 1, 5)
	if err != nil {
		return DiscoveryPolicy{}, err
	}
	results, err := discoveryInt(nested, "max_results_per_query", defaultDiscoveryPolicy.MaxResultsPerQuery, 1, 25)
	if err != nil {
		return DiscoveryPolicy{}, err
	}
	minViews, err := discoveryInt(nested, "min_view_count", defaultDiscoveryPolicy.MinViewCount, 0, 1_000_000_000)
	if err != nil {
		return DiscoveryPolicy{}, err
	}
	region := defaultDiscoveryPolicy.RegionCode
	if raw, exists := nested["region_code"]; exists {
		region, err = discoveryRegion(raw)
		if err != nil {
			return DiscoveryPolicy{}, err
		}
	} else if raw, exists := contentMix["region_code"]; exists {
		region, err = discoveryRegion(raw)
		if err != nil {
			return DiscoveryPolicy{}, err
		}
	}

	return DiscoveryPolicy{
		Enabled:            enabled,
		IntervalMinutes:    interval,
		MaxQueriesPerRun:   queries,
		MaxResultsPerQuery: results,
		MinViewCount:       minViews,
		RegionCode:         region,
	}, nil
}

func discoveryBool(value map[string]any, field string, fallback bool) (bool, error) {
	raw, exists := value[field]
	if !exists {
		return fallback, nil
	}
	parsed, ok := raw.(bool)
	if !ok {
		return false, fmt.Errorf("%s must be a boolean", field)
	}
	return parsed, nil
}

func discoveryInt(value map[string]any, field string, fallback, minimum, maximum int) (int, error) {
	raw, exists := value[field]
	if !exists {
		return fallback, nil
	}
	number, ok := raw.(json.Number)
	if !ok || !isLexicallyIntegralJSONNumber(number.String()) {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", field, minimum, maximum)
	}
	parsed, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil || parsed < int64(minimum) || parsed > int64(maximum) {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", field, minimum, maximum)
	}
	return int(parsed), nil
}

func isLexicallyIntegralJSONNumber(value string) bool {
	if value == "" {
		return false
	}
	start := 0
	if value[0] == '-' {
		start = 1
	}
	if start == len(value) {
		return false
	}
	if value[start] == '0' {
		return start+1 == len(value)
	}
	if value[start] < '1' || value[start] > '9' {
		return false
	}
	for index := start + 1; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func discoveryRegion(value any) (string, error) {
	region, ok := value.(string)
	if !ok || len(region) != 2 || region[0] < 'A' || region[0] > 'Z' || region[1] < 'A' || region[1] > 'Z' {
		return "", fmt.Errorf("region_code must be two uppercase ASCII letters")
	}
	return region, nil
}
