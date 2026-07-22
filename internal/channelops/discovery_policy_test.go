package channelops

import (
	"encoding/json"
	"testing"
)

func TestDiscoveryPolicyFromContentMixDefaultsDisabled(t *testing.T) {
	policy, err := DiscoveryPolicyFromContentMix(map[string]any{})
	if err != nil {
		t.Fatalf("DiscoveryPolicyFromContentMix: %v", err)
	}
	if policy != (DiscoveryPolicy{
		Enabled:            false,
		IntervalMinutes:    360,
		MaxQueriesPerRun:   3,
		MaxResultsPerQuery: 10,
		MinViewCount:       1000,
		RegionCode:         "US",
	}) {
		t.Fatalf("policy = %#v", policy)
	}
}

func TestDiscoveryPolicyFromContentMixParsesJSONBoundsAndLegacyRegion(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want DiscoveryPolicy
	}{
		{
			name: "lower bounds",
			raw:  `{"youtube_discovery":{"enabled":true,"interval_minutes":60,"max_queries_per_run":1,"max_results_per_query":1,"min_view_count":0,"region_code":"CA"}}`,
			want: DiscoveryPolicy{true, 60, 1, 1, 0, "CA"},
		},
		{
			name: "upper bounds",
			raw:  `{"youtube_discovery":{"enabled":false,"interval_minutes":1440,"max_queries_per_run":5,"max_results_per_query":25,"min_view_count":1000000000,"region_code":"US"}}`,
			want: DiscoveryPolicy{false, 1440, 5, 25, 1000000000, "US"},
		},
		{
			name: "legacy top level region when nested absent",
			raw:  `{"region_code":"GB","youtube_discovery":{"enabled":true}}`,
			want: DiscoveryPolicy{true, 360, 3, 10, 1000, "GB"},
		},
		{
			name: "nested region takes precedence over legacy top level region",
			raw:  `{"region_code":"not-a-region","youtube_discovery":{"enabled":true,"region_code":"GB"}}`,
			want: DiscoveryPolicy{true, 360, 3, 10, 1000, "GB"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contentMix := decodeDiscoveryContentMix(t, tt.raw)
			if got, ok := contentMix["youtube_discovery"].(map[string]any); ok {
				for _, field := range []string{"interval_minutes", "max_queries_per_run", "max_results_per_query", "min_view_count"} {
					if _, exists := got[field]; exists {
						if _, ok := got[field].(float64); !ok {
							t.Fatalf("decoded %s type = %T, want float64", field, got[field])
						}
					}
				}
			}
			policy, err := DiscoveryPolicyFromContentMix(contentMix)
			if err != nil {
				t.Fatalf("DiscoveryPolicyFromContentMix: %v", err)
			}
			if policy != tt.want {
				t.Fatalf("policy = %#v, want %#v", policy, tt.want)
			}
		})
	}
}

func TestDiscoveryPolicyFromContentMixRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"content mix is not object", `null`},
		{"nested policy is not object", `{"youtube_discovery":true}`},
		{"enabled numeric", `{"youtube_discovery":{"enabled":1}}`},
		{"enabled string", `{"youtube_discovery":{"enabled":"true"}}`},
		{"interval below minimum", `{"youtube_discovery":{"interval_minutes":59}}`},
		{"interval above maximum", `{"youtube_discovery":{"interval_minutes":1441}}`},
		{"interval fractional", `{"youtube_discovery":{"interval_minutes":360.5}}`},
		{"interval boolean", `{"youtube_discovery":{"interval_minutes":true}}`},
		{"queries below minimum", `{"youtube_discovery":{"max_queries_per_run":0}}`},
		{"queries above maximum", `{"youtube_discovery":{"max_queries_per_run":6}}`},
		{"results below minimum", `{"youtube_discovery":{"max_results_per_query":0}}`},
		{"results above maximum", `{"youtube_discovery":{"max_results_per_query":26}}`},
		{"views below minimum", `{"youtube_discovery":{"min_view_count":-1}}`},
		{"views above maximum", `{"youtube_discovery":{"min_view_count":1000000001}}`},
		{"nested region lower case", `{"youtube_discovery":{"region_code":"us"}}`},
		{"nested region invalid ascii", `{"youtube_discovery":{"region_code":"U1"}}`},
		{"nested region invalid length", `{"youtube_discovery":{"region_code":"USA"}}`},
		{"legacy region invalid", `{"region_code":"us"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DiscoveryPolicyFromContentMix(decodeDiscoveryContentMix(t, tt.raw)); err == nil {
				t.Fatal("DiscoveryPolicyFromContentMix error = nil")
			}
		})
	}
}

func TestDiscoveryPolicyFromContentMixAcceptsOnlyJSONDecodedIntegralNumbers(t *testing.T) {
	valid := map[string]any{
		"youtube_discovery": map[string]any{
			"enabled":               true,
			"interval_minutes":      float64(360),
			"max_queries_per_run":   float64(3),
			"max_results_per_query": float64(10),
			"min_view_count":        float64(1000),
		},
	}
	if _, err := DiscoveryPolicyFromContentMix(valid); err != nil {
		t.Fatalf("DiscoveryPolicyFromContentMix JSON-decoded values: %v", err)
	}

	nonJSONNumber := map[string]any{
		"youtube_discovery": map[string]any{"interval_minutes": 360},
	}
	if _, err := DiscoveryPolicyFromContentMix(nonJSONNumber); err == nil {
		t.Fatal("DiscoveryPolicyFromContentMix accepted non-JSON decoded integer")
	}
}

func TestDiscoveryIdempotencyKey(t *testing.T) {
	if got := DiscoveryIdempotencyKey("channel-1", "youtube_search", "2026-07-21-18"); got != "ingest_discovery:channel-1:youtube_search:2026-07-21-18" {
		t.Fatalf("key = %q", got)
	}
}

func decodeDiscoveryContentMix(t *testing.T, raw string) map[string]any {
	t.Helper()
	var contentMix map[string]any
	if err := json.Unmarshal([]byte(raw), &contentMix); err != nil {
		t.Fatalf("decode test content mix: %v", err)
	}
	return contentMix
}
