package channelops

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type MaterialReference struct {
	MaterialID       string
	AssetID          string
	StartMS          *int
	EndMS            *int
	SegmentSignature string
	Metadata         map[string]any
}

type materialRefKey struct {
	materialID string
	signature  string
}

func ExtractMaterialReferences(payloads ...map[string]any) []MaterialReference {
	seen := map[materialRefKey]struct{}{}
	refs := []MaterialReference{}

	for _, payload := range payloads {
		walkMaps(payload, func(item map[string]any) {
			ref, ok := referenceFromMap(item)
			if !ok {
				return
			}
			key := materialRefKey{materialID: ref.MaterialID, signature: ref.SegmentSignature}
			if _, exists := seen[key]; exists {
				return
			}
			seen[key] = struct{}{}
			refs = append(refs, ref)
		})
	}

	return refs
}

func referenceFromMap(item map[string]any) (MaterialReference, bool) {
	materialID := stringValue(item["material_id"])
	if materialID == "" {
		materialID = stringValue(item["materialId"])
	}
	assetID := stringValue(item["asset_id"])
	if assetID == "" {
		assetID = stringValue(item["assetId"])
	}
	if materialID == "" {
		materialID = assetID
	}
	if materialID == "" {
		return MaterialReference{}, false
	}

	startMS := millisValue(item["start_ms"], item["start_sec"])
	endMS := millisValue(item["end_ms"], item["end_sec"])
	signature := stringValue(item["segment_signature"])
	if signature == "" {
		signature = stringValue(item["segmentSignature"])
	}
	if signature == "" {
		signature = SegmentSignature(materialID, startMS, endMS)
	}

	return MaterialReference{
		MaterialID:       materialID,
		AssetID:          assetID,
		StartMS:          startMS,
		EndMS:            endMS,
		SegmentSignature: signature,
		Metadata:         item,
	}, true
}

func SegmentSignature(materialID string, startMS *int, endMS *int) string {
	start := ""
	end := ""
	if startMS != nil {
		start = strconv.Itoa(*startMS)
	}
	if endMS != nil {
		end = strconv.Itoa(*endMS)
	}
	sum := sha256.Sum256([]byte(materialID + ":" + start + ":" + end))
	return hex.EncodeToString(sum[:])
}

func walkMaps(value any, visit func(map[string]any)) {
	switch typed := value.(type) {
	case map[string]any:
		visit(typed)
		for _, child := range typed {
			walkMaps(child, visit)
		}
	case []any:
		for _, child := range typed {
			walkMaps(child, visit)
		}
	}
}

func millisValue(msValue any, secValue any) *int {
	if msValue != nil {
		if parsed, ok := intValue(msValue); ok {
			return &parsed
		}
	}
	if secValue != nil {
		if parsed, ok := floatValue(secValue); ok {
			scaled := parsed * 1000
			if !isFinite(scaled) || scaled < minIntFloat || scaled > maxIntFloat {
				return nil
			}
			ms := int(scaled)
			return &ms
		}
	}
	return nil
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

const (
	maxInt = int64(^uint(0) >> 1)
	minInt = -maxInt - 1
)

var (
	maxIntFloat = float64(maxInt)
	minIntFloat = float64(minInt)
)

func intValue(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int8:
		return int(typed), true
	case int16:
		return int(typed), true
	case int32:
		return int(typed), true
	case int64:
		if typed > int64(maxInt) || typed < int64(minInt) {
			return 0, false
		}
		return int(typed), true
	case uint:
		if uint64(typed) > uint64(maxInt) {
			return 0, false
		}
		return int(typed), true
	case uint8:
		return int(typed), true
	case uint16:
		return int(typed), true
	case uint32:
		if uint64(typed) > uint64(maxInt) {
			return 0, false
		}
		return int(typed), true
	case uint64:
		if typed > uint64(maxInt) {
			return 0, false
		}
		return int(typed), true
	case float32:
		return intFromFloat(float64(typed))
	case float64:
		return intFromFloat(typed)
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, strconv.IntSize)
		return int(parsed), err == nil
	default:
		return 0, false
	}
}

func intFromFloat(value float64) (int, bool) {
	if !isFinite(value) || value < minIntFloat || value > maxIntFloat {
		return 0, false
	}
	return int(value), true
}

func floatValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return finiteFloat(typed)
	case float32:
		return finiteFloat(float64(typed))
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return finiteFloat(parsed)
	default:
		return 0, false
	}
}

func finiteFloat(value float64) (float64, bool) {
	if !isFinite(value) {
		return 0, false
	}
	return value, true
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
