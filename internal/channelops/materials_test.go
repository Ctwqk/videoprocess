package channelops

import (
	"math"
	"testing"
)

func TestExtractMaterialReferencesPrefersMaterialID(t *testing.T) {
	refs := ExtractMaterialReferences(map[string]any{
		"clips": []any{
			map[string]any{
				"material_id": "mat-1",
				"asset_id":    "asset-1",
				"start_sec":   1.5,
				"end_sec":     4.0,
			},
		},
	})

	if len(refs) != 1 {
		t.Fatalf("refs len = %d", len(refs))
	}
	if refs[0].MaterialID != "mat-1" || refs[0].AssetID != "asset-1" {
		t.Fatalf("ref = %#v", refs[0])
	}
	if refs[0].StartMS == nil || *refs[0].StartMS != 1500 {
		t.Fatalf("StartMS = %#v", refs[0].StartMS)
	}
}

func TestExtractMaterialReferencesFallsBackToAssetID(t *testing.T) {
	refs := ExtractMaterialReferences(map[string]any{"asset_id": "asset-legacy"})

	if len(refs) != 1 {
		t.Fatalf("refs len = %d", len(refs))
	}
	if refs[0].MaterialID != "asset-legacy" {
		t.Fatalf("MaterialID = %s", refs[0].MaterialID)
	}
}

func TestExtractMaterialReferencesDedupesMaterialAndSegmentSignature(t *testing.T) {
	refs := ExtractMaterialReferences(map[string]any{
		"clips": []any{
			map[string]any{"material_id": "mat-1", "segment_signature": "seg-a", "start_ms": 100},
			map[string]any{"material_id": "mat-1", "segment_signature": "seg-a", "start_ms": 200},
			map[string]any{"material_id": "mat-1", "segment_signature": "seg-b", "start_ms": 200},
		},
	})

	if len(refs) != 2 {
		t.Fatalf("refs len = %d refs=%#v", len(refs), refs)
	}
	if refs[0].SegmentSignature != "seg-a" || refs[1].SegmentSignature != "seg-b" {
		t.Fatalf("refs = %#v", refs)
	}
}

func TestExtractMaterialReferencesReadsCamelCaseSegmentSignature(t *testing.T) {
	refs := ExtractMaterialReferences(map[string]any{
		"materialId":       "mat-1",
		"segmentSignature": "seg-camel",
	})

	if len(refs) != 1 {
		t.Fatalf("refs len = %d refs=%#v", len(refs), refs)
	}
	if refs[0].SegmentSignature != "seg-camel" {
		t.Fatalf("SegmentSignature = %q", refs[0].SegmentSignature)
	}
}

func TestExtractMaterialReferencesDedupeKeyDoesNotCollideOnColon(t *testing.T) {
	refs := ExtractMaterialReferences(map[string]any{
		"clips": []any{
			map[string]any{"material_id": "a:b", "segment_signature": "c"},
			map[string]any{"material_id": "a", "segment_signature": "b:c"},
		},
	})

	if len(refs) != 2 {
		t.Fatalf("refs len = %d refs=%#v", len(refs), refs)
	}
}

func TestExtractMaterialReferencesSkipsPayloadWithoutMaterialOrAssetID(t *testing.T) {
	refs := ExtractMaterialReferences(map[string]any{
		"clips": []any{
			map[string]any{"material_id": nil, "asset_id": nil, "start_sec": 1.5},
			map[string]any{"title": "no material"},
		},
	})

	if len(refs) != 0 {
		t.Fatalf("refs len = %d refs=%#v", len(refs), refs)
	}
}

func TestExtractMaterialReferencesKeepsRefWithInvalidStartValues(t *testing.T) {
	refs := ExtractMaterialReferences(
		map[string]any{"material_id": "mat-inf", "start_sec": math.Inf(1)},
		map[string]any{"material_id": "mat-overflow", "start_ms": uint64(math.MaxUint64)},
	)

	if len(refs) != 2 {
		t.Fatalf("refs len = %d refs=%#v", len(refs), refs)
	}
	for _, ref := range refs {
		if ref.StartMS != nil {
			t.Fatalf("StartMS for %s = %#v, want nil", ref.MaterialID, ref.StartMS)
		}
	}
}
