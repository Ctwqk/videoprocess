package pipeline

import "testing"

func TestBuiltinRegistryLoadsManifestNodeTypes(t *testing.T) {
	registry := BuiltinRegistry()
	if len(registry) < 30 {
		t.Fatalf("len(BuiltinRegistry()) = %d, want at least 30", len(registry))
	}

	required := []string{
		"source",
		"trim",
		"transcode",
		"export",
		"vertical_crop",
		"watermark",
		"title_overlay",
		"bgm",
		"replace_audio",
		"concat_horizontal",
		"concat_vertical",
		"concat_many",
		"concat_timeline",
		"concat_vertical_timeline",
		"montage_assembler",
		"smart_trim",
		"zip_records",
		"youtube_upload",
		"x_upload",
		"xiaohongshu_upload",
	}
	for _, typeName := range required {
		if _, ok := registry[typeName]; !ok {
			t.Fatalf("BuiltinRegistry() missing required node type %q", typeName)
		}
	}
}

func TestWatermarkInputPortsMatchManifest(t *testing.T) {
	watermark, ok := BuiltinRegistry()["watermark"]
	if !ok {
		t.Fatal("BuiltinRegistry() missing watermark")
	}
	if len(watermark.Inputs) != 2 {
		t.Fatalf("watermark inputs = %#v, want two ports", watermark.Inputs)
	}
	got := []string{watermark.Inputs[0].Name, watermark.Inputs[1].Name}
	want := []string{"video", "overlay"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("watermark input names = %#v, want %#v", got, want)
		}
	}
}

func TestXiaohongshuUploadExists(t *testing.T) {
	if _, ok := BuiltinRegistry()["xiaohongshu_upload"]; !ok {
		t.Fatal("BuiltinRegistry() missing xiaohongshu_upload")
	}
}
