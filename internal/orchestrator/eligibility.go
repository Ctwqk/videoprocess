package orchestrator

import "github.com/Ctwqk/videoprocess/internal/contracts"

type EligibilityResult struct {
	Eligible bool
	Reason   string
}

var firstWaveGoNodeTypes = []string{
	"bgm",
	"concat_horizontal",
	"concat_many",
	"concat_timeline",
	"concat_vertical",
	"concat_vertical_timeline",
	"export",
	"montage_assembler",
	"replace_audio",
	"title_overlay",
	"transcode",
	"trim",
	"vertical_crop",
	"watermark",
}

func FirstWaveGoNodeTypes() []string {
	out := make([]string, len(firstWaveGoNodeTypes))
	copy(out, firstWaveGoNodeTypes)
	return out
}

// ClassifyGoEligibility is owned by Task 2. Until that implementation lands,
// it fails closed so Go job writes cannot accidentally claim Python-owned work.
func ClassifyGoEligibility(def contracts.PipelineDefinition) EligibilityResult {
	return EligibilityResult{
		Eligible: false,
		Reason:   "Go eligibility classifier is not implemented yet",
	}
}
