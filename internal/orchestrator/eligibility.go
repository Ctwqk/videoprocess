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

func FirstWaveGoNodeTypes() []string{
	out := make([]string, len(firstWaveGoNodeTypes))
	copy(out, firstWaveGoNodeTypes)
	return out
}

func FirstWaveGoNodeTypesSet() map[string]struct{} {
	out := make(map[string]struct{}, len(firstWaveGoNodeTypes))
	for _, nodeType := range firstWaveGoNodeTypes{
		out[nodeType] = struct{}{}
	}
	return out
}

func sourceHasAssetID(node contracts.PipelineNode) bool{
	if node.Data.AssetID != nil && *node.Data.AssetID != ""{
		return true
	}
	
	if node.Data.Config == nil{
		return false
	}

	raw, ok := node.Data.Config["asset_id"]
	if !ok{
		return false
	}
	assetID, ok := raw.(string)
	return ok && assetID != ""
}


// ClassifyGoEligibility is owned by Task 2. Until that implementation lands,
// it fails closed so Go job writes cannot accidentally claim Python-owned work.
func ClassifyGoEligibility(def contracts.PipelineDefinition) EligibilityResult {
	allowed := FirstWaveGoNodeTypesSet()
	for _, node := range def.Nodes {
		if node.Type == "source" {
			if !sourceHasAssetID(node){
				return EligibilityResult{
					Eligible: false,
					Reason: "source node",
				}
			}
			continue
		}
		if _, ok := allowed[node.Type]; !ok  {
			return EligibilityResult{
				Eligible: false,
				Reason: `node type "` + node.Type + `" remains Python-owned`,
			}
		}
		
	}
	return EligibilityResult{Eligible: true}
}
