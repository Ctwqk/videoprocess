package pipeline

type PortType string

const (
	PortVideo         PortType = "video"
	PortAudio         PortType = "audio"
	PortImage         PortType = "image"
	PortSubtitle      PortType = "subtitle"
	PortAnyMedia      PortType = "any_media"
	PortSearchResults PortType = "search_results"
	PortURLValue      PortType = "url_value"
	PortAssetValue    PortType = "asset_value"
)

type PortDefinition struct {
	Name        string   `json:"name"`
	PortType    PortType `json:"port_type"`
	Required    bool     `json:"required"`
	Description string   `json:"description"`
}

type NodeTypeDefinition struct {
	TypeName    string           `json:"type_name"`
	DisplayName string           `json:"display_name"`
	Category    string           `json:"category"`
	Inputs      []PortDefinition `json:"inputs"`
	Outputs     []PortDefinition `json:"outputs"`
	WorkerType  string           `json:"worker_type"`
}

func BuiltinRegistry() map[string]NodeTypeDefinition {
	return map[string]NodeTypeDefinition{
		"source": {
			TypeName:    "source",
			DisplayName: "Source",
			Category:    "source",
			WorkerType:  "none",
			Outputs: []PortDefinition{
				{Name: "output", PortType: PortAnyMedia, Required: true},
			},
		},
		"trim": {
			TypeName:    "trim",
			DisplayName: "Trim",
			Category:    "transform",
			WorkerType:  "ffmpeg_go",
			Inputs: []PortDefinition{
				{Name: "input", PortType: PortVideo, Required: true},
			},
			Outputs: []PortDefinition{
				{Name: "output", PortType: PortVideo, Required: true},
			},
		},
		"transcode": {
			TypeName:    "transcode",
			DisplayName: "Transcode",
			Category:    "transform",
			WorkerType:  "ffmpeg",
			Inputs: []PortDefinition{
				{Name: "input", PortType: PortAnyMedia, Required: true},
			},
			Outputs: []PortDefinition{
				{Name: "output", PortType: PortAnyMedia, Required: true},
			},
		},
		"export": {
			TypeName:    "export",
			DisplayName: "Export",
			Category:    "output",
			WorkerType:  "ffmpeg",
			Inputs: []PortDefinition{
				{Name: "input", PortType: PortAnyMedia, Required: true},
			},
			Outputs: []PortDefinition{
				{Name: "output", PortType: PortAnyMedia, Required: true},
			},
		},
		"smart_trim": {
			TypeName:    "smart_trim",
			DisplayName: "Smart Trim",
			Category:    "ai_transform",
			WorkerType:  "vision",
			Inputs: []PortDefinition{
				{Name: "input", PortType: PortVideo, Required: true},
			},
			Outputs: []PortDefinition{
				{Name: "output", PortType: PortVideo, Required: true},
			},
		},
	}
}
