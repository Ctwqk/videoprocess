package contracts

type PipelineNodeData struct {
	Label   string         `json:"label"`
	Config  map[string]any `json:"config"`
	AssetID *string        `json:"asset_id"`
}

type PipelineNode struct {
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Position map[string]float64 `json:"position"`
	Data     PipelineNodeData   `json:"data"`
}

type PipelineEdge struct {
	ID           string `json:"id"`
	Source       string `json:"source"`
	Target       string `json:"target"`
	SourceHandle string `json:"sourceHandle"`
	TargetHandle string `json:"targetHandle"`
}

type PipelineDefinition struct {
	Nodes    []PipelineNode     `json:"nodes"`
	Edges    []PipelineEdge     `json:"edges"`
	Viewport map[string]float64 `json:"viewport"`
}

type ValidationError struct {
	Type       string   `json:"type"`
	Message    string   `json:"message"`
	NodeID     *string  `json:"node_id"`
	EdgeID     *string  `json:"edge_id"`
	Nodes      []string `json:"nodes"`
	SourcePort *string  `json:"source_port"`
	TargetPort *string  `json:"target_port"`
	ParamName  *string  `json:"param_name"`
}

type ValidationWarning struct {
	Type    string  `json:"type"`
	Message string  `json:"message"`
	NodeID  *string `json:"node_id"`
}

type ValidationResult struct {
	Valid    bool                `json:"valid"`
	Errors   []ValidationError   `json:"errors"`
	Warnings []ValidationWarning `json:"warnings"`
}
