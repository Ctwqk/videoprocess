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

type ParamDefinition struct {
	Name        string `json:"name"`
	ParamType   string `json:"param_type"`
	Required    bool   `json:"required"`
	Default     any    `json:"default"`
	Options     []any  `json:"options"`
	MinValue    any    `json:"min_value"`
	MaxValue    any    `json:"max_value"`
	Description string `json:"description"`
}

type NodeTypeDefinition struct {
	TypeName    string            `json:"type_name"`
	DisplayName string            `json:"display_name"`
	Category    string            `json:"category"`
	Description string            `json:"description"`
	Icon        string            `json:"icon"`
	Inputs      []PortDefinition  `json:"inputs"`
	Outputs     []PortDefinition  `json:"outputs"`
	Params      []ParamDefinition `json:"params"`
	WorkerType  string            `json:"worker_type"`
}

func BuiltinRegistry() map[string]NodeTypeDefinition {
	registry, err := loadBuiltinRegistryManifest()
	if err != nil {
		panic(err)
	}
	return registry
}

type nodeRegistryManifest struct {
	SchemaVersion int                  `json:"schema_version"`
	NodeTypes     []NodeTypeDefinition `json:"node_types"`
}

func registryFromManifest(manifest nodeRegistryManifest) (map[string]NodeTypeDefinition, error) {
	if manifest.SchemaVersion != 1 {
		return nil, errInvalidRegistryManifest("unsupported schema_version")
	}
	if len(manifest.NodeTypes) == 0 {
		return nil, errInvalidRegistryManifest("node_types must not be empty")
	}
	registry := make(map[string]NodeTypeDefinition, len(manifest.NodeTypes))
	for _, node := range manifest.NodeTypes {
		if node.TypeName == "" {
			return nil, errInvalidRegistryManifest("node type missing type_name")
		}
		if _, ok := registry[node.TypeName]; ok {
			return nil, errInvalidRegistryManifest("duplicate node type " + node.TypeName)
		}
		registry[node.TypeName] = node
	}
	return registry, nil
}
