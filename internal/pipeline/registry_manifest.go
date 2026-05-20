package pipeline

import (
	_ "embed"
	"encoding/json"
	"errors"
)

//go:embed testdata/node_registry_manifest.json
var builtinRegistryManifestJSON []byte

type errInvalidRegistryManifest string

func (e errInvalidRegistryManifest) Error() string {
	return "invalid node registry manifest: " + string(e)
}

func loadBuiltinRegistryManifest() (map[string]NodeTypeDefinition, error) {
	var manifest nodeRegistryManifest
	if err := json.Unmarshal(builtinRegistryManifestJSON, &manifest); err != nil {
		return nil, errors.Join(errInvalidRegistryManifest("decode failed"), err)
	}
	return registryFromManifest(manifest)
}
