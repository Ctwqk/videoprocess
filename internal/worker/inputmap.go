package worker

import (
	"context"
	"fmt"
)

type ResolvedInputs struct {
	Paths     map[string]string
	MediaInfo map[string]any
}

func (h MediaTaskHandler) BuildInputMap(ctx context.Context, inputArtifacts map[string]any) (ResolvedInputs, func(), error) {
	if len(inputArtifacts) == 0 {
		return ResolvedInputs{}, func() {}, fmt.Errorf("missing input artifacts")
	}
	inputs := ResolvedInputs{
		Paths:     make(map[string]string, len(inputArtifacts)),
		MediaInfo: make(map[string]any, len(inputArtifacts)),
	}
	cleanups := make([]func(), 0, len(inputArtifacts))
	cleanupAll := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	for portName, rawID := range inputArtifacts {
		artifactID, ok := rawID.(string)
		if !ok || artifactID == "" {
			cleanupAll()
			return ResolvedInputs{}, func() {}, fmt.Errorf("missing input artifact on %s port", portName)
		}
		artifact, err := h.env.Store.GetArtifact(ctx, artifactID)
		if err != nil {
			cleanupAll()
			return ResolvedInputs{}, func() {}, fmt.Errorf("load input artifact %s: %w", artifactID, err)
		}
		inputPath, cleanup, err := h.resolveInput(ctx, artifact)
		if err != nil {
			cleanupAll()
			return ResolvedInputs{}, func() {}, err
		}
		cleanups = append(cleanups, cleanup)
		inputs.Paths[portName] = inputPath
		inputs.MediaInfo[portName] = cloneValue(artifact.MediaInfo)
	}
	return inputs, cleanupAll, nil
}

func cloneConfig(config map[string]any) map[string]any {
	cloned := make(map[string]any, len(config)+1)
	for key, value := range config {
		cloned[key] = value
	}
	return cloned
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		cloned := make(map[string]any, len(typed))
		for key, child := range typed {
			cloned[key] = cloneValue(child)
		}
		return cloned
	case []any:
		cloned := make([]any, len(typed))
		for i, child := range typed {
			cloned[i] = cloneValue(child)
		}
		return cloned
	default:
		return typed
	}
}
