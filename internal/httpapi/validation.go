package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/pipeline"
)

func (s *Server) validatePipeline(w http.ResponseWriter, r *http.Request) {
	var def contracts.PipelineDefinition
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&def); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid pipeline definition"})
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid pipeline definition"})
		return
	}
	if def.Nodes == nil || def.Edges == nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid pipeline definition"})
		return
	}
	writeJSON(w, http.StatusOK, pipeline.Validate(def))
}
