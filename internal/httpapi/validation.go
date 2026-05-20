package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/pipeline"
)

func (s *Server) validatePipeline(w http.ResponseWriter, r *http.Request) {
	var def contracts.PipelineDefinition
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid pipeline definition"})
		return
	}
	writeJSON(w, http.StatusOK, pipeline.Validate(def))
}
