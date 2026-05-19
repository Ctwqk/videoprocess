package httpapi

import (
	"net/http"
	"sort"

	"github.com/Ctwqk/videoprocess/internal/pipeline"
	"github.com/go-chi/chi/v5"
)

func (s *Server) listNodeTypes(w http.ResponseWriter, r *http.Request) {
	registry := pipeline.BuiltinRegistry()
	items := make([]pipeline.NodeTypeDefinition, 0, len(registry))
	for _, item := range registry {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].TypeName < items[j].TypeName
	})
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) getNodeType(w http.ResponseWriter, r *http.Request) {
	typeName := chi.URLParam(r, "typeName")
	item, ok := pipeline.BuiltinRegistry()[typeName]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "Node type not found"})
		return
	}
	writeJSON(w, http.StatusOK, item)
}
