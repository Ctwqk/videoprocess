package httpapi

import (
	"net/http"
	"strconv"

	"github.com/Ctwqk/videoprocess/internal/store"
)

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {
	s.respondPipelineList(w, r, nil)
}

func (s *Server) listTemplates(w http.ResponseWriter, r *http.Request) {
	t := true
	s.respondPipelineList(w, r, &t)
}

func (s *Server) respondPipelineList(w http.ResponseWriter, r *http.Request, override *bool) {
	if s.store == nil {
		if !s.allowStubStore {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, emptyPage())
		return
	}
	opts := paginationFromRequest(r)
	isTemplate := override
	if isTemplate == nil {
		if raw := r.URL.Query().Get("is_template"); raw != "" {
			parsed, err := strconv.ParseBool(raw)
			if err == nil {
				isTemplate = &parsed
			}
		}
	}
	items, total, err := s.store.ListPipelines(r.Context(), opts, isTemplate)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func paginationFromRequest(r *http.Request) store.PageOptions {
	skip, _ := strconv.Atoi(r.URL.Query().Get("skip"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	return store.PageOptions{Skip: skip, Limit: limit}
}

// emptyPage matches the FastAPI shape `{"items": [], "total": 0}` with an
// always-non-null `items` array to mirror Pydantic serialization.
func emptyPage() map[string]any {
	return map[string]any{"items": []any{}, "total": 0}
}
