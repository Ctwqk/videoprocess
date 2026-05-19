package httpapi

import "net/http"

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		if !s.allowStubStore {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, emptyPage())
		return
	}
	opts := paginationFromRequest(r)
	var pipelineID *string
	if raw := r.URL.Query().Get("pipeline_id"); raw != "" {
		pipelineID = &raw
	}
	var status *string
	if raw := r.URL.Query().Get("status"); raw != "" {
		status = &raw
	}
	items, total, err := s.store.ListJobs(r.Context(), opts, pipelineID, status)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

func (s *Server) scheduleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"state": "OPEN"})
}

func (s *Server) listAssets(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		if !s.allowStubStore {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, emptyPage())
		return
	}
	opts := paginationFromRequest(r)
	items, total, err := s.store.ListAssets(r.Context(), opts)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}
