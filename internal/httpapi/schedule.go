package httpapi

import "net/http"

func (s *Server) scheduleStatus(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		if !s.allowStubStore {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "schedule store unavailable"})
		return
	}
	row, err := s.store.GetVideoScheduleStatus(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, row)
}
