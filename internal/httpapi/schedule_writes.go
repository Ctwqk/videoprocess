package httpapi

import (
	"net/http"
)

func (s *Server) openVideoSchedule(w http.ResponseWriter, r *http.Request) {
	s.setVideoSchedule(w, r, "OPEN")
}

func (s *Server) drainVideoSchedule(w http.ResponseWriter, r *http.Request) {
	s.setVideoSchedule(w, r, "DRAINING")
}

func (s *Server) closeVideoSchedule(w http.ResponseWriter, r *http.Request) {
	s.setVideoSchedule(w, r, "CLOSED")
}

func (s *Server) setVideoSchedule(w http.ResponseWriter, r *http.Request, state string) {
	if s.schedule == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
		return
	}
	row, err := s.schedule.SetState(r.Context(), state)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, row)
}
