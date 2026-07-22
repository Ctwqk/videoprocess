package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

func (s *Server) openVideoSchedule(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("expected_job_id") {
		expectedJobID := r.URL.Query().Get("expected_job_id")
		parsedJobID, err := uuid.Parse(expectedJobID)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "invalid expected_job_id"})
			return
		}
		if s.schedule == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
			return
		}
		row, err := s.schedule.OpenExpectedJob(r.Context(), parsedJobID.String())
		if errors.Is(err, ErrScheduleGuardMismatch) {
			writeJSON(w, http.StatusConflict, map[string]string{"detail": "guarded schedule open conflict"})
			return
		}
		if err != nil {
			slog.ErrorContext(r.Context(), "guarded schedule open failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "guarded_schedule_open_failed"})
			return
		}
		writeJSON(w, http.StatusOK, row)
		return
	}
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
