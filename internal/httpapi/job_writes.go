package httpapi

import (
	"net/http"
	"strings"

	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/go-chi/chi/v5"
)

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	unsupportedWrite(w, "job creation remains Python-owned until a Python start-job handoff is configured")
}

func (s *Server) createJobBatch(w http.ResponseWriter, r *http.Request) {
	unsupportedWrite(w, "job batch creation remains Python-owned until a Python start-job handoff is configured")
}

func (s *Server) rerunJob(w http.ResponseWriter, r *http.Request) {
	unsupportedWrite(w, "job rerun remains Python-owned until a Python start-job handoff is configured")
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "jobID")
	s.withWriteStore(w, func(st *store.Store) {
		row, err := st.CancelJob(r.Context(), id)
		if err != nil {
			s.writeDetailResult(w, row, err, "Job not found")
			return
		}
		writeJSON(w, http.StatusOK, row)
	})
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "jobID")
	s.withWriteStore(w, func(st *store.Store) {
		err := st.DeleteJob(r.Context(), id)
		if err == nil {
			writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
			return
		}
		if store.IsNotFound(err) {
			notFound(w, "Job not found")
			return
		}
		if store.IsConflict(err) {
			conflict(w, strings.TrimPrefix(err.Error(), store.ErrConflict.Error()+": "))
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
	})
}
