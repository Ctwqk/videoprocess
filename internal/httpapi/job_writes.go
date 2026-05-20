package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/go-chi/chi/v5"
)

type goJobCreateRequest struct {
	PipelineID string         `json:"pipeline_id"`
	Inputs     map[string]any `json:"inputs"`
}

type goJobBatchRequest struct {
	PipelineID string           `json:"pipeline_id"`
	Inputs     []map[string]any `json:"inputs"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	if !s.goJobsEnabled || s.goJobs == nil {
		unsupportedWrite(w, "Go orchestrator job writes are disabled")
		return
	}
	var body goJobCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		badRequest(w, "Invalid request body")
		return
	}
	row, err := s.goJobs.CreateJob(r.Context(), body.PipelineID, body.Inputs)
	if err != nil {
		writeGoJobError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, row)
}

func (s *Server) createJobBatch(w http.ResponseWriter, r *http.Request) {
	if !s.goJobsEnabled || s.goJobs == nil {
		unsupportedWrite(w, "Go orchestrator job writes are disabled")
		return
	}
	var body goJobBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		badRequest(w, "Invalid request body")
		return
	}
	rows, err := s.goJobs.CreateJobBatch(r.Context(), body.PipelineID, body.Inputs)
	if err != nil {
		writeGoJobError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, rows)
}

func (s *Server) rerunJob(w http.ResponseWriter, r *http.Request) {
	if !s.goJobsEnabled || s.goJobs == nil {
		unsupportedWrite(w, "Go orchestrator job writes are disabled")
		return
	}
	row, err := s.goJobs.RerunJob(r.Context(), chi.URLParam(r, "jobID"))
	if err != nil {
		writeGoJobError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, row)
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

type unsupportedGoJobError interface {
	UnsupportedReason() string
	Error() string
}

func writeGoJobError(w http.ResponseWriter, err error) {
	var unsupported unsupportedGoJobError
	if errors.As(err, &unsupported) {
		unsupportedWrite(w, unsupported.Error())
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
}
