package httpapi

import (
	"net/http"

	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/go-chi/chi/v5"
)

func (s *Server) getPipeline(w http.ResponseWriter, r *http.Request) {
	s.withStore(w, func(st *store.Store) {
		row, err := st.GetPipeline(r.Context(), chi.URLParam(r, "pipelineID"))
		s.writeDetailResult(w, row, err, "Pipeline not found")
	})
}

func (s *Server) getAsset(w http.ResponseWriter, r *http.Request) {
	s.withStore(w, func(st *store.Store) {
		row, err := st.GetAssetDetail(r.Context(), chi.URLParam(r, "assetID"))
		s.writeDetailResult(w, row, err, "Asset not found")
	})
}

func (s *Server) getArtifact(w http.ResponseWriter, r *http.Request) {
	s.withStore(w, func(st *store.Store) {
		row, err := st.GetArtifactDetail(r.Context(), chi.URLParam(r, "artifactID"))
		s.writeDetailResult(w, row, err, "Artifact not found")
	})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	s.withStore(w, func(st *store.Store) {
		row, err := st.GetJobDetail(r.Context(), chi.URLParam(r, "jobID"))
		s.writeDetailResult(w, row, err, "Job not found")
	})
}

func (s *Server) withStore(w http.ResponseWriter, fn func(*store.Store)) {
	if s.store == nil {
		if !s.allowStubStore {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "Not found"})
		return
	}
	fn(s.store)
}

func (s *Server) writeDetailResult(w http.ResponseWriter, row any, err error, notFoundDetail string) {
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]string{"detail": notFoundDetail})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, row)
}
