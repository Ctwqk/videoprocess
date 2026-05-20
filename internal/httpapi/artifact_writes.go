package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/go-chi/chi/v5"
)

func (s *Server) downloadArtifact(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "artifactID")
	s.withStore(w, func(st *store.Store) {
		row, err := st.GetArtifact(r.Context(), id)
		if err != nil {
			s.writeDetailResult(w, row, err, "Artifact not found")
			return
		}
		s.writeStoredObject(w, r, row.StoragePath, row.Filename, row.MimeType)
	})
}

func (s *Server) cleanupArtifacts(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "storage unavailable"})
		return
	}
	var jobID *string
	if raw := r.URL.Query().Get("job_id"); raw != "" {
		jobID = &raw
	}
	s.withWriteStore(w, func(st *store.Store) {
		candidates, err := st.CleanupArtifactCandidates(r.Context(), jobID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		result := store.ArtifactCleanupResult{}
		for _, candidate := range candidates {
			if candidate.DeleteStorage {
				if err := s.storage.Delete(r.Context(), candidate.StoragePath); err != nil {
					slog.Warn("artifact cleanup storage delete failed", "artifact_id", candidate.ID, "path", candidate.StoragePath, "error", err)
				}
			}
			if err := st.DeleteArtifactRecord(r.Context(), candidate.ID); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
				return
			}
			result.DeletedCount++
			result.FreedBytes += candidate.FileSize
		}
		writeJSON(w, http.StatusOK, result)
	})
}
