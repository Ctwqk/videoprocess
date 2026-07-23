package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/go-chi/chi/v5"
)

func (s *Server) uploadAsset(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "storage unavailable"})
		return
	}
	s.withWriteStore(w, func(st *store.Store) {
		file, header, err := r.FormFile("file")
		if err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "file field is required"})
			return
		}
		defer file.Close()
		data, err := io.ReadAll(file)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		original := header.Filename
		if strings.TrimSpace(original) == "" {
			original = "unknown"
		}
		ext := filepath.Ext(original)
		filename := randomHexName(ext)
		storagePath := "assets/" + filename
		mimeType := header.Header.Get("Content-Type")
		if mimeType == "" {
			mimeType = mime.TypeByExtension(ext)
		}
		if err := s.storage.Save(r.Context(), storagePath, data); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		var mimePtr *string
		if mimeType != "" {
			mimePtr = &mimeType
		}
		row, err := st.CreateAsset(r.Context(), store.CreateAssetInput{
			Filename:       filename,
			OriginalName:   original,
			MimeType:       mimePtr,
			FileSize:       int64(len(data)),
			StorageBackend: s.storageBackendName(),
			StoragePath:    storagePath,
			MediaInfo:      nil,
		})
		if err != nil {
			_ = s.storage.Delete(r.Context(), storagePath)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, row)
	})
}

func (s *Server) downloadAsset(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "assetID")
	s.withStore(w, func(st *store.Store) {
		row, err := st.GetAssetForDownload(r.Context(), id)
		if err != nil {
			s.writeDetailResult(w, row, err, "Asset not found")
			return
		}
		s.writeStoredObject(w, r, row.StoragePath, row.OriginalName, row.MimeType)
	})
}

func (s *Server) deleteAsset(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "storage unavailable"})
		return
	}
	id := chi.URLParam(r, "assetID")
	s.withWriteStore(w, func(st *store.Store) {
		row, err := st.PrepareDeleteAsset(r.Context(), id)
		if err != nil {
			if store.IsNotFound(err) {
				notFound(w, "Asset not found")
				return
			}
			if store.IsConflict(err) {
				conflict(w, strings.TrimPrefix(err.Error(), store.ErrConflict.Error()+": "))
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		if err := s.storage.Delete(r.Context(), row.StoragePath); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		if err := st.DeleteAssetRecord(r.Context(), id); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
	})
}

func (s *Server) writeStoredObject(w http.ResponseWriter, r *http.Request, storagePath string, filename string, mimeType *string) {
	if s.storage == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "storage unavailable"})
		return
	}
	if localPath, ok := s.storage.LocalPath(storagePath); ok {
		file, err := os.Open(localPath)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() {
			if err == nil {
				err = fmt.Errorf("stored object is not a regular file")
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
			return
		}
		if mimeType != nil && *mimeType != "" {
			w.Header().Set("Content-Type", *mimeType)
		}
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
		http.ServeContent(w, r, filename, info.ModTime(), file)
		return
	}
	data, err := s.storage.Read(r.Context(), storagePath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	if mimeType != nil && *mimeType != "" {
		w.Header().Set("Content-Type", *mimeType)
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func randomHexName(ext string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "asset" + ext
	}
	return hex.EncodeToString(bytes[:]) + ext
}

func (s *Server) storageBackendName() string {
	if s.storageBackend != "" {
		return s.storageBackend
	}
	return "local"
}
