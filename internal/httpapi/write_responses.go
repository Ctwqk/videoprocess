package httpapi

import "net/http"

func badRequest(w http.ResponseWriter, detail string) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"detail": detail})
}

func notFound(w http.ResponseWriter, detail string) {
	writeJSON(w, http.StatusNotFound, map[string]string{"detail": detail})
}

func conflict(w http.ResponseWriter, detail string) {
	writeJSON(w, http.StatusConflict, map[string]string{"detail": detail})
}

func unsupportedWrite(w http.ResponseWriter, detail string) {
	writeJSON(w, http.StatusNotImplemented, map[string]string{"detail": detail})
}
