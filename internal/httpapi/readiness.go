package httpapi

import (
	"context"
	"net/http"
	"time"
)

type ReadinessProbe func(context.Context) error

type ReadinessDeps struct {
	Postgres ReadinessProbe
	Redis    ReadinessProbe
	Storage  ReadinessProbe
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	payload := map[string]string{"status": "ready"}
	status := http.StatusOK
	check := func(name string, probe ReadinessProbe) {
		if probe == nil {
			return
		}
		if err := probe(ctx); err != nil {
			payload["status"] = "not_ready"
			payload[name] = "error"
			status = http.StatusServiceUnavailable
			return
		}
		payload[name] = "ok"
	}

	check("postgres", s.readiness.Postgres)
	check("redis", s.readiness.Redis)
	check("storage", s.readiness.Storage)

	writeJSON(w, status, payload)
}
