package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Server struct{}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Get("/health", s.health)
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/node-types", s.listNodeTypes)
		r.Get("/node-types/{typeName}", s.getNodeType)
		r.Get("/pipelines", s.listPipelines)
		r.Get("/templates", s.listTemplates)
		r.Get("/jobs", s.listJobs)
	})
	r.Route("/internal/schedule/video", func(r chi.Router) {
		r.Get("/status", s.scheduleStatus)
	})
	return r
}
