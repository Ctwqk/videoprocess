package httpapi

import (
	"net/http"

	"github.com/Ctwqk/videoprocess/internal/storage"
	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/go-chi/chi/v5"
)

// Server holds shared dependencies for HTTP handlers. The store is optional;
// when nil, list endpoints respond with empty pages so unit tests can run
// without Postgres. Production cmd/vp-api wires a real store.
type Server struct {
	store          *store.Store
	storage        storage.Backend
	storageBackend string
	readiness      ReadinessDeps
	allowStubStore bool
	goJobsEnabled  bool
	goJobs         GoJobService
	schedule       ScheduleController
}

type ServerOptions struct {
	Readiness      ReadinessDeps
	AllowStubStore bool
	Storage        storage.Backend
	StorageBackend string
	GoJobsEnabled  bool
	GoJobs         GoJobService
	Schedule       ScheduleController
}

// NewServer constructs a Server without a backing store. Useful for tests
// that only exercise the routing/health surface.
func NewServer() *Server {
	return &Server{allowStubStore: true}
}

// NewServerWithStore is the production constructor used by cmd/vp-api.
func NewServerWithStore(s *store.Store) *Server {
	return &Server{store: s, allowStubStore: true, schedule: NewStoreScheduleController(s)}
}

func NewServerWithOptions(s *store.Store, opts ServerOptions) *Server {
	schedule := opts.Schedule
	if schedule == nil && s != nil {
		schedule = NewStoreScheduleController(s)
	}
	return &Server{
		store:          s,
		storage:        opts.Storage,
		storageBackend: opts.StorageBackend,
		readiness:      opts.Readiness,
		allowStubStore: opts.AllowStubStore,
		goJobsEnabled:  opts.GoJobsEnabled,
		goJobs:         opts.GoJobs,
		schedule:       schedule,
	}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(requestID)
	r.Use(metricsMiddleware)
	r.Use(recoverPanic)
	r.Use(logRequests)
	r.Get("/health", s.health)
	r.Get("/readyz", s.readyz)
	r.Handle("/metrics", metricsHandler())
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/node-types", s.listNodeTypes)
		r.Get("/node-types/{typeName}", s.getNodeType)
		r.Get("/pipelines", s.listPipelines)
		r.Post("/pipelines", s.createPipeline)
		r.Post("/pipelines/validate", s.validatePipeline)
		r.Get("/pipelines/{pipelineID}", s.getPipeline)
		r.Put("/pipelines/{pipelineID}", s.updatePipeline)
		r.Delete("/pipelines/{pipelineID}", s.deletePipeline)
		r.Post("/pipelines/{pipelineID}/duplicate", s.duplicatePipeline)
		r.Get("/templates", s.listTemplates)
		r.Get("/assets", s.listAssets)
		r.Get("/assets/{assetID}", s.getAsset)
		r.Post("/assets/upload", s.uploadAsset)
		r.Get("/assets/{assetID}/download", s.downloadAsset)
		r.Delete("/assets/{assetID}", s.deleteAsset)
		r.Get("/artifacts/{artifactID}", s.getArtifact)
		r.Get("/artifacts/{artifactID}/download", s.downloadArtifact)
		r.Delete("/artifacts/cleanup", s.cleanupArtifacts)
		r.Get("/jobs", s.listJobs)
		r.Post("/jobs", s.createJob)
		r.Post("/jobs/batch", s.createJobBatch)
		r.Get("/jobs/{jobID}", s.getJob)
		r.Post("/jobs/{jobID}/cancel", s.cancelJob)
		r.Post("/jobs/{jobID}/rerun", s.rerunJob)
		r.Delete("/jobs/{jobID}", s.deleteJob)
	})
	r.Route("/internal/schedule/video", func(r chi.Router) {
		r.Get("/status", s.scheduleStatus)
		r.Post("/open", s.openVideoSchedule)
		r.Post("/drain", s.drainVideoSchedule)
		r.Post("/close", s.closeVideoSchedule)
	})
	return r
}
