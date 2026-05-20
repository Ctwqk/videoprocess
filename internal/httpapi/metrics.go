package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests handled by api-go.",
	}, []string{"method", "route", "status"})
	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration for api-go.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})
	httpRequestErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_request_errors_total",
		Help: "Total HTTP requests with 5xx status handled by api-go.",
	}, []string{"method", "route", "status"})
)

func init() {
	httpRequestsTotal.WithLabelValues("GET", "/metrics", "200")
	httpRequestDuration.WithLabelValues("GET", "/metrics", "200")
	httpRequestErrorsTotal.WithLabelValues("GET", "/metrics", "500")
}

func metricsHandler() http.Handler {
	return promhttp.Handler()
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = r.URL.Path
		}
		status := strconv.Itoa(rec.status)
		httpRequestsTotal.WithLabelValues(r.Method, route, status).Inc()
		httpRequestDuration.WithLabelValues(r.Method, route, status).Observe(time.Since(start).Seconds())
		if rec.status >= http.StatusInternalServerError {
			httpRequestErrorsTotal.WithLabelValues(r.Method, route, status).Inc()
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}
