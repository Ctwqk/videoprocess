package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	NewServer().Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["status"] != "ok" {
		t.Fatalf("status payload = %#v", payload)
	}
}

func TestNodeTypesIncludesTrim(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/node-types/trim", nil)
	rec := httptest.NewRecorder()

	NewServer().Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["type_name"] != "trim" {
		t.Fatalf("type_name = %#v", payload["type_name"])
	}
	if payload["worker_type"] != "ffmpeg_go" {
		t.Fatalf("worker_type = %#v", payload["worker_type"])
	}
}

func TestListEndpointsShapeMatchesPython(t *testing.T) {
	// Without a store the API should still return the FastAPI shape
	// `{"items": [...], "total": N}` rather than 500 or undefined.
	cases := []string{
		"/api/v1/pipelines",
		"/api/v1/templates",
		"/api/v1/jobs",
		"/api/v1/assets",
	}
	for _, path := range cases {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()

		NewServer().Router().ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d body=%s", path, rec.Code, rec.Body.String())
		}
		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		items, ok := payload["items"].([]any)
		if !ok {
			t.Fatalf("%s: items field has wrong type %T", path, payload["items"])
		}
		if items == nil {
			t.Fatalf("%s: items must be an empty array, not null", path)
		}
		if _, ok := payload["total"]; !ok {
			t.Fatalf("%s: missing total key", path)
		}
	}
}

func TestReadyzReportsHealthyDependencies(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	srv := NewServerWithOptions(nil, ServerOptions{
		AllowStubStore: true,
		Readiness: ReadinessDeps{
			Postgres: func(context.Context) error { return nil },
		},
	})

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["status"] != "ready" || payload["postgres"] != "ok" {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestReadyzFailsWhenDependencyFails(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	srv := NewServerWithOptions(nil, ServerOptions{
		Readiness: ReadinessDeps{
			Postgres: func(context.Context) error { return errors.New("down") },
		},
	})

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestListEndpointsFailClosedWhenStubStoreDisabled(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
	rec := httptest.NewRecorder()

	NewServerWithOptions(nil, ServerOptions{AllowStubStore: false}).Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestDetailEndpointsFailClosedWhenStubStoreDisabled(t *testing.T) {
	cases := []string{
		"/api/v1/pipelines/00000000-0000-0000-0000-000000000001",
		"/api/v1/assets/00000000-0000-0000-0000-000000000002",
		"/api/v1/artifacts/00000000-0000-0000-0000-000000000003",
		"/api/v1/jobs/00000000-0000-0000-0000-000000000004",
	}
	for _, path := range cases {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()

		NewServerWithOptions(nil, ServerOptions{AllowStubStore: false}).Router().ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s status = %d body=%s", path, rec.Code, rec.Body.String())
		}
		var payload map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("%s payload: %v", path, err)
		}
		if payload["detail"] != "database unavailable" {
			t.Fatalf("%s payload = %#v", path, payload)
		}
	}
}

func TestScheduleStatusFailsClosedWithoutStore(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/internal/schedule/video/status", nil)
	rec := httptest.NewRecorder()

	NewServerWithOptions(nil, ServerOptions{AllowStubStore: false}).Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["detail"] != "database unavailable" {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestRecoveryMiddlewareReturnsFastAPIStyleError(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/panic-test", nil)
	rec := httptest.NewRecorder()
	r := chi.NewRouter()
	r.Use(recoverPanic)
	r.Get("/panic-test", func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["detail"] != "internal server error" {
		t.Fatalf("payload = %#v", payload)
	}
}
