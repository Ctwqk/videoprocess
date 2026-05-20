package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestMetricsEndpointExposesHTTPMetrics(t *testing.T) {
	server := NewServer()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, metric := range []string{
		"http_requests_total",
		"http_request_duration_seconds",
		"http_request_errors_total",
	} {
		if !strings.Contains(body, metric) {
			t.Fatalf("metrics body missing %s: %s", metric, body)
		}
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

func TestValidatePipelineReturnsValidationResult(t *testing.T) {
	body := `{
		"nodes": [
			{"id":"source_1","type":"source","position":{},"data":{"label":"Source","asset_id":"00000000-0000-0000-0000-000000000001"}},
			{"id":"export_1","type":"export","position":{},"data":{"label":"Export"}}
		],
		"edges": [
			{"id":"edge_1","source":"source_1","target":"export_1","sourceHandle":"output","targetHandle":"input"}
		],
		"viewport": {"x":0,"y":0,"zoom":1}
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/validate", strings.NewReader(body))
	rec := httptest.NewRecorder()

	NewServer().Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["valid"] != true {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestValidatePipelineRejectsMalformedJSON(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "broken json", body: `{"nodes": [`},
		{name: "missing required fields", body: `{}`},
		{name: "trailing object", body: `{"nodes":[],"edges":[],"viewport":{}} {}`},
		{name: "trailing garbage", body: `{"nodes":[],"edges":[],"viewport":{}} trailing`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/validate", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()

			NewServer().Router().ServeHTTP(rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
			}
			var payload map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload["detail"] != "invalid pipeline definition" {
				t.Fatalf("payload = %#v", payload)
			}
		})
	}
}

func TestPipelineCreateRejectsUnsupportedGraphBeforeStore(t *testing.T) {
	body := `{
		"name": "python-owned",
		"description": "",
		"is_template": false,
		"template_tags": [],
		"definition": {
			"nodes": [
				{"id":"search_1","type":"youtube_search","position":{},"data":{"label":"Search","config":{}}}
			],
			"edges": [],
			"viewport": {"x":0,"y":0,"zoom":1}
		}
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", strings.NewReader(body))
	rec := httptest.NewRecorder()

	NewServer().Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Python") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestPipelineCreateRequiresStoreAfterValidation(t *testing.T) {
	body := `{
		"name": "go-owned",
		"description": "",
		"is_template": false,
		"template_tags": [],
		"definition": {
			"nodes": [
				{"id":"source_1","type":"source","position":{},"data":{"label":"Source","asset_id":"00000000-0000-0000-0000-000000000001","config":{}}},
				{"id":"export_1","type":"export","position":{},"data":{"label":"Export","config":{}}}
			],
			"edges": [
				{"id":"edge_1","source":"source_1","target":"export_1","sourceHandle":"output","targetHandle":"input"}
			],
			"viewport": {"x":0,"y":0,"zoom":1}
		}
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", strings.NewReader(body))
	rec := httptest.NewRecorder()

	NewServer().Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestPipelineCreateRejectsMalformedJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", strings.NewReader("{"))
	rec := httptest.NewRecorder()

	NewServer().Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"detail"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestJobCreationIsExplicitlyPythonOwned(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", strings.NewReader(`{"pipeline_id":"00000000-0000-0000-0000-000000000000"}`))
	rec := httptest.NewRecorder()

	NewServer().Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Python") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestJobCancelRequiresStore(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/00000000-0000-0000-0000-000000000000/cancel", nil)
	rec := httptest.NewRecorder()

	NewServer().Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
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
