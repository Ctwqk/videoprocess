package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
	if payload["worker_type"] != "ffmpeg" {
		t.Fatalf("worker_type = %#v", payload["worker_type"])
	}
}
