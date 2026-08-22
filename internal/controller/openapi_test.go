package controller

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Rationale: one deterministic code-first document must carry the API base
// path and typed operation identity consumed by both client generators.
func TestOpenAPIDocumentIsDeterministicAndCodeFirst(t *testing.T) {
	t.Parallel()
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	first, err := server.OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument() error = %v", err)
	}
	second, err := server.OpenAPIDocument()
	if err != nil {
		t.Fatalf("second OpenAPIDocument() error = %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("OpenAPI serialization changed without a contract change")
	}

	var document struct {
		OpenAPI string `json:"openapi"`
		Servers []struct {
			URL string `json:"url"`
		} `json:"servers"`
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(first, &document); err != nil {
		t.Fatalf("decode OpenAPI: %v", err)
	}
	if document.OpenAPI != "3.1.0" || len(document.Servers) != 1 || document.Servers[0].URL != "/api/v1" {
		t.Fatalf("OpenAPI/base servers = %q %#v", document.OpenAPI, document.Servers)
	}
	if operation := document.Paths["/host"]["get"]; operation.OperationID != "host.show" {
		t.Fatalf("GET /host operation = %#v, want host.show", operation)
	}
}

// Rationale: the operational endpoint must serve exactly the same bytes that
// generation consumes rather than a checked-in or separately assembled copy.
func TestOpenAPIEndpointServesCodeFirstDocument(t *testing.T) {
	t.Parallel()
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	want, err := server.OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument() error = %v", err)
	}
	response := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("OpenAPI response = %d %#v", response.Code, response.Header())
	}
	if !bytes.Equal(response.Body.Bytes(), want) {
		t.Fatal("served OpenAPI differs from code-first generation bytes")
	}
}
