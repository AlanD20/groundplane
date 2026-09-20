package controller

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Rationale: the operational endpoint must serve exactly the same bytes that
// generation consumes rather than a checked-in or separately assembled copy.
// Delivery: operational endpoint serves generation bytes; not product parity.
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
