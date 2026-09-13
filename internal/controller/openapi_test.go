package controller

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
)

// Rationale: one deterministic code-first document must carry the API base
// path and typed operation identity consumed by both client generators.
// Delivery: deterministic generation metadata and one operation identity; not runtime coverage.
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

// Rationale: the committed document is the input to both client generators,
// so it must be byte-identical to the deterministic live Huma API object.
// Delivery: committed generated artifact drift guard; not product coverage.
func TestCommittedOpenAPIMatchesCodeFirstDocument(t *testing.T) {
	t.Parallel()
	want, err := New(nil, nil, Options{}).OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument() error = %v", err)
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	got, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "..", "..", "openapi.json"))
	if err != nil {
		t.Fatalf("read committed OpenAPI: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("committed openapi.json has generation drift; run make api")
	}
}

// Rationale: every human API failure has one closed five-member schema; Huma
// extensions must not widen generated clients beyond the authoritative tuple.
// Delivery: closed generated error schema; serialization is tested separately.
func TestOpenAPIProblemSchemaIsExact(t *testing.T) {
	t.Parallel()
	document, err := New(nil, nil, Options{}).OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument() error = %v", err)
	}
	var contract struct {
		Components struct {
			Schemas map[string]struct {
				AdditionalProperties *bool                      `json:"additionalProperties"`
				Properties           map[string]json.RawMessage `json:"properties"`
				Required             []string                   `json:"required"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(document, &contract); err != nil {
		t.Fatalf("decode OpenAPI: %v", err)
	}
	schema := contract.Components.Schemas["Error"]
	if schema.AdditionalProperties == nil || *schema.AdditionalProperties {
		t.Fatalf("Error.additionalProperties = %v, want false", schema.AdditionalProperties)
	}
	properties := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		properties = append(properties, name)
	}
	slices.Sort(properties)
	wantProperties := []string{"code", "detail", "status", "title", "type"}
	if !slices.Equal(properties, wantProperties) {
		t.Fatalf("Error properties = %v, want %v", properties, wantProperties)
	}
	wantRequired := []string{"type", "title", "status", "detail", "code"}
	if !slices.Equal(schema.Required, wantRequired) {
		t.Fatalf("Error required = %v, want %v", schema.Required, wantRequired)
	}
}
