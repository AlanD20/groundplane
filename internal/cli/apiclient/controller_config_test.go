package apiclient

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: the CLI wrapper must use the generated PUT signature so the
// OpenAPI-required key and exact YAML replacement reach the Controller.
// QA: HOST-08/09; protected config transport only, not file or restart effects.
func TestControllerConfigGeneratedClientCarriesIdempotencyKey(t *testing.T) {
	t.Parallel()
	const initialRevision = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const updatedRevision = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const content = "# retained\nlisten:\n  http: [127.0.0.1:8080]\n"
	var putCalls int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.Method + " " + request.URL.Path {
		case "GET /api/v1/controller/config":
			_ = json.NewEncoder(writer).Encode(apiTypes.ControllerConfigDocument{
				Path: "/etc/groundplane/controller.yaml", Content: content, Revision: initialRevision,
			})
		case "PUT /api/v1/controller/config":
			putCalls++
			if key := request.Header.Get(idempotencyKeyHeader); !idempotencyKeyPattern.MatchString(key) {
				t.Errorf("Idempotency-Key = %q", key)
			}
			var replacement apiTypes.ControllerConfigReplacement
			if err := json.NewDecoder(request.Body).Decode(&replacement); err != nil {
				t.Errorf("decode replacement: %v", err)
			}
			if replacement.Content != content || replacement.ExpectedRevision != initialRevision {
				t.Errorf("replacement = %#v", replacement)
			}
			_ = json.NewEncoder(writer).Encode(apiTypes.ControllerConfigDocument{
				Path:    "/etc/groundplane/controller.yaml",
				Content: content, Revision: updatedRevision, RestartRequired: true,
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client := New(server.URL)
	document, err := client.SetControllerConfig(t.Context(), apiTypes.ControllerConfigReplacement{
		Content: content, ExpectedRevision: initialRevision,
	})
	if err != nil {
		t.Fatalf("SetControllerConfig() error = %v", err)
	}
	if putCalls != 1 ||
		document.Content != content ||
		document.Revision != updatedRevision ||
		!document.RestartRequired {
		t.Fatalf("SetControllerConfig() = %#v calls=%d", document, putCalls)
	}
	if strings.TrimSpace(document.Path) != "/etc/groundplane/controller.yaml" {
		t.Fatalf("document path = %q", document.Path)
	}
}
