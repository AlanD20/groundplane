package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: the operator command must read the exact file bytes, fetch the
// current revision, and delegate one protected PUT through the human API.
func TestControllerConfigSetPublishesExactFileAgainstCurrentRevision(t *testing.T) {
	t.Parallel()
	const initialRevision = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const updatedRevision = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const content = "# retained operator comment\nlisten:\n  http: [127.0.0.1:8080]\n"
	var received apiTypes.ControllerConfigReplacement
	var idempotencyKey string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.Method + " " + request.URL.Path {
		case "GET /api/v1/controller/config":
			_ = json.NewEncoder(writer).Encode(apiTypes.ControllerConfigDocument{
				Path: "/etc/groundplane/controller.yaml", Content: "old", Revision: initialRevision,
			})
		case "PUT /api/v1/controller/config":
			idempotencyKey = request.Header.Get("Idempotency-Key")
			if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
				t.Errorf("decode replacement: %v", err)
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

	directory := t.TempDir()
	inputPath := filepath.Join(directory, "replacement.yaml")
	if err := os.WriteFile(inputPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write replacement: %v", err)
	}
	var output bytes.Buffer
	root := NewRootCmd(Dependencies{})
	root.SetOut(&output)
	root.SetArgs([]string{
		"--config", filepath.Join(directory, "missing-cli.yaml"),
		"--host", server.URL,
		"--output", "JSON",
		"controller", "config", "set", "--file", inputPath,
	})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("controller config set: %v", err)
	}
	if received.Content != content || received.ExpectedRevision != initialRevision {
		t.Fatalf("replacement = %#v", received)
	}
	if len(idempotencyKey) < 16 {
		t.Fatalf("Idempotency-Key = %q", idempotencyKey)
	}
	if !strings.Contains(output.String(), `"content": "# retained operator comment\n`) ||
		!strings.Contains(output.String(), `"restart_required": true`) {
		t.Fatalf("output = %q", output.String())
	}
}
