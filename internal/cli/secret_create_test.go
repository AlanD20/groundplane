package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: platform create must read plaintext only from stdin, send one
// protected API mutation, and render only the redacted resource response.
func TestSecretAddReadsPlatformValueFromStdin(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/v1/secrets" ||
			len(request.Header.Values("Idempotency-Key")) != 1 {
			t.Errorf(
				"Secret create request method/path/key count = %s/%s/%d",
				request.Method,
				request.URL.Path,
				len(request.Header.Values("Idempotency-Key")),
			)
		}
		var body apiTypes.SecretCreateRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode Secret create request: %v", err)
		}
		if !body.Platform || body.ProjectID != "" || body.Key != "TOKEN" || body.Kind != "env_var" ||
			body.Value != "private-token" {
			t.Errorf("Secret create request owner/metadata did not match")
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(
			writer,
			`{"id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAV","scope":"platform","key":"TOKEN","kind":"env_var","ref":"secrets/.env.edge","updated_at":"2026-08-22T19:00:00Z"}`,
		)
	}))
	defer server.Close()
	command := newSecretCmd()
	command.SetIn(strings.NewReader("private-token"))
	output := executeNoun(t, command, server.URL, Scope{}, "--platform", "add", "TOKEN", "--value-file", "-")
	if strings.Contains(output, "private-token") || !strings.Contains(output, `"key": "TOKEN"`) {
		t.Fatalf("Secret create output was not redacted metadata: %q", output)
	}
}

// Rationale: the CLI must reject oversized values before constructing an HTTP
// request so the public 255 KiB bound applies equally to files and stdin.
func TestReadSecretValueRejectsOversizedInput(t *testing.T) {
	t.Parallel()
	_, err := readSecretValue("-", strings.NewReader(strings.Repeat("x", apiTypes.MaximumSecretValueBytes+1)))
	kind, ok := errs.KindOf(err)
	if !ok || kind != errs.KindValidationFailed {
		t.Fatalf("readSecretValue(oversized) error = %v", err)
	}
}
