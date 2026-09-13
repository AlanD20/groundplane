package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// QA: ENT-01, ENT-03, ENT-06, UI-01; local CLI-to-HTTP behavior only, not storage or runtime materialization.
// Rationale: Entry edit must use the generated operation with complete mutable
// state, and secret literal bytes must come from a bounded file/stdin rather
// than process argv.
func TestEntryEditUsesGeneratedClientAndValueFileForSecret(t *testing.T) {
	t.Parallel()
	entryID := ids.New(ids.KindEnvEntry)
	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			switch request.Method {
			case http.MethodGet:
				if request.URL.Path != "/api/v1/entries/"+entryID {
					t.Errorf("GET path = %q", request.URL.Path)
				}
				_, _ = io.WriteString(
					writer,
					`{"id":"`+entryID+`","type":"env","key":"TOKEN","source":{"kind":"literal"},"exposure":["all"],"secret":true}`,
				)
			case http.MethodPatch:
				if request.URL.Path != "/api/v1/entries/"+entryID ||
					len(request.Header.Values("Idempotency-Key")) != 1 {
					t.Errorf(
						"PATCH path/idempotency = %q/%d",
						request.URL.Path,
						len(request.Header.Values("Idempotency-Key")),
					)
				}
				var body apiTypes.EntryEditRequest
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					t.Errorf("decode Entry edit request: %v", err)
				}
				if body.Source.Kind != "literal" || body.Source.Literal != "private-token" ||
					len(body.Exposure) != 1 || body.Exposure[0] != "api" {
					t.Errorf("Entry edit request = %#v", body)
				}
				_, _ = io.WriteString(
					writer,
					`{"id":"`+entryID+`","type":"env","key":"TOKEN","source":{"kind":"literal"},"exposure":["api"],"secret":true}`,
				)
			default:
				t.Errorf("method = %q", request.Method)
				writer.WriteHeader(http.StatusMethodNotAllowed)
			}
		}),
	)
	defer server.Close()
	command := newEntryCmd()
	command.SetIn(strings.NewReader("private-token"))
	output := executeNoun(
		t,
		command,
		server.URL,
		Scope{},
		"edit",
		entryID,
		"--value-file",
		"-",
		"--service",
		"api",
	)
	if strings.Contains(output, "private-token") ||
		!strings.Contains(output, `"id": "`+entryID+`"`) {
		t.Fatalf("Entry edit output = %q", output)
	}
}
