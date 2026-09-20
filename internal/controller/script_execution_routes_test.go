package controller

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: create and edit preserve exact access decisions and reject malformed
// or oversized contexts before the mutation service can publish anything.
func TestScriptExecutionHTTPBoundary(t *testing.T) {
	image := "setup@sha256:" + strings.Repeat("a", 64)
	grant := `{"volume_id":"vol_01ARZ3NDEKTSV4RRFFQ69G5FAV","target":"/etc/tls","read_only":false}`
	explicit := `{"mode":"explicit","image":"` + image + `","user":"0:0"`
	for _, test := range []struct {
		name, execution string
		valid           bool
	}{
		{"inherited", `{"mode":"inherited"}`, true},
		{"explicit write", explicit + `,"volumes":[` + grant + `]}`, true},
		{"explicit no grants", explicit + `}`, true},
		{"null", `null`, false},
		{"mixed", `{"mode":"inherited","volumes":[]}`, false},
		{"missing access", explicit + `,"volumes":[{"volume_id":"v","target":"/data"}]}`, false},
		{"null entries", explicit + `,"entry_ids":null}`, false},
		{"too many volumes", explicit + `,"volumes":[` + strings.Repeat(grant+",", 32) + grant + `]}`, false},
		{"too many entries", explicit + `,"entry_ids":[` + strings.Repeat(`"ent_01ARZ3NDEKTSV4RRFFQ69G5FAV",`, 64) + `"ent_01ARZ3NDEKTSV4RRFFQ69G5FAV"]}`, false},
	} {
		for _, method := range []string{http.MethodPost, http.MethodPatch} {
			t.Run(method+"/"+test.name, func(t *testing.T) {
				mutations := &scriptExecutionRouteMutations{}
				server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ScriptMutations: mutations})
				path, body, status := "/api/v1/scripts", `{"environment_id":"env","service_id":"svc",`+
					`"slug":"prepare","script":"echo prepare","when":"pre-deploy","execution":`+test.execution+`}`, http.StatusCreated
				if method == http.MethodPatch {
					path += "/scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
					body, status = `{"execution":`+test.execution+`}`, http.StatusOK
				}
				request := httptest.NewRequest(method, path, strings.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Idempotency-Key", "script-execution-0001")
				response := httptest.NewRecorder()
				server.HTTPHandler().ServeHTTP(response, request)
				if !test.valid {
					if response.Code < 400 || mutations.calls != 0 {
						t.Fatalf("invalid execution accepted: %d, calls=%d", response.Code, mutations.calls)
					}
					return
				}
				var got apiTypes.ScriptExecution
				if response.Code != status || mutations.calls != 1 {
					t.Fatalf(
						"execution HTTP status=%d, calls=%d: %s",
						response.Code,
						mutations.calls,
						response.Body.String(),
					)
				}
				if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if got.Mode == "explicit" && got.Image != image {
					t.Fatal("HTTP lost pinned execution identity")
				}
				if len(got.Volumes) != 0 && got.Volumes[0].ReadOnly {
					t.Fatal("HTTP lost explicit writable decision")
				}
			})
		}
	}
}

type scriptExecutionRouteMutations struct{ scriptOrderRouteMutations }

func (mutations *scriptExecutionRouteMutations) CreateScript(
	_ context.Context, input apiTypes.ScriptCreate, _ string,
) (etcd.IdempotencyResponse, error) {
	mutations.calls++
	body, err := json.Marshal(input.Execution)
	return etcd.IdempotencyResponse{Status: http.StatusCreated, ContentKind: "application/json", Body: body}, err
}

func (mutations *scriptExecutionRouteMutations) EditScript(
	_ context.Context, _ string, input apiTypes.ScriptEdit, _ string,
) (etcd.IdempotencyResponse, error) {
	mutations.calls++
	body, err := json.Marshal(input.Execution)
	return etcd.IdempotencyResponse{Status: http.StatusOK, ContentKind: "application/json", Body: body}, err
}
