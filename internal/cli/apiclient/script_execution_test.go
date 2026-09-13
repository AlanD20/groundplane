package apiclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: generated transport must preserve exact execution grants in both
// directions instead of silently losing them during public-model conversion.
// QA: SCRIPT-01/06; grant transport/decoding only, not runner isolation.
func TestScriptClientExecutionRoundTrip(t *testing.T) {
	execution := apiTypes.ScriptExecution{
		Mode:  "explicit",
		Image: "setup@sha256:" + strings.Repeat("a", 64),
		User:  "0:0",
		Volumes: []apiTypes.ScriptVolumeGrant{
			{VolumeID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Target: "/etc/tls", ReadOnly: false},
		},
		EntryIDs: []string{"ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	}
	for _, method := range []string{http.MethodPost, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				wantPath := "/api/v1/scripts"
				if method == http.MethodPatch {
					wantPath += "/scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
				}
				if request.Method != method || request.URL.Path != wantPath {
					t.Errorf("request = %s %s, want %s %s", request.Method, request.URL.Path, method, wantPath)
				}
				var input struct {
					Execution *apiTypes.ScriptExecution `json:"execution"`
				}
				if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
					t.Error(err)
				}
				if !reflect.DeepEqual(input.Execution, &execution) {
					t.Errorf("client lost execution: %#v", input.Execution)
				}
				writer.Header().Set("Content-Type", "application/json")
				if method == http.MethodPost {
					writer.WriteHeader(http.StatusCreated)
				}
				if err := json.NewEncoder(writer).Encode(apiTypes.Script{ID: "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV",
					Origin: "api", ActiveGeneration: 1, Execution: execution}); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			client := New(server.URL)
			var response apiTypes.Script
			var err error
			if method == http.MethodPost {
				response, err = client.CreateScript(context.Background(), apiTypes.ScriptCreate{Execution: &execution})
			} else {
				response, err = client.EditScript(context.Background(), "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV", apiTypes.ScriptEdit{Execution: &execution})
			}
			if err != nil || !reflect.DeepEqual(response.Execution, execution) {
				t.Fatalf("client response lost execution: %#v, %v", response.Execution, err)
			}
		})
	}
}

// Rationale: a malformed Controller response must not turn missing read_only
// into false or an invalid/missing mode into apparently inherited execution.
// QA: SCRIPT-01/06; grant transport/decoding only, not runner isolation.
func TestScriptClientRejectsMalformedExecutionResponse(t *testing.T) {
	for _, execution := range []string{
		`null`, `{}`, `{"mode":"inherited","image":""}`,
		`{"mode":"explicit","image":"setup@sha256:` + strings.Repeat("a", 64) +
			`","user":"0:0","volumes":[{"volume_id":"v","target":"/data"}]}`,
	} {
		t.Run(execution, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				if _, err := io.WriteString(writer, `{"id":"scr","origin":"api","active_generation":1,"order":0,"execution":`+execution+`}`); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			if _, err := New(server.URL).GetScript(context.Background(), "scr"); err == nil {
				t.Fatal("client accepted malformed execution")
			}
		})
	}
}
