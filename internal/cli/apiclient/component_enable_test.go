package apiclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/oklog/ulid/v2"
)

// QA: CMP-02; exact enable request only, not workload activation.
// Rationale: Preserve an absent body versus explicit typed enable config and accepted Task identity.
func TestEnableComponentSendsOptionalTypedConfig(t *testing.T) {
	tests := []struct {
		name    string
		request apiTypes.ComponentEnableRequest
		body    string
	}{
		{name: "no config preserves empty request body"},
		{
			name: "typed config uses enable wrapper",
			request: apiTypes.ComponentEnableRequest{Config: &apiTypes.ComponentConfigMutationInput{
				Caddy: &apiTypes.CaddyComponentConfigMutationInput{
					ZoneIDs: []string{"net_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
				},
			}},
			body: `{"config":{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAV"]}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodPost || request.URL.Path != "/api/v1/components/cmp_1/enable" {
					t.Errorf("request = %s %s", request.Method, request.URL.Path)
				}
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Fatal(err)
				}
				if string(body) != test.body {
					t.Errorf("body = %s, want %s", body, test.body)
				}
				key := request.Header.Get(idempotencyKeyHeader)
				if _, err := ulid.ParseStrict(key); err != nil {
					t.Errorf("Idempotency-Key = %q: %v", key, err)
				}
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(http.StatusAccepted)
				_, _ = io.WriteString(writer, `{"task_id":"task_1"}`)
			}))
			defer server.Close()

			accepted, err := New(server.URL).EnableComponent(context.Background(), "cmp_1", test.request)
			if err != nil {
				t.Fatalf("EnableComponent() error = %v", err)
			}
			if accepted.TaskID != "task_1" {
				t.Fatalf("TaskID = %q, want task_1", accepted.TaskID)
			}
		})
	}
}
