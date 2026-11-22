package cli

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMonitoringShowDefaultsHostTargetAndKeepsFullJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/api/v1/monitoring/host/local" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		query := request.URL.Query()
		if query.Get("range") != "24h" || query.Get("points") != "48" || query.Get("limit") != "25" {
			t.Errorf("query = %v", query)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{
  "scope":{"kind":"host","id":"local","name":"groundplane"},
  "window":{"range":"24h","start":"2026-11-21T16:00:00Z","end":"2026-11-22T16:00:00Z","bucket_seconds":1800},
  "history_status":"available",
  "host":{"live":{"status":"fresh","observed_at":"2026-11-22T15:59:55Z","coverage":{"eligible_resources":1,"sampled_resources":1,"eligible_volumes":0,"sampled_volumes":0},"values":{}},"history":[],"interfaces":[],"devices":[]}
}`))
	}))
	defer server.Close()

	output := executeNoun(
		t,
		newMonitoringCmd(),
		server.URL,
		Scope{},
		"show", "host", "--range", "24h", "--points", "48",
	)
	for _, want := range []string{`"history_status": "available"`, `"bucket_seconds": 1800`, `"id": "local"`} {
		if !strings.Contains(output, want) {
			t.Fatalf("monitoring JSON = %q, want %q", output, want)
		}
	}
}
