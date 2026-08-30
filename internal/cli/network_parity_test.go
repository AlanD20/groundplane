package cli

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/spf13/cobra"
)

const (
	c07EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	c07ZoneID        = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	c07RouteID       = "rte_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	c07ServiceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestNetworkCommandTreesMatchC07Operations(t *testing.T) {
	t.Parallel()
	assertPrimaryCommands(t, newZoneCmd(), []string{"add", "list", "removal-impact", "remove", "show"})
	assertPrimaryCommands(t, newRouteCmd(), []string{"add", "edit", "list", "remove", "show"})
}

func TestZoneAddUsesCanonicalCreateOperation(t *testing.T) {
	t.Parallel()
	server := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/zones",
		`{"environment_id":"`+c07EnvironmentID+`","internal":true,"name":"frontend","subnet":"10.40.1.0/24"}`,
		http.StatusCreated,
		`{"id":"`+c07ZoneID+`","environment_id":"`+c07EnvironmentID+`","name":"frontend","subnet":"10.40.1.0/24","internal":true,"owner_kind":"environment","owner_id":"`+c07EnvironmentID+`"}`,
	)
	defer server.Close()

	output := executeNoun(
		t, newZoneCmd(), server.URL, Scope{Environment: c07EnvironmentID, AsID: true},
		"add", "frontend", "--subnet", "10.40.1.0/24", "--internal",
	)
	if !strings.Contains(output, `"id": "`+c07ZoneID+`"`) {
		t.Fatalf("zone add output = %q", output)
	}
}

func TestZoneRemovePreviewsImpactThenDispatchesCanonicalDelete(t *testing.T) {
	t.Parallel()
	impactToken := strings.Repeat("a", 64)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		switch requests {
		case 1:
			if request.Method != http.MethodGet || request.URL.RequestURI() !=
				"/api/v1/zones/"+c07ZoneID+"/removal-impact" {
				t.Errorf("impact request = %s %s", request.Method, request.URL.RequestURI())
			}
			if values := request.Header.Values("Idempotency-Key"); len(values) != 0 {
				t.Errorf("impact Idempotency-Key values = %q", values)
			}
			_, _ = io.WriteString(
				writer,
				`{"zone_id":"`+c07ZoneID+`","zone_name":"frontend","mode":"ordinary","impact_token":"`+impactToken+`","attaches":[],"services":[],"databases":[]}`,
			)
		case 2:
			if request.Method != http.MethodDelete || request.URL.RequestURI() !=
				"/api/v1/zones/"+c07ZoneID+"?impact_token="+impactToken {
				t.Errorf("delete request = %s %s", request.Method, request.URL.RequestURI())
			}
			keys := request.Header.Values("Idempotency-Key")
			if len(keys) != 1 {
				t.Errorf("delete Idempotency-Key values = %q", keys)
			} else if _, err := ulid.ParseStrict(keys[0]); err != nil {
				t.Errorf("delete Idempotency-Key = %q: %v", keys[0], err)
			}
			writer.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(writer, `{"task_id":"task_c07"}`)
		default:
			t.Errorf("unexpected request %d", requests)
		}
	}))
	defer server.Close()

	output := executeNoun(
		t, newZoneCmd(), server.URL, Scope{AsID: true}, "remove", c07ZoneID,
	)
	if requests != 2 || !strings.Contains(output, `"task_id": "task_c07"`) {
		t.Fatalf("zone remove requests/output = %d/%q", requests, output)
	}
}

func TestRouteMutationsUseCanonicalOperations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		status   int
		response string
		args     []string
	}{
		{
			name:     "add",
			method:   http.MethodPost,
			path:     "/api/v1/routes",
			body:     `{"environment_id":"` + c07EnvironmentID + `","exposure":"public","host":"app.example.com","path":"/api/*","target_port":8080,"target_service_id":"` + c07ServiceID + `"}`,
			status:   http.StatusAccepted,
			response: `{"route":{"id":"` + c07RouteID + `","environment_id":"` + c07EnvironmentID + `","host":"app.example.com","path":"/api/*","exposure":"public","target_service_id":"` + c07ServiceID + `","target_port":8080,"status":"pending"},"task_id":"task_c07"}`,
			args: []string{
				"add",
				"--hostname",
				"app.example.com",
				"--path",
				"/api/*",
				"--exposure",
				"public",
				"--service",
				c07ServiceID,
				"--target-port",
				"8080",
			},
		},
		{
			name:     "edit",
			method:   http.MethodPatch,
			path:     "/api/v1/routes/" + c07RouteID,
			body:     `{"exposure":"internal"}`,
			status:   http.StatusAccepted,
			response: `{"route":{"id":"` + c07RouteID + `","environment_id":"` + c07EnvironmentID + `","host":"app.example.com","path":"/api/*","exposure":"internal","target_service_id":"` + c07ServiceID + `","target_port":8080,"status":"pending"},"task_id":"task_c07"}`,
			args:     []string{"edit", c07RouteID, "--exposure", "internal"},
		},
		{
			name: "remove", method: http.MethodDelete, path: "/api/v1/routes/" + c07RouteID,
			status: http.StatusAccepted, response: `{"task_id":"task_c07"}`,
			args: []string{"remove", c07RouteID},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := exactRequestServer(t, test.method, test.path, test.body, test.status, test.response)
			defer server.Close()
			executeNoun(
				t, newRouteCmd(), server.URL,
				Scope{Environment: c07EnvironmentID, AsID: true}, test.args...,
			)
		})
	}
}

func assertPrimaryCommands(t *testing.T, command *cobra.Command, want []string) {
	t.Helper()
	got := make([]string, len(command.Commands()))
	for index, child := range command.Commands() {
		got[index] = child.Name()
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s subcommands = %q, want %q", command.Name(), got, want)
	}
}
