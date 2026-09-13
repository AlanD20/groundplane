package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/AlanD20/groundplane/internal/cli/apiclient"
	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// QA: OBS-01, OBS-02, UI-01; local CLI presentation only, not live container counts or cross-surface parity.
// Rationale: the actual Service list TABLE journey must expose runtime intent
// and all current-serving counts as separate values from one fresh snapshot.
func TestServiceListTablePresentsObservation(t *testing.T) {
	t.Parallel()
	service := observedCLIService(time.Now().UTC().Add(-time.Second))
	server := serviceReadServer(
		t,
		"/api/v1/services",
		apiTypes.Page[apiTypes.Service]{Items: []apiTypes.Service{service}},
	)
	defer server.Close()

	output := executeServiceCommand(
		t,
		newServiceCmd(),
		server.URL,
		Scope{Environment: service.EnvironmentID, AsID: true},
		clicommon.FormatTable,
		"list",
	)
	rows := cliTableRows(t, output)
	if len(rows) != 2 || len(rows[0]) != len(rows[1]) {
		t.Fatalf("Service list TABLE rows = %#v", rows)
	}
	values := make(map[string]string, len(rows[0]))
	for index, header := range rows[0] {
		values[header] = rows[1][index]
	}
	for header, want := range map[string]string{
		"RUNTIME_INTENT":     "stopped",
		"OBSERVATION_STATE":  "degraded",
		"SERVING_RELEASE_ID": "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"EXPECTED_REPLICAS":  "11",
		"RUNNING":            "1",
		"HEALTHY":            "2",
		"STARTING":           "3",
		"UNHEALTHY":          "4",
		"TRANSITIONAL":       "5",
		"STOPPED":            "6",
		"FAILED":             "7",
	} {
		if values[header] != want {
			t.Fatalf("Service list TABLE %s = %q, want %q; rows = %#v", header, values[header], want, rows)
		}
	}
}

// QA: OBS-01, OBS-02, UI-01; local CLI presentation only, not live observation or desired/runtime agreement.
// Rationale: the actual Service show TABLE journey must flatten the same
// snapshot counts without replacing the distinct desired runtime intent.
func TestServiceShowTablePresentsObservation(t *testing.T) {
	t.Parallel()
	service := observedCLIService(time.Now().UTC().Add(-time.Second))
	detail := apiTypes.ServiceDetail{Service: service, NativeCompose: "services: {}"}
	server := serviceReadServer(t, "/api/v1/services/"+service.ID, detail)
	defer server.Close()

	output := executeServiceCommand(
		t,
		newServiceCmd(),
		server.URL,
		Scope{AsID: true},
		clicommon.FormatTable,
		"show", service.ID,
	)
	rows := cliTableRows(t, output)
	if len(rows) < 2 || len(rows[0]) != 2 || rows[0][0] != "FIELD" || rows[0][1] != "VALUE" {
		t.Fatalf("Service show TABLE rows = %#v", rows)
	}
	values := make(map[string]string, len(rows)-1)
	for _, row := range rows[1:] {
		if len(row) != 2 {
			t.Fatalf("Service show TABLE row = %#v", row)
		}
		values[row[0]] = row[1]
	}
	for field, want := range map[string]string{
		"runtime_intent":                 "stopped",
		"replicas":                       "9",
		"observation_state":              "degraded",
		"observation_serving_release_id": "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"observation_expected_replicas":  "11",
		"observation_running":            "1",
		"observation_healthy":            "2",
		"observation_starting":           "3",
		"observation_unhealthy":          "4",
		"observation_transitional":       "5",
		"observation_stopped":            "6",
		"observation_failed":             "7",
	} {
		if values[field] != want {
			t.Fatalf("Service show TABLE %s = %q, want %q; rows = %#v", field, values[field], want, rows)
		}
	}
}

// QA: OBS-03; local clock-based presentation only, not stalled API/Console refresh behavior.
// Rationale: machine-readable list output must not preserve an expired
// snapshot that scripts could mistake for current health.
func TestServiceListJSONExpiresObservationLocally(t *testing.T) {
	t.Parallel()
	service := observedCLIService(time.Now().UTC().Add(-time.Minute))
	server := serviceReadServer(
		t,
		"/api/v1/services",
		apiTypes.Page[apiTypes.Service]{Items: []apiTypes.Service{service}},
	)
	defer server.Close()

	output := executeServiceCommand(
		t,
		newServiceCmd(),
		server.URL,
		Scope{Environment: service.EnvironmentID, AsID: true},
		clicommon.FormatJSON,
		"list",
	)
	page := apiTypes.Page[apiTypes.Service]{}
	if err := json.Unmarshal([]byte(output), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Observation == nil ||
		page.Items[0].Observation.State != apiTypes.ServiceObservationUnavailable ||
		page.Items[0].Observation.ObservedAt != nil || page.Items[0].Observation.Replicas != nil {
		t.Fatalf("expired Service list JSON = %s", output)
	}
}

// QA: OBS-01, SVC-01; pure response-field projection only, not an executed create/edit operation.
// Rationale: create/edit continue to use the desired Service field set and
// must not gain a synthetic observation from read-only presentation.
func TestServiceMutationFieldsOmitObservation(t *testing.T) {
	t.Parallel()
	fields := serviceFields(observedCLIService(time.Now().UTC().Add(-time.Second)))
	for field := range fields {
		if strings.HasPrefix(field, "observation_") {
			t.Fatalf("mutation Service fields include %q", field)
		}
	}
}

func observedCLIService(observedAt time.Time) apiTypes.Service {
	expiresAt := observedAt.Add(15 * time.Second)
	releaseID := "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	expected := uint32(11)
	return apiTypes.Service{
		ID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Name: "api", Image: "example/api:1", RuntimeIntent: apiTypes.ServiceRuntimeIntentStopped,
		Observation: &apiTypes.ServiceObservation{
			State: apiTypes.ServiceObservationDegraded, ObservedAt: &observedAt, ExpiresAt: &expiresAt,
			ServingReleaseID: &releaseID, ExpectedReplicas: &expected,
			Replicas: &apiTypes.ServiceReplicaCounts{
				Running: 1, Healthy: 2, Starting: 3, Unhealthy: 4,
				Transitional: 5, Stopped: 6, Failed: 7,
			},
		},
		Replicas: 9,
	}
}

func cliTableRows(t *testing.T, output string) [][]string {
	t.Helper()
	var rows [][]string
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") || !strings.HasSuffix(line, "|") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		for index := range cells {
			cells[index] = strings.TrimSpace(cells[index])
		}
		rows = append(rows, cells)
	}
	return rows
}

func serviceReadServer(t *testing.T, path string, response any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != path {
			t.Errorf("Service request = %s %s", request.Method, request.URL.RequestURI())
		}
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(response); err != nil {
			t.Errorf("encode Service response: %v", err)
		}
	}))
}

func executeServiceCommand(
	t *testing.T,
	command *cobra.Command,
	baseURL string,
	scope Scope,
	format clicommon.Format,
	args ...string,
) string {
	t.Helper()
	var output bytes.Buffer
	app := &App{Client: apiclient.New(baseURL), Out: clicommon.NewWriter(format, true, &output), Scope: scope}
	command.SetContext(context.WithValue(context.Background(), appKey{}, app))
	command.SetOut(&output)
	command.SetErr(io.Discard)
	command.SetArgs(args)
	if err := command.Execute(); err != nil {
		t.Fatalf("execute Service command: %v", err)
	}
	return output.String()
}
