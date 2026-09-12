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
	for _, value := range []string{
		"RUNTIME_INTENT", "OBSERVATION_STATE", "EXPECTED_REPLICAS", "TRANSITIONAL",
		"stopped", "degraded", "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
	} {
		if !strings.Contains(output, value) {
			t.Fatalf("Service list TABLE = %q, missing %q", output, value)
		}
	}
}

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
	for _, value := range []string{
		"runtime_intent", "stopped", "observation_state", "degraded",
		"observation_running", "observation_healthy", "observation_failed",
	} {
		if !strings.Contains(output, value) {
			t.Fatalf("Service show TABLE = %q, missing %q", output, value)
		}
	}
}

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
	expected := uint32(7)
	return apiTypes.Service{
		ID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Name: "api", Image: "example/api:1", RuntimeIntent: apiTypes.ServiceRuntimeIntentStopped,
		Observation: &apiTypes.ServiceObservation{
			State: apiTypes.ServiceObservationDegraded, ObservedAt: &observedAt, ExpiresAt: &expiresAt,
			ServingReleaseID: &releaseID, ExpectedReplicas: &expected,
			Replicas: &apiTypes.ServiceReplicaCounts{
				Running: 1, Healthy: 1, Starting: 1, Unhealthy: 1,
				Transitional: 1, Stopped: 1, Failed: 1,
			},
		},
		Replicas: 9,
	}
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
