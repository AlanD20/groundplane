package cli

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/cli/apiclient"
	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	"github.com/oklog/ulid/v2"
	"github.com/spf13/cobra"
)

func TestRouterShowRequestsEnvironmentProjection(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(
		t,
		http.MethodGet,
		"/api/v1/environments/production/router",
		"",
		http.StatusOK,
		`{}`,
	)
	defer server.Close()

	output := executeNoun(t, newRouterCmd(), server.URL, Scope{Environment: "production"}, "show")
	if output != "{}\n" {
		t.Fatalf("output = %q, want %q", output, "{}\n")
	}
}

func TestAgentRemoveDispatchesRemovalTask(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(
		t,
		http.MethodDelete,
		"/api/v1/agents/agt_1",
		"",
		http.StatusAccepted,
		`{"task_id":"task_2"}`,
	)
	defer server.Close()

	output := executeNoun(t, newAgentCmd(), server.URL, Scope{}, "remove", "agt_1")
	want := "task task_2 dispatched — `groundplane task show task_2` to follow\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestAgentJoinDispatchesCreationTaskWithoutToken(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/agents",
		"",
		http.StatusAccepted,
		`{"task_id":"task_join"}`,
	)
	defer server.Close()

	output := executeNoun(t, newAgentCmd(), server.URL, Scope{}, "join")
	want := "task task_join dispatched — `groundplane task show task_join` to follow\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestAgentConfigShowUsesConfigSingleton(t *testing.T) {
	// Rationale: the accepted Agent config singleton needs a read peer beside
	// config set; otherwise CLI/API parity is incomplete.
	t.Parallel()

	server := exactRequestServer(
		t,
		http.MethodGet,
		"/api/v1/agents/agt_1/config",
		"",
		http.StatusOK,
		`{}`,
	)
	defer server.Close()

	if output := executeNoun(
		t,
		newAgentCmd(),
		server.URL,
		Scope{},
		"config",
		"show",
		"agt_1",
	); output != "{}\n" {
		t.Fatalf("output = %q, want %q", output, "{}\n")
	}
}

func TestAgentConfigSetSendsKeyedLabels(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(
		t,
		http.MethodPut,
		"/api/v1/agents/agt_1/config",
		`{"labels":{"arch":"arm64","zone":"edge"},"max_concurrent_tasks":3,"pull_interval_seconds":2}`,
		http.StatusOK,
		`{}`,
	)
	defer server.Close()

	executeNoun(
		t,
		newAgentCmd(),
		server.URL,
		Scope{},
		"config",
		"set",
		"agt_1",
		"--pull-interval",
		"2",
		"--max-concurrent",
		"3",
		"--label",
		"arch=arm64",
		"--label",
		"zone=edge",
	)
}

func TestParseAgentLabelsRejectsMalformedAndDuplicateKeys(t *testing.T) {
	t.Parallel()

	for _, values := range [][]string{{"missing-value"}, {"=empty-key"}, {"arch=arm64", "arch=amd64"}} {
		if _, err := parseAgentLabels(values); err == nil {
			t.Fatalf("parseAgentLabels(%q) error = nil", values)
		}
	}
}

func TestDedicatedRenameRoutesReturnUpdatedEntities(t *testing.T) {
	// Rationale: rename is a synchronous POST update with one dedicated route,
	// not a generic PATCH or a Task action.
	t.Parallel()

	tests := []struct {
		name    string
		command *cobra.Command
		path    string
		args    []string
		body    string
		scope   Scope
	}{
		{
			name: "tenant", command: newTenantCmd(),
			path: "/api/v1/tenants/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/rename",
			args: []string{"rename", "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV", "--slug", "acme-inc"},
			body: `{"slug":"acme-inc"}`, scope: Scope{AsID: true},
		},
		{
			name: "project", command: newProjectCmd(),
			path: "/api/v1/projects/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/rename",
			args: []string{"rename", "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV", "--slug", "api-v2"},
			body: `{"slug":"api-v2"}`, scope: Scope{AsID: true},
		},
		{
			name:    "environment",
			command: newEnvironmentCmd(),
			path:    "/api/v1/environments/env_01ARZ3NDEKTSV4RRFFQ69G5FAV/rename",
			args:    []string{"rename", "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", "--name", "prod"},
			body:    `{"name":"prod"}`,
			scope:   Scope{AsID: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := exactRequestServer(
				t,
				http.MethodPost,
				test.path,
				test.body,
				http.StatusOK,
				`{}`,
			)
			defer server.Close()
			executeNoun(t, test.command, server.URL, test.scope, test.args...)
		})
	}
}

func TestComponentActionsAndConfigUseStableID(t *testing.T) {
	// Rationale: every component detail, action, and config operation must use
	// the component's stable id rather than its mutable kind label.
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		status   int
		response string
		args     []string
	}{
		{
			name:     "show",
			method:   http.MethodGet,
			path:     "/api/v1/components/cmp_1",
			status:   http.StatusOK,
			response: `{}`,
			args:     []string{"show", "cmp_1"},
		},
		{
			name:     "enable",
			method:   http.MethodPost,
			path:     "/api/v1/components/cmp_1/enable",
			status:   http.StatusAccepted,
			response: `{"task_id":"task_1"}`,
			args:     []string{"enable", "cmp_1"},
		},
		{
			name:     "disable",
			method:   http.MethodPost,
			path:     "/api/v1/components/cmp_1/disable",
			status:   http.StatusAccepted,
			response: `{"task_id":"task_1"}`,
			args:     []string{"disable", "cmp_1"},
		},
		{
			name:     "update",
			method:   http.MethodPost,
			path:     "/api/v1/components/cmp_1/update",
			status:   http.StatusAccepted,
			response: `{"task_id":"task_1"}`,
			args:     []string{"update", "cmp_1"},
		},
		{
			name:     "config show",
			method:   http.MethodGet,
			path:     "/api/v1/components/cmp_1/config",
			status:   http.StatusOK,
			response: `{}`,
			args:     []string{"config", "show", "cmp_1"},
		},
		{
			name:     "config set",
			method:   http.MethodPut,
			path:     "/api/v1/components/cmp_1/config",
			status:   http.StatusOK,
			response: `{}`,
			args:     []string{"config", "set", "cmp_1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := exactRequestServer(t, test.method, test.path, "", test.status, test.response)
			defer server.Close()
			executeNoun(t, newComponentCmd(), server.URL, Scope{}, test.args...)
		})
	}
}

func TestTaskEventsStreamsCanonicalEndpoint(t *testing.T) {
	// Rationale: the CLI must expose the same SSE Task event stream the Console
	// follows, rather than requiring polling or a Console session.
	t.Parallel()

	server := httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodGet ||
				request.URL.RequestURI() != "/api/v1/tasks/task_1/events" {
				t.Errorf("request = %s %s", request.Method, request.URL.RequestURI())
			}
			if accept := request.Header.Get("Accept"); accept != "text/event-stream" {
				t.Errorf("Accept = %q, want text/event-stream", accept)
			}
			writer.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(writer, "data: ready\n\n")
		}),
	)
	defer server.Close()

	output := executeNoun(t, newTaskCmd(), server.URL, Scope{}, "events", "task_1")
	if output != "ready\n" {
		t.Fatalf("output = %q, want %q", output, "ready\n")
	}
}

func TestServiceAttachDispatchesTaskWithBothTargets(t *testing.T) {
	// Rationale: an attach belongs to one explicit consumer service and one
	// backing service, and provisioning is a Task rather than a synchronous create.
	t.Parallel()

	body := `{"backing_service_id":"bks_1","grants":null,"name":"","service_id":"svc_1"}`
	server := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/attaches",
		body,
		http.StatusAccepted,
		`{"task_id":"task_attach"}`,
	)
	defer server.Close()

	output := executeNoun(t, newServiceCmd(), server.URL, Scope{}, "attach", "svc_1", "bks_1")
	want := "task task_attach dispatched — `groundplane task show task_attach` to follow\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestTaskRetryDispatchesNewAttempt(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/tasks/task_1/retry",
		"",
		http.StatusAccepted,
		`{"task_id":"task_2"}`,
	)
	defer server.Close()

	output := executeNoun(t, newTaskCmd(), server.URL, Scope{}, "retry", "task_1")
	want := "task task_2 dispatched — `groundplane task show task_2` to follow\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestBackupExportKeyPostsAndPrintsIdentity(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/environments/production/export-key",
		"",
		http.StatusOK,
		`{"value":"AGE-SECRET-KEY-1TEST"}`,
	)
	defer server.Close()

	output := executeNoun(
		t,
		newBackupCmd(),
		server.URL,
		Scope{Environment: "production"},
		"export-key",
	)
	if output != "AGE-SECRET-KEY-1TEST\n" {
		t.Fatalf("output = %q, want %q", output, "AGE-SECRET-KEY-1TEST\n")
	}
}

func TestReleaseGroupCommandTreeUsesLockedVerbs(t *testing.T) {
	t.Parallel()

	command := newReleaseGroupCmd()
	want := map[string]bool{
		"list": true, "show": true, "add": true, "edit": true,
		"remove": true, "deploy": true, "rollback": true,
	}
	if len(command.Commands()) != len(want) {
		t.Fatalf("subcommand count = %d, want %d", len(command.Commands()), len(want))
	}

	for _, child := range command.Commands() {
		if !want[child.Name()] {
			t.Errorf("unexpected primary subcommand %q", child.Name())
		}
		if child.Name() == "add" && len(child.Aliases) != 0 {
			t.Errorf("add aliases = %q, want none", child.Aliases)
		}
		if child.Name() == "remove" && (len(child.Aliases) != 1 || child.Aliases[0] != "delete") {
			t.Errorf("remove aliases = %q, want [delete]", child.Aliases)
		}
	}
}

func exactRequestServer(
	t *testing.T,
	method, path, body string,
	status int,
	response string,
) *httptest.Server {
	t.Helper()

	return httptest.NewServer(
		http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method != method {
				t.Errorf("method = %q, want %q", request.Method, method)
			}
			if request.URL.RequestURI() != path {
				t.Errorf("path = %q, want %q", request.URL.RequestURI(), path)
			}
			keys := request.Header.Values("Idempotency-Key")
			switch method {
			case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
				if len(keys) != 1 {
					t.Errorf("mutation Idempotency-Key values = %q, want exactly one", keys)
				} else if _, err := ulid.ParseStrict(keys[0]); err != nil {
					t.Errorf("mutation Idempotency-Key = %q, want raw ULID: %v", keys[0], err)
				}
			default:
				if len(keys) != 0 {
					t.Errorf("safe request Idempotency-Key values = %q, want none", keys)
				}
			}

			gotBody, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read request body: %v", err)
			}
			if string(gotBody) != body {
				t.Errorf("body = %q, want %q", gotBody, body)
			}

			writer.WriteHeader(status)
			if response != "" {
				_, _ = io.WriteString(writer, response)
			}
		}),
	)
}

func executeNoun(
	t *testing.T,
	command *cobra.Command,
	baseURL string,
	scope Scope,
	args ...string,
) string {
	t.Helper()

	var output bytes.Buffer
	app := &App{
		Client: apiclient.New(baseURL),
		Out:    clicommon.NewWriter(clicommon.FormatJSON, true, &output),
		Scope:  scope,
	}
	command.SetContext(context.WithValue(context.Background(), appKey{}, app))
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs(args)

	if err := command.Execute(); err != nil {
		t.Fatalf("execute command: %v", err)
	}
	return output.String()
}
