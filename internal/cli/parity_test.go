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
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"

	server := exactRequestServer(
		t,
		http.MethodGet,
		"/api/v1/environments/"+environmentID+"/router",
		"",
		http.StatusOK,
		`{}`,
	)
	defer server.Close()

	output := executeNoun(t, newRouterCmd(), server.URL, Scope{Environment: environmentID, AsID: true}, "show")
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
	want := "{\n  \"task_id\": \"task_2\"\n}\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestAgentUpdateDispatchesSelectedAgentTask(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/agents/agt_1/update",
		"",
		http.StatusAccepted,
		`{"task_id":"task_update"}`,
	)
	defer server.Close()

	output := executeNoun(t, newAgentCmd(), server.URL, Scope{}, "update", "agt_1")
	want := "{\n  \"task_id\": \"task_update\"\n}\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestAgentUpdateAllResolvesSingletonThenUsesPerIDEndpoint(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		switch requests {
		case 1:
			if request.Method != http.MethodGet || request.URL.Path != "/api/v1/agents" {
				t.Fatalf("list request = %s %s", request.Method, request.URL.Path)
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(
				writer,
				`{"items":[{"id":"agt_1","enrollment_task_id":"task_1","host":"host","status":"healthy","version":null,"labels":{},"ready_at":null,"last_report_at":null,"in_flight":0}]}`,
			)
		case 2:
			if request.Method != http.MethodPost || request.URL.Path != "/api/v1/agents/agt_1/update" {
				t.Fatalf("update request = %s %s", request.Method, request.URL.Path)
			}
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(writer, `{"task_id":"task_update_all"}`)
		default:
			t.Fatalf("unexpected request %d", requests)
		}
	}))
	defer server.Close()

	output := executeNoun(t, newAgentCmd(), server.URL, Scope{}, "update", "--all")
	want := "{\n  \"task_id\": \"task_update_all\"\n}\n"
	if output != want || requests != 2 {
		t.Fatalf("output/requests = %q/%d", output, requests)
	}
}

func TestAgentUpdateRequiresExactlyIDOrAll(t *testing.T) {
	t.Parallel()

	for name, args := range map[string][]string{
		"neither": {"update"},
		"both":    {"update", "agt_1", "--all"},
	} {
		t.Run(name, func(t *testing.T) {
			command := newAgentCmd()
			command.SetArgs(args)
			if err := command.Execute(); err == nil {
				t.Fatalf("agent %v error = nil", args)
			}
		})
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
	want := "{\n  \"task_id\": \"task_join\"\n}\n"
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
		`{"pull_interval_seconds":2,"max_concurrent_tasks":3,"labels":{"arch":"arm64"}}`,
	)
	defer server.Close()

	want := "{\n  \"pull_interval_seconds\": 2,\n  \"max_concurrent_tasks\": 3,\n  \"labels\": {\n" +
		"    \"arch\": \"arm64\"\n  }\n}\n"
	if output := executeNoun(
		t,
		newAgentCmd(),
		server.URL,
		Scope{},
		"config",
		"show",
		"agt_1",
	); output != want {
		t.Fatalf("output = %q, want %q", output, want)
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

func TestEnvironmentEditPatchesNetworkPoolAndReturnsEnvironment(t *testing.T) {
	t.Parallel()
	server := exactRequestServer(
		t,
		http.MethodPatch,
		"/api/v1/environments/env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		`{"network_pool":"10.40.0.0/15"}`,
		http.StatusOK,
		`{"id":"env_01ARZ3NDEKTSV4RRFFQ69G5FAV","project_id":"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV","name":"production","network_pool":"10.40.0.0/15","volume_dir":"/var/lib/groundplane/vol/platform/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV","provisioning_state":"ready","create_task_id":null}`,
	)
	defer server.Close()
	executeNoun(
		t,
		newEnvironmentCmd(),
		server.URL,
		Scope{AsID: true},
		"edit",
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"--network-pool",
		"10.40.0.0/15",
	)
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
			body := ""
			if test.name == "config set" {
				body = `{"config":{}}`
			}
			server := exactRequestServer(t, test.method, test.path, body, test.status, test.response)
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
			if request.Method != http.MethodGet {
				t.Errorf("request = %s %s", request.Method, request.URL.RequestURI())
			}
			switch request.URL.RequestURI() {
			case "/api/v1/tasks/task_1/events":
				if accept := request.Header.Get("Accept"); accept != "text/event-stream" {
					t.Errorf("Accept = %q, want text/event-stream", accept)
				}
				writer.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(
					writer,
					"id: 1\ndata: {\"sequence\":1,\"step_id\":\"step_1\",\"state\":\"completed\","+
						"\"attempt\":1,\"ordinal\":1,\"received_at\":\"2026-08-23T04:30:00Z\"}\n\n",
				)
			case "/api/v1/tasks/task_1":
				writer.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(
					writer,
					`{"id":"task_1","operation_id":"op_1","type":"run","target":"svc_1","status":"completed"}`,
				)
			default:
				t.Errorf("request = %s %s", request.Method, request.URL.RequestURI())
			}
		}),
	)
	defer server.Close()

	output := executeNoun(t, newTaskCmd(), server.URL, Scope{}, "events", "task_1")
	want := "{\"sequence\":1,\"step_id\":\"step_1\",\"state\":\"completed\"," +
		"\"attempt\":1,\"ordinal\":1,\"received_at\":\"2026-08-23T04:30:00Z\"}\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestServiceAttachDispatchesTaskWithBothTargets(t *testing.T) {
	// Rationale: an attach belongs to one explicit consumer service and one
	// backing service, and provisioning is a Task rather than a synchronous create.
	t.Parallel()

	body := `{"backing_service_id":"bks_1","service_ids":["svc_1"]}`
	server := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/attaches",
		body,
		http.StatusAccepted,
		`{"task_id":"task_attach"}`,
	)
	defer server.Close()

	output := executeNoun(t, newServiceCmd(), server.URL, Scope{AsID: true}, "attach", "svc_1", "bks_1")
	want := "{\n  \"task_id\": \"task_attach\"\n}\n"
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
	want := "{\n  \"task_id\": \"task_2\"\n}\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestBackupRunPostsWithoutBodyOrSourceSelection(t *testing.T) {
	t.Parallel()

	command := newBackupCmd()
	run, _, err := command.Find([]string{"run"})
	if err != nil {
		t.Fatalf("find backup run command: %v", err)
	}
	if source := run.Flags().Lookup("source"); source != nil {
		t.Fatalf("backup run source flag = %#v, want absent", source)
	}

	server := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/environments/env_01ARZ3NDEKTSV4RRFFQ69G5FAV/backup-run",
		"",
		http.StatusAccepted,
		`{"task_id":"task_backup"}`,
	)
	defer server.Close()

	output := executeNoun(t, command, server.URL, Scope{
		Environment: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		AsID:        true,
	}, "run")
	want := "{\n  \"task_id\": \"task_backup\"\n}\n"
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
		"AGE-SECRET-KEY-1TEST\n",
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
				if path == "/api/v1/environments/production/export-key" {
					if len(keys) != 0 {
						t.Errorf("backup export Idempotency-Key values = %q, want none", keys)
					}
					break
				}
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
