package cli

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/AlanD20/groundplane/internal/cli/apiclient"
	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	"github.com/oklog/ulid/v2"
	"github.com/spf13/cobra"
)

// QA: CMP-02, UI-01; local read route and rendering only, not preview consistency or serving bytes.
// Rationale: router show must read the Environment-scoped projection through
// its canonical endpoint rather than synthesize state from Component metadata.
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

// QA: HOST-04, UI-01; local Task dispatch only, not credential revocation or re-enrollment.
// Rationale: Agent removal must address the selected stable id through the
// asynchronous lifecycle endpoint and expose that same Task identity.
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

// QA: UP-02, UI-01; local selected-Agent request only, not image selection or runtime replacement.
// Rationale: a selected Agent update must use the per-id endpoint and expose
// the Controller-owned replacement Task rather than acting locally.
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

// QA: UP-02, UI-01; local singleton lookup and dispatch only, not Agent replacement or enrollment.
// Rationale: --all means the one local Agent, so the CLI must resolve that
// singleton and still use the canonical stable-id update endpoint.
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

// QA: UP-02, UI-03; local argument admission only, not API validation or absence of host effects.
// Rationale: an Agent update must select exactly one explicit id or the singleton
// --all mode so ambiguous or accidental replacement cannot be dispatched.
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

// QA: HOST-04, UI-01; local bodyless dispatch only, not enrollment, authentication, or secret isolation.
// Rationale: Agent join must create the local Agent through one bodyless Task
// request and must not accept or emit a channel token on the CLI surface.
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

// QA: HOST-07, UI-01; local read route and rendering only, not effective Agent configuration.
// Rationale: the accepted Agent config singleton needs a read peer beside
// config set; otherwise CLI/API parity is incomplete.
func TestAgentConfigShowUsesConfigSingleton(t *testing.T) {
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

// QA: HOST-07, UI-01; local complete-replacement encoding only, not drain or Agent reconfiguration.
// Rationale: Agent configuration must preserve repeatable labels as keyed values
// beside the exact interval and concurrency decisions in one PUT.
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

// QA: HOST-07, UI-03; pure CLI label parsing only, not server validation or applied configuration.
// Rationale: malformed or duplicate label keys must fail before map construction
// can discard input or silently select one competing value.
func TestParseAgentLabelsRejectsMalformedAndDuplicateKeys(t *testing.T) {
	t.Parallel()

	for _, values := range [][]string{{"missing-value"}, {"=empty-key"}, {"arch=arm64", "arch=amd64"}} {
		if _, err := parseAgentLabels(values); err == nil {
			t.Fatalf("parseAgentLabels(%q) error = nil", values)
		}
	}
}

// QA: OWN-02, UI-01; local stable-id request routing only, not persisted rename or descendant preservation.
// Rationale: rename is a synchronous POST update with one dedicated route,
// not a generic PATCH or a Task action.
func TestDedicatedRenameRoutesUseSynchronousPOST(t *testing.T) {
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

// QA: NET-02, UI-01; local stable-id PATCH shaping only, not pool containment or reservation conflict checks.
// Rationale: Environment pool replacement is a synchronous protected PATCH with
// only the new network pool, not a whole-Environment update or Task action.
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

// QA: CMP-01, UI-01, UI-02; local endpoint and config encoding only, not Component effects or ownership checks.
// Rationale: every component detail, action, and config operation must use
// the component's stable id rather than its mutable kind label.
func TestComponentActionsAndConfigUseStableID(t *testing.T) {
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
			response: `{"config":null}`,
			args:     []string{"config", "show", "cmp_1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := exactRequestServer(t, test.method, test.path, "", test.status, test.response)
			defer server.Close()
			executeNoun(t, newComponentCmd(), server.URL, Scope{}, test.args...)
		})
	}

	t.Run("config set", func(t *testing.T) {
		const template = ". {\n    {groundplane}\n}\n"
		const currentConfig = `{"corefile_template":". {\n    {groundplane}\n    log\n}\n",` +
			`"upstream_auto":false,"upstream_resolvers":[],"forwarders":[],"tailnet_delegation":false}`
		const componentResponse = `{"id":"cmp_1","owner":"platform","owner_id":"platform",` +
			`"environment_id":"","kind":"coredns","enabled":true,"config":` + currentConfig +
			`,"healthy":true,"status":"healthy"}`
		const configResponse = `{"config":` + currentConfig + `,"managed_files":[]}`
		const body = `{"config":{"corefile_template":". {\n    {groundplane}\n}\n",` +
			`"upstream_auto":true,"upstream_resolvers":["1.1.1.1"],` +
			`"forwarders":[{"domain":"example.com","resolvers":["9.9.9.9"]}],` +
			`"tailnet_delegation":true}}`
		const response = `{"resource":{"corefile_template":". {\n    {groundplane}\n}\n",` +
			`"upstream_auto":true,"upstream_resolvers":["1.1.1.1"],` +
			`"forwarders":[{"domain":"example.com","resolvers":["9.9.9.9"]}],` +
			`"tailnet_delegation":true},"reconcile_task_id":null}`
		path := t.TempDir() + "/Corefile"
		if err := os.WriteFile(path, []byte(template), 0o600); err != nil {
			t.Fatal(err)
		}

		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			requests++
			writer.Header().Set("Content-Type", "application/json")
			keys := request.Header.Values("Idempotency-Key")
			if requests < 3 && len(keys) != 0 {
				t.Errorf("safe request Idempotency-Key values = %q, want none", keys)
			}
			switch requests {
			case 1:
				assertComponentRequest(t, request, http.MethodGet, "/api/v1/components/cmp_1", "")
				_, _ = io.WriteString(writer, componentResponse)
			case 2:
				assertComponentRequest(t, request, http.MethodGet, "/api/v1/components/cmp_1/config", "")
				_, _ = io.WriteString(writer, configResponse)
			case 3:
				assertComponentRequest(t, request, http.MethodPut, "/api/v1/components/cmp_1/config", body)
				if len(keys) != 1 {
					t.Errorf("mutation Idempotency-Key values = %q, want exactly one", keys)
				} else if _, err := ulid.ParseStrict(keys[0]); err != nil {
					t.Errorf("mutation Idempotency-Key = %q, want raw ULID: %v", keys[0], err)
				}
				_, _ = io.WriteString(writer, response)
			default:
				t.Fatalf("unexpected request %d", requests)
			}
		}))
		defer server.Close()

		executeNoun(t, newComponentCmd(), server.URL, Scope{},
			"config", "set", "cmp_1", "--template-file", path,
			"--upstream-auto", "--upstream", "1.1.1.1",
			"--forward", "example.com=9.9.9.9", "--tailnet-delegation",
		)
		if requests != 3 {
			t.Fatalf("requests = %d, want component show, config show, config set", requests)
		}
	})
}

// QA: CMP-01, HTTP-03, UI-01; local replacement shaping only, not Caddy validation, reload, or traffic.
// Rationale: complete native policy and reserved Route references are opaque
// transport bytes; importing them must not erase existing Zone placement or
// depend on successfully rendering the template that the operator is replacing.
func TestComponentConfigSetImportsCaddyTemplateWithoutErasingZone(t *testing.T) {
	t.Parallel()
	const zoneID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	template := "http://{gp.route:app.example.com:/:host} {\n\trespond /internal/* 404\n" +
		"\treverse_proxy {gp.route:app.example.com:/:upstream}\n}\n"
	path := t.TempDir() + "/Caddyfile"
	if err := os.WriteFile(path, []byte(template), 0o600); err != nil {
		t.Fatal(err)
	}

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		writer.Header().Set("Content-Type", "application/json")
		switch requests {
		case 1:
			if request.Method != http.MethodGet || request.URL.Path != "/api/v1/components/cmp_1" {
				t.Fatalf("show request = %s %s", request.Method, request.URL.Path)
			}
			_, _ = io.WriteString(writer,
				`{"kind":"caddy","config":{"zone_ids":["`+zoneID+`"],"caddyfile_template":"retired {routes}"}}`)
		case 2:
			if request.Method == http.MethodGet {
				writer.Header().Set("Content-Type", "application/problem+json")
				writer.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = io.WriteString(writer,
					`{"status":422,"code":"validation.failed","detail":"saved template cannot be rendered"}`)
				return
			}
			if request.Method != http.MethodPut || request.URL.Path != "/api/v1/components/cmp_1/config" {
				t.Fatalf("set request = %s %s", request.Method, request.URL.Path)
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			want := `{"config":{"zone_ids":["` + zoneID +
				`"],"caddyfile_template":"http://{gp.route:app.example.com:/:host} {\n\trespond /internal/* 404\n\treverse_proxy {gp.route:app.example.com:/:upstream}\n}\n"}}`
			if string(body) != want {
				t.Fatalf("body = %s, want %s", body, want)
			}
			_, _ = io.WriteString(
				writer,
				`{"resource":{"zone_ids":["`+zoneID+`"],"caddyfile_template":"http://{gp.route:app.example.com:/:host} {\n\trespond /internal/* 404\n\treverse_proxy {gp.route:app.example.com:/:upstream}\n}\n"},"reconcile_task_id":null}`,
			)
		default:
			t.Fatalf("unexpected request %d", requests)
		}
	}))
	defer server.Close()

	executeNoun(t, newComponentCmd(), server.URL, Scope{}, "config", "set", "cmp_1", "--file", path)
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

// QA: TASK-06, UI-01; local finite SSE request and rendering only, not reconnect, compaction, or terminal drain.
// Rationale: the CLI must expose the same SSE Task event stream the Console
// follows, rather than requiring polling or a Console session.
func TestTaskEventsStreamsCanonicalEndpoint(t *testing.T) {
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

// QA: ATT-01, ATT-03, UI-01; local request shaping only, not credential provisioning or network attachment.
// Rationale: an attach belongs to one explicit consumer service and one
// backing service, and provisioning is a Task rather than a synchronous create.
func TestServiceAttachDispatchesTaskWithBothTargets(t *testing.T) {
	t.Parallel()

	body := `{"backing_service_id":"bks_1","credential":{"mode":"new"},"service_id":"svc_1"}`
	server := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/attaches",
		body,
		http.StatusAccepted,
		`{"task_id":"task_attach"}`,
	)
	defer server.Close()

	output := executeNoun(
		t, newServiceCmd(), server.URL, Scope{AsID: true},
		"attach", "svc_1", "bks_1", "--new-credential",
	)
	want := "{\n  \"task_id\": \"task_attach\"\n}\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

// QA: TASK-05, UI-01; local bodyless dispatch only, not eligibility, frozen inputs, or execution safety.
// Rationale: Retry must address the original Task through the canonical action
// endpoint while exposing the distinct Task id of the new attempt.
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

// QA: BAK-05, UI-01; local command shape and dispatch only, not policy admission, capture, or Retry behavior.
// Rationale: manual Backup uses the policy's complete frozen source set, so the
// CLI must expose no source selector and send one bodyless run request.
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

// QA: BAK-14, UI-01; local raw-byte transport only, not key-era ownership, no-store headers, or persistence.
// Rationale: key export is the exceptional bodyless read-like POST and the CLI
// must preserve its exact attachment bytes instead of applying structured formatting.
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

// Delivery: locked Release Group command inventory only; not product QA evidence.
// Rationale: the flat Release Group noun must retain the locked authoring and
// execution verbs, removal alias, and explicit rollback-preview/tag affordance.
func TestReleaseGroupCommandTreeUsesLockedVerbs(t *testing.T) {
	t.Parallel()

	command := newReleaseGroupCmd()
	want := map[string]bool{
		"list": true, "show": true, "add": true, "edit": true,
		"remove": true, "deploy": true, "rollback": true, "rollback-preview": true,
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
		if child.Name() == "rollback" && child.Flags().Lookup("tag") == nil {
			t.Error("rollback is missing the optional --tag flag")
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
