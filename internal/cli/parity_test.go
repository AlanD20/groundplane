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
	"github.com/spf13/cobra"
)

func TestRouterShowRequestsEnvironmentProjection(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(t, http.MethodGet, "/api/v1/environments/production/router", "", http.StatusOK, `{}`)
	defer server.Close()

	output := executeNoun(t, newRouterCmd(), server.URL, Scope{Environment: "production"}, "show")
	if output != "{}\n" {
		t.Fatalf("output = %q, want %q", output, "{}\n")
	}
}

func TestAgentRemoveDispatchesRemovalTask(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(t, http.MethodDelete, "/api/v1/agents/agt_1", "", http.StatusAccepted, `{"task_id":"task_2"}`)
	defer server.Close()

	output := executeNoun(t, newAgentCmd(), server.URL, Scope{}, "remove", "agt_1")
	want := "task task_2 dispatched — `groundplane task show task_2` to follow\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestAgentJoinDispatchesCreationTaskWithoutToken(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(t, http.MethodPost, "/api/v1/agents", "", http.StatusAccepted, `{"task_id":"task_join"}`)
	defer server.Close()

	output := executeNoun(t, newAgentCmd(), server.URL, Scope{}, "join")
	want := "task task_join dispatched — `groundplane task show task_join` to follow\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestTaskRetryDispatchesNewAttempt(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(t, http.MethodPost, "/api/v1/tasks/task_1/retry", "", http.StatusAccepted, `{"task_id":"task_2"}`)
	defer server.Close()

	output := executeNoun(t, newTaskCmd(), server.URL, Scope{}, "retry", "task_1")
	want := "task task_2 dispatched — `groundplane task show task_2` to follow\n"
	if output != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestBackupExportKeyPostsAndPrintsIdentity(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(t, http.MethodPost, "/api/v1/environments/production/export-key", "", http.StatusOK, `{"value":"AGE-SECRET-KEY-1TEST"}`)
	defer server.Close()

	output := executeNoun(t, newBackupCmd(), server.URL, Scope{Environment: "production"}, "export-key")
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

func exactRequestServer(t *testing.T, method, path, body string, status int, response string) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != method {
			t.Errorf("method = %q, want %q", request.Method, method)
		}
		if request.URL.RequestURI() != path {
			t.Errorf("path = %q, want %q", request.URL.RequestURI(), path)
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
	}))
}

func executeNoun(t *testing.T, command *cobra.Command, baseURL string, scope Scope, args ...string) string {
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
