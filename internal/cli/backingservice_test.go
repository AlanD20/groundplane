package cli

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: omitted and empty authentication flags must fail locally without
// publishing a request or relying on a server to choose a mode.
func TestBackingServiceCreateRequiresValkeyAuthentication(t *testing.T) {
	for _, extra := range [][]string{nil, {"--authentication", ""}} {
		command := newBackingServiceCmd()
		command.SetContext(context.WithValue(context.Background(), appKey{}, &App{}))
		command.SetOut(io.Discard)
		command.SetErr(io.Discard)
		command.SetArgs(append([]string{"create", "cache", "--adapter", "valkey:9", "--name", "Cache",
			"--network-pool", "10.80.0.0/24", "--zone-name", "data", "--zone-subnet", "10.80.0.0/24"}, extra...))
		err := command.Execute()
		if kind, _ := errs.KindOf(err); kind != errs.KindValidationFailed {
			t.Fatalf("missing authentication error = %v", err)
		}
	}
}

// Rationale: an explicitly selected password mode is sent unchanged.
func TestBackingServiceCreateSendsValkeyAuthentication(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(
		t,
		http.MethodPost,
		"/api/v1/backing-services",
		`{"adapter":"valkey:9","authentication":"password","name":"Shared Valkey","network_pool":"10.20.0.0/16","slug":"shared-valkey","zone":{"internal":true,"name":"data","subnet":"10.20.1.0/24"}}`,
		http.StatusCreated,
		`{"backing_service":{"authentication":"password","backing_network_id":"net_1","environment_id":"env_1","project_id":"prj_1","service_id":"svc_1"},"task_id":"task_1"}`,
	)
	defer server.Close()

	output := executeNoun(
		t,
		newBackingServiceCmd(),
		server.URL,
		Scope{},
		"create", "shared-valkey",
		"--name", "Shared Valkey",
		"--adapter", "valkey:9",
		"--authentication", "password",
		"--network-pool", "10.20.0.0/16",
		"--zone-name", "data",
		"--zone-subnet", "10.20.1.0/24",
		"--zone-internal",
	)
	if want := "\"authentication\": \"password\""; !strings.Contains(output, want) {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestBackingServiceShowIncludesAuthentication(t *testing.T) {
	t.Parallel()

	server := exactRequestServer(
		t,
		http.MethodGet,
		"/api/v1/backing-services/prj_1",
		"",
		http.StatusOK,
		`{"authentication":"none","backing_network_id":"net_1","environment_id":"env_1","project_id":"prj_1","service_id":"svc_1"}`,
	)
	defer server.Close()

	output := executeNoun(t, newBackingServiceCmd(), server.URL, Scope{AsID: true}, "show", "prj_1")
	if want := "\"authentication\": \"none\""; !strings.Contains(output, want) {
		t.Fatalf("output = %q, want %q", output, want)
	}
}
