package cli

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/cli/apiclient"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// QA: UP-01, UI-01; local protected request dispatch only, not staging, activation, recovery, or continuity.
// Rationale: native update is an API command even though its parent also owns
// local administration. Exactly one digest-only protected request is dispatched.
func TestControllerUpdateDispatchesPinnedRelease(t *testing.T) {
	t.Parallel()
	release := "sha256:" + strings.Repeat("a", 64)
	server := exactRequestServer(t, http.MethodPost, "/api/v1/controller/update",
		`{"release":"`+release+`"}`, http.StatusAccepted, `{"task_id":"task_update"}`)
	defer server.Close()
	var output bytes.Buffer
	root := NewRootCmd(Dependencies{})
	root.SetOut(&output)
	root.SetArgs([]string{"--config", filepath.Join(t.TempDir(), "missing.yaml"),
		"--host", server.URL, "--output", "JSON", "controller", "update", "--release", release})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"task_id": "task_update"`) {
		t.Fatalf("accepted output = %q", output.String())
	}
}

// QA: UP-03, UI-03; local digest admission only, not candidate compatibility or host preservation.
// Rationale: path, URL, tag and missing inputs must fail before HTTP dispatch.
func TestControllerUpdateRejectsUnpinnedInputs(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		t.Error("unpinned Controller update reached the API")
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	for _, args := range [][]string{nil, {"--release", "latest"}, {"--release", "/binary"},
		{"--release", "https://example.test/binary"}, {"--release", "sha256:" + strings.Repeat("A", 64)}} {
		command := newControllerUpdateCmd()
		command.SetContext(context.WithValue(context.Background(), appKey{}, &App{Client: apiclient.New(server.URL)}))
		command.SetArgs(args)
		command.SetOut(&bytes.Buffer{})
		command.SetErr(&bytes.Buffer{})
		err := command.Execute()
		if err == nil {
			t.Fatalf("args %q accepted", args)
		}
		if args != nil {
			if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
				t.Fatalf("args %q error = %v, want validation failure", args, err)
			}
		}
	}
}
