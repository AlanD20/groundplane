package cli

import (
	"bytes"
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

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

// Rationale: path, URL, tag and missing inputs must fail before HTTP dispatch.
func TestControllerUpdateRejectsUnpinnedInputs(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{nil, {"--release", "latest"}, {"--release", "/binary"},
		{"--release", "https://example.test/binary"}, {"--release", "sha256:" + strings.Repeat("A", 64)}} {
		command := newControllerUpdateCmd()
		command.SetArgs(args)
		command.SetOut(&bytes.Buffer{})
		command.SetErr(&bytes.Buffer{})
		if err := command.Execute(); err == nil {
			t.Fatalf("args %q accepted", args)
		}
	}
}
