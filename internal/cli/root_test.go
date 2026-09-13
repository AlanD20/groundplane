package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Delivery: closed API/local/tool command classification; no command is executed.
// Rationale: representative commands must retain the execution class that
// controls whether they load human CLI config or invoke same-host tooling.
func TestCommandExecutionClasses(t *testing.T) {
	t.Parallel()

	root := NewRootCmd(Dependencies{})
	tests := []struct {
		path  []string
		class executionClass
	}{
		{path: []string{"tenant", "list"}, class: executionAPI},
		{path: []string{"controller", "serve"}, class: executionLocal},
		{path: []string{"controller", "key", "show"}, class: executionLocal},
		{path: []string{"controller", "etcd", "show"}, class: executionLocal},
		{path: []string{"agent-run", "run"}, class: executionLocal},
		{path: []string{"version"}, class: executionTool},
		{path: []string{"completion", "bash"}, class: executionTool},
	}

	for _, test := range tests {
		command, _, err := root.Find(test.path)
		if err != nil {
			t.Fatalf("find %v: %v", test.path, err)
		}
		if got := commandExecutionClass(command); got != test.class {
			t.Errorf("execution class for %v = %q, want %q", test.path, got, test.class)
		}
	}
}

// Delivery: local Controller process command with an injected runner; no daemon is started.
// Rationale: controller serve must not parse unrelated human API client config
// before invoking its local process runner.
func TestControllerServeSkipsCLIConfigAndInvokesRunner(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "invalid-cli.yaml")
	if err := os.WriteFile(configPath, []byte("unknown: true\n"), 0o600); err != nil {
		t.Fatalf("write CLI config: %v", err)
	}

	called := false
	root := NewRootCmd(Dependencies{
		RunController: func(ctx context.Context) error {
			called = true
			return nil
		},
	})
	root.SetArgs([]string{"--config", configPath, "controller", "serve"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("controller serve: %v", err)
	}
	if !called {
		t.Fatal("Controller runner was not invoked")
	}
}

// Delivery: local Controller process error propagation with an injected runner.
// Rationale: controller serve must return its runner's causal error so startup
// failures are not converted into success or an unrelated CLI error.
func TestControllerServePreservesRunnerError(t *testing.T) {
	t.Parallel()

	want := errors.New("serve failed")
	root := NewRootCmd(Dependencies{
		RunController: func(context.Context) error { return want },
	})
	root.SetArgs([]string{"controller", "serve"})
	if err := root.ExecuteContext(context.Background()); !errors.Is(err, want) {
		t.Fatalf("controller serve error = %v, want %v", err, want)
	}
}

// Delivery: local version/completion behavior; no API request or daemon action is exercised.
// Rationale: standalone tooling must remain usable even when the human API
// client configuration is malformed.
func TestToolCommandsSkipCLIConfig(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "invalid-cli.yaml")
	if err := os.WriteFile(configPath, []byte("unknown: true\n"), 0o600); err != nil {
		t.Fatalf("write CLI config: %v", err)
	}

	for _, args := range [][]string{{"version"}, {"completion", "bash"}} {
		var output bytes.Buffer
		root := NewRootCmd(Dependencies{})
		root.SetOut(&output)
		root.SetErr(&output)
		root.SetArgs(append([]string{"--config", configPath}, args...))
		if err := root.ExecuteContext(context.Background()); err != nil {
			t.Fatalf("execute %v: %v", args, err)
		}
		if output.Len() == 0 {
			t.Fatalf("execute %v produced no output", args)
		}
	}
}

// Delivery: locked completion command inventory; no generated script is executed.
// Rationale: completion must expose exactly the documented Bash, Fish, and Zsh
// generators without Cobra adding an unsupported default command.
func TestCompletionCommandHasOnlyLockedShells(t *testing.T) {
	t.Parallel()

	root := NewRootCmd(Dependencies{})
	completion, _, err := root.Find([]string{"completion"})
	if err != nil {
		t.Fatalf("find completion: %v", err)
	}

	got := make([]string, 0, len(completion.Commands()))
	for _, child := range completion.Commands() {
		got = append(got, child.Name())
	}
	sort.Strings(got)
	want := []string{"bash", "fish", "zsh"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("completion shells = %v, want %v", got, want)
	}

	if command, _, findErr := root.Find(
		[]string{"completion", "powershell"},
	); findErr == nil &&
		command.Name() == "powershell" {
		t.Fatal("unexpected powershell completion command")
	}
	if !root.CompletionOptions.DisableDefaultCmd {
		t.Fatal("Cobra default completion command is enabled")
	}
}

// QA: UI-03; representative API-command preflight only, with no HTTP request executed.
// Rationale: API commands must strictly load human CLI config instead of
// inheriting the exemptions reserved for closed local/tool commands.
func TestAPICommandsStillLoadCLIConfig(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "invalid-cli.yaml")
	if err := os.WriteFile(configPath, []byte("unknown: true\n"), 0o600); err != nil {
		t.Fatalf("write CLI config: %v", err)
	}

	root := NewRootCmd(Dependencies{})
	root.SetArgs([]string{"--config", configPath, "tenant", "list"})
	err := root.ExecuteContext(context.Background())
	if err == nil || !strings.Contains(err.Error(), "field unknown not found") {
		t.Fatalf("API command error = %v, want strict CLI config error", err)
	}
}

// Delivery: local foreground Agent command refusal; no Agent process is started.
// Rationale: the unimplemented local command must bypass human CLI config yet
// fail closed with its specific error rather than appearing successful.
func TestUnimplementedAgentRunFailsClosedWithoutCLIConfig(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "invalid-cli.yaml")
	if err := os.WriteFile(configPath, []byte("unknown: true\n"), 0o600); err != nil {
		t.Fatalf("write CLI config: %v", err)
	}

	args := []string{"agent-run", "run"}
	root := NewRootCmd(Dependencies{})
	root.SetArgs(append([]string{"--config", configPath}, args...))
	err := root.ExecuteContext(context.Background())
	if !errors.Is(err, errs.New(errs.KindNotImplemented, "")) {
		t.Fatalf("execute %v error = %v, want %q", args, err, errs.CodeNotImplemented)
	}
}
