package composehelper

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: SVC-06. Argument-only tests missed an unsupported Compose flag.
// Execute the generated commands with the shipped tooling, require a stopped
// proxy to start, and prove a running proxy retains its identity and start time.
func TestBlueGreenProxyStartWithShippedCompose(t *testing.T) {
	tooling, image := os.Getenv(
		"GROUNDPLANE_HELPER_TEST_IMAGE",
	), os.Getenv(
		"GROUNDPLANE_PROXY_TEST_IMAGE",
	)
	if tooling == "" || image == "" {
		t.Skip("requires explicitly selected local Agent and proxy images and Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r := runner.New(slog.Default())
	project := fmt.Sprintf("gp-proxy-start-%d", time.Now().UnixNano())
	artifact := &agentpb.ComposeArtifact{
		ProjectName: project,
		CanonicalYaml: []byte(
			fmt.Sprintf(
				"services:\n  api:\n    image: %s\n    command: [sleep, '300']\n    network_mode: none\n  api--green:\n    image: %s\n    command: [sleep, '300']\n    network_mode: none\n",
				image,
				image,
			),
		),
		Services: []*agentpb.ComposeService{
			{
				ServiceId:   "api",
				ComposeName: "api",
				Role:        agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			},
			{
				ServiceId:   "api",
				ComposeName: "api--green",
				Role:        agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
				Slot:        "green",
			},
		},
	}
	invoke := func(ctx context.Context, stdin []byte, args ...string) (runner.Result, error) {
		return r.Run(ctx, runner.RunCmdOpts{Name: "docker", Stdin: stdin, CaptureLimitBytes: 65536,
			Args: append([]string{"run", "--rm", "--interactive", "--network", "none", "--mount",
				"type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock", "--entrypoint", "docker", tooling}, args...),
		})
	}
	run := func(stdin []byte, args ...string) string {
		t.Helper()
		result, err := invoke(ctx, stdin, args...)
		if err != nil || result.ExitCode != 0 {
			t.Fatalf(
				"shipped docker %v: %v exit=%d stderr=%s",
				args,
				err,
				result.ExitCode,
				result.Stderr,
			)
		}
		return strings.TrimSpace(string(result.Stdout))
	}
	base := []string{"compose", "--project-name", project, "--file", "-"}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		result, err := invoke(cleanupCtx, artifact.CanonicalYaml,
			append(base, "down", "--volumes", "--remove-orphans", "--timeout", "1")...)
		if err != nil || result.ExitCode != 0 {
			t.Errorf("remove owned project %s: %v exit=%d", project, err, result.ExitCode)
		}
	})
	run(artifact.CanonicalYaml, append(base, "up", "--detach", "api")...)
	id := run(artifact.CanonicalYaml, append(base, "ps", "--all", "--quiet", "api")...)
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ComposeWorkloadApply{
		ComposeWorkloadApply: &agentpb.ComposeWorkloadApply{ServiceId: "api", Target: "green"},
	}}
	commands, err := commandsFor(&agentpb.ComposeHelperRequest{}, step, artifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, stopped := range []bool{true, false} {
		before := run(nil, "inspect", "--format", "{{.Id}} {{.State.StartedAt}}", id)
		if stopped {
			run(nil, "stop", "--time", "1", id)
		}
		for _, command := range commands {
			run(command.Stdin, command.Args...)
		}
		if actual := run(artifact.CanonicalYaml, append(base, "ps", "--quiet", "api")...); actual != id {
			t.Fatalf("running proxy identity=%q, want %q", actual, id)
		}
		if after := run(nil, "inspect", "--format", "{{.Id}} {{.State.StartedAt}}", id); !stopped &&
			after != before {
			t.Fatalf("already-running proxy restarted: before=%s after=%s", before, after)
		}
	}
}
