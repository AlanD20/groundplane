package composeobserver

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// SVC-18: real observation assembly must retain only execution-failure classes,
// not raw probe output, and must use the latest probe rather than an old failure.
func TestObserveClassifiesHealthcheckExecutionFailure(t *testing.T) {
	for _, test := range []struct {
		name    string
		command string
		code    int
		status  container.HealthStatus
		want    agentpb.ComposeHelperDiagnostic
	}{
		{"missing pgrep", "pgrep -f -- 'nginx' >/dev/null", 127, container.Starting, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_PGREP_MISSING},
		{"missing curl", "curl -fsS -- 'http://127.0.0.1/' >/dev/null", 127, container.Unhealthy, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_CURL_MISSING},
		{"missing nc", "nc -z -- '127.0.0.1' '80'", 127, container.Starting, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_NC_MISSING},
		{"custom dependency", "operator-check", 127, container.Unhealthy, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_MISSING},
		{"custom command after pgrep", "pgrep -f -- 'nginx'; missing-tool >/dev/null", 127, container.Unhealthy, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_MISSING},
		{"not executable", "operator-check", 126, container.Starting, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_NOT_EXECUTABLE},
		{"ordinary startup failure", "operator-check", 1, container.Starting, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED},
		{"latest successful probe", "operator-check", 0, container.Starting, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED},
		{"healthy ignores stale failure", "operator-check", 127, container.Healthy, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED},
	} {
		t.Run(test.name, func(t *testing.T) {
			artifact := observerArtifact(t)
			labels := dockerLabels(artifact)
			engine := &fakeEngine{containers: []container.Summary{{ID: "candidate", Labels: labels}},
				inspects: map[string]client.ContainerInspectResult{"candidate": {Container: container.InspectResponse{
					ID: "candidate", Name: "/candidate", Image: "sha256:image-id",
					Config: &container.Config{Image: "example/image", Labels: labels,
						Healthcheck: &container.HealthConfig{Test: []string{"CMD-SHELL", test.command}}},
					State: &container.State{
						Status: container.StateRunning,
						Health: &container.Health{Status: test.status,
							Log: []*container.HealthcheckResult{
								{ExitCode: 127},
								{ExitCode: test.code, Output: "private-probe-output"},
							}},
					},
				}}}}
			observer, err := NewWithEngine(engine)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := observer.observeArtifact(context.Background(), artifact, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(observed.Containers) != 1 || observed.Containers[0].GetHealthcheckDiagnostic() != test.want {
				t.Fatalf("healthcheck classification = %v, want %v", observed.Containers, test.want)
			}
		})
	}
}

// SVC-18: direct native commands fail before obtaining a process exit status.
// A Docker timeout also records -1 but must retain ordinary readiness retries.
func TestDirectHealthcheckStartFailureAndTimeout(t *testing.T) {
	for _, test := range []struct {
		name, executable, output string
		want                     agentpb.ComposeHelperDiagnostic
	}{
		{"missing builtin", "pgrep", `OCI runtime exec failed: exec failed: unable to start container process: exec: "pgrep": executable file not found in $PATH`, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_PGREP_MISSING},
		{"missing custom", "/app/check", `OCI runtime exec failed: exec failed: exec: "/app/check": stat /app/check: no such file or directory`, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_MISSING},
		{"not executable", "/app/check", `OCI runtime exec failed: exec: "/app/check": permission denied`, agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_NOT_EXECUTABLE},
		{"timeout", "pgrep", "Health check exceeded timeout (3s)", agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED},
		{"unrelated runtime failure", "pgrep", "container runtime unavailable", agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := healthcheckDiagnostic(
				&container.Config{Healthcheck: &container.HealthConfig{Test: []string{"CMD", test.executable}}},
				&container.State{
					Health: &container.Health{
						Status: container.Starting,
						Log:    []*container.HealthcheckResult{{ExitCode: -1, Output: test.output}},
					},
				},
			)
			if got != test.want {
				t.Fatalf("diagnostic=%v, want=%v", got, test.want)
			}
		})
	}
}
