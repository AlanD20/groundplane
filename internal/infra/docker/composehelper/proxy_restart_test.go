package composehelper

import (
	"context"
	"crypto/sha256"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: REL-02/SVC-15. Live routing can match while restart bytes are stale
// or absent. Neither probe nor recovery may acknowledge that split state.
func TestRestorationRequiresLiveAndRestartProxyConfig(t *testing.T) {
	config := []byte(`{"apps":{}}`)
	digest := sha256.Sum256(config)
	for _, test := range []struct {
		name    string
		stored  []byte
		exit    int
		want    bool
		initial bool
	}{
		{"same", config, 0, true, false},
		{"stale", []byte(`{"apps":{"http":{}}}`), 0, false, false},
		{"partial", []byte(`{"apps":`), 0, false, false},
		{"missing", nil, 1, false, false},
		{"sealed predecessor startup", config, 0, true, true},
		{"stale predecessor startup", []byte(`{"apps":{"http":{}}}`), 0, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := runner.NewFake()
			startupPath := serviceproxy.RuntimeConfigPath
			if test.initial {
				startupPath = serviceproxy.InitialConfigPath
			}
			fake.RunFunc = func(_ context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
				if slices.Equal(opts.Args, []string{"exec", "aaaaaaaaaaaaaaaa", "cat", "/proc/1/cmdline"}) {
					return runner.Result{Stdout: []byte("caddy\x00run\x00--config\x00" + startupPath + "\x00")}, nil
				}
				if slices.Equal(
					opts.Args,
					[]string{"exec", "aaaaaaaaaaaaaaaa", "cat", startupPath},
				) {
					return runner.Result{Stdout: test.stored, ExitCode: test.exit}, nil
				}
				if !slices.Contains(opts.Args, "http://127.0.0.1:2019/config/") {
					t.Fatalf("probe performed unexpected command: %v", opts.Args)
				}
				return runner.Result{Stdout: config}, nil
			}
			got, _ := observeRestorationProxy(context.Background(), fake, "aaaaaaaaaaaaaaaa", digest[:])
			if got != test.want {
				t.Fatalf("restoration proven=%v want=%v", got, test.want)
			}
		})
	}
}

// Rationale: SVC-06. Candidate startup must also start an existing stopped
// proxy, but applying new proxy YAML would replace the stable container and
// break connections. Start is the idempotent non-reconciling operation.
func TestBlueGreenStartsExistingProxyWithoutRecreatingIt(t *testing.T) {
	artifact := &agentpb.ComposeArtifact{ProjectName: "gp-test", Services: []*agentpb.ComposeService{
		{ServiceId: "api", ComposeName: "api", Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY},
		{
			ServiceId:   "api",
			ComposeName: "api--green",
			Role:        agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			Slot:        "green",
		},
	}}
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ComposeWorkloadApply{
		ComposeWorkloadApply: &agentpb.ComposeWorkloadApply{ServiceId: "api", Target: "green"},
	}}
	commands, err := commandsFor(&agentpb.ComposeHelperRequest{}, step, artifact)
	if err != nil || len(commands) != 3 {
		t.Fatalf("commands=%v error=%v", commands, err)
	}
	if !slices.Equal(commands[1].Args[len(commands[1].Args)-3:], []string{"up", "--detach", "api--green"}) ||
		!slices.Equal(commands[2].Args[len(commands[2].Args)-3:], []string{"start", "--wait", "api"}) {
		t.Fatalf("candidate/proxy startup changed stable proxy: %v", commands)
	}
}
