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
			got, _ := observeRestorationProxy(
				context.Background(),
				fake,
				"aaaaaaaaaaaaaaaa",
				agentpb.ServiceProxyImplementation_SERVICE_PROXY_IMPLEMENTATION_CADDY,
				digest[:],
			)
			if got != test.want {
				t.Fatalf("restoration proven=%v want=%v", got, test.want)
			}
		})
	}
}
