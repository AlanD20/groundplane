package serviceproxy

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
)

// Rationale: REL-02/SVC-15. A successful live switch alone missed the boot
// outage. Execute the actual startup/activation procedure in the proxy image,
// restart the same container, and require the independently expected HTTP body.
// Recreating a container must discard its old selection and use sealed input.
func TestProxySelectionSurvivesRestart(t *testing.T) {
	image := os.Getenv("GROUNDPLANE_PROXY_TEST_IMAGE")
	if image == "" {
		t.Skip("requires an explicitly selected local proxy image and Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	r := runner.New(slog.Default())
	run := func(stdin []byte, args ...string) string {
		t.Helper()
		result, err := r.Run(ctx, runner.RunCmdOpts{Name: "docker", Args: args, Stdin: stdin, CaptureLimitBytes: 65536})
		if err != nil || result.ExitCode != 0 {
			t.Fatalf("docker %v: %v exit=%d stderr=%s", args, err, result.ExitCode, result.Stderr)
		}
		return strings.TrimSpace(string(result.Stdout))
	}
	config := func(body string) []byte {
		return []byte(
			fmt.Sprintf(
				`{"admin":{"listen":"127.0.0.1:2019"},"apps":{"http":{"servers":{"probe":{"listen":[":8080"],"routes":[{"handle":[{"handler":"static_response","body":%q}]}]}}}}}`,
				body,
			),
		)
	}
	initial := filepath.Join(t.TempDir(), "initial.json")
	if err := os.WriteFile(initial, config("initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	start := func() string {
		id := run(nil, "run", "--detach", "--network", "none", "--mount",
			"type=bind,src="+initial+",dst="+InitialConfigPath+",readonly", image, "sh", "-ec", Startup)
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			result, err := r.Run(
				cleanupCtx,
				runner.RunCmdOpts{Name: "docker", Args: []string{"rm", "--force", "--volumes", id}},
			)
			if err != nil || result.ExitCode != 0 {
				t.Errorf("remove owned proxy %s: %v exit=%d", id, err, result.ExitCode)
			}
		})
		return id
	}
	assertBody := func(id, want string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			result, err := r.Run(ctx, runner.RunCmdOpts{Name: "docker",
				Args: []string{"exec", id, "wget", "-qO-", "http://127.0.0.1:8080/"}, CaptureLimitBytes: 65536})
			if err == nil && result.ExitCode == 0 && string(result.Stdout) == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("HTTP body=%q exit=%d error=%v, want %q", result.Stdout, result.ExitCode, err, want)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	id := start()
	assertBody(id, "initial")
	if got := run(nil, "exec", id, "cat", "/proc/1/cmdline"); got != RunningCommand {
		t.Fatalf("startup command=%q, want %q", got, RunningCommand)
	}
	for _, want := range []string{"selected-blue", "restored-prior"} {
		run(config(want), "exec", "--interactive", id, "sh", "-ec", Activate)
		assertBody(id, want)
		run(nil, "restart", id)
		assertBody(id, want)
	}
	assertBody(start(), "initial")
}
