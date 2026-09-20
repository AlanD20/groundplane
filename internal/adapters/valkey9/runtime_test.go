//go:build valkey_auth_integration

package valkey9

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: real AUTH and restart behavior cannot be established from command
// strings alone. Each case owns one internal network and portless test containers.
func TestValkeyAuthenticationLifecycle(t *testing.T) {
	for _, mode := range []core.BackingAuthentication{
		core.BackingAuthenticationUsernamePassword,
		core.BackingAuthenticationPassword,
		core.BackingAuthenticationNone,
	} {
		t.Run(string(mode), func(t *testing.T) { testAuthenticationLifecycle(t, mode) })
	}
}

func testAuthenticationLifecycle(t *testing.T, mode core.BackingAuthentication) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	executor := runner.New(nil)
	data := t.TempDir()
	networkName := "gp-auth-" + strings.ToLower(ids.New(ids.KindTask))
	network, networkErr := executor.Run(
		ctx,
		runner.RunCmdOpts{
			Name: "docker",
			Args: []string{"network", "create", "--internal", "--label", "groundplane.test=valkey-auth", networkName},
		},
	)
	if networkErr != nil || network.ExitCode != 0 {
		t.Fatal("create isolated test network")
	}
	networkID := strings.TrimSpace(string(network.Stdout))
	if len(networkID) != 64 || strings.Trim(networkID, "0123456789abcdef") != "" {
		t.Fatal("invalid new network id")
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		result, cleanupErr := executor.Run(
			cleanupCtx,
			runner.RunCmdOpts{Name: "docker", Args: []string{"network", "rm", networkID}},
		)
		if cleanupErr != nil || result.ExitCode != 0 {
			t.Errorf("test network cleanup: %v", cleanupErr)
		}
	})
	spec := (&adapter{}).CreationSpec(mode)
	args := []string{"run", "--detach", "--pull=never", "--network", networkID, "--network-alias", "backing",
		"--label", "groundplane.test=valkey-auth", "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"--mount", "type=bind,src=" + data + ",dst=/data", "--entrypoint", spec.Command[0],
		"--env", "VALKEY_PASSWORD=fixture-admin-only", "--env", "REDISCLI_AUTH=fixture-admin-only",
		"--env", "VALKEY_AUTHENTICATION=" + string(mode), (&adapter{}).DefaultImage()}
	args = append(args, spec.Command[1:]...)
	started, err := executor.Run(ctx, runner.RunCmdOpts{Name: "docker", Args: args})
	if err != nil || started.ExitCode != 0 {
		t.Fatalf("start: %v: %s", err, started.Stderr)
	}
	container := strings.TrimSpace(string(started.Stdout))
	if len(container) != 64 || strings.Trim(container, "0123456789abcdef") != "" {
		t.Fatal("invalid new container id")
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		result, cleanupErr := executor.Run(
			cleanupCtx,
			runner.RunCmdOpts{Name: "docker", Args: []string{"rm", "--force", container}},
		)
		if cleanupErr != nil || result.ExitCode != 0 {
			t.Errorf("test container cleanup: %v", cleanupErr)
		}
	})
	command := func(admin bool, input string) runner.Result {
		t.Helper()
		cliArgs := []string{"exec", "-i", container, "env", "-u", "REDISCLI_AUTH", "valkey-cli", "-e", "--raw"}
		if admin {
			cliArgs = []string{"exec", "-i", container, "valkey-cli", "--user", "groundplane", "-e", "--raw"}
		}
		result, _ := executor.Run(
			ctx,
			runner.RunCmdOpts{Name: "docker", Args: cliArgs, Stdin: []byte(input), Timeout: 5 * time.Second},
		) // Failure is asserted by each caller.
		// CLI stdin runs its REPL, which returns zero for server errors even
		// with -e. These fixed fixture commands contain no arbitrary replies.
		for _, failure := range []string{"NOAUTH ", "WRONGPASS ", "NOPERM ", "ERR "} {
			if strings.Contains(string(result.Stdout), failure) {
				result.ExitCode = 1
			}
		}
		return result
	}
	waitReady := func() {
		t.Helper()
		for range 50 {
			if result := command(true, "PING\n"); result.ExitCode == 0 &&
				strings.TrimSpace(string(result.Stdout)) == "PONG" {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal("startup deadline")
			case <-time.After(100 * time.Millisecond):
			}
		}
		t.Fatal("Valkey did not become ready")
	}
	waitReady()
	unauthenticated := command(false, "PING\n")
	if (unauthenticated.ExitCode == 0) != (mode == core.BackingAuthenticationNone) {
		t.Fatal("incorrect unauthenticated access policy")
	}
	owners := []adapters.Input{
		{Authentication: mode, Role: "owner_one", Password: []byte("owner-password-1")},
		{Authentication: mode, Role: "owner_two", Password: []byte("owner-password-2")},
	}
	if mode == core.BackingAuthenticationPassword {
		owners[0].Role, owners[1].Role = "default", "default"
	}
	if mode == core.BackingAuthenticationNone {
		owners[0].Role, owners[0].Password, owners[1].Role, owners[1].Password = "", nil, "", nil
	}
	execute := func(steps []adapters.Step) {
		t.Helper()
		defer adapters.ClearSteps(steps)
		for _, step := range steps {
			cliArgs := append([]string{"exec", "-i", container, step.Program}, step.Args...)
			result, runErr := executor.Run(ctx, runner.RunCmdOpts{Name: "docker", Args: cliArgs, Stdin: step.Stdin})
			if runErr != nil || result.ExitCode != 0 {
				t.Fatal("compiled adapter operation failed")
			}
		}
	}
	login := func(owner adapters.Input) string {
		if mode == core.BackingAuthenticationNone {
			return ""
		}
		if mode == core.BackingAuthenticationPassword {
			return "AUTH " + string(owner.Password) + "\n"
		}
		return "AUTH " + owner.Role + " " + string(owner.Password) + "\n"
	}
	for _, owner := range owners {
		execute((&adapter{}).ProvisionSteps(owner))
		result := command(false, login(owner)+"PING\n")
		if result.ExitCode != 0 || !strings.Contains(string(result.Stdout), "PONG") {
			t.Fatal("owner authentication failed")
		}
		if result := command(false, login(owner)+"ACL LIST\n"); result.ExitCode == 0 {
			t.Fatal("consumer can administer ACLs")
		}
	}
	remoteArgs := []string{"run", "--rm", "--pull=never", "--network", networkID, "--entrypoint", "valkey-cli"}
	if mode != core.BackingAuthenticationNone {
		remoteArgs = append(remoteArgs, "--env", "REDISCLI_AUTH")
	}
	remoteArgs = append(remoteArgs, (&adapter{}).DefaultImage(), "-h", "backing", "-e", "--raw")
	if mode == core.BackingAuthenticationUsernamePassword {
		remoteArgs = append(remoteArgs, "--user", owners[1].Role)
	}
	remoteArgs = append(remoteArgs, "PING")
	remote, remoteErr := executor.Run(
		ctx,
		runner.RunCmdOpts{
			Name: "docker",
			Args: remoteArgs,
			Env:  []string{"REDISCLI_AUTH=" + string(owners[1].Password)},
		},
	)
	if remoteErr != nil || remote.ExitCode != 0 || strings.TrimSpace(string(remote.Stdout)) != "PONG" {
		t.Fatalf("separate network client cannot connect: %v: %s %s", remoteErr, remote.Stdout, remote.Stderr)
	}
	restart := func() {
		t.Helper()
		result, restartErr := executor.Run(ctx, runner.RunCmdOpts{Name: "docker", Args: []string{"restart", container}})
		if restartErr != nil || result.ExitCode != 0 {
			t.Fatal("restart failed")
		}
		waitReady()
	}
	restart()
	for _, owner := range owners {
		if result := command(false, login(owner)+"PING\n"); result.ExitCode != 0 {
			t.Fatal("credential did not survive restart")
		}
	}
	execute((&adapter{}).DetachSteps(owners[0]))
	restart()
	if mode != core.BackingAuthenticationNone {
		if result := command(false, login(owners[0])); result.ExitCode == 0 {
			t.Fatal("detached credential remains valid")
		}
	}
	if result := command(false, login(owners[1])+"PING\n"); result.ExitCode != 0 {
		t.Fatal("detach revoked another owner")
	}
	if result := command(true, "PING\n"); result.ExitCode != 0 {
		t.Fatal("detach revoked management access")
	}
	// A saved credential is not durable if ACL SAVE fails. Exercise the real
	// compiled command, not the fixture REPL's interpreted response status.
	if err := os.Chmod(data, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(data, 0700); err != nil {
			t.Errorf("restore test directory permissions: %v", err)
		}
	})
	steps := aclSteps([]string{"ACL", "DELUSER", "already_absent"}, nil)
	defer adapters.ClearSteps(steps)
	save := steps[1]
	saveArgs := append([]string{"exec", "-i", container, save.Program}, save.Args...)
	result, saveErr := executor.Run(ctx, runner.RunCmdOpts{Name: "docker", Args: saveArgs})
	if saveErr == nil || result.ExitCode == 0 {
		t.Fatal("ACL persistence failure was reported as success")
	}
}
