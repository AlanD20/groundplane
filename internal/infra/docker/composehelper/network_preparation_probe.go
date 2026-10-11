package composehelper

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Definitive absence or an exactly owned bridge settles creation uncertainty.
// This procedure has no create, disconnect, remove, or container command.
func executeManagedNetworkPreparationProbe(ctx context.Context, taskRunner runner.Runner, timeout uint32,
	artifact *agentpb.ComposeArtifact, probe *agentpb.ManagedNetworkPreparationProbe,
) (*agentpb.ComposeHelperResponse, error) {
	network, err := selectedEnsuredNetwork(
		artifact,
		&agentpb.ManagedNetworkEnsure{ArtifactId: probe.GetArtifactId(), NetworkId: probe.GetNetworkId()},
	)
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	read := func(args ...string) (runner.Result, error) {
		result, err := taskRunner.Run(bounded, runner.RunCmdOpts{Name: DockerExecutable, Args: args, Dir: WorkDirectory,
			Env: append(
				[]string(nil),
				fixedEnvironment...), ReplaceEnv: true, CaptureLimitBytes: maximumComponentConfig})
		if bounded.Err() != nil {
			return runner.Result{}, bounded.Err()
		}
		if err != nil {
			return runner.Result{}, errs.Wrap(errs.KindInternal, err)
		}
		if result.ExitCode != 0 {
			return runner.Result{}, errs.New(
				errs.KindStateConflict,
				"network reconciliation could not read Docker state",
			)
		}
		return result, nil
	}
	listed, err := read(
		"network",
		"ls",
		"--filter",
		"name=^"+network.Name+"$",
		"--format",
		"{{.ID}} {{.Name}}",
		"--no-trunc",
	)
	if err != nil {
		return nil, err
	}
	identity := strings.Fields(string(listed.Stdout))
	if len(identity) == 0 {
		return completedResponse(), nil
	}
	if len(identity) != 2 || identity[1] != network.Name || !validOwnedNetworkID(identity[0]) {
		return nil, errs.New(errs.KindStateConflict, "network reconciliation found an ambiguous bridge identity")
	}
	inspected, err := read("network", "inspect", "--format", "{{json .}}", identity[0])
	if err != nil {
		return nil, err
	}
	var actual inspectedOwnedNetwork
	if json.Unmarshal(inspected.Stdout, &actual) != nil || actual.ID != identity[0] ||
		!ownedNetworkMatches(actual, network) {
		return nil, errs.New(
			errs.KindStateConflict,
			"network reconciliation found a bridge that differs from captured ownership or configuration",
		)
	}
	return completedResponse(), nil
}
