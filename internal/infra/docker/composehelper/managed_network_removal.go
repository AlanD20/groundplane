package composehelper

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"math"
	"sort"
	"strings"
	"time"
)

func executeManagedNetworkRemove(
	ctx context.Context,
	taskRunner runner.Runner,
	timeoutSeconds uint32,
	remove *agentpb.ManagedNetworkRemove,
) (*agentpb.ComposeHelperResponse, error) {
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	run := func(args ...string) (runner.Result, *agentpb.ComposeHelperResponse, error) {
		result, runErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: args, Dir: WorkDirectory,
			Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		})
		if contextErr := executionCtx.Err(); contextErr != nil {
			return runner.Result{}, nil, contextErr
		}
		if result.ExitCode < 0 || result.ExitCode > math.MaxInt32 {
			return runner.Result{}, nil, errs.New(
				errs.KindInternal,
				"managed network command returned an invalid exit code",
			)
		}
		if runErr != nil && result.ExitCode == 0 {
			return runner.Result{}, nil, errs.Wrap(errs.KindInternal, runErr)
		}
		if runErr != nil || result.ExitCode != 0 {
			exitCode := result.ExitCode
			if exitCode == 0 {
				exitCode = 1
			}
			return result, failedResponse(int32(exitCode)), nil
		}
		return result, nil, nil
	}

	listed, failure, err := run(
		"network", "ls", "--filter", "name=^"+remove.DockerName+"$", "--format", "{{.Name}}",
	)
	if err != nil || failure != nil {
		return failure, err
	}
	found := false
	for _, name := range strings.Fields(string(listed.Stdout)) {
		if name == remove.DockerName {
			found = true
			break
		}
	}
	if !found {
		return completedResponse(), nil
	}

	inspected, failure, err := run(
		"network", "inspect", "--format", "{{json .Labels}}", remove.DockerName,
	)
	if err != nil || failure != nil {
		return failure, err
	}
	labels := map[string]string{}
	if json.Unmarshal([]byte(strings.TrimSpace(string(inspected.Stdout))), &labels) != nil ||
		labels["com.groundplane.managed"] != "true" || labels["com.groundplane.kind"] != "network" ||
		labels["com.groundplane.environment-id"] != remove.EnvironmentId {
		return failedResponse(1), nil
	}

	connected, failure, err := run(
		"network", "inspect", "--format",
		`{{range $id, $_ := .Containers}}{{$id}}{{"\n"}}{{end}}`, remove.DockerName,
	)
	if err != nil || failure != nil {
		return failure, err
	}
	containerIDs := strings.Fields(string(connected.Stdout))
	sort.Strings(containerIDs)
	for _, containerID := range containerIDs {
		if len(containerID) > 128 || strings.ContainsAny(containerID, "\x00/\\") {
			return nil, errs.New(errs.KindInternal, "managed network contains an invalid container identity")
		}
		_, failure, err = run("network", "disconnect", "--force", remove.DockerName, containerID)
		if err != nil || failure != nil {
			return failure, err
		}
	}
	_, failure, err = run("network", "rm", remove.DockerName)
	if err != nil || failure != nil {
		return failure, err
	}
	return completedResponse(), nil
}
