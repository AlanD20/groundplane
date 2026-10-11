package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A scaled Compose name selects many containers. Retire only the immutable id
// captured before publication, after checking it against the entire old/new set.
func executeCapturedRollingRemoval(ctx context.Context, taskRunner runner.Runner,
	request *agentpb.ComposeHelperRequest, prior *agentpb.ComposeArtifact, step *agentpb.ExecutionStep,
) (*agentpb.ComposeHelperResponse, bool, error) {
	replica, err := executionplan.RollingRetirement(request.GetPlan(), step)
	if err != nil || replica.ContainerID == "" {
		return nil, false, err
	}
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(request.GetTimeoutSeconds())*time.Second)
	defer cancel()
	serviceID := step.GetComposeWorkloadRemove().GetServiceId()
	member, err := executionplan.RollingMember(request.GetPlan(), serviceID)
	if err != nil {
		return nil, true, err
	}
	rolling, err := executionplan.OpenRollingPlan(member)
	if err != nil {
		return nil, true, err
	}
	workload, err := executionplan.RollingReplicaService(prior, serviceID, replica)
	if err != nil {
		return nil, true, err
	}
	candidate := composeArtifactByID(request.GetPlan(), member.GetCandidateArtifactId())
	proxy, err := servingPredecessorProxy(prior, serviceID)
	if err != nil {
		return nil, true, err
	}
	inventory, err := inspectServingInventory(
		executionCtx,
		taskRunner,
		prior,
		candidate,
		serviceID,
		member.GetCandidateReleaseId(),
		proxy,
	)
	if err != nil {
		return nil, true, err
	}
	if err := validateRemainingCapturedContainers(inventory.predecessors, rolling, replica.Ordinal); err != nil {
		return nil, true, err
	}
	if slices.Contains(inventory.predecessors, replica.ContainerID) {
		if err := verifyCapturedContainer(executionCtx, taskRunner, replica, workload); err != nil {
			return nil, true, err
		}
		for _, args := range [][]string{
			{"container", "stop", "--time", "30", "--", replica.ContainerID},
			{"container", "rm", "--", replica.ContainerID},
		} {
			result, err := taskRunner.Run(executionCtx, runner.RunCmdOpts{
				Name: DockerExecutable, Args: args, Dir: WorkDirectory,
				Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
			})
			if err != nil {
				return nil, true, errs.Wrap(errs.KindRequestFailed, err)
			}
			if result.ExitCode != 0 {
				return failedResponse(int32(result.ExitCode)), true, nil
			}
		}
	}
	after, err := inspectServingInventory(
		executionCtx,
		taskRunner,
		prior,
		candidate,
		serviceID,
		member.GetCandidateReleaseId(),
		proxy,
	)
	if err != nil {
		return nil, true, err
	}
	if err := validateRemainingCapturedContainers(after.predecessors, rolling, replica.Ordinal+1); err != nil {
		return nil, true, err
	}
	return completedResponse(), true, nil
}

func validateRemainingCapturedContainers(observed []string, plan rollingrelease.Plan, first uint32) error {
	allowed := make(map[string]bool, len(plan.Prior))
	for _, replica := range plan.Prior {
		if replica.Ordinal >= first {
			allowed[replica.ContainerID] = true
		}
	}
	for _, id := range observed {
		if !allowed[id] {
			return errs.New(
				errs.KindStateConflict,
				"scaled predecessor contains an uncaptured or already retired container",
			)
		}
	}
	return nil
}

func verifyCapturedContainer(ctx context.Context, taskRunner runner.Runner,
	replica rollingrelease.Replica, workload *agentpb.ComposeService,
) error {
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{.Name}}\n{{json .Config.Labels}}", replica.ContainerID},
		Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
	})
	if err != nil || result.ExitCode != 0 {
		return errs.New(errs.KindStateConflict, "captured predecessor container is no longer observable")
	}
	name, raw, found := strings.Cut(strings.TrimSpace(string(result.Stdout)), "\n")
	labels := map[string]string{}
	if !found || strings.TrimPrefix(name, "/") != replica.ContainerName ||
		json.Unmarshal([]byte(raw), &labels) != nil ||
		!hasAllExpectedLabels(labels, workload.GetExpectedLabels()) ||
		labels["com.docker.compose.service"] != replica.ComposeName ||
		labels["com.docker.compose.container-number"] != strconv.FormatUint(uint64(replica.ComposeReplica), 10) ||
		labels["com.docker.compose.oneoff"] != "False" {
		return errs.New(errs.KindStateConflict, "captured predecessor container name or Compose identity changed")
	}
	return inspectRestorationImage(ctx, taskRunner, replica.ContainerID, workload)
}
