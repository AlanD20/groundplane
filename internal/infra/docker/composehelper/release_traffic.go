package composehelper

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ExecuteReleaseTraffic switches only the captured stable proxy. Completion
// proves live routing AND its restart source. It never retires the old workload;
// that has a separate acknowledged boundary.
func ExecuteReleaseTraffic(
	ctx context.Context,
	taskRunner runner.Runner,
	input releaseexecution.Input,
	report releaseexecution.CheckpointReport,
) (*agentpb.ReleaseHelperResponse, error) {
	if ctx == nil || taskRunner == nil || report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil {
		return nil, invalidReleaseHelper()
	}
	stage, err := report.Progress.StageID(input)
	if err != nil || stage != report.StageID {
		return nil, invalidReleaseHelper()
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(input.Attempt, input.Release)
	if err != nil {
		return nil, err
	}
	deadline := input.Attempt.Deadline
	if cursor := report.Progress.Rolling; cursor != nil && cursor.ReplicaDeadline.Before(deadline) {
		deadline = cursor.ReplicaDeadline
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	workloads := artifacts.Workloads
	if cursor := report.Progress.Rolling; cursor != nil {
		changes, err := artifacts.Rolling.Changes()
		if err != nil {
			return nil, err
		}
		workloads = nil
		if candidate := changes[cursor.Change-1].Candidate; candidate != nil {
			for _, service := range artifacts.Workloads {
				if service.ComposeName == candidate.ComposeName {
					workloads = append(workloads, service)
				}
			}
			if len(workloads) != 1 {
				return nil, invalidReleaseHelper()
			}
		}
	}
	ready, err := servingMemberProven(bounded, taskRunner, artifacts.Candidate, workloads, nil)
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, errs.New(errs.KindStateConflict, "candidate is no longer ready; proxy traffic was not switched")
	}
	if artifacts.Proxy == nil {
		// A portless rolling workload still has a membership boundary between
		// readiness and retirement, but there is no routing process to mutate.
		if report.Progress.Rolling == nil || report.Progress.Rolling.Phase != rollingrelease.SwitchTraffic {
			return nil, invalidReleaseHelper()
		}
		return &agentpb.ReleaseHelperResponse{Succeeded: true}, nil
	}
	traffic, err := report.Progress.Traffic(input)
	if err != nil {
		return nil, err
	}
	prior := &agentpb.ComposeArtifact{ProjectName: artifacts.Candidate.ProjectName}
	expectedProxy := artifacts.Proxy
	if artifacts.Prior != nil {
		prior = proto.CloneOf(artifacts.Prior.Artifact)
		if artifacts.Prior.Retained != nil {
			prior.Services = append(prior.Services, proto.CloneOf(artifacts.Prior.Retained).Services...)
		}
		if artifacts.Prior.Proxy != nil {
			expectedProxy = artifacts.Prior.Proxy
		}
	}
	inventory, err := inspectServingInventory(
		bounded,
		taskRunner,
		prior,
		artifacts.Candidate,
		input.Release.ServiceId,
		input.Release.ReleaseId,
		expectedProxy,
	)
	if err != nil {
		return nil, err
	}
	response := &agentpb.ReleaseHelperResponse{}
	id := inventory.proxyID
	if id != "" {
		implementation := expectedProxy.ProxyImplementation
		if inventory.proxyNeedsRestore {
			implementation = artifacts.Proxy.ProxyImplementation
		}
		already, err := observeRestorationProxy(bounded, taskRunner, id, implementation, traffic.AfterSHA256[:])
		if err != nil {
			return nil, err
		}
		if already {
			return releaseTrafficResponse(input, traffic, false), nil
		}
		before, err := observeRestorationProxy(bounded, taskRunner, id, implementation, traffic.BeforeSHA256[:])
		if err != nil {
			return nil, err
		}
		if !before {
			return nil, errs.New(
				errs.KindStateConflict,
				"proxy routing differs from both captured states; reconcile before cutover",
			)
		}
		if input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE {
			for _, args := range [][]string{{"container", "stop", "--time", "30", "--", id}, {"container", "rm", "--", id}} {
				response.MutationAttempted = true
				if err := runReleaseProxyCommand(bounded, taskRunner, runner.RunCmdOpts{Name: DockerExecutable, Args: args, Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 32 * 1024}); err != nil {
					return nil, err
				}
			}
			id = ""
		}
	}
	if id == "" {
		commands, err := candidateStartupCommands(artifacts.Candidate, []*agentpb.ComposeService{artifacts.Proxy})
		if err != nil {
			return nil, err
		}
		defer func() {
			for _, command := range commands {
				clear(command.Stdin)
			}
		}()
		for index, command := range commands {
			if index > 0 {
				response.MutationAttempted = true
			}
			if err := runReleaseProxyCommand(bounded, taskRunner, command); err != nil {
				return nil, err
			}
		}
		id, err = ownedProxyContainer(bounded, taskRunner, artifacts.Candidate, artifacts.Proxy, nil)
		if err != nil {
			return nil, err
		}
	}
	response.MutationAttempted = true
	failure, err := activateCapturedProxy(bounded, taskRunner, id, artifacts.Proxy.ProxyImplementation, traffic.After)
	if err != nil {
		return nil, err
	}
	if failure != nil {
		return nil, errs.New(errs.KindRequestFailed, "captured proxy routing could not be activated")
	}
	proven, err := awaitRestorationProxy(
		bounded,
		taskRunner,
		id,
		artifacts.Proxy.ProxyImplementation,
		traffic.AfterSHA256[:],
	)
	if err != nil {
		return nil, err
	}
	if !proven {
		return nil, errs.New(
			errs.KindStateConflict,
			"proxy live and restart configuration do not match the requested cutover",
		)
	}
	return releaseTrafficResponse(input, traffic, response.MutationAttempted), nil
}

func releaseTrafficResponse(
	input releaseexecution.Input,
	traffic releaseexecution.TrafficChange,
	mutated bool,
) *agentpb.ReleaseHelperResponse {
	return &agentpb.ReleaseHelperResponse{
		Succeeded:         true,
		MutationAttempted: mutated,
		ProxyEvidence: &agentpb.ServiceProxyEvidence{
			ServiceId: input.Release.ServiceId, ReleaseId: input.Release.ReleaseId, Target: traffic.Target, ProxyGeneration: traffic.Generation, ConfigSha256: slices.Clone(traffic.AfterSHA256[:]),
		},
	}
}

func runReleaseProxyCommand(ctx context.Context, taskRunner runner.Runner, command runner.RunCmdOpts) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := taskRunner.Run(ctx, command)
	clear(result.Stdout)
	clear(result.Stderr)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return errs.New(errs.KindRequestFailed, "captured proxy container operation failed")
	}
	return nil
}
