package composehelper

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// InspectReleaseEffect never changes containers, files or routing. Missing proof
// leaves the effect unresolved; it is not permission to repeat a mutation.
func InspectReleaseEffect(ctx context.Context, taskRunner runner.Runner, input releaseexecution.Input, report releaseexecution.CheckpointReport) (*agentpb.ReleaseHelperResponse, error) {
	if ctx == nil || taskRunner == nil || report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil || report.Progress.Validate(input) != nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.Rollout {
		return nil, invalidReleaseHelper()
	}
	stage, err := report.Progress.StageID(input)
	if err != nil || stage != report.StageID {
		return nil, invalidReleaseHelper()
	}
	if report.Progress.Action == releaseexecution.VerifyServing {
		response, err := VerifyReleaseServing(ctx, taskRunner, input, report)
		if response != nil {
			response.Inspection = true
		}
		return response, err
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
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	prior := &agentpb.ComposeArtifact{ProjectName: artifacts.Candidate.ProjectName}
	proxy := artifacts.Proxy
	if artifacts.Prior != nil {
		prior = proto.CloneOf(artifacts.Prior.Artifact)
		if artifacts.Prior.Retained != nil {
			prior.Services = append(prior.Services, proto.CloneOf(artifacts.Prior.Retained).Services...)
		}
		if artifacts.Prior.Proxy != nil {
			proxy = artifacts.Prior.Proxy
		}
	}
	inventory, err := inspectServingInventory(bounded, taskRunner, prior, artifacts.Candidate, input.Release.ServiceId, input.Release.ReleaseId, proxy)
	if err != nil {
		return nil, err
	}
	progress := report.Progress
	startup, switching := progress.Action == releaseexecution.StartCandidate, progress.Action == releaseexecution.SwitchTraffic
	workloads := artifacts.Workloads
	var retired *rollingrelease.Replica
	if cursor := progress.Rolling; cursor != nil {
		changes, err := artifacts.Rolling.Changes()
		if err != nil {
			return nil, err
		}
		change := changes[cursor.Change-1]
		startup, switching = cursor.Phase == rollingrelease.StartCandidate, cursor.Phase == rollingrelease.SwitchTraffic
		if startup || switching {
			workloads = nil
			if change.Candidate != nil {
				for _, service := range artifacts.Workloads {
					if service.ComposeName == change.Candidate.ComposeName {
						workloads = append(workloads, service)
					}
				}
				if len(workloads) != 1 {
					return nil, invalidReleaseHelper()
				}
			} else if startup {
				return nil, invalidReleaseHelper()
			}
		}
		if cursor.Phase == rollingrelease.RemovePredecessor {
			retired = change.Retire
		}
		if cursor.Phase == rollingrelease.RemoveRetained {
			retired = change.Retained
		}
	}
	response := &agentpb.ReleaseHelperResponse{Succeeded: true, Inspection: true}
	switch {
	case startup:
		if input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE && len(inventory.predecessors) != 0 {
			return nil, unresolvedReleaseEffect()
		}
		if err := inspectStartedRelease(bounded, taskRunner, inventory, workloads); err != nil {
			return nil, err
		}
	case switching:
		ready, err := servingMemberProven(bounded, taskRunner, artifacts.Candidate, workloads, nil)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, unresolvedReleaseEffect()
		}
		if artifacts.Proxy != nil {
			traffic, err := progress.Traffic(input)
			if err != nil {
				return nil, err
			}
			if inventory.proxyID == "" {
				return nil, unresolvedReleaseEffect()
			}
			proven, err := observeRestorationProxy(bounded, taskRunner, inventory.proxyID, artifacts.Proxy.ProxyImplementation, traffic.AfterSHA256[:])
			if err != nil {
				return nil, err
			}
			if !proven {
				return nil, unresolvedReleaseEffect()
			}
			response = releaseTrafficResponse(input, traffic, false)
			response.Inspection = true
		}
	case retired != nil:
		if progress.Rolling.Phase == rollingrelease.RemovePredecessor && time.Now().Before(progress.Rolling.DrainUntil) {
			return nil, unresolvedReleaseEffect()
		}
		if artifacts.Proxy != nil {
			traffic, err := progress.Traffic(input)
			if err != nil {
				return nil, err
			}
			if inventory.proxyID == "" {
				return nil, unresolvedReleaseEffect()
			}
			proven, err := observeRestorationProxy(bounded, taskRunner, inventory.proxyID, artifacts.Proxy.ProxyImplementation, traffic.AfterSHA256[:])
			if err != nil {
				return nil, err
			}
			if !proven {
				return nil, unresolvedReleaseEffect()
			}
		}
		ids, err := releaseReplicaRemovalIDs(bounded, taskRunner, inventory, *retired, prior, input.Release.ServiceId)
		if err != nil {
			return nil, err
		}
		if len(ids) != 0 {
			return nil, unresolvedReleaseEffect()
		}
		response.AbsenceObserved = true
	case progress.Action == releaseexecution.RemovePrevious || progress.Action == releaseexecution.RetireSwitchedPrevious:
		if len(inventory.predecessors) != 0 {
			return nil, unresolvedReleaseEffect()
		}
		if progress.Action == releaseexecution.RemovePrevious && len(inventory.candidates) != 0 {
			return nil, unresolvedReleaseEffect()
		}
		if progress.Action == releaseexecution.RetireSwitchedPrevious {
			if err := proveReleaseRetirement(bounded, taskRunner, input, artifacts, inventory.proxyID); err != nil {
				return nil, err
			}
		}
		response.AbsenceObserved = true
	default:
		return nil, invalidReleaseHelper()
	}
	return response, releaseexecution.ValidateHelperResult(input, progress, response)
}

func inspectStartedRelease(ctx context.Context, taskRunner runner.Runner, inventory servingInventory, services []*agentpb.ComposeService) error {
	if len(services) == 0 {
		return unresolvedReleaseEffect()
	}
	counts := make(map[string]uint32, len(services))
	for _, id := range inventory.candidates {
		name, err := restorationContainerComposeName(ctx, taskRunner, id)
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(services, func(service *agentpb.ComposeService) bool { return service.ComposeName == name }) {
			continue
		}
		result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{.State.Running}}", id}, Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 1024})
		if err != nil {
			return err
		}
		if result.ExitCode != 0 || strings.TrimSpace(string(result.Stdout)) != "true" {
			return unresolvedReleaseEffect()
		}
		counts[name]++
	}
	for _, service := range services {
		if counts[service.ComposeName] != service.ExpectedReplicas {
			return unresolvedReleaseEffect()
		}
	}
	return nil
}

func unresolvedReleaseEffect() error {
	return errs.New(errs.KindStateConflict, "interrupted Release action has no complete matching runtime proof; recovery is required")
}
