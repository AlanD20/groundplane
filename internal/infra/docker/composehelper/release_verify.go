package composehelper

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// VerifyReleaseServing observes the final runtime without starting, stopping or
// repairing anything. A successful command or earlier readiness report cannot
// substitute for this final ownership, workload and live/restart routing proof.
func VerifyReleaseServing(
	ctx context.Context,
	taskRunner runner.Runner,
	input releaseexecution.Input,
	report releaseexecution.CheckpointReport,
) (*agentpb.ReleaseHelperResponse, error) {
	if ctx == nil || taskRunner == nil || report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil ||
		report.Progress.Validate(
			input,
		) != nil || !report.Progress.InFlight || report.Progress.Phase != releaseexecution.Rollout ||
		report.Progress.Action != releaseexecution.VerifyServing {
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
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
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
	retainsSlot := input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN &&
		artifacts.Prior != nil && (artifacts.Prior.Target == "blue" || artifacts.Prior.Target == "green")
	if !retainsSlot && len(inventory.predecessors) != 0 {
		return nil, errs.New(errs.KindStateConflict, "obsolete workload containers remain after the Release")
	}
	ready, err := servingMemberProven(bounded, taskRunner, artifacts.Candidate, artifacts.Workloads, nil)
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, errs.New(
			errs.KindStateConflict,
			"Release workload is not running or its configured healthcheck is not passing",
		)
	}
	if artifacts.Proxy == nil {
		return &agentpb.ReleaseHelperResponse{Succeeded: true}, nil
	}
	if inventory.proxyID == "" {
		return nil, errs.New(errs.KindStateConflict, "Release stable proxy is missing")
	}
	traffic, err := report.Progress.Traffic(input)
	if err != nil {
		return nil, err
	}
	proven, err := observeRestorationProxy(
		bounded,
		taskRunner,
		inventory.proxyID,
		artifacts.Proxy.ProxyImplementation,
		traffic.AfterSHA256[:],
	)
	if err != nil {
		return nil, err
	}
	if !proven {
		return nil, errs.New(
			errs.KindStateConflict,
			"Release proxy live and restart routing do not match the captured candidate",
		)
	}
	return releaseTrafficResponse(input, traffic, false), nil
}
