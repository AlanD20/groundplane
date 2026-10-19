package composehelper

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// InspectRecoveryRuntime checks the failed Release's captured ownership and
// image identities without executing Compose or changing traffic. A foreign or
// ambiguous container fails inspection instead of becoming a cleanup target.
func InspectRecoveryRuntime(
	ctx context.Context,
	taskRunner runner.Runner,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (releaseexecution.RecoveryInventory, error) {
	if ctx == nil || taskRunner == nil || report.Validate(input) != nil || report.Ack == nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.InspectFailedRuntime {
		return releaseexecution.RecoveryInventory{}, invalidReleaseHelper()
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return releaseexecution.RecoveryInventory{}, err
	}
	_, observed, err := recoveryRuntimeInventory(bounded, taskRunner, input)
	if err != nil {
		return releaseexecution.RecoveryInventory{}, err
	}
	result := releaseexecution.RecoveryInventory{CandidateIDs: observed.candidates,
		PredecessorIDs: observed.predecessors, ProxyID: observed.proxyID, ObservedAt: time.Now().UTC()}
	if err := result.Validate(input, report); err != nil {
		return releaseexecution.RecoveryInventory{}, err
	}
	return result, nil
}

func recoveryRuntimeInventory(
	ctx context.Context,
	taskRunner runner.Runner,
	input releaseexecution.RecoveryInput,
) (executionplan.ReleaseWorkArtifacts, servingInventory, error) {
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(input.Input.Attempt, input.Input.Release)
	if err != nil {
		return artifacts, servingInventory{}, err
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
	inventory, err := inspectServingInventory(ctx, taskRunner, prior, artifacts.Candidate,
		input.Input.Release.ServiceId, input.Input.Release.ReleaseId, proxy)
	return artifacts, inventory, err
}
