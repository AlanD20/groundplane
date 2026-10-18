package composehelper

import (
	"context"
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type PreviousRemovalResult struct {
	MutationAttempted bool
	AbsenceObserved   bool
}

// ExecuteReleasePreviousRemoval removes exact inspected Docker identities, not
// whatever happens to have a Compose service name. It never removes volumes or
// the stable proxy. A partial stop/remove remains an in-flight boundary until
// fresh inventory proves absence; an exit status alone is insufficient.
func ExecuteReleasePreviousRemoval(ctx context.Context, taskRunner runner.Runner, input releaseexecution.Input, report releaseexecution.CheckpointReport) (PreviousRemovalResult, error) {
	result := PreviousRemovalResult{}
	if ctx == nil || taskRunner == nil {
		return result, errs.New(errs.KindInternal, "Release removal requires a context and subprocess runner")
	}
	retiring := report.Progress.Action == releaseexecution.RetireSwitchedPrevious && input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN
	recreating := report.Progress.Action == releaseexecution.RemovePrevious && input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE
	if report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil || report.Progress.Validate(input) != nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.Rollout || (!retiring && !recreating) {
		return result, errs.New(errs.KindStateConflict, "previous workload removal has no acknowledged removal boundary")
	}
	stageID, err := report.Progress.StageID(input)
	if err != nil || stageID != report.StageID {
		return result, errs.New(errs.KindStateConflict, "previous workload removal receipt names another action")
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(input.Attempt, input.Release)
	if err != nil {
		return result, err
	}
	if artifacts.Prior == nil {
		return result, errs.New(errs.KindStateConflict, "previous workload removal has no captured predecessor")
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return result, err
	}
	prior := proto.CloneOf(artifacts.Prior.Artifact)
	if artifacts.Prior.Retained != nil {
		prior.Services = append(prior.Services, proto.CloneOf(artifacts.Prior.Retained).Services...)
	}
	inventory, err := inspectServingInventory(bounded, taskRunner, prior, artifacts.Candidate, input.Release.ServiceId, input.Release.ReleaseId, artifacts.Prior.Proxy)
	if err != nil {
		return result, err
	}
	if retiring {
		if err := proveReleaseRetirement(bounded, taskRunner, input, artifacts, inventory.proxyID); err != nil {
			return result, err
		}
	} else if len(inventory.candidates) != 0 {
		return result, errs.New(errs.KindStateConflict, "candidate runtime exists before previous-workload removal was completed; reconcile the Release")
	}
	for _, id := range inventory.predecessors {
		for _, args := range [][]string{{"container", "stop", "--time", "30", "--", id}, {"container", "rm", "--", id}} {
			if err := bounded.Err(); err != nil {
				return result, err
			}
			result.MutationAttempted = true
			output, err := taskRunner.Run(bounded, runner.RunCmdOpts{Name: DockerExecutable, Args: args,
				Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 32 * 1024})
			clear(output.Stdout)
			clear(output.Stderr)
			if err != nil {
				return result, err
			}
			if output.ExitCode != 0 {
				return result, errs.New(errs.KindRequestFailed, "previous workload stop/removal failed; its absence has not been proven")
			}
		}
	}
	observed, err := inspectServingInventory(bounded, taskRunner, prior, artifacts.Candidate, input.Release.ServiceId, input.Release.ReleaseId, artifacts.Prior.Proxy)
	if err != nil {
		return result, err
	}
	if len(observed.predecessors) != 0 || !retiring && len(observed.candidates) != 0 {
		return result, errs.New(errs.KindStateConflict, "previous workload removal did not reach the required absent state")
	}
	if retiring {
		if err := proveReleaseRetirement(bounded, taskRunner, input, artifacts, observed.proxyID); err != nil {
			return result, err
		}
	}
	result.AbsenceObserved = true
	return result, nil
}

// Retirement is permitted only after the replacement is ready and both live
// routing and the proxy restart source point to it. Recheck after removal too.
func proveReleaseRetirement(ctx context.Context, taskRunner runner.Runner, input releaseexecution.Input, artifacts executionplan.ReleaseWorkArtifacts, proxyID string) error {
	if artifacts.Proxy == nil || proxyID == "" || len(input.Release.TrafficConfigurations) != 1 {
		return errs.New(errs.KindStateConflict, "previous workload retirement requires the captured serving proxy")
	}
	ready, err := servingMemberProven(ctx, taskRunner, artifacts.Candidate, artifacts.Workloads, nil)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(input.Release.TrafficConfigurations[0])
	routed, err := observeRestorationProxy(ctx, taskRunner, proxyID, artifacts.Proxy.ProxyImplementation, digest[:])
	if err != nil {
		return err
	}
	if !ready || !routed {
		return errs.New(errs.KindStateConflict, "replacement readiness and live/restart routing must be proven before retiring the previous workload")
	}
	return nil
}
