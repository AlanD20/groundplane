package composehelper

import (
	"context"
	"math"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func verifyRollingProxyHandoff(ctx context.Context, taskRunner runner.Runner,
	plan *agentpb.ExecutionPlan, candidate *agentpb.ComposeArtifact, switching *agentpb.ServiceProxySwitch,
) error {
	prior := composeArtifactByID(plan, switching.GetPriorArtifactId())
	proxy, err := servingPredecessorProxy(prior, switching.GetServiceId())
	candidateProxy, candidateErr := servingPredecessorProxy(candidate, switching.GetServiceId())
	if err != nil || candidateErr != nil || proxy == nil || candidateProxy == nil ||
		proxy.GetComposeName() != candidateProxy.GetComposeName() || proxy.GetProxyImplementation() != candidateProxy.GetProxyImplementation() {
		return errs.New(errs.KindStateConflict, "rolling-to-blue-green requires the captured stable proxy")
	}
	inventory, err := inspectServingInventory(ctx, taskRunner, prior, candidate,
		switching.GetServiceId(), switching.GetReleaseId(), proxy)
	if err != nil {
		return err
	}
	if inventory.proxyID == "" || inventory.proxyNeedsRestore {
		return errs.New(errs.KindStateConflict, "rolling-to-blue-green stable proxy ownership changed")
	}
	id, err := ownedProxyContainer(ctx, taskRunner, prior, proxy, nil)
	if err != nil {
		return err
	}
	proven, err := observeRestorationProxy(
		ctx,
		taskRunner,
		id,
		proxy.GetProxyImplementation(),
		proxy.GetProxyConfigSha256(),
	)
	if err != nil || !proven {
		return errs.New(
			errs.KindStateConflict,
			"rolling-to-blue-green proxy no longer has the captured serving configuration",
		)
	}
	return nil
}

// Only immutable container ids that match captured names, ownership and images
// can be retired. Re-read afterwards; successful Docker exit alone is not proof.
// The proxy is excluded so its reserved address and active connections survive.
func retireSwitchedPredecessor(ctx context.Context, taskRunner runner.Runner,
	plan *agentpb.ExecutionPlan, candidate *agentpb.ComposeArtifact, switching *agentpb.ServiceProxySwitch,
) (*agentpb.ComposeHelperResponse, error) {
	prior := composeArtifactByID(plan, switching.GetPriorArtifactId())
	if prior == nil {
		return nil, errs.New(errs.KindValidationFailed, "switched predecessor artifact is missing")
	}
	proxy, err := servingPredecessorProxy(prior, switching.GetServiceId())
	if err != nil {
		return nil, err
	}
	if switching.GetFromTarget() == "rolling" {
		// Forward replay is forbidden after the effect barrier. A lost switch
		// report enters recovery; it cannot restart this drain with a new budget.
		drain := time.Duration(rollingrelease.DrainSeconds) * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			drain = min(drain, time.Until(deadline)/2)
		}
		if drain <= 0 {
			return nil, errs.New(errs.KindStateConflict, "proxy handoff deadline expired before predecessor drain")
		}
		timer := time.NewTimer(drain)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	inventory, err := inspectServingInventory(ctx, taskRunner, prior, candidate,
		switching.GetServiceId(), switching.GetReleaseId(), proxy)
	if err != nil {
		return nil, err
	}
	if len(inventory.predecessors) != 0 {
		for _, command := range [][]string{{"container", "stop", "--time", "30", "--"}, {"container", "rm", "--"}} {
			result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
				Name: DockerExecutable, Args: append(slices.Clone(command), inventory.predecessors...),
				Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
			})
			if err != nil {
				return nil, errs.Wrap(errs.KindRequestFailed, err)
			}
			if result.ExitCode != 0 {
				if result.ExitCode < 0 || result.ExitCode > math.MaxInt32 {
					return nil, errs.New(errs.KindInternal, "predecessor retirement returned an invalid exit code")
				}
				return failedResponse(int32(result.ExitCode)), nil
			}
		}
	}
	after, err := inspectServingInventory(ctx, taskRunner, prior, candidate,
		switching.GetServiceId(), switching.GetReleaseId(), proxy)
	if err != nil {
		return nil, err
	}
	if len(after.predecessors) != 0 {
		return nil, errs.New(errs.KindStateConflict, "old workload containers remain after the proxy switch")
	}
	return nil, nil
}
