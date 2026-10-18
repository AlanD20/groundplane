package composehelper

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RestoreRecoveryServing restores the captured serving workload before routing
// traffic back to it. It does not run deployment hooks or restore application
// data. Captured configuration restoration must already have been acknowledged.
func RestoreRecoveryServing(ctx context.Context, taskRunner runner.Runner, input releaseexecution.RecoveryInput, report releaseexecution.RecoveryReport, inspectOnly bool) error {
	if ctx == nil || taskRunner == nil || report.Validate(input) != nil || report.Ack == nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.RestoreServingRuntime {
		return invalidReleaseHelper()
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return err
	}
	artifacts, inventory, err := recoveryRuntimeInventory(bounded, taskRunner, input)
	if err != nil {
		return err
	}
	if artifacts.Prior == nil {
		// First deployment: no runtime is invented. The next phase removes
		// the candidate and its proxy, restoring the captured absent state.
		return nil
	}
	if inspectOnly {
		return recoveryPredecessorServing(bounded, taskRunner, artifacts, inventory)
	}
	prior := artifacts.Prior
	if artifacts.Rolling != nil && prior.Proxy != nil && inventory.proxyID != "" {
		complete := proto.CloneOf(prior.Artifact)
		if prior.Retained != nil {
			complete.Services = append(complete.Services, proto.CloneOf(prior.Retained).Services...)
		}
		if err := restoreRollingTraffic(bounded, taskRunner, complete, artifacts.Candidate, *prior,
			*artifacts.Rolling, input.Input.Release.TrafficConfigurations); err != nil {
			return err
		}
	} else if input.Input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE {
		// Recreate uses the same Compose name. Remove only the failed
		// candidate identity before bringing back the captured predecessor.
		for _, id := range inventory.candidates {
			if err := stopRecoveryContainer(bounded, taskRunner, id); err != nil {
				return err
			}
		}
	}
	for _, service := range prior.Workloads {
		if err := ensureRecoveryService(bounded, taskRunner, prior.Artifact, service, false); err != nil {
			return err
		}
	}
	// Do not activate prior routing until every prior workload is ready.
	if prior.Proxy != nil {
		if err := ensureRecoveryService(bounded, taskRunner, prior.Artifact, prior.Proxy, false); err != nil {
			return err
		}
		artifacts, inventory, err = recoveryRuntimeInventory(bounded, taskRunner, input)
		if err != nil {
			return err
		}
		proven, err := restorationProxyProven(bounded, taskRunner, inventory.proxyID, prior.Proxy, true)
		if err != nil {
			return err
		}
		if !proven {
			return errs.New(errs.KindStateConflict, "captured previous routing was not restored")
		}
	}
	artifacts, inventory, err = recoveryRuntimeInventory(bounded, taskRunner, input)
	if err != nil {
		return err
	}
	return recoveryPredecessorServing(bounded, taskRunner, artifacts, inventory)
}

func ensureRecoveryService(ctx context.Context, taskRunner runner.Runner, artifact *agentpb.ComposeArtifact, service *agentpb.ComposeService, inspectOnly bool) error {
	ready, err := servingPredecessorProven(ctx, taskRunner, artifact, service)
	if err != nil || ready {
		return err
	}
	if inspectOnly {
		return errs.New(errs.KindStateConflict, "captured previous container is not ready")
	}
	input, err := composeCLIInput(artifact.CanonicalYaml)
	if err != nil {
		return err
	}
	base := []string{"compose", "--project-name", artifact.ProjectName, "--project-directory", composeProjectDirectory(artifact), "--file", "-"}
	for _, suffix := range [][]string{{"config", "--quiet"}, {"up", "--detach", "--no-deps", "--pull", "never", "--", service.ComposeName}} {
		result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable, Args: append(slices.Clone(base), suffix...),
			Dir: composeProjectDirectory(artifact), Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, Stdin: input, CaptureLimitBytes: 32 * 1024})
		clear(result.Stdout)
		clear(result.Stderr)
		if err != nil {
			return err
		}
		if result.ExitCode != 0 {
			return errs.New(errs.KindRequestFailed, "captured previous container could not be restored")
		}
	}
	ready, err = waitServingMember(ctx, taskRunner, artifact, []*agentpb.ComposeService{service}, nil)
	if err != nil {
		return err
	}
	if !ready {
		return errs.New(errs.KindStateConflict, "restored previous container did not become ready")
	}
	return nil
}

func stopRecoveryContainer(ctx context.Context, taskRunner runner.Runner, id string) error {
	if !validComponentContainerID(id) {
		return invalidReleaseHelper()
	}
	for _, args := range [][]string{{"container", "stop", "--time", "30", "--", id}, {"container", "rm", "--", id}} {
		result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable, Args: args,
			Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 32 * 1024})
		clear(result.Stdout)
		clear(result.Stderr)
		if err != nil {
			return err
		}
		if result.ExitCode != 0 {
			return errs.New(errs.KindRequestFailed, "failed Release container could not be stopped or removed")
		}
	}
	return nil
}

func recoveryRetained(ctx context.Context, taskRunner runner.Runner, artifacts executionplan.ReleaseWorkArtifacts, serviceID string, inspectOnly bool) error {
	if artifacts.Prior == nil || artifacts.Prior.Retained == nil {
		return nil
	}
	for _, service := range artifacts.Prior.Retained.Services {
		if service.ServiceId != serviceID {
			continue
		}
		if err := ensureRecoveryService(ctx, taskRunner, artifacts.Prior.Retained, service, inspectOnly); err != nil {
			return err
		}
	}
	return nil
}
