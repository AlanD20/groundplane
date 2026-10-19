package composehelper

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Preserving a completed switch does not run Compose, reload routing or probe
// readiness. A later hook can make the application unhealthy without changing
// which runtime GP applied. This inspection proves ownership and routing only;
// application health remains a separate, live observation.
func inspectPreservedRelease(
	ctx context.Context,
	taskRunner runner.Runner,
	input releaseexecution.RecoveryInput,
) error {
	if ctx == nil || taskRunner == nil || input.Validate() != nil || !input.PreservesServing() {
		return invalidReleaseHelper()
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	runtime, err := executionplan.PrepareReleaseWorkRuntime(input.Input.Attempt, input.Input.Release)
	if err != nil {
		return err
	}
	serving, err := executionplan.OpenNativeServingRuntime(input.Input.Release.EnvironmentId, runtime.ServiceID,
		runtime.CurrentArtifact, runtime.RetainedPriorArtifact)
	if err != nil {
		return err
	}
	retained := serving.Retained
	if retained == nil {
		retained = &agentpb.ComposeArtifact{ProjectName: serving.Artifact.ProjectName}
	}
	inventory, err := inspectServingInventory(bounded, taskRunner, retained, serving.Artifact,
		runtime.ServiceID, runtime.ReleaseID, serving.Proxy)
	if err != nil {
		return err
	}
	var expected uint64
	for _, workload := range serving.Workloads {
		expected += uint64(workload.ExpectedReplicas)
	}
	if uint64(len(inventory.candidates)) != expected {
		return errs.New(
			errs.KindStateConflict,
			"preserved deployment containers are missing; GP has not restarted or replaced them",
		)
	}
	if serving.Proxy == nil {
		return nil
	}
	if inventory.proxyID == "" {
		return errs.New(errs.KindStateConflict, "preserved deployment proxy is missing")
	}
	proven, err := observeRestorationProxy(bounded, taskRunner, inventory.proxyID,
		serving.Proxy.ProxyImplementation, runtime.ProxyConfigSHA256)
	if err != nil {
		return err
	}
	if !proven {
		return errs.New(
			errs.KindStateConflict,
			"preserved deployment live and restart routing no longer match its completed switch",
		)
	}
	return nil
}
