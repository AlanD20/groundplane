package composehelpercontainer

import (
	"context"
	"strconv"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func releaseHelperName(attempt workexecution.Attempt) string {
	if attempt.Direction == workexecution.Recovery {
		return "gp-recovery-executor-" + attempt.WorkItemID
	}
	return "gp-release-executor-" + attempt.ID
}

func releaseHelperLabels(attempt workexecution.Attempt) map[string]string {
	labels := map[string]string{
		"io.groundplane.role":         "release-executor",
		"io.groundplane.task":         attempt.TaskID,
		"io.groundplane.work-item":    attempt.WorkItemID,
		"io.groundplane.host":         attempt.Executor.HostID,
		"io.groundplane.input":        attempt.Input.ID,
		"io.groundplane.input-sha256": attempt.Input.SHA256,
	}
	if attempt.Direction == workexecution.Recovery {
		// Cleanup retries restore the same captured input. Their process/work
		// ownership is exclusive; use stable helper ownership so the next
		// attempt can stop an orphan without adopting its execution identity.
		labels["io.groundplane.role"] = "recovery-executor"
	} else {
		labels["io.groundplane.attempt"] = attempt.ID
		labels["io.groundplane.agent"] = attempt.Executor.AgentID
		labels["io.groundplane.agent-generation"] = strconv.FormatUint(attempt.Executor.Generation, 10)
	}
	return labels
}

// StopRestorationHelpers stops only the source and interrupted-restoration
// helpers named by validated captured authority. Group sources remain successful;
// failed-forward cleanup retains its actual failure. Docker effects still require
// inspection after process exclusion; stopping a helper is not runtime proof.
func (executor *Executor) StopRestorationHelpers(ctx context.Context, input releaseexecution.RecoveryInput) error {
	if executor == nil || executor.engine == nil || ctx == nil || input.ValidateIdentity() != nil {
		return errs.New(errs.KindValidationFailed, "Release helper cleanup requires its captured restoration authority")
	}
	if err := executor.stopAttemptHelper(ctx, input.Forward); err != nil {
		return err
	}
	if input.Interruption != nil {
		return executor.stopAttemptHelper(ctx, input.Interruption.Attempt)
	}
	return nil
}

// StopActiveRestorationHelper precedes resumed work under exclusive local
// ownership. Typed authority admits an initial Group restoration as well as
// ordinary failed-attempt cleanup, without pretending both are the same action.
func (executor *Executor) StopActiveRestorationHelper(ctx context.Context, input releaseexecution.RecoveryInput) error {
	if executor == nil || executor.engine == nil || ctx == nil || input.ValidateIdentity() != nil {
		return errs.New(errs.KindValidationFailed, "restoration helper cleanup requires its active captured attempt")
	}
	return executor.stopAttemptHelper(ctx, input.Input.Attempt)
}

func (executor *Executor) stopAttemptHelper(ctx context.Context, attempt workexecution.Attempt) error {
	return executor.stopNamedHelper(ctx, releaseHelperName(attempt), releaseHelperLabels(attempt))
}

func (executor *Executor) stopNamedHelper(ctx context.Context, name string, labels map[string]string) error {
	bounded, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	inspected, err := executor.engine.ContainerInspect(bounded, name, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return operationError(bounded, "inspect interrupted Release helper", err)
	}
	observed := inspected.Container
	if observed.ID == "" || observed.Name != "/"+name || observed.Config == nil || observed.State == nil {
		return errs.New(errs.KindStateConflict, "interrupted Release helper identity is unavailable")
	}
	for key, expected := range labels {
		if observed.Config.Labels[key] != expected {
			return errs.New(errs.KindStateConflict, "interrupted Release helper belongs to a different execution")
		}
	}
	// Use the inspected ID, never the reusable name, for mutations.
	if observed.State.Running || observed.State.Restarting || observed.State.Paused {
		timeout := 0
		if _, err := executor.engine.ContainerStop(bounded, observed.ID, client.ContainerStopOptions{Timeout: &timeout}); err != nil && !containerderrdefs.IsNotFound(err) {
			return operationError(bounded, "stop interrupted Release helper", err)
		}
	}
	// Non-force removal refuses a still-running executor rather than treating a
	// requested stop as proof that it has ended. No volumes or workloads removed.
	if _, err := executor.engine.ContainerRemove(bounded, observed.ID, client.ContainerRemoveOptions{}); err != nil && !containerderrdefs.IsNotFound(err) {
		return operationError(bounded, "remove interrupted Release helper", err)
	}
	remaining, err := executor.engine.ContainerInspect(bounded, name, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return operationError(bounded, "confirm Release helper removal", err)
	}
	if remaining.Container.ID != "" {
		return errs.New(errs.KindStateConflict, "Release helper still exists or was recreated during cleanup")
	}
	return errs.New(errs.KindStateConflict, "Release helper removal returned no absence proof")
}
