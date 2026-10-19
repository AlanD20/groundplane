package scriptrunner

import (
	"context"
	"strings"
	"time"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Quiesce stops an exactly owned Script container without starting it, removing
// evidence, or treating termination as successful execution. A caller must hold
// exclusive execution ownership so no old Agent can restart the container.
func (runner *Runner) Quiesce(
	ctx context.Context,
	request scriptexecution.QuiescenceRequest,
) (scriptexecution.QuiescenceProof, error) {
	if runner == nil || runner.client == nil || ctx == nil {
		return scriptexecution.QuiescenceProof{}, errs.New(errs.KindInternal, "Script quiescence is not configured")
	}
	if err := validateQuiescenceRequest(request); err != nil {
		return scriptexecution.QuiescenceProof{}, err
	}
	bounded, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	observed, err := runner.client.ContainerInspect(bounded, request.Projection.Name, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return scriptexecution.QuiescenceProof{Absent: true, ObservedAt: time.Now().UTC()}, nil
	}
	if err != nil {
		return scriptexecution.QuiescenceProof{}, operationError(bounded, "inspect Script before stopping", err)
	}
	id := observed.Container.ID
	if observed.Container.Name != "/"+request.Projection.Name || observed.Container.State == nil {
		return scriptexecution.QuiescenceProof{}, errs.New(
			errs.KindStateConflict,
			"Script container identity or state is unavailable",
		)
	}
	if err := validateCleanupProjection(observed, id, request.Projection); err != nil {
		return scriptexecution.QuiescenceProof{}, err
	}
	state := observed.Container.State
	if state.Running || state.Restarting || state.Paused {
		timeout := stopSeconds
		if _, err := runner.client.ContainerStop(bounded, id, client.ContainerStopOptions{Signal: "SIGTERM", Timeout: &timeout}); err != nil &&
			!containerderrdefs.IsNotFound(err) {
			return scriptexecution.QuiescenceProof{}, operationError(bounded, "stop captured Script", err)
		}
	}
	// Inspect by the same name again to reject replacement as well as a
	// still-running process. Keep the stopped container and body for evidence.
	confirmed, err := runner.client.ContainerInspect(bounded, request.Projection.Name, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return scriptexecution.QuiescenceProof{ContainerID: id, Absent: true, ObservedAt: time.Now().UTC()}, nil
	}
	if err != nil {
		return scriptexecution.QuiescenceProof{}, operationError(bounded, "confirm Script stopped", err)
	}
	if err := validateCleanupProjection(confirmed, id, request.Projection); err != nil {
		return scriptexecution.QuiescenceProof{}, err
	}
	state = confirmed.Container.State
	if state == nil || state.Running || state.Restarting || state.Paused {
		return scriptexecution.QuiescenceProof{}, errs.New(errs.KindStateConflict, "Script process has not stopped")
	}
	return scriptexecution.QuiescenceProof{ContainerID: id, ObservedAt: time.Now().UTC()}, nil
}

func validateQuiescenceRequest(request scriptexecution.QuiescenceRequest) error {
	if ids.Validate(ids.KindTask, request.TaskID) != nil || ids.Validate(ids.KindWorkItem, request.WorkItemID) != nil ||
		ids.Validate(
			ids.KindOperation,
			request.OperationID,
		) != nil || request.ExecutionID == "" || request.Projection == nil ||
		request.Projection.Name != "gp-script-"+strings.ToLower(request.ExecutionID) {
		return errs.New(errs.KindValidationFailed, "Script quiescence requires captured container ownership")
	}
	labels := make(map[string]string, len(request.Projection.Labels))
	for _, label := range request.Projection.Labels {
		if label == nil || label.Key == "" {
			return errs.New(errs.KindValidationFailed, "Script quiescence ownership label is missing")
		}
		if _, exists := labels[label.Key]; exists {
			return errs.New(errs.KindValidationFailed, "Script quiescence ownership label is duplicated")
		}
		labels[label.Key] = label.Value
	}
	if labels["com.groundplane.task-id"] != request.TaskID ||
		labels["com.groundplane.work-item-id"] != request.WorkItemID ||
		labels["com.groundplane.operation-id"] != request.OperationID ||
		labels["com.groundplane.script-execution-id"] != request.ExecutionID ||
		labels["com.groundplane.managed"] != "true" ||
		labels["com.groundplane.kind"] != "script-runner" {
		return errs.New(errs.KindValidationFailed, "Script quiescence labels differ from captured work")
	}
	return nil
}
