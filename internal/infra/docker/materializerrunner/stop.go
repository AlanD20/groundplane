package materializerrunner

import (
	"context"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// StopCaptured stops only the captured file helper. The caller must first
// establish exclusive execution ownership; this method cannot fence its Agent.
// It neither deletes configuration nor grants permission to restore it.
func (r *Runner) StopCaptured(ctx context.Context, owner Ownership) error {
	if r == nil || r.client == nil || ctx == nil || validateOwnership(owner) != nil {
		return errs.New(errs.KindValidationFailed, "materializer cleanup requires captured ownership")
	}
	bounded, cancel := context.WithTimeout(ctx, r.cleanupTimeout)
	defer cancel()
	name := helperName(owner)
	inspected, err := r.client.ContainerInspect(bounded, name, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return operationError(bounded, "inspect captured materializer", err)
	}
	observed := inspected.Container
	if observed.ID == "" || observed.Name != "/"+name || observed.Config == nil || observed.State == nil {
		return errs.New(errs.KindStateConflict, "captured materializer identity is unavailable")
	}
	for key, expected := range helperLabels(owner) {
		if observed.Config.Labels[key] != expected {
			return errs.New(errs.KindStateConflict, "materializer belongs to a different captured configuration")
		}
	}
	if observed.State.Running || observed.State.Restarting || observed.State.Paused {
		timeout := 0
		if _, err := r.client.ContainerStop(bounded, observed.ID, client.ContainerStopOptions{Timeout: &timeout}); err != nil && !containerderrdefs.IsNotFound(err) {
			return operationError(bounded, "stop captured materializer", err)
		}
	}
	if _, err := r.client.ContainerRemove(bounded, observed.ID, client.ContainerRemoveOptions{}); err != nil && !containerderrdefs.IsNotFound(err) {
		return operationError(bounded, "remove captured materializer", err)
	}
	_, err = r.client.ContainerInspect(bounded, name, client.ContainerInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return operationError(bounded, "confirm captured materializer absence", err)
	}
	return errs.New(errs.KindStateConflict, "captured materializer still exists after cleanup")
}
