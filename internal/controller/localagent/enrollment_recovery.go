package localagent

import (
	"context"
	"maps"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecoverEnrollment accounts for the singleton created by this exact input.
// Missing durable state never authorizes a new enrollment on reconciliation.
// A cancelled/failed provisioning removes only its own captured generation.
func (manager *Manager) RecoverEnrollment(ctx context.Context, request EnrollRequest, finish bool) (bool, error) {
	if err := validateEnrollRequest(request); err != nil {
		return false, err
	}
	if err := manager.enter(ctx); err != nil {
		return false, err
	}
	defer manager.leave()
	stored, err := manager.repository.GetSingleton(ctx)
	if isAgentNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := validateStored(stored); err != nil {
		return false, err
	}
	record := stored.Record
	if record.ID != request.AgentID {
		return false, nil
	}
	if record.EnrollmentTaskID != request.EnrollmentTaskID || record.Image != request.Image ||
		record.Generation != initialGeneration ||
		record.Phase == PhaseProvisioning && (record.Config.PullIntervalSeconds != request.Config.PullIntervalSeconds ||
			record.Config.MaxConcurrentTasks != request.Config.MaxConcurrentTasks || !maps.Equal(record.Config.Labels, request.Config.Labels)) {
		return false, errs.New(errs.KindStateConflict, "Agent enrollment recovery differs from its captured input")
	}
	if !finish || record.Phase == PhaseDeleting {
		return false, manager.resumeDelete(ctx, stored)
	}
	if record.Phase != PhaseProvisioning && record.Phase != PhaseReady {
		return false, errs.New(errs.KindStateConflict, "Agent enrollment recovery phase changed")
	}
	if record.Phase == PhaseReady {
		if err := manager.convergeRuntime(ctx, record); err != nil {
			return false, err
		}
		if err := manager.waitReplacementReady(ctx, record); err != nil {
			return false, err
		}
		return true, nil
	}
	ready, err := manager.provision(ctx, stored)
	if err != nil {
		return false, err
	}
	return ready.Record.Phase == PhaseReady, nil
}
