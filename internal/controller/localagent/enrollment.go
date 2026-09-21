package localagent

import (
	"context"
	"fmt"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Enroll atomically creates the singleton durable record, materializes its
// runtime, converges the container, and waits for authenticated readiness.
func (manager *Manager) Enroll(ctx context.Context, request EnrollRequest) (Agent, error) {
	if err := manager.enter(ctx); err != nil {
		return Agent{}, err
	}
	defer manager.leave()

	if err := validateEnrollRequest(request); err != nil {
		return Agent{}, err
	}
	createdAt := manager.clock.Now()
	if !isNonzeroUTC(createdAt) {
		return Agent{}, errs.New(errs.KindInternal, "local agent clock returned a non-UTC time")
	}
	credential, err := manager.runtime.GenerateCredential(ctx, request.AgentID)
	if err != nil {
		return Agent{}, safePortError(ctx, err, "local agent credential generation failed")
	}
	if err := validateCredential(credential, false); err != nil {
		return Agent{}, errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("local agent runtime returned an invalid credential: %w", err),
		)
	}
	record := Record{
		ID:               request.AgentID,
		EnrollmentTaskID: request.EnrollmentTaskID,
		Image:            request.Image,
		Generation:       initialGeneration,
		Phase:            PhaseProvisioning,
		Config:           cloneConfig(request.Config),
		Credential:       cloneCredential(credential),
		CreatedAt:        createdAt,
	}
	stored, err := manager.repository.CreateSingleton(ctx, cloneRecord(record))
	if err != nil {
		return Agent{}, safePortError(ctx, err, "local agent durable creation failed")
	}
	if err := validateStored(stored); err != nil {
		return Agent{}, err
	}
	ready, err := manager.provision(ctx, stored)
	if err != nil {
		return Agent{}, err
	}
	return projectAgent(ready.Record), nil
}
