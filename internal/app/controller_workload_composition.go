package app

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	scriptoperations "github.com/AlanD20/groundplane/internal/controller/scripts"
	serviceoperations "github.com/AlanD20/groundplane/internal/controller/services"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	agentregistration "github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
)

func newControllerServiceMutations(
	serviceMutationRepository *serviceoperations.MutationRepository,
	planResolver *taskplanning.TaskPlanResolver,
	attachFactValues *attachments.FactService,
	intentCoordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository,
) (handlers.ServiceMutator, error) {
	serviceMutationIdempotency, err := serviceoperations.NewMutationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Service mutation idempotency: %w", err)
	}
	serviceLifecycleIdempotency, err := serviceoperations.NewLifecycleIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Service lifecycle idempotency: %w", err)
	}
	serviceLifecycle, err := serviceoperations.NewLifecycleService(
		serviceMutationRepository,
		planResolver,
		serviceLifecycleIdempotency,
		attachFactValues,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Service lifecycle service: %w", err)
	}
	serviceMutations, err := serviceoperations.NewMutationService(serviceMutationRepository, serviceMutationIdempotency, serviceLifecycle)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Service mutation service: %w", err)
	}
	return serviceMutations, nil
}

func newControllerScriptMutations(
	scriptMutationRepository *scriptoperations.MutationRepository,
	scriptArtifacts *taskplanning.ScriptArtifactService,
	agents *agentregistration.Repository,
	images workloadseal.Resolver,
	intentCoordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository,
) (handlers.ScriptMutator, error) {
	scriptMutationIdempotency, err := scriptoperations.NewMutationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Script mutation idempotency: %w", err)
	}
	scriptPreparation, err := taskplanning.NewScriptRunnerPreparationService(
		scriptArtifacts,
		agents,
		images,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Script runner preparation: %w", err)
	}
	scriptDeletionIdempotency, err := scriptoperations.NewDeletionIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Script deletion idempotency: %w", err)
	}
	scriptDeletions, err := scriptoperations.NewDeletionService(scriptMutationRepository, scriptDeletionIdempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Script deletion service: %w", err)
	}
	scriptMutations, err := scriptoperations.NewMutationService(
		scriptMutationRepository, scriptMutationIdempotency, scriptPreparation, scriptDeletions,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Script mutation service: %w", err)
	}
	return scriptMutations, nil
}
