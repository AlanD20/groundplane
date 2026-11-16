package app

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	scriptoperations "github.com/AlanD20/groundplane/internal/controller/scripts"
	serviceoperations "github.com/AlanD20/groundplane/internal/controller/services"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
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
	accepted *acceptedaction.Submitter,
) (handlers.ServiceMutator, []taskdependencies.Registration, error) {
	serviceMutationIdempotency, err := serviceoperations.NewMutationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, nil, fmt.Errorf("controller: initialize Service mutation idempotency: %w", err)
	}
	serviceLifecycleIdempotency, err := serviceoperations.NewLifecycleIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, nil, fmt.Errorf("controller: initialize Service lifecycle idempotency: %w", err)
	}
	serviceLifecycle, err := serviceoperations.NewLifecycleService(
		serviceMutationRepository,
		serviceLifecycleIdempotency,
		attachFactValues,
		accepted,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("controller: initialize Service lifecycle service: %w", err)
	}
	serviceMutations, err := serviceoperations.NewMutationService(
		serviceMutationRepository,
		serviceMutationIdempotency,
		serviceLifecycle,
		accepted,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("controller: initialize Service mutation service: %w", err)
	}
	return serviceMutations, serviceMutations.AcceptedPreparations(), nil
}

func newControllerScriptMutations(
	scriptMutationRepository *scriptoperations.MutationRepository,
	scriptArtifacts *taskplanning.ScriptArtifactService,
	agents *agentregistration.Repository,
	images workloadseal.Resolver,
	intentCoordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository,
	accepted *acceptedaction.Submitter,
) (handlers.ScriptMutator, []taskdependencies.Registration, error) {
	scriptMutationIdempotency, err := scriptoperations.NewMutationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, nil, fmt.Errorf("controller: initialize Script mutation idempotency: %w", err)
	}
	scriptPreparation, err := taskplanning.NewScriptRunnerPreparationService(
		scriptArtifacts,
		agents,
		images,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("controller: initialize Script runner preparation: %w", err)
	}
	scriptDeletions, err := scriptoperations.NewDeletionService(scriptMutationRepository, accepted)
	if err != nil {
		return nil, nil, fmt.Errorf("controller: initialize Script deletion service: %w", err)
	}
	scriptMutations, err := scriptoperations.NewMutationService(
		scriptMutationRepository, scriptMutationIdempotency, scriptPreparation, scriptDeletions, accepted,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("controller: initialize Script mutation service: %w", err)
	}
	return scriptMutations, scriptMutations.AcceptedPreparations(), nil
}
