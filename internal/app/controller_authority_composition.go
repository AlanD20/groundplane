package app

import (
	"context"
	"fmt"
	agentchannel "github.com/AlanD20/groundplane/internal/controller/agentchannel"
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	agentregistration "github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"

	"github.com/AlanD20/groundplane/internal/common/config"

	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	hierarchycontroller "github.com/AlanD20/groundplane/internal/controller/hierarchy"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdreleasegroup "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type controllerAuthorityComposition struct {
	executionQueue              *workqueue.Repository
	acceptedActions             *acceptedaction.Submitter
	runnerRecords               *etcd.RunnerRepository
	tasks                       *etcd.TaskRepository
	idempotency                 *etcd.IdempotencyRepository
	hierarchyRecords            *etcd.HierarchyRepository
	environmentBlueprintRecords *etcd.EnvironmentBlueprintRepository
	releaseGroups               *etcdreleasegroup.Store
	releaseLedger               *etcd.ReleaseLedger
	hierarchyService            *hierarchycontroller.Service
	agents                      *agentregistration.Repository
	authenticator               agentchannel.Authenticator
	controllerKey               *ageinfra.ControllerKey
	intentProtector             *secretvalue.Protector
	intentCoordinator           *requestidempotency.Coordinator
}

func newControllerAuthorityComposition(
	ctx context.Context, cfg config.ControllerConfig, store etcd.EnvironmentBlueprintStore,
) (controllerAuthorityComposition, error) {
	runnerRecords, err := etcd.NewRunnerRepository(store)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: initialize Runner repository: %w", err)
	}
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: initialize Task repository: %w", err)
	}
	execution, err := workqueue.NewRepository(store)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: initialize execution schema owner: %w", err)
	}
	if err := execution.EnsureSchema(ctx); err != nil {
		return controllerAuthorityComposition{}, errs.Wrap(
			errs.KindInternal,
			wrapControllerRunError("admit native execution storage", err),
		)
	}
	idempotency, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: initialize idempotency repository: %w", err)
	}
	hierarchyRecords, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: initialize hierarchy repository: %w", err)
	}
	environmentBlueprintRecords, err := etcd.NewEnvironmentBlueprintRepository(store)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf(
			"controller: initialize Environment Blueprint repository: %w",
			err,
		)
	}
	releaseGroups, err := etcdreleasegroup.New(store)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: initialize release group repository: %w", err)
	}
	releaseLedger, err := etcd.NewReleaseLedger(store, tasks)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: initialize release ledger: %w", err)
	}
	if err := hierarchyRecords.ValidateEnvironmentVolumeDirs(ctx, cfg.Storage.VolumeRoot); err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf(
			"controller: validate persisted environment volume directories: %w",
			err,
		)
	}
	hierarchyRepository, err := hierarchycontroller.NewEtcdRepository(hierarchyRecords)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: initialize hierarchy adapter: %w", err)
	}
	hierarchyService, err := hierarchycontroller.NewService(hierarchyRepository)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: initialize hierarchy service: %w", err)
	}
	agents, err := agentregistration.NewRepository(store)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: initialize local Agent repository: %w", err)
	}
	authenticator, err := channeltransport.NewAuthenticator(agents)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf(
			"controller: initialize Agent channel authenticator: %w",
			err,
		)
	}
	controllerKey := &ageinfra.ControllerKey{Path: cfg.AgeKeyPath}
	if err := controllerKey.Load(ctx); err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf("controller: load Controller age key: %w", err)
	}
	intentProtector, err := secretvalue.NewControllerKeyProtector(controllerKey)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf(
			"controller: initialize idempotent intent protector: %w",
			err,
		)
	}
	intentCoordinator, err := requestidempotency.NewCoordinator(intentProtector)
	if err != nil {
		return controllerAuthorityComposition{}, fmt.Errorf(
			"controller: initialize idempotent intent coordinator: %w",
			err,
		)
	}
	acceptedActions, err := newAcceptedActions(store, intentProtector, execution, idempotency, intentCoordinator)
	if err != nil {
		return controllerAuthorityComposition{}, err
	}
	return controllerAuthorityComposition{
		executionQueue:              execution,
		acceptedActions:             acceptedActions,
		runnerRecords:               runnerRecords,
		tasks:                       tasks,
		idempotency:                 idempotency,
		hierarchyRecords:            hierarchyRecords,
		environmentBlueprintRecords: environmentBlueprintRecords,
		releaseGroups:               releaseGroups,
		releaseLedger:               releaseLedger,
		hierarchyService:            hierarchyService,
		agents:                      agents,
		authenticator:               authenticator,
		controllerKey:               controllerKey,
		intentProtector:             intentProtector,
		intentCoordinator:           intentCoordinator,
	}, nil
}
