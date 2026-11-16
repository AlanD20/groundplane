package app

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/connectors"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type controllerConnectorComposition struct {
	preparations []taskdependencies.Registration
	mutations    handlers.ConnectorMutator
	deletions    handlers.ConnectorDeleter
	checks       handlers.ConnectorChecker
}

// The root owns the borrowed store; initialization only reports its own failure.
func newControllerConnectorComposition(
	store controllerStore,
	hierarchyRecords *etcd.HierarchyRepository,
	secretRecords *etcd.SecretRepository,
	connectorRecords *etcd.ConnectorRepository,
	intentCoordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository,
	intentProtector *secretvalue.Protector,
	accepted *acceptedaction.Submitter,
) (*controllerConnectorComposition, error) {
	connectorCreationRepository, err := connectors.NewCreationRepository(
		hierarchyRecords,
		secretRecords,
		connectorRecords,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Connector creation repositories: %w", err)
	}
	connectorCreationIdempotency, err := connectors.NewCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Connector creation idempotency: %w", err)
	}
	connectorMutations, err := connectors.NewMutationService(
		connectorCreationRepository,
		intentProtector,
		connectorCreationIdempotency,
	)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Connector creation service: %w", err)
	}
	connectorDeletionRepository, err := connectors.NewDeletionRepository(hierarchyRecords, connectorRecords)
	if err != nil {
		return nil, errs.Wrap(
			errs.KindInternal,
			wrapControllerRunError("initialize Connector deletion repositories", err),
		)
	}
	connectorDeletionIdempotency, err := connectors.NewDeletionIdempotency(intentCoordinator, idempotency)
	if err != nil {
		return nil, errs.Wrap(
			errs.KindInternal,
			wrapControllerRunError("initialize Connector deletion idempotency", err),
		)
	}
	connectorDeletions, err := connectors.NewDeletionService(
		connectorDeletionRepository,
		connectorDeletionIdempotency,
		accepted,
	)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, wrapControllerRunError("initialize Connector deletion service", err))
	}
	checkRecords, err := connectorchecks.NewRepository(store)
	if err != nil {
		return nil, err
	}
	checks, err := connectors.NewCheckService(hierarchyRecords, connectorRecords, secretRecords,
		checkRecords, idempotency, intentCoordinator, intentProtector, accepted)
	if err != nil {
		return nil, err
	}
	return &controllerConnectorComposition{
		preparations: append(connectorDeletions.AcceptedPreparations(), checks.AcceptedPreparations()...),
		mutations:    connectorMutations,
		deletions:    connectorDeletions,
		checks:       checks,
	}, nil
}
