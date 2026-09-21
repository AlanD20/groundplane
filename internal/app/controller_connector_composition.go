package app

import (
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/internal/controller/connectors"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
)

type controllerConnectorComposition struct {
	mutations handlers.ConnectorMutator
	deletions handlers.ConnectorDeleter
}

// Initialization owns failure cleanup here because Connector creation and
// deletion preserve different shutdown-error policies.
func newControllerConnectorComposition(
	store io.Closer,
	hierarchyRecords *etcd.HierarchyRepository,
	secretRecords *etcd.SecretRepository,
	connectorRecords *etcd.ConnectorRepository,
	intentCoordinator *requestidempotency.Coordinator,
	idempotency *etcd.IdempotencyRepository,
	intentProtector *secretvalue.Protector,
) (*controllerConnectorComposition, error) {
	connectorCreationRepository, err := connectors.NewCreationRepository(
		hierarchyRecords,
		secretRecords,
		connectorRecords,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Connector creation repositories: %w", err)
	}
	connectorCreationIdempotency, err := connectors.NewCreationIdempotency(intentCoordinator, idempotency)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Connector creation idempotency: %w", err)
	}
	connectorMutations, err := connectors.NewCreationService(
		connectorCreationRepository,
		intentProtector,
		connectorCreationIdempotency,
	)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("controller: initialize Connector creation service: %w", err)
	}
	connectorDeletionRepository, err := connectors.NewDeletionRepository(hierarchyRecords, connectorRecords)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize Connector deletion repositories", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	connectorDeletionIdempotency, err := connectors.NewDeletionIdempotency(intentCoordinator, idempotency)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize Connector deletion idempotency", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	connectorDeletions, err := connectors.NewDeletionService(
		connectorDeletionRepository,
		connectorDeletionIdempotency,
	)
	if err != nil {
		closeErr := store.Close()
		return nil, errs.Wrap(errs.KindInternal, errors.Join(
			wrapControllerRunError("initialize Connector deletion service", err),
			wrapControllerRunError("close etcd", closeErr),
		))
	}
	return &controllerConnectorComposition{mutations: connectorMutations, deletions: connectorDeletions}, nil
}
