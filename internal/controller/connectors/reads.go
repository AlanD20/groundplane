package connectors

import (
	"context"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type connectorReadRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetConnector(context.Context, string) (etcd.Versioned[connectorrecord.Record], error)
	ListConnectors(context.Context, string, etcd.PageRequest) (etcd.Page[connectorrecord.Record], error)
}

type connectorReadService struct {
	repository connectorReadRepository
}

func NewReadService(repository connectorReadRepository) (*connectorReadService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Connector read repository is required")
	}
	return &connectorReadService{repository: repository}, nil
}

func (service *connectorReadService) ListConnectors(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[connectorrecord.Record], error) {
	if _, err := service.repository.GetEnvironment(ctx, environmentID); err != nil {
		return etcd.Page[connectorrecord.Record]{}, err
	}
	return service.repository.ListConnectors(ctx, environmentID, request)
}

func (service *connectorReadService) GetConnector(
	ctx context.Context,
	connectorID string,
) (etcd.Versioned[connectorrecord.Record], error) {
	return service.repository.GetConnector(ctx, connectorID)
}

type durableConnectorReadRepository struct {
	hierarchy  *etcd.HierarchyRepository
	connectors *etcd.ConnectorRepository
}

func NewReadRepository(
	hierarchy *etcd.HierarchyRepository,
	connectors *etcd.ConnectorRepository,
) (*durableConnectorReadRepository, error) {
	if hierarchy == nil || connectors == nil {
		return nil, errs.New(errs.KindInternal, "Connector read dependencies are required")
	}
	return &durableConnectorReadRepository{hierarchy: hierarchy, connectors: connectors}, nil
}

func (repository *durableConnectorReadRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableConnectorReadRepository) GetConnector(
	ctx context.Context,
	id string,
) (etcd.Versioned[connectorrecord.Record], error) {
	return repository.connectors.GetConnector(ctx, id)
}

func (repository *durableConnectorReadRepository) ListConnectors(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[connectorrecord.Record], error) {
	return repository.connectors.ListConnectors(ctx, environmentID, request)
}
