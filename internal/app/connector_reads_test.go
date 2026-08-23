package app

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeConnectorReadRepository struct {
	environment    etcd.Versioned[etcd.EnvironmentRecord]
	environmentErr error
	connector      etcd.Versioned[etcd.ConnectorRecord]
	connectorErr   error
	page           etcd.Page[etcd.ConnectorRecord]
	listErr        error
	listCalls      int
}

func (fake *fakeConnectorReadRepository) GetEnvironment(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, fake.environmentErr
}

func (fake *fakeConnectorReadRepository) GetConnector(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.ConnectorRecord], error) {
	return fake.connector, fake.connectorErr
}

func (fake *fakeConnectorReadRepository) ListConnectors(
	_ context.Context,
	_ string,
	_ etcd.PageRequest,
) (etcd.Page[etcd.ConnectorRecord], error) {
	fake.listCalls++
	return fake.page, fake.listErr
}

func TestConnectorReadsRequireExistingEnvironmentBeforeListing(t *testing.T) {
	// Rationale: an empty list must not make a missing Environment look like an
	// existing Environment with no Connectors.
	repository := &fakeConnectorReadRepository{
		environmentErr: errs.New(errs.KindEnvironmentNotFound, "Environment was not found"),
	}
	service, err := newConnectorReadService(repository)
	if err != nil {
		t.Fatalf("newConnectorReadService() error = %v", err)
	}
	_, err = service.ListConnectors(
		context.Background(), "env_01K3D7R40G0000000000000000", etcd.PageRequest{Limit: 50},
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindEnvironmentNotFound || repository.listCalls != 0 {
		t.Fatalf("ListConnectors() error/calls = %v/%d", err, repository.listCalls)
	}
}

func TestConnectorReadsReturnRepositoryRevisionAndDetail(t *testing.T) {
	page := etcd.Page[etcd.ConnectorRecord]{Revision: 42}
	connector := etcd.Versioned[etcd.ConnectorRecord]{Revision: 41, ReadRevision: 42}
	repository := &fakeConnectorReadRepository{page: page, connector: connector}
	service, err := newConnectorReadService(repository)
	if err != nil {
		t.Fatalf("newConnectorReadService() error = %v", err)
	}
	listed, err := service.ListConnectors(
		context.Background(), "env_01K3D7R40G0000000000000000", etcd.PageRequest{Limit: 50},
	)
	if err != nil || listed.Revision != 42 || repository.listCalls != 1 {
		t.Fatalf("ListConnectors() = %#v, %v; calls = %d", listed, err, repository.listCalls)
	}
	got, err := service.GetConnector(context.Background(), "con_01K3D7R40G0000000000000000")
	if err != nil || got.Revision != 41 || got.ReadRevision != 42 {
		t.Fatalf("GetConnector() = %#v, %v", got, err)
	}
}
