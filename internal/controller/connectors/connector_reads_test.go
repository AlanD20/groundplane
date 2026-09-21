package connectors

import (
	"context"
	"testing"

	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeConnectorReadRepository struct {
	environment    testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	environmentErr error
	connector      testkeyvalue.Versioned[testconnectors.Record]
	connectorErr   error
	page           testkeyvalue.Page[testconnectors.Record]
	listErr        error
	listCalls      int
}

func (fake *fakeConnectorReadRepository) GetEnvironment(
	_ context.Context,
	_ string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return fake.environment, fake.environmentErr
}

func (fake *fakeConnectorReadRepository) GetConnector(
	_ context.Context,
	_ string,
) (testkeyvalue.Versioned[testconnectors.Record], error) {
	return fake.connector, fake.connectorErr
}

func (fake *fakeConnectorReadRepository) ListConnectors(
	_ context.Context,
	_ string,
	_ testkeyvalue.PageRequest,
) (testkeyvalue.Page[testconnectors.Record], error) {
	fake.listCalls++
	return fake.page, fake.listErr
}

func TestConnectorReadsRequireExistingEnvironmentBeforeListing(t *testing.T) {
	// Rationale: an empty list must not make a missing Environment look like an
	// existing Environment with no Connectors.
	repository := &fakeConnectorReadRepository{
		environmentErr: errs.New(errs.KindEnvironmentNotFound, "Environment was not found"),
	}
	service, err := NewReadService(repository)
	if err != nil {
		t.Fatalf("NewReadService() error = %v", err)
	}
	_, err = service.ListConnectors(
		context.Background(), "env_01K3D7R40G0000000000000000", testkeyvalue.PageRequest{Limit: 50},
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindEnvironmentNotFound || repository.listCalls != 0 {
		t.Fatalf("ListConnectors() error/calls = %v/%d", err, repository.listCalls)
	}
}

func TestConnectorReadsReturnRepositoryRevisionAndDetail(t *testing.T) {
	page := testkeyvalue.Page[testconnectors.Record]{Revision: 42}
	connector := testkeyvalue.Versioned[testconnectors.Record]{Revision: 41, ReadRevision: 42}
	repository := &fakeConnectorReadRepository{page: page, connector: connector}
	service, err := NewReadService(repository)
	if err != nil {
		t.Fatalf("NewReadService() error = %v", err)
	}
	listed, err := service.ListConnectors(
		context.Background(), "env_01K3D7R40G0000000000000000", testkeyvalue.PageRequest{Limit: 50},
	)
	if err != nil || listed.Revision != 42 || repository.listCalls != 1 {
		t.Fatalf("ListConnectors() = %#v, %v; calls = %d", listed, err, repository.listCalls)
	}
	got, err := service.GetConnector(context.Background(), "con_01K3D7R40G0000000000000000")
	if err != nil || got.Revision != 41 || got.ReadRevision != 42 {
		t.Fatalf("GetConnector() = %#v, %v", got, err)
	}
}
