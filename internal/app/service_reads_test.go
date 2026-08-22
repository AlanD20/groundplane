package app

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeServiceReadRepository struct {
	serviceReadRepository
	environment etcd.Versioned[etcd.EnvironmentRecord]
	page        etcd.Page[etcd.ServiceRecord]
	wantRequest etcd.PageRequest
	listed      bool
}

func (fake *fakeServiceReadRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeServiceReadRepository) ListServices(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	fake.listed = environmentID == fake.environment.Record.ID && request == fake.wantRequest
	return fake.page, nil
}

// Rationale: Service labels are scoped to an Environment, so a collection read must verify that
// owner and preserve the repository's opaque cursor tuple instead of treating a missing owner as empty.
func TestServiceListVerifiesOwnerAndPreservesPagination(t *testing.T) {
	t.Parallel()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := etcd.PageRequest{Limit: 23, Cursor: "opaque-cursor"}
	want := etcd.Page[etcd.ServiceRecord]{NextCursor: "next-cursor", Revision: 91}
	repository := &fakeServiceReadRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{ID: environmentID}, Revision: 12, ReadRevision: 12,
		},
		page: want, wantRequest: request,
	}
	service, err := newServiceReadService(repository)
	if err != nil {
		t.Fatalf("newServiceReadService() error = %v", err)
	}
	got, err := service.ListServices(context.Background(), environmentID, request)
	if err != nil || !reflect.DeepEqual(got, want) || !repository.listed {
		t.Fatalf("ListServices() = %#v, %v, listed %t", got, err, repository.listed)
	}
}
