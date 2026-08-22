package app

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeBackingServiceReadRepository struct {
	backingServiceReadRepository
	detail      etcd.Versioned[etcd.BackingServiceRecord]
	page        etcd.Page[etcd.BackingServiceRecord]
	wantRequest etcd.PageRequest
	listed      bool
}

func (fake *fakeBackingServiceReadRepository) GetBackingService(
	context.Context,
	string,
) (etcd.Versioned[etcd.BackingServiceRecord], error) {
	return fake.detail, nil
}

func (fake *fakeBackingServiceReadRepository) ListBackingServices(
	_ context.Context,
	request etcd.PageRequest,
) (etcd.Page[etcd.BackingServiceRecord], error) {
	fake.listed = request == fake.wantRequest
	return fake.page, nil
}

// Rationale: the application boundary must preserve the facade repository's opaque cursor and
// fixed-revision projection instead of reconstructing backing hierarchy state independently.
func TestBackingServiceReadPreservesRepositoryPage(t *testing.T) {
	t.Parallel()
	request := etcd.PageRequest{Limit: 23, Cursor: "opaque-cursor"}
	want := etcd.Page[etcd.BackingServiceRecord]{NextCursor: "next-cursor", Revision: 91}
	repository := &fakeBackingServiceReadRepository{page: want, wantRequest: request}
	service, err := newBackingServiceReadService(repository)
	if err != nil {
		t.Fatalf("newBackingServiceReadService() error = %v", err)
	}
	got, err := service.ListBackingServices(context.Background(), request)
	if err != nil || !reflect.DeepEqual(got, want) || !repository.listed {
		t.Fatalf("ListBackingServices() = %#v, %v, listed %t", got, err, repository.listed)
	}
}
