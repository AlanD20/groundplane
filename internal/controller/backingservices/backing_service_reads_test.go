package backingservices

import (
	"context"
	"reflect"
	"testing"

	testbackingservices "github.com/AlanD20/groundplane/internal/infra/etcd/backingservices"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type fakeBackingServiceReadRepository struct {
	backingServiceReadRepository
	detail      testkeyvalue.Versioned[testbackingservices.Record]
	page        testkeyvalue.Page[testbackingservices.Record]
	wantRequest testkeyvalue.PageRequest
	listed      bool
}

func (fake *fakeBackingServiceReadRepository) GetBackingService(
	context.Context,
	string,
) (testkeyvalue.Versioned[testbackingservices.Record], error) {
	return fake.detail, nil
}

func (fake *fakeBackingServiceReadRepository) ListBackingServices(
	_ context.Context,
	request testkeyvalue.PageRequest,
) (testkeyvalue.Page[testbackingservices.Record], error) {
	fake.listed = request == fake.wantRequest
	return fake.page, nil
}

// Rationale: the application boundary must preserve the facade repository's opaque cursor and
// fixed-revision projection instead of reconstructing backing hierarchy state independently.
func TestBackingServiceReadPreservesRepositoryPage(t *testing.T) {
	t.Parallel()
	request := testkeyvalue.PageRequest{Limit: 23, Cursor: "opaque-cursor"}
	want := testkeyvalue.Page[testbackingservices.Record]{NextCursor: "next-cursor", Revision: 91}
	repository := &fakeBackingServiceReadRepository{page: want, wantRequest: request}
	service, err := NewReadService(repository)
	if err != nil {
		t.Fatalf("NewReadService() error = %v", err)
	}
	got, err := service.ListBackingServices(context.Background(), request)
	if err != nil || !reflect.DeepEqual(got, want) || !repository.listed {
		t.Fatalf("ListBackingServices() = %#v, %v, listed %t", got, err, repository.listed)
	}
}
