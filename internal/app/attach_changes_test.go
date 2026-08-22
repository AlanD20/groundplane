package app

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type fakeAttachRenameRepository struct {
	attachMutationRepository
}

type fakeAttachListRepository struct {
	attachMutationRepository
	environment etcd.Versioned[etcd.EnvironmentRecord]
	page        etcd.Page[etcd.AttachRecord]
	wantRequest etcd.PageRequest
	called      bool
}

func (fake *fakeAttachListRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeAttachListRepository) ListAttaches(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.AttachRecord], error) {
	fake.called = environmentID == fake.environment.Record.ID && request == fake.wantRequest
	return fake.page, nil
}

type fakeAttachRenameIdempotency struct {
	attachMutationIdempotency
	locator  etcd.IdempotencyLocator
	response etcd.IdempotencyResponse
}

func (fake *fakeAttachRenameIdempotency) ResolveReplayLocator(
	context.Context,
	etcd.IdempotencyReplayTarget,
	string,
	string,
	string,
) (etcd.IdempotencyLocator, bool, error) {
	return fake.locator, true, nil
}

func (*fakeAttachRenameIdempotency) PrepareRename(
	context.Context,
	string,
	string,
	apiTypes.AttachRenameRequest,
) (attachMutationEvidence, error) {
	return attachMutationEvidence{}, nil
}

func (fake *fakeAttachRenameIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	attachMutationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionReplay, Response: fake.response}, true, nil
}

// Rationale: an Attach may be deleted after a successful rename, so retrying the same request must use
// the stable replay-target index and return the exact response without attempting to read the Attach.
func TestRenameAttachReplaysBeforeReadingTheTarget(t *testing.T) {
	t.Parallel()
	want := etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"att_replayed"}`),
	}
	idempotency := &fakeAttachRenameIdempotency{
		locator: etcd.IdempotencyLocator{
			ScopeKind: etcd.IdempotencyScopeEnvironment,
			ScopeID:   "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Method:    http.MethodPost,
			Route:     attachRenameRoute,
			Key:       "attach-rename-key-0001",
		},
		response: want,
	}
	service := &attachMutationService{
		repository: &fakeAttachRenameRepository{}, idempotency: idempotency,
	}
	got, err := service.RenameAttach(
		context.Background(),
		"att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		apiTypes.AttachRenameRequest{Name: "renamed-database"},
		"attach-rename-key-0001",
	)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("RenameAttach(replay) = %#v, %v; want %#v", got, err, want)
	}
}

// Rationale: the public Attach collection is Environment-scoped, so its application boundary must
// verify that owner exists and pass the caller's opaque pagination tuple through unchanged.
func TestListAttachesRequiresAndPreservesEnvironmentScope(t *testing.T) {
	t.Parallel()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := etcd.PageRequest{Limit: 17, Cursor: "opaque-cursor"}
	want := etcd.Page[etcd.AttachRecord]{NextCursor: "next-cursor", Revision: 42}
	repository := &fakeAttachListRepository{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{
				NetworkPool: "10.40.0.0/16",
				ID:          environmentID,
			},
			Revision:     10,
			ReadRevision: 10,
		},
		page: want, wantRequest: request,
	}
	service := &attachMutationService{repository: repository}
	got, err := service.ListAttaches(context.Background(), environmentID, request)
	if err != nil || !reflect.DeepEqual(got, want) || !repository.called {
		t.Fatalf("ListAttaches() = %#v, %v, called %t", got, err, repository.called)
	}
}
