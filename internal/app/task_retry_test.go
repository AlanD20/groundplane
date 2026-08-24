package app

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeTaskRetryRepository struct {
	scope     etcd.TaskRetryScope
	sourceID  string
	retryID   string
	marker    etcd.IdempotencyMarker
	initiated bool
}

func (repository *fakeTaskRetryRepository) GetTaskRetryScope(
	context.Context,
	string,
) (etcd.TaskRetryScope, error) {
	return repository.scope, nil
}

func (repository *fakeTaskRetryRepository) RetryTask(
	_ context.Context,
	sourceID string,
	retryID string,
	actor etcd.TaskActor,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.sourceID = sourceID
	repository.retryID = retryID
	if actor != etcd.TaskActorOperator {
		return etcd.IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "retry actor is not operator")
	}
	repository.marker = marker
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

func (repository *fakeTaskRetryRepository) RetryTaskWithInitiation(
	ctx context.Context,
	sourceID string,
	retryID string,
	_ etcd.TaskInitiation,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.initiated = true
	return repository.RetryTask(ctx, sourceID, retryID, etcd.TaskActorOperator, marker)
}

type fakeTaskRetryIdempotency struct {
	scope idempotentintent.Scope
}

func (idempotency *fakeTaskRetryIdempotency) Prepare(
	_ context.Context,
	_ string,
	scope idempotentintent.Scope,
) (taskRetryEvidence, error) {
	idempotency.scope = scope
	return taskRetryEvidence{}, nil
}

func (*fakeTaskRetryIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	taskRetryEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{}, false, nil
}

func (*fakeTaskRetryIdempotency) ResolveKnown(
	context.Context,
	taskRetryEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (*fakeTaskRetryIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	taskRetryEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

// Rationale: a public retry must preserve the source operation owner for
// idempotency while allocating one new durable attempt id.
func TestTaskRetryUsesSourceOwnerAndReturnsNewAttempt(t *testing.T) {
	const sourceID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	repository := &fakeTaskRetryRepository{scope: etcd.TaskRetryScope{
		Kind: etcd.IdempotencyScopeEnvironment, ID: environmentID,
	}}
	idempotency := &fakeTaskRetryIdempotency{}
	service, err := newTaskRetryService(repository, idempotency)
	if err != nil {
		t.Fatalf("newTaskRetryService() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	response, err := service.RetryTask(context.Background(), sourceID, "task-retry-key-0001")
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatalf("RetryTask() response = %s, %v", response.Body, err)
	}
	if response.Status != http.StatusAccepted || repository.sourceID != sourceID ||
		accepted.TaskID == sourceID || accepted.TaskID != repository.retryID ||
		ids.Validate(ids.KindTask, accepted.TaskID) != nil ||
		idempotency.scope != (idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID}) ||
		repository.marker.Locator.Route != taskRetryRoute || repository.marker.Locator.ScopeID != environmentID ||
		repository.marker.TaskID != accepted.TaskID || repository.marker.CreatedAt != now {
		t.Fatalf("Task retry = %#v / %#v / %#v", response, repository.marker, idempotency.scope)
	}
}

func TestSystemTaskRetryUsesImmediateSourceOwnerScope(t *testing.T) {
	// Rationale: cascade authority changes the actor and parent fence, never the
	// durable owner used to scope retry idempotency.
	const sourceID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const projectID = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	repository := &fakeTaskRetryRepository{scope: etcd.TaskRetryScope{
		Kind: etcd.IdempotencyScopeProject, ID: projectID,
	}}
	idempotency := &fakeTaskRetryIdempotency{}
	service, err := newTaskRetryService(repository, idempotency)
	if err != nil {
		t.Fatalf("newTaskRetryService() error = %v", err)
	}
	service.now = func() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) }
	if _, err := service.RetryTaskWithInitiation(
		context.Background(), sourceID, "system-retry-key-0001", etcd.TaskInitiation{},
	); err != nil {
		t.Fatalf("RetryTaskWithInitiation() error = %v", err)
	}
	if !repository.initiated ||
		idempotency.scope != (idempotentintent.Scope{Kind: idempotentintent.ScopeProject, ID: projectID}) ||
		repository.marker.Locator.ScopeKind != etcd.IdempotencyScopeProject ||
		repository.marker.Locator.ScopeID != projectID {
		t.Fatalf("system retry scope/initiation = %#v/%#v", idempotency.scope, repository.marker)
	}
}
