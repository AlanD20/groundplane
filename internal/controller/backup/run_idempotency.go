package backup

import (
	"context"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
	"time"
)

// BackupRunIdempotency is the protected bodyless intent lifecycle used by the
// manual backup service. It is deliberately shaped like the other app
// mutation services so marker replay occurs before any durable state reads.
type backupRunIdempotency interface {
	Prepare(context.Context, string, string) (backupRunEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		backupRunEvidence,
	) (requestidempotency.Resolution, bool, error)
	NewMarker(
		backupRunEvidence,
		etcd.IdempotencyLocator,
		etcd.IdempotencyResponse,
		string,
		time.Time,
	) (etcd.IdempotencyMarker, error)
	ResolveKnown(
		context.Context,
		backupRunEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		backupRunEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type backupRunEvidence struct {
	candidate requestidempotency.ProtectedEvidence
}

// durableBackupRunIdempotency is the production adapter for the protected
// bodyless intent. The request body is explicitly NoBody; the idempotency key
// remains only in the locator and is never persisted as task parameters.
type durableBackupRunIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewDurableBackupRunIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableBackupRunIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "backup run idempotency dependencies are required")
	}
	return &durableBackupRunIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableBackupRunIdempotency) Prepare(
	ctx context.Context,
	environmentID string,
	route string,
) (backupRunEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  route,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:   []requestidempotency.PathBinding{{Name: "id", Value: environmentID}},
		Query:  requestidempotency.Object(),
		Body:   requestidempotency.NoBody(),
	})
	if err != nil {
		return backupRunEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return backupRunEvidence{}, err
	}
	return backupRunEvidence{candidate: candidate}, nil
}

func (service *durableBackupRunIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence backupRunEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableBackupRunIdempotency) NewMarker(
	evidence backupRunEvidence,
	locator etcd.IdempotencyLocator,
	response etcd.IdempotencyResponse,
	taskID string,
	now time.Time,
) (etcd.IdempotencyMarker, error) {
	intent, err := evidence.candidate.DurableRecord()
	if err != nil {
		return etcd.IdempotencyMarker{}, err
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, Intent: intent, Response: response,
		TaskID: taskID, CreatedAt: now, UpdatedAt: now,
	}
	marker.Intent.Ciphertext = append([]byte(nil), intent.Ciphertext...)
	marker.Response.Body = append([]byte(nil), response.Body...)
	return marker, nil
}

func (service *durableBackupRunIdempotency) ResolveKnown(
	ctx context.Context,
	evidence backupRunEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableBackupRunIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence backupRunEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(
		ctx,
		service.repository,
		locator,
		evidence.candidate,
		original,
	)
}
