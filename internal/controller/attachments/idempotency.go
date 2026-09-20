package attachments

import (
	"context"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

type attachMutationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type attachMutationIdempotency interface {
	PrepareCreate(context.Context, string, apiTypes.AttachRequest) (attachMutationEvidence, error)
	PrepareDetach(context.Context, string, string) (attachMutationEvidence, error)
	PrepareRename(context.Context, string, string, apiTypes.AttachRenameRequest) (attachMutationEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		attachMutationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		attachMutationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		attachMutationEvidence,
		error,
	) (requestidempotency.Resolution, error)
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
}

type durableAttachMutationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewMutationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableAttachMutationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutation idempotency is not configured")
	}
	return &durableAttachMutationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableAttachMutationIdempotency) PrepareCreate(
	ctx context.Context,
	environmentID string,
	request apiTypes.AttachRequest,
) (attachMutationEvidence, error) {
	grants := make([]requestidempotency.Value, len(request.GrantAttachIDs))
	for index, grantID := range request.GrantAttachIDs {
		grants[index] = requestidempotency.String(grantID)
	}
	return service.protect(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: attachCreationRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{
				Name:  "backing_service_id",
				Value: requestidempotency.String(request.BackingServiceID),
			},
			requestidempotency.Field{
				Name: "credential",
				Value: requestidempotency.Object(
					requestidempotency.Field{
						Name:  "attach_id",
						Value: requestidempotency.String(request.Credential.AttachID),
					},
					requestidempotency.Field{
						Name:  "mode",
						Value: requestidempotency.String(string(request.Credential.Mode)),
					},
				),
			},
			requestidempotency.Field{Name: "grant_attach_ids", Value: requestidempotency.List(grants...)},
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(request.Name)},
			requestidempotency.Field{Name: "service_id", Value: requestidempotency.String(request.ServiceID)},
		)),
	})
}

func (service *durableAttachMutationIdempotency) PrepareDetach(
	ctx context.Context,
	environmentID string,
	attachID string,
) (attachMutationEvidence, error) {
	return service.protect(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: attachDeletionRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: attachID}},
		Query: requestidempotency.Object(), Body: requestidempotency.NoBody(),
	})
}

func (service *durableAttachMutationIdempotency) PrepareRename(
	ctx context.Context,
	environmentID string,
	attachID string,
	request apiTypes.AttachRenameRequest,
) (attachMutationEvidence, error) {
	return service.protect(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: attachRenameRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: attachID}},
		Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(request.Name)},
		)),
	})
}

func (service *durableAttachMutationIdempotency) protect(
	ctx context.Context,
	intent requestidempotency.CanonicalIntentV1,
) (attachMutationEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, intent)
	if err != nil {
		return attachMutationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return attachMutationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return attachMutationEvidence{}, err
	}
	return attachMutationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableAttachMutationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence attachMutationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableAttachMutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence attachMutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableAttachMutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence attachMutationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func (service *durableAttachMutationIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}
