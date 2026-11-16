package controllerupgrade

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"

	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) protect(
	ctx context.Context,
	release string,
	after []string,
) (requestidempotency.ProtectedEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: UpdateRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(after)},
			requestidempotency.Field{Name: "release", Value: requestidempotency.String(release)},
		)),
	})
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, err
	}
	defer digest.Destroy()
	return service.intents.ProtectIntent(ctx, version, digest)
}

func acceptedReplay(resolution requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "native update replay is invalid")
	}
	return copyResponse(resolution.Response), nil
}

func copyResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}
