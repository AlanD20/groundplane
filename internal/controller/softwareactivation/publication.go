package softwareactivation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) protect(
	ctx context.Context,
	preparationTaskID string, after []string, key string,
) (requestidempotency.ProtectedEvidence, idempotencyrecord.IdempotencyLocator, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: Route,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(after)},
			requestidempotency.Field{Name: "preparation_task_id", Value: requestidempotency.String(preparationTaskID)},
		)),
	})
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, idempotencyrecord.IdempotencyLocator{}, err
	}
	defer digest.Destroy()
	evidence, err := service.intents.ProtectIntent(ctx, version, digest)
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, idempotencyrecord.IdempotencyLocator{}, err
	}
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopePlatform, ScopeID: "-",
		Method: http.MethodPost, Route: Route, Key: key,
	}
	if err := idempotencyrecord.ValidateIdempotencyLocator(locator); err != nil {
		evidence.Destroy()
		return requestidempotency.ProtectedEvidence{}, idempotencyrecord.IdempotencyLocator{}, err
	}
	return evidence, locator, nil
}

func decodeReplay(resolution requestidempotency.Resolution) (Accepted, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay || resolution.Response.Status != http.StatusAccepted ||
		resolution.Response.ContentKind != "application/json" {
		return Accepted{}, errs.New(errs.KindInternal, "software activation replay is invalid")
	}
	defer clear(resolution.Response.Body)
	return decodeAccepted(resolution.Response.Body)
}

func decodeAccepted(value []byte) (Accepted, error) {
	var accepted Accepted
	if err := json.Unmarshal(value, &accepted); err != nil || ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return Accepted{}, errs.New(errs.KindInternal, "software activation accepted response is corrupt")
	}
	return accepted, nil
}
