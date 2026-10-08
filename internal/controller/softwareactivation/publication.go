package softwareactivation

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) protect(
	ctx context.Context,
	preparationTaskID, key string,
) (requestidempotency.ProtectedEvidence, idempotencyrecord.IdempotencyLocator, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: Route,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(
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

func (service *Service) resolvePublication(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence requestidempotency.ProtectedEvidence,
	result etcd.IdempotencyTransactionResult,
	publicationErr error,
	accepted Accepted,
) (Accepted, error) {
	if publicationErr != nil {
		if !errors.Is(publicationErr, context.DeadlineExceeded) &&
			!errors.Is(publicationErr, errs.New(errs.KindStorageUnavailable, "")) {
			return Accepted{}, publicationErr
		}
		if err := ctx.Err(); err != nil {
			return Accepted{}, err
		}
		read, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		replay, found, err := service.intents.ResolveOperationRootExisting(read, service.evidence, locator, evidence)
		if err != nil || !found {
			if err != nil && !errors.Is(err, context.DeadlineExceeded) &&
				!errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
				return Accepted{}, err
			}
			return Accepted{}, publicationErr
		}
		return decodeReplay(replay)
	}
	outcome, marker, conflict, err := result.Classify()
	if err != nil {
		return Accepted{}, err
	}
	switch outcome {
	case etcd.IdempotencyKnownApplied:
		return accepted, nil
	case etcd.IdempotencyKnownConflict:
		return Accepted{}, conflict
	case etcd.IdempotencyKnownExisting:
		stored := append([]byte(nil), marker.Response.Body...)
		resolution, err := service.intents.ResolveMarker(ctx, evidence, marker)
		if errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) {
			defer clear(stored)
			return decodeAccepted(stored)
		}
		clear(stored)
		if err != nil {
			return Accepted{}, err
		}
		return decodeReplay(resolution)
	default:
		return Accepted{}, errs.New(errs.KindInternal, "software activation publication outcome is invalid")
	}
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
