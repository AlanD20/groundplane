package controllerupgrade

import (
	"context"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"
	"time"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) protect(ctx context.Context, release string) (requestidempotency.ProtectedEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: UpdateRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "release", Value: requestidempotency.String(release)},
		)),
	})
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, err
	}
	defer digest.Destroy()
	return service.intents.ProtectIntent(ctx, version, digest)
}

func (service *Service) resolvePublication(
	ctx context.Context, locator idempotencyrecord.IdempotencyLocator, evidence requestidempotency.ProtectedEvidence,
	result etcd.IdempotencyTransactionResult, publicationErr error, response idempotencyrecord.IdempotencyResponse,
) (idempotencyrecord.IdempotencyResponse, error) {
	if publicationErr != nil {
		if !errors.Is(publicationErr, context.DeadlineExceeded) &&
			!errors.Is(publicationErr, errs.New(errs.KindStorageUnavailable, "")) {
			return idempotencyrecord.IdempotencyResponse{}, publicationErr
		}
		if err := ctx.Err(); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		read, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		replay, found, err := service.intents.ResolveOperationRootExisting(read, service.evidence, locator, evidence)
		if ctx.Err() != nil {
			return idempotencyrecord.IdempotencyResponse{}, ctx.Err()
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
				return idempotencyrecord.IdempotencyResponse{}, publicationErr
			}
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if !found {
			return idempotencyrecord.IdempotencyResponse{}, publicationErr
		}
		return acceptedReplay(replay)
	}
	outcome, marker, conflict, err := result.Classify()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch outcome {
	case etcd.IdempotencyKnownApplied:
		return copyResponse(response), nil
	case etcd.IdempotencyKnownConflict:
		return idempotencyrecord.IdempotencyResponse{}, conflict
	case etcd.IdempotencyKnownExisting:
		// ResolveMarker authenticates the exact protected request before it
		// reports InProgress. Only this immutable accepted native Task replays
		// pending acceptance; ordinary Task retry semantics are unchanged.
		accepted := copyResponse(marker.Response)
		replay, err := service.intents.ResolveMarker(ctx, evidence, marker)
		if errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) {
			return accepted, nil
		}
		clear(accepted.Body)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		return acceptedReplay(replay)
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"native update publication outcome is invalid",
		)
	}
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
