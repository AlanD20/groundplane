package controllerupgrade

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) protect(ctx context.Context, release string) (idempotentintent.ProtectedEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost, Route: UpdateRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopePlatform},
		Query: idempotentintent.Object(), Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "release", Value: idempotentintent.String(release)},
		)),
	})
	if err != nil {
		return idempotentintent.ProtectedEvidence{}, err
	}
	defer digest.Destroy()
	return service.intents.ProtectIntent(ctx, version, digest)
}

func (service *Service) resolvePublication(
	ctx context.Context, locator etcd.IdempotencyLocator, evidence idempotentintent.ProtectedEvidence,
	result etcd.IdempotencyTransactionResult, publicationErr error, response etcd.IdempotencyResponse,
) (etcd.IdempotencyResponse, error) {
	if publicationErr != nil {
		if !errors.Is(publicationErr, context.DeadlineExceeded) &&
			!errors.Is(publicationErr, errs.New(errs.KindStorageUnavailable, "")) {
			return etcd.IdempotencyResponse{}, publicationErr
		}
		if err := ctx.Err(); err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		read, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		replay, found, err := service.intents.ResolveOperationRootExisting(read, service.evidence, locator, evidence)
		if ctx.Err() != nil {
			return etcd.IdempotencyResponse{}, ctx.Err()
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
				return etcd.IdempotencyResponse{}, publicationErr
			}
			return etcd.IdempotencyResponse{}, err
		}
		if !found {
			return etcd.IdempotencyResponse{}, publicationErr
		}
		return acceptedReplay(replay)
	}
	outcome, marker, conflict, err := result.Classify()
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch outcome {
	case etcd.IdempotencyKnownApplied:
		return copyResponse(response), nil
	case etcd.IdempotencyKnownConflict:
		return etcd.IdempotencyResponse{}, conflict
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
			return etcd.IdempotencyResponse{}, err
		}
		return acceptedReplay(replay)
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "native update publication outcome is invalid")
	}
}

func acceptedReplay(resolution idempotentintent.Resolution) (etcd.IdempotencyResponse, error) {
	if resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "native update replay is invalid")
	}
	return copyResponse(resolution.Response), nil
}

func copyResponse(response etcd.IdempotencyResponse) etcd.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}
