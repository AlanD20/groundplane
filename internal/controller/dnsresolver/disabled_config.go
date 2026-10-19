package dnsresolver

import (
	"context"
	"encoding/json"
	"net/http"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *PlatformMutationService) saveDisabledConfig(
	ctx context.Context,
	current keyvalue.Versioned[componentrecord.Record],
	desired core.Component,
	locator idempotency.IdempotencyLocator,
	intent platformComponentProtectedIntent,
) (idempotency.IdempotencyResponse, error) {
	encoded, err := json.Marshal(
		apiTypes.ComponentConfigMutationResult{Resource: publicCoreDNSConfig(*desired.Config.CoreDNS)},
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	response := idempotency.IdempotencyResponse{Status: http.StatusOK, ContentKind: "application/json", Body: encoded}
	marker, err := idempotency.NewCompletedDirectIdempotencyMarker(
		locator,
		intent.durable,
		response,
		service.now().UTC(),
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	result, publishErr := service.tasks.SaveDisabledResolverConfig(ctx, current, desired, marker)
	var resolution requestidempotency.Resolution
	if publishErr != nil {
		if !unknownPlatformComponentMutationOutcome(publishErr) {
			return idempotency.IdempotencyResponse{}, publishErr
		}
		resolution, err = service.coordinator.ResolveUnknown(
			ctx,
			service.idempotency,
			locator,
			intent.protected,
			publishErr,
		)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, intent.protected, result)
	}
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"CoreDNS configuration save has no accepted result",
		)
	}
	return cloneIdempotencyResponse(response), nil
}
