package edgechecks

import (
	"context"
	"net/http"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) CheckTunnelConnections(
	ctx context.Context,
	environmentID, connectorID string,
	input apiTypes.TunnelCheckRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	target := contract.PublicTarget{Hostname: input.Hostname, Path: input.Path}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindComponent, connectorID) != nil ||
		ids.Validate(
			ids.KindService,
			input.OriginServiceID,
		) != nil || input.Revision <= 0 || input.OriginPort == 0 || target.Validate() != nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Tunnel check requires its current revision, an origin Service HTTP port and a concrete public hostname/path",
		)
	}
	const route = "/api/v1/components/{id}/tunnel-checks"
	intent := requestidempotency.CanonicalIntentV1{Method: http.MethodPost, Route: route,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: connectorID}},
		Query: requestidempotency.Object(
			requestidempotency.Field{Name: "environment", Value: requestidempotency.String(environmentID)},
		),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "revision", Value: requestidempotency.Integer(input.Revision)},
			requestidempotency.Field{
				Name:  "origin_service_id",
				Value: requestidempotency.String(input.OriginServiceID),
			},
			requestidempotency.Field{Name: "origin_port", Value: requestidempotency.Integer(int64(input.OriginPort))},
			requestidempotency.Field{Name: "hostname", Value: requestidempotency.String(input.Hostname)},
			requestidempotency.Field{Name: "path", Value: requestidempotency.String(input.Path)},
		))}
	version, digest, err := requestidempotency.Canonicalize(ctx, intent)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID:   environmentID,
		Method:    http.MethodPost,
		Route:     route,
		Key:       key,
	}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	resolution, existing, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, evidence)
	if err != nil || existing {
		return resolution.Response, err
	}
	ctx = imagefence.WithScope(ctx)
	source, err := service.routes.LoadTunnelCheckSource(
		ctx,
		environmentID,
		connectorID,
		input.Revision,
		input.OriginServiceID,
		input.OriginPort,
		target,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	return service.admitSource(ctx, environmentID, source, locator, evidence)
}

func (service *Service) GetTunnelConnectionCheck(
	ctx context.Context,
	environmentID, connectorID, taskID string,
) (apiTypes.EdgeCheckResult, error) {
	var result apiTypes.EdgeCheckResult
	var err error
	if taskID == "" {
		result, err = service.GetLatestConnectionCheck(ctx, environmentID, connectorID)
	} else {
		result, err = service.GetConnectionCheck(ctx, environmentID, taskID)
	}
	if err != nil {
		return apiTypes.EdgeCheckResult{}, err
	}
	if result.Configuration.ConnectorID != connectorID || result.Configuration.RouteID != "" {
		return apiTypes.EdgeCheckResult{}, errs.New(
			errs.KindRequestNotFound,
			"standalone Tunnel check was not found for this Component",
		)
	}
	return result, nil
}
