package edgechecks

import (
	"context"
	"net/http"
	"slices"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const checkRoute = "/api/v1/routes/checks"

type observationCatalog interface {
	Digest() [32]byte
	FindActionByCapability(
		componentsdk.Capability,
		componentsdk.ActionID,
	) (componentsdk.Definition, componentsdk.ActionDefinition, bool)
	ResolveEdgeTunnelObservation(
		componentsdk.ImplementationKey,
		componentsdk.ActionID,
	) (registeredcatalog.EdgeTunnelObservationRecipe, bool)
}

type Service struct {
	routes       *etcd.RouteRepository
	checks       *checkrecord.Repository
	idempotency  *etcd.IdempotencyRepository
	coordinator  *requestidempotency.Coordinator
	images       workloadseal.Resolver
	observations serviceobservation.Channel
	action       *agentpb.ComponentApply
	readyURL     string
	submitter    *acceptedaction.Submitter
}

func NewService(routes *etcd.RouteRepository, checks *checkrecord.Repository, markers *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator, images workloadseal.Resolver, observations serviceobservation.Channel,
	catalog observationCatalog, actionID componentsdk.ActionID,
	submitter *acceptedaction.Submitter,
) (*Service, error) {
	if routes == nil || checks == nil || markers == nil || coordinator == nil || images == nil || observations == nil ||
		catalog == nil || submitter == nil ||
		actionID == "" {
		return nil, errs.New(errs.KindInternal, "Tunnel check admission dependencies are required")
	}
	definition, action, found := catalog.FindActionByCapability(componentsdk.CapabilityEdgeTunnel, actionID)
	if !found || action.Operation() != componentsdk.OperationObserve {
		return nil, errs.New(errs.KindInternal, "Tunnel observation action is unavailable in the compiled catalog")
	}
	recipe, found := catalog.ResolveEdgeTunnelObservation(definition.Implementation(), action.ID())
	if !found {
		return nil, errs.New(errs.KindInternal, "Tunnel observation recipe is unavailable in the compiled catalog")
	}
	definitionDigest, catalogDigest := definition.Digest(), catalog.Digest()
	return &Service{
		submitter:    submitter,
		routes:       routes,
		checks:       checks,
		idempotency:  markers,
		coordinator:  coordinator,
		images:       images,
		observations: observations,
		action: &agentpb.ComponentApply{
			DefinitionDigest: slices.Clone(definitionDigest[:]),
			CatalogDigest:    slices.Clone(catalogDigest[:]),
			ActionId:         string(action.ID()),
		},
		readyURL: recipe.ReadyURL(),
	}, nil
}

func (service *Service) CheckConnections(ctx context.Context, environmentID string, input apiTypes.EdgeCheckRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindRoute, input.RouteID) != nil ||
		ids.Validate(ids.KindComponent, input.ConnectorID) != nil || input.Revision < 0 || len(input.Path) > 2048 {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Tunnel check requires an Environment, Route revision and connector",
		)
	}
	intent := requestidempotency.CanonicalIntentV1{Method: http.MethodPost, Route: checkRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Query: requestidempotency.Object(
			requestidempotency.Field{Name: "environment", Value: requestidempotency.String(environmentID)},
		),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "route_id", Value: requestidempotency.String(input.RouteID)},
			requestidempotency.Field{Name: "revision", Value: requestidempotency.Integer(input.Revision)},
			requestidempotency.Field{Name: "connector_id", Value: requestidempotency.String(input.ConnectorID)},
			requestidempotency.Field{Name: "path", Value: requestidempotency.String(input.Path)},
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)}))}
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
		Route:     checkRoute,
		Key:       key,
	}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	resolution, existing, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, evidence)
	if err != nil || existing {
		return resolution.Response, err
	}
	return acceptCheck(
		ctx,
		service,
		environmentID,
		input.RouteID,
		"Public route",
		"route-check",
		input.After,
		input,
		locator,
		evidence,
	)
}
