package edgechecks

import (
	"context"
	"net/http"
	"slices"
	"time"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
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
}

func NewService(routes *etcd.RouteRepository, checks *checkrecord.Repository, markers *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator, images workloadseal.Resolver, observations serviceobservation.Channel,
	catalog observationCatalog, actionID componentsdk.ActionID,
) (*Service, error) {
	if routes == nil || checks == nil || markers == nil || coordinator == nil || images == nil || observations == nil ||
		catalog == nil ||
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
		ids.Validate(ids.KindComponent, input.ConnectorID) != nil || input.Revision <= 0 || len(input.Path) > 2048 {
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
			requestidempotency.Field{Name: "path", Value: requestidempotency.String(input.Path)}))}
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
	return service.admit(ctx, environmentID, input, locator, evidence)
}

func (service *Service) admit(ctx context.Context, environmentID string, input apiTypes.EdgeCheckRequest,
	locator idempotency.IdempotencyLocator, evidence requestidempotency.ProtectedEvidence,
) (idempotency.IdempotencyResponse, error) {
	ctx = imagefence.WithScope(ctx)
	source, err := service.routes.LoadEdgeCheckSource(
		ctx,
		environmentID,
		input.RouteID,
		input.Revision,
		input.ConnectorID,
		input.Path,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	return service.admitSource(ctx, environmentID, source, locator, evidence)
}

func (service *Service) admitSource(ctx context.Context, environmentID string, source etcd.EdgeCheckSource,
	locator idempotency.IdempotencyLocator, evidence requestidempotency.ProtectedEvidence,
) (idempotency.IdempotencyResponse, error) {
	checkOwner := checkrecord.Owner{
		TaskID:        ids.New(ids.KindTask),
		WorkItemID:    ids.New(ids.KindWorkItem),
		PlanID:        ids.New(ids.KindPlan),
		OperationID:   ids.New(ids.KindOperation),
		EnvironmentID: environmentID,
	}
	if err := source.BindObservationAction(service.action, service.readyURL, checkOwner.OperationID); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	images, err := service.images.ResolveWorkloadImages(
		ctx,
		source.AgentID(),
		[]*agentpb.WorkloadImageSelector{source.ImageSelector()},
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	observed, err := service.observations.ObserveServices(ctx, source.AgentID(), source.ObservationTargets())
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if err := source.BindHostSelection(images, observed); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	configuration, err := source.Configuration()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	environment, project := source.Scope()
	now := time.Now().UTC()
	check, err := checkrecord.NewRecord(checkOwner, configuration, now)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	hash, err := checkrecord.PlanHash(check)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	targetName := configuration.Request.Target.Hostname
	if len(targetName) > 160 {
		targetName = targetName[:157] + "..."
	}
	task := workexecution.Task{
		ID: checkOwner.TaskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionCheck,
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		},
		TargetID: configuration.Request.TargetID(), TargetName: targetName,
		Intent: workexecution.Input{ID: checkOwner.PlanID, SHA256: hash}, ExpectedItems: 1,
		CreatedAt: now, UpdatedAt: now, Deadline: now.Add(time.Duration(contract.TaskTimeoutSeconds) * time.Second),
	}
	return service.publish(ctx, source, check, task, locator, evidence)
}
