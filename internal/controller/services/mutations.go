package services

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
	"sort"
	"time"
)

const (
	serviceCreationRoute           = "/services"
	serviceEditRoute               = "/services/{id}"
	maximumServiceMutationAttempts = 3
)

type serviceMutationService struct {
	repository  serviceMutationRepository
	idempotency serviceMutationIdempotency
	lifecycle   *serviceLifecycleService
	now         func() time.Time
}

func NewMutationService(
	repository serviceMutationRepository,
	idempotency serviceMutationIdempotency,
	lifecycle *serviceLifecycleService,
) (*serviceMutationService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Service mutation service is not configured")
	}
	return &serviceMutationService{
		repository:  repository,
		idempotency: idempotency,
		lifecycle:   lifecycle,
		now:         time.Now,
	}, nil
}

func (service *serviceMutationService) CreateService(
	ctx context.Context,
	input apiTypes.ServiceCreate,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Service creation context is required",
		)
	}
	input = normalizeServiceCreate(input)
	desired := serviceDesiredFromCreate(input)
	if err := validateDirectServiceDesired(input.EnvironmentID, desired); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := serviceMutationIntent{
		method:        http.MethodPost,
		route:         serviceCreationRoute,
		environmentID: input.EnvironmentID,
		body:          serviceCreateIntentValue(input),
	}
	for attempt := 0; attempt < maximumServiceMutationAttempts; attempt++ {
		response, err := service.createServiceOnce(ctx, input, desired, idempotencyKey, intent)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumServiceMutationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Service creation retry bound was not enforced",
	)
}

func (service *serviceMutationService) createServiceOnce(
	ctx context.Context,
	input apiTypes.ServiceCreate,
	desired core.Service,
	idempotencyKey string,
	intent serviceMutationIntent,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := serviceMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return serviceReplayResponse(resolution)
	}
	environment, project, err := service.serviceHierarchy(ctx, input.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Service creation",
		)
	}
	return service.publishServiceDesiredMutation(
		ctx, environment, project, nil,
		servicerecord.ServiceRecord{EnvironmentID: input.EnvironmentID, Desired: desired},
		servicerecord.ServiceMutationReferences{},
		serviceMutationAuditFromCreate(input), blueprints.EnvironmentServiceMutationCreate,
		http.StatusCreated, locator, evidence,
	)
}

func (service *serviceMutationService) EditService(
	ctx context.Context,
	serviceID string,
	input apiTypes.ServiceEdit,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service edit context is required")
	}
	if ids.Validate(ids.KindService, serviceID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Service edit requires a stable Service id",
		)
	}
	input = normalizeServiceEdit(input)
	for attempt := 0; attempt < maximumServiceMutationAttempts; attempt++ {
		response, err := service.editServiceOnce(ctx, serviceID, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumServiceMutationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Service edit retry bound was not enforced",
	)
}

func (service *serviceMutationService) editServiceOnce(
	ctx context.Context,
	serviceID string,
	input apiTypes.ServiceEdit,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	current, err := service.repository.GetService(ctx, serviceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if err := service.rejectComponentGeneratedServiceMutation(ctx, current.Record); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := serviceMutationIntent{
		method:        http.MethodPatch,
		route:         serviceEditRoute,
		environmentID: current.Record.EnvironmentID,
		serviceID:     serviceID,
		body:          serviceEditIntentValue(input),
	}
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := serviceMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return serviceReplayResponse(resolution)
	}
	desired := applyServiceEdit(current.Record.Desired, input)
	if err := validateDirectServiceDesired(current.Record.EnvironmentID, desired); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	environment, project, err := service.serviceHierarchy(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	replacement, err := servicerecord.ReplaceServiceDesired(current.Record, desired)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	references, err := service.resolveServiceReferences(ctx, replacement)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.publishServiceDesiredMutation(
		ctx, environment, project, &current, replacement, references,
		serviceMutationAuditFromEdit(input), blueprints.EnvironmentServiceMutationEdit,
		http.StatusOK, locator, evidence,
	)
}

func (service *serviceMutationService) rejectComponentGeneratedServiceMutation(
	ctx context.Context,
	record servicerecord.ServiceRecord,
) error {
	projection, found, err := service.repository.GetEnvironmentComposeProjection(ctx, record.EnvironmentID)
	if err != nil {
		return err
	}
	if !found {
		return errs.New(errs.KindStateConflict, "Service desired projection is missing")
	}
	return rejectComponentGeneratedServiceTarget(projection.Record, record.Desired.ID)
}

func (service *serviceMutationService) serviceHierarchy(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	return environment, project, nil
}

func (service *serviceMutationService) resolveServiceReferences(
	ctx context.Context,
	record servicerecord.ServiceRecord,
) (servicerecord.ServiceMutationReferences, error) {
	zones, err := service.listAllZones(ctx, record.EnvironmentID)
	if err != nil {
		return servicerecord.ServiceMutationReferences{}, err
	}
	zoneByName := make(map[string]etcdstore.Versioned[zonerecord.Record], len(zones))
	for _, zone := range zones {
		zoneByName[zone.Record.Desired.Name] = zone
	}
	references := servicerecord.ServiceMutationReferences{
		Zones: make([]etcdstore.Versioned[zonerecord.Record], 0, len(record.Desired.Zones)),
	}
	for _, name := range record.Desired.Zones {
		zone, ok := zoneByName[name]
		if !ok {
			return servicerecord.ServiceMutationReferences{}, errs.Newf(
				errs.KindValidationFailed,
				"Service Zone %q was not found",
				name,
			)
		}
		references.Zones = append(references.Zones, zone)
	}
	services, err := service.listAllServices(ctx, record.EnvironmentID)
	if err != nil {
		return servicerecord.ServiceMutationReferences{}, err
	}
	serviceByName := make(map[string]etcdstore.Versioned[servicerecord.ServiceRecord], len(services))
	for _, candidate := range services {
		serviceByName[candidate.Record.Desired.Name] = candidate
	}
	dependencyNames := make([]string, 0, len(record.Desired.DependsOn))
	for name := range record.Desired.DependsOn {
		dependencyNames = append(dependencyNames, name)
	}
	sort.Strings(dependencyNames)
	for _, name := range dependencyNames {
		dependency, ok := serviceByName[name]
		if !ok || dependency.Record.Desired.ID == record.Desired.ID {
			return servicerecord.ServiceMutationReferences{}, errs.Newf(
				errs.KindValidationFailed,
				"Service dependency %q was not found",
				name,
			)
		}
		references.Dependencies = append(references.Dependencies, dependency)
	}
	return references, nil
}

func (service *serviceMutationService) listAllZones(
	ctx context.Context,
	environmentID string,
) ([]etcdstore.Versioned[zonerecord.Record], error) {
	items := []etcdstore.Versioned[zonerecord.Record]{}
	cursor := ""
	for {
		page, err := service.repository.ListZones(ctx, environmentID, etcdstore.PageRequest{Limit: 200, Cursor: cursor})
		if err != nil {
			return nil, err
		}
		items = append(items, page.Items...)
		if page.NextCursor == "" {
			return items, nil
		}
		cursor = page.NextCursor
	}
}

func (service *serviceMutationService) listAllServices(
	ctx context.Context,
	environmentID string,
) ([]etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	items := []etcdstore.Versioned[servicerecord.ServiceRecord]{}
	cursor := ""
	for {
		page, err := service.repository.ListServices(
			ctx,
			environmentID,
			etcdstore.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		items = append(items, page.Items...)
		if page.NextCursor == "" {
			return items, nil
		}
		cursor = page.NextCursor
	}
}

func (service *serviceMutationService) serviceResponseMarker(
	locator idempotencyrecord.IdempotencyLocator,
	intent idempotencyrecord.ProtectedIntentRecord,
	record servicerecord.ServiceRecord,
	status int,
	createdAt time.Time,
) (idempotencyrecord.IdempotencyResponse, idempotencyrecord.IdempotencyMarker, error) {
	body, err := json.Marshal(serviceAPIResponse(record))
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, errs.Wrap(
			errs.KindInternal,
			err,
		)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status:      status,
		ContentKind: "application/json",
		Body:        append([]byte(nil), body...),
	}
	marker, err := idempotencyrecord.NewCompletedDirectIdempotencyMarker(locator, intent, response, createdAt)
	if err != nil {
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, err
	}
	return response, marker, nil
}

func (service *serviceMutationService) resolveServiceMutation(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence serviceMutationEvidence,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	response idempotencyrecord.IdempotencyResponse,
) (idempotencyrecord.IdempotencyResponse, error) {
	var resolution requestidempotency.Resolution
	var err error
	if mutationErr != nil {
		if !isUnknownServiceMutationOutcome(mutationErr) {
			clear(response.Body)
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return response, nil
	case requestidempotency.ResolutionReplay:
		clear(response.Body)
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Service mutation resolution is invalid",
		)
	}
}

func serviceMutationLocator(intent serviceMutationIntent, idempotencyKey string) idempotencyrecord.IdempotencyLocator {
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
		ScopeID:   intent.environmentID,
		Method:    intent.method,
		Route:     intent.route,
		Key:       idempotencyKey,
	}
}

func serviceReplayResponse(resolution requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Service replay resolution is invalid",
		)
	}
	return requestidempotency.CloneResponse(resolution.Response), nil
}

func isUnknownServiceMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
