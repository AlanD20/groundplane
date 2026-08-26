package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	serviceCreationRoute           = "/services"
	serviceEditRoute               = "/services/{id}"
	maximumServiceMutationAttempts = 3
)

type serviceMutationRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
	ListZones(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ZoneRecord], error)
	CreateServiceIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.ServiceRecord,
		etcd.ServiceMutationReferences,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	ReplaceDesiredIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.ServiceRecord],
		core.Service,
		etcd.ServiceMutationReferences,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type durableServiceMutationRepository struct {
	hierarchy *etcd.HierarchyRepository
	services  *etcd.ServiceRepository
	zones     *etcd.ZoneRepository
}

func newDurableServiceMutationRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
	zones *etcd.ZoneRepository,
) (*durableServiceMutationRepository, error) {
	if hierarchy == nil || services == nil || zones == nil {
		return nil, errs.New(errs.KindInternal, "Service mutation repositories are not configured")
	}
	return &durableServiceMutationRepository{hierarchy: hierarchy, services: services, zones: zones}, nil
}

func (repository *durableServiceMutationRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableServiceMutationRepository) GetProject(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableServiceMutationRepository) GetService(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (repository *durableServiceMutationRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return repository.services.ListServices(ctx, environmentID, request)
}

func (repository *durableServiceMutationRepository) ListZones(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ZoneRecord], error) {
	return repository.zones.ListZones(ctx, environmentID, request)
}

func (repository *durableServiceMutationRepository) CreateServiceIdempotent(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	record etcd.ServiceRecord,
	references etcd.ServiceMutationReferences,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.services.CreateServiceIdempotent(ctx, environment, project, record, references, marker)
}

func (repository *durableServiceMutationRepository) ReplaceDesiredIdempotent(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	current etcd.Versioned[etcd.ServiceRecord],
	desired core.Service,
	references etcd.ServiceMutationReferences,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.services.ReplaceDesiredIdempotent(ctx, environment, project, current, desired, references, marker)
}

type serviceMutationEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type serviceMutationIntent struct {
	method        string
	route         string
	environmentID string
	serviceID     string
	body          idempotentintent.Value
}

type serviceMutationIdempotency interface {
	Prepare(context.Context, serviceMutationIntent) (serviceMutationEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		serviceMutationEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		serviceMutationEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		serviceMutationEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableServiceMutationIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func (service *durableServiceMutationIdempotency) MatchesStaged(
	ctx context.Context,
	evidence serviceMutationEvidence,
	existing etcd.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func newDurableServiceMutationIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableServiceMutationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Service mutation idempotency is not configured")
	}
	return &durableServiceMutationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableServiceMutationIdempotency) Prepare(
	ctx context.Context,
	intent serviceMutationIntent,
) (serviceMutationEvidence, error) {
	path := []idempotentintent.PathBinding(nil)
	if intent.serviceID != "" {
		path = []idempotentintent.PathBinding{{Name: "id", Value: intent.serviceID}}
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: intent.method, Route: intent.route,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: intent.environmentID},
		Path:  path, Query: idempotentintent.Object(), Body: idempotentintent.JSONBody(intent.body),
	})
	if err != nil {
		return serviceMutationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return serviceMutationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return serviceMutationEvidence{}, err
	}
	return serviceMutationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableServiceMutationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence serviceMutationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableServiceMutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence serviceMutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableServiceMutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence serviceMutationEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type serviceMutationService struct {
	repository  serviceMutationRepository
	idempotency serviceMutationIdempotency
	lifecycle   *serviceLifecycleService
	now         func() time.Time
}

func newServiceMutationService(
	repository serviceMutationRepository,
	idempotency serviceMutationIdempotency,
) (*serviceMutationService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Service mutation service is not configured")
	}
	return &serviceMutationService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *serviceMutationService) CreateService(
	ctx context.Context,
	input apiTypes.ServiceCreate,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service creation context is required")
	}
	input = normalizeServiceCreate(input)
	desired := serviceDesiredFromCreate(input)
	if err := validateDirectServiceDesired(input.EnvironmentID, desired); err != nil {
		return etcd.IdempotencyResponse{}, err
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
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service creation retry bound was not enforced")
}

func (service *serviceMutationService) createServiceOnce(
	ctx context.Context,
	input apiTypes.ServiceCreate,
	desired core.Service,
	idempotencyKey string,
	intent serviceMutationIntent,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := serviceMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return serviceReplayResponse(resolution)
	}
	environment, project, err := service.serviceHierarchy(ctx, input.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if project.Record.Kind != etcd.ProjectKindTenant ||
		environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Environment is not ready for Service creation",
		)
	}
	desired.ID = ids.New(ids.KindService)
	record, err := etcd.NewServiceRecord(input.EnvironmentID, desired, "")
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	references, err := service.resolveServiceReferences(ctx, record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, marker, err := service.serviceResponseMarker(locator, evidence, record, http.StatusCreated)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.CreateServiceIdempotent(
		ctx,
		environment,
		project,
		record,
		references,
		marker,
	)
	return service.resolveServiceMutation(ctx, locator, evidence, result, mutationErr, response)
}

func (service *serviceMutationService) EditService(
	ctx context.Context,
	serviceID string,
	input apiTypes.ServiceEdit,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service edit context is required")
	}
	if ids.Validate(ids.KindService, serviceID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(
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
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service edit retry bound was not enforced")
}

func (service *serviceMutationService) editServiceOnce(
	ctx context.Context,
	serviceID string,
	input apiTypes.ServiceEdit,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.repository.GetService(ctx, serviceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
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
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := serviceMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return serviceReplayResponse(resolution)
	}
	desired := applyServiceEdit(current.Record.Desired, input)
	if err := validateDirectServiceDesired(current.Record.EnvironmentID, desired); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	environment, project, err := service.serviceHierarchy(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	replacement, err := etcd.ReplaceServiceDesired(current.Record, desired)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	references, err := service.resolveServiceReferences(ctx, replacement)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, marker, err := service.serviceResponseMarker(locator, evidence, replacement, http.StatusOK)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.ReplaceDesiredIdempotent(
		ctx,
		environment,
		project,
		current,
		desired,
		references,
		marker,
	)
	return service.resolveServiceMutation(ctx, locator, evidence, result, mutationErr, response)
}

func (service *serviceMutationService) serviceHierarchy(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentRecord], etcd.Versioned[etcd.ProjectRecord], error) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcd.Versioned[etcd.EnvironmentRecord]{}, etcd.Versioned[etcd.ProjectRecord]{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.Versioned[etcd.EnvironmentRecord]{}, etcd.Versioned[etcd.ProjectRecord]{}, err
	}
	return environment, project, nil
}

func (service *serviceMutationService) resolveServiceReferences(
	ctx context.Context,
	record etcd.ServiceRecord,
) (etcd.ServiceMutationReferences, error) {
	zones, err := service.listAllZones(ctx, record.EnvironmentID)
	if err != nil {
		return etcd.ServiceMutationReferences{}, err
	}
	zoneByName := make(map[string]etcd.Versioned[etcd.ZoneRecord], len(zones))
	for _, zone := range zones {
		zoneByName[zone.Record.Desired.Name] = zone
	}
	references := etcd.ServiceMutationReferences{
		Zones: make([]etcd.Versioned[etcd.ZoneRecord], 0, len(record.Desired.Zones)),
	}
	for _, name := range record.Desired.Zones {
		zone, ok := zoneByName[name]
		if !ok {
			return etcd.ServiceMutationReferences{}, errs.Newf(
				errs.KindValidationFailed,
				"Service Zone %q was not found",
				name,
			)
		}
		references.Zones = append(references.Zones, zone)
	}
	services, err := service.listAllServices(ctx, record.EnvironmentID)
	if err != nil {
		return etcd.ServiceMutationReferences{}, err
	}
	serviceByName := make(map[string]etcd.Versioned[etcd.ServiceRecord], len(services))
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
			return etcd.ServiceMutationReferences{}, errs.Newf(
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
) ([]etcd.Versioned[etcd.ZoneRecord], error) {
	items := []etcd.Versioned[etcd.ZoneRecord]{}
	cursor := ""
	for {
		page, err := service.repository.ListZones(ctx, environmentID, etcd.PageRequest{Limit: 200, Cursor: cursor})
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
) ([]etcd.Versioned[etcd.ServiceRecord], error) {
	items := []etcd.Versioned[etcd.ServiceRecord]{}
	cursor := ""
	for {
		page, err := service.repository.ListServices(ctx, environmentID, etcd.PageRequest{Limit: 200, Cursor: cursor})
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
	locator etcd.IdempotencyLocator,
	evidence serviceMutationEvidence,
	record etcd.ServiceRecord,
	status int,
) (etcd.IdempotencyResponse, etcd.IdempotencyMarker, error) {
	body, err := json.Marshal(serviceAPIResponse(record))
	if err != nil {
		return etcd.IdempotencyResponse{}, etcd.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := etcd.IdempotencyResponse{
		Status:      status,
		ContentKind: "application/json",
		Body:        append([]byte(nil), body...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		clear(response.Body)
		return etcd.IdempotencyResponse{}, etcd.IdempotencyMarker{}, err
	}
	return response, marker, nil
}

func (service *serviceMutationService) resolveServiceMutation(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence serviceMutationEvidence,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	response etcd.IdempotencyResponse,
) (etcd.IdempotencyResponse, error) {
	var resolution idempotentintent.Resolution
	var err error
	if mutationErr != nil {
		if !isUnknownServiceMutationOutcome(mutationErr) {
			clear(response.Body)
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		clear(response.Body)
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return response, nil
	case idempotentintent.ResolutionReplay:
		clear(response.Body)
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		clear(response.Body)
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service mutation resolution is invalid")
	}
}

func normalizeServiceCreate(input apiTypes.ServiceCreate) apiTypes.ServiceCreate {
	if input.Strategy == "" {
		input.Strategy = string(core.StrategyRecreate)
	}
	if input.OnFailure == "" {
		input.OnFailure = apiTypes.OnFailureSwitchBack
	}
	if input.Replicas == 0 {
		input.Replicas = 1
	}
	input.Zones = normalizeServiceStringSet(input.Zones)
	return input
}

func normalizeServiceEdit(input apiTypes.ServiceEdit) apiTypes.ServiceEdit {
	input.Zones = normalizeServiceStringSet(input.Zones)
	return input
}

func normalizeServiceStringSet(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	if len(result) < 2 {
		return result
	}
	write := 1
	for read := 1; read < len(result); read++ {
		if result[read] != result[write-1] {
			result[write] = result[read]
			write++
		}
	}
	return result[:write]
}

func serviceDesiredFromCreate(input apiTypes.ServiceCreate) core.Service {
	return core.Service{
		Name: input.Name, Image: input.Image, Zones: append([]string(nil), input.Zones...),
		Strategy: core.Strategy(input.Strategy), OnFailure: core.OnFailure(input.OnFailure),
		Healthcheck: serviceHealthcheckToCore(input.Healthcheck),
		Resources:   core.Resources{Mem: input.Resources.Mem, CPUs: input.Resources.CPUs},
		Expose:      append([]string(nil), input.Expose...), Restart: input.Restart, Replicas: input.Replicas,
	}
}

func applyServiceEdit(current core.Service, input apiTypes.ServiceEdit) core.Service {
	desired := current
	desired.Image = input.Image
	desired.Zones = append([]string(nil), input.Zones...)
	desired.Strategy = core.Strategy(input.Strategy)
	desired.OnFailure = core.OnFailure(input.OnFailure)
	desired.Healthcheck = serviceHealthcheckToCore(input.Healthcheck)
	desired.Resources = core.Resources{Mem: input.Resources.Mem, CPUs: input.Resources.CPUs}
	desired.Expose = append([]string(nil), input.Expose...)
	desired.Restart = input.Restart
	desired.Replicas = input.Replicas
	return desired
}

func serviceHealthcheckToCore(input apiTypes.ServiceHealthcheck) core.Healthcheck {
	return core.Healthcheck{
		HTTP:        input.HTTP,
		TCP:         input.TCP,
		Pgrep:       input.Pgrep,
		Interval:    input.Interval,
		Timeout:     input.Timeout,
		StartPeriod: input.StartPeriod,
		Retries:     input.Retries,
	}
}

func validateDirectServiceDesired(environmentID string, desired core.Service) error {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return errs.New(errs.KindValidationFailed, "Service mutation requires a stable Environment id")
	}
	if desired.ID == "" {
		desired.ID = ids.New(ids.KindService)
	}
	if desired.Replicas < 1 {
		return errs.New(errs.KindValidationFailed, "Service replicas must be positive")
	}
	if err := desired.Validate(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	for zoneName := range desired.Aliases {
		index := sort.SearchStrings(desired.Zones, zoneName)
		if index == len(desired.Zones) || desired.Zones[index] != zoneName {
			return errs.Newf(errs.KindValidationFailed, "Service aliases reference unjoined Zone %q", zoneName)
		}
	}
	return nil
}

func serviceCreateIntentValue(input apiTypes.ServiceCreate) idempotentintent.Value {
	return serviceIntentValue(
		input.EnvironmentID,
		input.Name,
		input.Image,
		input.Zones,
		input.Strategy,
		input.OnFailure,
		input.Healthcheck,
		input.Resources,
		input.Expose,
		input.Restart,
		input.Replicas,
	)
}

func serviceEditIntentValue(input apiTypes.ServiceEdit) idempotentintent.Value {
	return serviceIntentValue(
		"",
		"",
		input.Image,
		input.Zones,
		input.Strategy,
		input.OnFailure,
		input.Healthcheck,
		input.Resources,
		input.Expose,
		input.Restart,
		input.Replicas,
	)
}

func serviceIntentValue(
	environmentID string,
	name string,
	image string,
	zones []string,
	strategy string,
	onFailure apiTypes.OnFailure,
	healthcheck apiTypes.ServiceHealthcheck,
	resources apiTypes.ServiceResources,
	expose []string,
	restart string,
	replicas int,
) idempotentintent.Value {
	fields := []idempotentintent.Field{
		{Name: "expose", Value: serviceStringListIntentValue(expose)},
		{Name: "healthcheck", Value: serviceHealthcheckIntentValue(healthcheck)},
		{Name: "image", Value: idempotentintent.String(image)},
		{Name: "on_failure", Value: idempotentintent.String(string(onFailure))},
		{Name: "replicas", Value: idempotentintent.Integer(int64(replicas))},
		{Name: "resources", Value: idempotentintent.Object(
			idempotentintent.Field{
				Name:  "cpus",
				Value: idempotentintent.String(strconv.FormatFloat(resources.CPUs, 'g', -1, 64)),
			},
			idempotentintent.Field{Name: "mem", Value: idempotentintent.String(resources.Mem)},
		)},
		{Name: "restart", Value: idempotentintent.String(restart)},
		{Name: "strategy", Value: idempotentintent.String(strategy)},
		{Name: "zones", Value: serviceStringListIntentValue(zones)},
	}
	if environmentID != "" {
		fields = append(fields,
			idempotentintent.Field{Name: "environment_id", Value: idempotentintent.String(environmentID)},
			idempotentintent.Field{Name: "name", Value: idempotentintent.String(name)},
		)
	}
	return idempotentintent.Object(fields...)
}

func serviceStringListIntentValue(values []string) idempotentintent.Value {
	items := make([]idempotentintent.Value, len(values))
	for index, value := range values {
		items[index] = idempotentintent.String(value)
	}
	return idempotentintent.List(items...)
}

func serviceHealthcheckIntentValue(value apiTypes.ServiceHealthcheck) idempotentintent.Value {
	return idempotentintent.Object(
		idempotentintent.Field{Name: "http", Value: idempotentintent.String(value.HTTP)},
		idempotentintent.Field{Name: "interval", Value: idempotentintent.String(value.Interval)},
		idempotentintent.Field{Name: "pgrep", Value: idempotentintent.String(value.Pgrep)},
		idempotentintent.Field{Name: "retries", Value: idempotentintent.Integer(int64(value.Retries))},
		idempotentintent.Field{Name: "start_period", Value: idempotentintent.String(value.StartPeriod)},
		idempotentintent.Field{Name: "tcp", Value: idempotentintent.String(value.TCP)},
		idempotentintent.Field{Name: "timeout", Value: idempotentintent.String(value.Timeout)},
	)
}

func serviceMutationLocator(intent serviceMutationIntent, idempotencyKey string) etcd.IdempotencyLocator {
	return etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment,
		ScopeID:   intent.environmentID,
		Method:    intent.method,
		Route:     intent.route,
		Key:       idempotencyKey,
	}
}

func serviceReplayResponse(resolution idempotentintent.Resolution) (etcd.IdempotencyResponse, error) {
	if resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Service replay resolution is invalid")
	}
	return cloneIdempotencyResponse(resolution.Response), nil
}

func serviceAPIResponse(record etcd.ServiceRecord) apiTypes.Service {
	response := apiTypes.Service{
		ID: record.Desired.ID, EnvironmentID: record.EnvironmentID, Name: record.Desired.Name,
		Image: record.Desired.Image, RuntimeIntent: apiTypes.ServiceRuntimeIntent(record.Runtime.RuntimeIntent),
		Zones: append([]string(nil), record.Desired.Zones...), Strategy: string(record.Desired.Strategy),
		OnFailure: apiTypes.OnFailure(record.Desired.OnFailure),
		Resources: apiTypes.ServiceResources{Mem: record.Desired.Resources.Mem, CPUs: record.Desired.Resources.CPUs},
		Expose:    append([]string(nil), record.Desired.Expose...), Restart: record.Desired.Restart,
		Replicas: record.Desired.Replicas, Adapter: record.Desired.Adapter,
		FactsPrefix: record.Desired.FactsPrefix, Label: record.Desired.Label, BackingNetworkID: record.BackingNetworkID,
	}
	if record.Desired.Healthcheck != (core.Healthcheck{}) {
		response.Healthcheck = &apiTypes.ServiceHealthcheck{
			HTTP:        record.Desired.Healthcheck.HTTP,
			TCP:         record.Desired.Healthcheck.TCP,
			Pgrep:       record.Desired.Healthcheck.Pgrep,
			Interval:    record.Desired.Healthcheck.Interval,
			Timeout:     record.Desired.Healthcheck.Timeout,
			StartPeriod: record.Desired.Healthcheck.StartPeriod,
			Retries:     record.Desired.Healthcheck.Retries,
		}
	}
	response.Command = append([]string(nil), record.Desired.Command...)
	response.Mounts = make([]apiTypes.ServiceMount, len(record.Desired.Mounts))
	for index, mount := range record.Desired.Mounts {
		response.Mounts[index] = apiTypes.ServiceMount{
			Volume: mount.Volume,
			File:   mount.File,
			Mount:  mount.Mount,
			RO:     mount.RO,
		}
	}
	response.Aliases = cloneServiceStringSliceMap(record.Desired.Aliases)
	response.DependsOn = make(map[string]apiTypes.ServiceDependency, len(record.Desired.DependsOn))
	for name, dependency := range record.Desired.DependsOn {
		response.DependsOn[name] = apiTypes.ServiceDependency{
			Condition: dependency.Condition,
			Phases:    append([]string(nil), dependency.Phases...),
		}
	}
	if len(response.DependsOn) == 0 {
		response.DependsOn = nil
	}
	response.Logging = apiTypes.ServiceLogging{
		MaxSize: record.Desired.Logging.MaxSize,
		MaxFile: record.Desired.Logging.MaxFile,
	}
	return response
}

func cloneServiceStringSliceMap(source map[string][]string) map[string][]string {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string][]string, len(source))
	for key, values := range source {
		result[key] = append([]string(nil), values...)
	}
	return result
}

func isUnknownServiceMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
