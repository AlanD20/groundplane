package scripts

import (
	"context"
	"encoding/json"
	"errors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/scriptdefinition"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	scriptCreationRoute           = "/scripts"
	scriptEditRoute               = "/scripts/{id}"
	maximumScriptMutationAttempts = 3
)

type scriptMutationRepository interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	GetScript(context.Context, string) (etcdstore.Versioned[scriptrecord.Record], error)
	CreateScriptIdempotent(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[servicerecord.ServiceRecord],
		scriptrecord.Record,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	ReplaceDesiredIdempotent(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[servicerecord.ServiceRecord],
		etcdstore.Versioned[scriptrecord.Record],
		core.Script,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	LoadExecutionSources(context.Context, string) (etcd.ScriptExecutionSources, error)
	PublishExecutionWithTask(
		context.Context,
		etcd.ScriptExecutionSources,
		etcd.ScriptExecutionRecord,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type MutationRepository struct {
	hierarchy *etcd.HierarchyRepository
	services  *etcd.ServiceRepository
	scripts   *etcd.ScriptRepository
	releases  *etcd.ReleaseLedger
}

func NewMutationRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
	scripts *etcd.ScriptRepository,
	releases *etcd.ReleaseLedger,
) (*MutationRepository, error) {
	if hierarchy == nil || services == nil || scripts == nil || releases == nil {
		return nil, errs.New(errs.KindInternal, "Script mutation repositories are not configured")
	}
	return &MutationRepository{
		hierarchy: hierarchy, services: services, scripts: scripts, releases: releases,
	}, nil
}

func (repository *MutationRepository) LoadExecutionSources(
	ctx context.Context,
	scriptID string,
) (etcd.ScriptExecutionSources, error) {
	return repository.scripts.LoadExecutionSources(ctx, repository.releases, scriptID)
}

func (repository *MutationRepository) PublishExecutionWithTask(
	ctx context.Context,
	sources etcd.ScriptExecutionSources,
	execution etcd.ScriptExecutionRecord,
	task etcd.TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.scripts.PublishExecutionWithTask(ctx, sources, execution, task, marker)
}

func (repository *MutationRepository) GetEnvironment(
	ctx context.Context, id string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *MutationRepository) GetProject(
	ctx context.Context, id string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *MutationRepository) GetService(
	ctx context.Context, id string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (repository *MutationRepository) GetScript(
	ctx context.Context, id string,
) (etcdstore.Versioned[scriptrecord.Record], error) {
	return repository.scripts.GetScript(ctx, id)
}

func (repository *MutationRepository) CreateScriptIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	record scriptrecord.Record,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.scripts.CreateScriptIdempotent(ctx, environment, project, target, record, marker)
}

func (repository *MutationRepository) ReplaceDesiredIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	current etcdstore.Versioned[scriptrecord.Record],
	desired core.Script,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.scripts.ReplaceDesiredIdempotent(ctx, environment, project, target, current, desired, marker)
}

type scriptMutationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type scriptMutationIntent struct {
	method        string
	route         string
	environmentID string
	path          []requestidempotency.PathBinding
	body          requestidempotency.Value
	noBody        bool
}

type scriptMutationIdempotency interface {
	Prepare(context.Context, scriptMutationIntent) (scriptMutationEvidence, error)
	ResolveExisting(
		context.Context, idempotencyrecord.IdempotencyLocator, scriptMutationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context, scriptMutationEvidence, etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context, idempotencyrecord.IdempotencyLocator, scriptMutationEvidence, error,
	) (requestidempotency.Resolution, error)
}

type durableScriptMutationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewMutationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableScriptMutationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Script mutation idempotency is not configured")
	}
	return &durableScriptMutationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableScriptMutationIdempotency) Prepare(
	ctx context.Context,
	intent scriptMutationIntent,
) (scriptMutationEvidence, error) {
	body := requestidempotency.JSONBody(intent.body)
	if intent.noBody {
		body = requestidempotency.NoBody()
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: intent.method,
		Route:  intent.route,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: intent.environmentID},
		Path:   intent.path, Query: requestidempotency.Object(), Body: body,
	})
	if err != nil {
		return scriptMutationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return scriptMutationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return scriptMutationEvidence{}, err
	}
	return scriptMutationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableScriptMutationIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence scriptMutationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableScriptMutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence scriptMutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableScriptMutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence scriptMutationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type scriptMutationService struct {
	repository  scriptMutationRepository
	idempotency scriptMutationIdempotency
	deletions   *scriptDeletionService
	now         func() time.Time
	preparation *taskplanning.ScriptRunnerPreparationService
}

func NewMutationService(
	repository scriptMutationRepository,
	idempotency scriptMutationIdempotency,
	preparation *taskplanning.ScriptRunnerPreparationService,
	deletions *scriptDeletionService,
) (*scriptMutationService, error) {
	if repository == nil || idempotency == nil || preparation == nil {
		return nil, errs.New(errs.KindInternal, "Script mutation service is not configured")
	}
	return &scriptMutationService{
		repository: repository, idempotency: idempotency, preparation: preparation, deletions: deletions, now: time.Now,
	}, nil
}

func (service *scriptMutationService) RemoveScript(
	ctx context.Context,
	scriptID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if service == nil || service.deletions == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script deletion service is not configured")
	}
	return service.deletions.RemoveScript(ctx, scriptID, idempotencyKey)
}

func (service *scriptMutationService) CreateScript(
	ctx context.Context,
	input apiTypes.ScriptCreate,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script creation context is required")
	}
	if err := scriptdefinition.ValidateCreation(input); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := scriptMutationIntent{
		method: http.MethodPost, route: scriptCreationRoute, environmentID: input.EnvironmentID,
		body: scriptdefinition.CreateIntentBody(input),
	}
	for attempt := 0; attempt < maximumScriptMutationAttempts; attempt++ {
		response, err := service.createScriptOnce(ctx, input, idempotencyKey, intent)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumScriptMutationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script creation retry bound was not enforced")
}

func (service *scriptMutationService) createScriptOnce(
	ctx context.Context,
	input apiTypes.ScriptCreate,
	idempotencyKey string,
	intent scriptMutationIntent,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := scriptMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return scriptReplayResponse(resolution)
	}
	environment, project, target, err := service.scriptHierarchy(ctx, input.EnvironmentID, input.ServiceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	record, err := scriptrecord.NewRecord(
		input.EnvironmentID,
		input.ServiceID,
		scriptdefinition.CreateDesired(input, ids.New(ids.KindScript), target.Record.Desired.Name),
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	response, marker, err := service.scriptResponseMarker(locator, evidence, record, http.StatusCreated)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.CreateScriptIdempotent(
		ctx, environment, project, target, record, marker,
	)
	return service.resolveScriptMutation(ctx, locator, evidence, result, mutationErr, response)
}

func (service *scriptMutationService) EditScript(
	ctx context.Context,
	scriptID string,
	input apiTypes.ScriptEdit,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script edit context is required")
	}
	if ids.Validate(ids.KindScript, scriptID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Script edit requires a stable Script id",
		)
	}
	if err := scriptdefinition.ValidateEdit(input); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumScriptMutationAttempts; attempt++ {
		response, err := service.editScriptOnce(ctx, scriptID, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumScriptMutationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script edit retry bound was not enforced")
}

func (service *scriptMutationService) editScriptOnce(
	ctx context.Context,
	scriptID string,
	input apiTypes.ScriptEdit,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	current, err := service.repository.GetScript(ctx, scriptID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := scriptEditMutationIntent(scriptID, current.Record.EnvironmentID, input)
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := scriptMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		return scriptReplayResponse(resolution)
	}
	environment, project, target, err := service.scriptHierarchy(
		ctx, current.Record.EnvironmentID, current.Record.ServiceID,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	desired := scriptdefinition.EditDesired(current.Record.Desired, target.Record.Desired.Name, input)
	replacement, err := scriptrecord.ReplaceDesired(current.Record, desired)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	response, marker, err := service.scriptResponseMarker(locator, evidence, replacement, http.StatusOK)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.ReplaceDesiredIdempotent(
		ctx, environment, project, target, current, desired, marker,
	)
	return service.resolveScriptMutation(ctx, locator, evidence, result, mutationErr, response)
}

func scriptEditMutationIntent(
	scriptID string,
	environmentID string,
	input apiTypes.ScriptEdit,
) scriptMutationIntent {
	return scriptMutationIntent{
		method: http.MethodPatch, route: scriptEditRoute, environmentID: environmentID,
		path: []requestidempotency.PathBinding{{Name: "id", Value: scriptID}},
		body: scriptdefinition.EditIntentBody(input),
	}
}

func (service *scriptMutationService) scriptHierarchy(
	ctx context.Context,
	environmentID string,
	targetServiceID string,
) (
	etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	etcdstore.Versioned[servicerecord.ServiceRecord],
	error,
) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	target, err := service.repository.GetService(ctx, targetServiceID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{},
			etcdstore.Versioned[servicerecord.ServiceRecord]{}, err
	}
	return environment, project, target, nil
}

func (service *scriptMutationService) scriptResponseMarker(
	locator idempotencyrecord.IdempotencyLocator,
	evidence scriptMutationEvidence,
	record scriptrecord.Record,
	status int,
) (idempotencyrecord.IdempotencyResponse, idempotencyrecord.IdempotencyMarker, error) {
	body, err := json.Marshal(scriptdefinition.Response(record))
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status: status, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	marker, err := idempotencyrecord.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, err
	}
	return response, marker, nil
}

func (service *scriptMutationService) resolveScriptMutation(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence scriptMutationEvidence,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	response idempotencyrecord.IdempotencyResponse,
) (idempotencyrecord.IdempotencyResponse, error) {
	var resolution requestidempotency.Resolution
	var err error
	if mutationErr != nil {
		if !isUnknownScriptMutationOutcome(mutationErr) {
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
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script mutation resolution is invalid")
	}
}

func scriptMutationLocator(intent scriptMutationIntent, idempotencyKey string) idempotencyrecord.IdempotencyLocator {
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: intent.environmentID,
		Method: intent.method, Route: intent.route, Key: idempotencyKey,
	}
}

func scriptReplayResponse(resolution requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script replay resolution is invalid")
	}
	return requestidempotency.CloneResponse(resolution.Response), nil
}

func isUnknownScriptMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
