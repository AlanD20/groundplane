package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/scriptdefinition"
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
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	GetScript(context.Context, string) (etcd.Versioned[etcd.ScriptRecord], error)
	CreateScriptIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.ServiceRecord],
		etcd.ScriptRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	ReplaceDesiredIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.ServiceRecord],
		etcd.Versioned[etcd.ScriptRecord],
		core.Script,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	LoadExecutionSources(context.Context, string) (etcd.ScriptExecutionSources, error)
	PublishExecutionWithTask(
		context.Context,
		etcd.ScriptExecutionSources,
		etcd.ScriptExecutionRecord,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type durableScriptMutationRepository struct {
	hierarchy *etcd.HierarchyRepository
	services  *etcd.ServiceRepository
	scripts   *etcd.ScriptRepository
	releases  *etcd.ReleaseLedger
}

func newDurableScriptMutationRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
	scripts *etcd.ScriptRepository,
	releases *etcd.ReleaseLedger,
) (*durableScriptMutationRepository, error) {
	if hierarchy == nil || services == nil || scripts == nil || releases == nil {
		return nil, errs.New(errs.KindInternal, "Script mutation repositories are not configured")
	}
	return &durableScriptMutationRepository{
		hierarchy: hierarchy, services: services, scripts: scripts, releases: releases,
	}, nil
}

func (repository *durableScriptMutationRepository) LoadExecutionSources(
	ctx context.Context,
	scriptID string,
) (etcd.ScriptExecutionSources, error) {
	return repository.scripts.LoadExecutionSources(ctx, repository.releases, scriptID)
}

func (repository *durableScriptMutationRepository) PublishExecutionWithTask(
	ctx context.Context,
	sources etcd.ScriptExecutionSources,
	execution etcd.ScriptExecutionRecord,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.scripts.PublishExecutionWithTask(ctx, sources, execution, task, marker)
}

func (repository *durableScriptMutationRepository) GetEnvironment(
	ctx context.Context, id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableScriptMutationRepository) GetProject(
	ctx context.Context, id string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableScriptMutationRepository) GetService(
	ctx context.Context, id string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (repository *durableScriptMutationRepository) GetScript(
	ctx context.Context, id string,
) (etcd.Versioned[etcd.ScriptRecord], error) {
	return repository.scripts.GetScript(ctx, id)
}

func (repository *durableScriptMutationRepository) CreateScriptIdempotent(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	target etcd.Versioned[etcd.ServiceRecord],
	record etcd.ScriptRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.scripts.CreateScriptIdempotent(ctx, environment, project, target, record, marker)
}

func (repository *durableScriptMutationRepository) ReplaceDesiredIdempotent(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	target etcd.Versioned[etcd.ServiceRecord],
	current etcd.Versioned[etcd.ScriptRecord],
	desired core.Script,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.scripts.ReplaceDesiredIdempotent(ctx, environment, project, target, current, desired, marker)
}

type scriptMutationEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type scriptMutationIntent struct {
	method        string
	route         string
	environmentID string
	path          []idempotentintent.PathBinding
	body          idempotentintent.Value
	noBody        bool
}

type scriptMutationIdempotency interface {
	Prepare(context.Context, scriptMutationIntent) (scriptMutationEvidence, error)
	ResolveExisting(
		context.Context, etcd.IdempotencyLocator, scriptMutationEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context, scriptMutationEvidence, etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context, etcd.IdempotencyLocator, scriptMutationEvidence, error,
	) (idempotentintent.Resolution, error)
}

type durableScriptMutationIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableScriptMutationIdempotency(
	coordinator *idempotentintent.Coordinator,
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
	body := idempotentintent.JSONBody(intent.body)
	if intent.noBody {
		body = idempotentintent.NoBody()
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: intent.method,
		Route:  intent.route,
		Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: intent.environmentID},
		Path:   intent.path, Query: idempotentintent.Object(), Body: body,
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
	locator etcd.IdempotencyLocator,
	evidence scriptMutationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableScriptMutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence scriptMutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableScriptMutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence scriptMutationEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type scriptMutationService struct {
	repository  scriptMutationRepository
	idempotency scriptMutationIdempotency
	deletions   *scriptDeletionService
	now         func() time.Time
	preparation *controller.ScriptRunnerPreparationService
}

func newScriptMutationService(
	repository scriptMutationRepository,
	idempotency scriptMutationIdempotency,
	preparation *controller.ScriptRunnerPreparationService,
) (*scriptMutationService, error) {
	if repository == nil || idempotency == nil || preparation == nil {
		return nil, errs.New(errs.KindInternal, "Script mutation service is not configured")
	}
	return &scriptMutationService{
		repository: repository, idempotency: idempotency, preparation: preparation, now: time.Now,
	}, nil
}

func (service *scriptMutationService) RemoveScript(
	ctx context.Context,
	scriptID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if service == nil || service.deletions == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script deletion service is not configured")
	}
	return service.deletions.RemoveScript(ctx, scriptID, idempotencyKey)
}

func (service *scriptMutationService) CreateScript(
	ctx context.Context,
	input apiTypes.ScriptCreate,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script creation context is required")
	}
	if err := scriptdefinition.ValidateCreation(input); err != nil {
		return etcd.IdempotencyResponse{}, err
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
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script creation retry bound was not enforced")
}

func (service *scriptMutationService) createScriptOnce(
	ctx context.Context,
	input apiTypes.ScriptCreate,
	idempotencyKey string,
	intent scriptMutationIntent,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := scriptMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return scriptReplayResponse(resolution)
	}
	environment, project, target, err := service.scriptHierarchy(ctx, input.EnvironmentID, input.ServiceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	record, err := etcd.NewScriptRecord(
		input.EnvironmentID,
		input.ServiceID,
		scriptdefinition.CreateDesired(input, ids.New(ids.KindScript), target.Record.Desired.Name),
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, marker, err := service.scriptResponseMarker(locator, evidence, record, http.StatusCreated)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
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
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script edit context is required")
	}
	if ids.Validate(ids.KindScript, scriptID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Script edit requires a stable Script id",
		)
	}
	if err := scriptdefinition.ValidateEdit(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumScriptMutationAttempts; attempt++ {
		response, err := service.editScriptOnce(ctx, scriptID, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumScriptMutationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script edit retry bound was not enforced")
}

func (service *scriptMutationService) editScriptOnce(
	ctx context.Context,
	scriptID string,
	input apiTypes.ScriptEdit,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.repository.GetScript(ctx, scriptID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intent := scriptEditMutationIntent(scriptID, current.Record.EnvironmentID, input)
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := scriptMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return scriptReplayResponse(resolution)
	}
	environment, project, target, err := service.scriptHierarchy(
		ctx, current.Record.EnvironmentID, current.Record.ServiceID,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	desired := scriptdefinition.EditDesired(current.Record.Desired, target.Record.Desired.Name, input)
	replacement, err := etcd.ReplaceScriptDesired(current.Record, desired)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, marker, err := service.scriptResponseMarker(locator, evidence, replacement, http.StatusOK)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
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
		path: []idempotentintent.PathBinding{{Name: "id", Value: scriptID}},
		body: scriptdefinition.EditIntentBody(input),
	}
}

func (service *scriptMutationService) scriptHierarchy(
	ctx context.Context,
	environmentID string,
	targetServiceID string,
) (
	etcd.Versioned[etcd.EnvironmentRecord],
	etcd.Versioned[etcd.ProjectRecord],
	etcd.Versioned[etcd.ServiceRecord],
	error,
) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcd.Versioned[etcd.EnvironmentRecord]{}, etcd.Versioned[etcd.ProjectRecord]{},
			etcd.Versioned[etcd.ServiceRecord]{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.Versioned[etcd.EnvironmentRecord]{}, etcd.Versioned[etcd.ProjectRecord]{},
			etcd.Versioned[etcd.ServiceRecord]{}, err
	}
	target, err := service.repository.GetService(ctx, targetServiceID)
	if err != nil {
		return etcd.Versioned[etcd.EnvironmentRecord]{}, etcd.Versioned[etcd.ProjectRecord]{},
			etcd.Versioned[etcd.ServiceRecord]{}, err
	}
	return environment, project, target, nil
}

func (service *scriptMutationService) scriptResponseMarker(
	locator etcd.IdempotencyLocator,
	evidence scriptMutationEvidence,
	record etcd.ScriptRecord,
	status int,
) (etcd.IdempotencyResponse, etcd.IdempotencyMarker, error) {
	body, err := json.Marshal(scriptdefinition.Response(record))
	if err != nil {
		return etcd.IdempotencyResponse{}, etcd.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := etcd.IdempotencyResponse{
		Status: status, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		clear(response.Body)
		return etcd.IdempotencyResponse{}, etcd.IdempotencyMarker{}, err
	}
	return response, marker, nil
}

func (service *scriptMutationService) resolveScriptMutation(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence scriptMutationEvidence,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	response etcd.IdempotencyResponse,
) (etcd.IdempotencyResponse, error) {
	var resolution idempotentintent.Resolution
	var err error
	if mutationErr != nil {
		if !isUnknownScriptMutationOutcome(mutationErr) {
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
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script mutation resolution is invalid")
	}
}

func scriptMutationLocator(intent scriptMutationIntent, idempotencyKey string) etcd.IdempotencyLocator {
	return etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: intent.environmentID,
		Method: intent.method, Route: intent.route, Key: idempotencyKey,
	}
}

func scriptReplayResponse(resolution idempotentintent.Resolution) (etcd.IdempotencyResponse, error) {
	if resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Script replay resolution is invalid")
	}
	return cloneIdempotencyResponse(resolution.Response), nil
}

func isUnknownScriptMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
