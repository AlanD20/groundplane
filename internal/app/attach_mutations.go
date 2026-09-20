package app

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingendpoint"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	attachCreationRoute        = "/attaches"
	attachDeletionRoute        = "/attaches/{id}"
	attachRenameRoute          = "/attaches/{id}/rename"
	attachMutationTimeout      = int64(120)
	maximumAttachMutationTries = 3
)

type attachMutationRepository interface {
	GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	GetEnvironmentBlueprintHead(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(
		context.Context,
		string,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	GetAttach(context.Context, string) (etcd.Versioned[etcd.AttachRecord], error)
	GetAttachTaskRenderInput(context.Context, string) (etcd.Versioned[etcd.AttachTaskRenderInput], error)
	ListAttaches(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.AttachRecord], error)
	RenameAttachIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.AttachRecord],
		string,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	CreateAttachWithTaskHookInputs(
		context.Context,
		etcd.AttachCreateScope,
		etcd.AttachRecord,
		*etcd.AttachEncryptedFacts,
		*etcd.BackingHookEncryptedInputs,
		etcd.AttachTaskRenderInput,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	BeginAttachDetachWithTaskHookInputs(
		context.Context,
		etcd.AttachCreateScope,
		etcd.Versioned[etcd.AttachRecord],
		*etcd.BackingHookEncryptedInputs,
		etcd.AttachTaskRenderInput,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	BeginAttachDetachWithTaskInitiationHookInputs(
		context.Context,
		etcd.AttachCreateScope,
		etcd.Versioned[etcd.AttachRecord],
		*etcd.BackingHookEncryptedInputs,
		etcd.AttachTaskRenderInput,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
		etcd.TaskInitiation,
	) (etcd.IdempotencyTransactionResult, error)
}

type attachMutationFacts interface {
	SealCustomHookBundle(
		context.Context,
		string,
		string,
		string,
		backinghook.Configuration,
	) ([]etcd.AttachFactSetMetadata, *etcd.AttachEncryptedFacts, *etcd.BackingHookEncryptedInputs, error)
	SealBackingHookTaskInputs(
		context.Context,
		string,
		string,
		backinghook.Configuration,
	) (*etcd.BackingHookEncryptedInputs, error)
	SealFactSets(
		context.Context,
		string,
		adapters.Adapter,
		adapters.Input,
		[]AttachGrantInput,
	) ([]etcd.AttachFactSetMetadata, *etcd.AttachEncryptedFacts, error)
	ResolveReadyDatabase(
		context.Context,
		etcd.Versioned[etcd.AttachRecord],
		func(string) error,
	) error
	ResolveTaskIdentity(
		context.Context,
		etcd.Versioned[etcd.AttachRecord],
		string,
		controllerpkg.AttachPlanIdentityConsumer,
	) error
}

type attachDraftPlanSealer interface {
	SealDraft(
		context.Context,
		etcd.Versioned[etcd.AttachRecord],
		etcd.AttachTaskRenderInput,
		etcd.TaskRecord,
		*controllerpkg.AttachPlanIdentity,
		*etcd.AttachEncryptedFacts,
		*etcd.BackingHookEncryptedInputs,
	) (serviceruntimerecord.AttachPreparation, error)
}

type attachMutationEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type attachMutationIdempotency interface {
	PrepareCreate(context.Context, string, apiTypes.AttachRequest) (attachMutationEvidence, error)
	PrepareDetach(context.Context, string, string) (attachMutationEvidence, error)
	PrepareRename(context.Context, string, string, apiTypes.AttachRenameRequest) (attachMutationEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		attachMutationEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		attachMutationEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		attachMutationEvidence,
		error,
	) (idempotentintent.Resolution, error)
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
}

type durableAttachMutationIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableAttachMutationIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableAttachMutationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutation idempotency is not configured")
	}
	return &durableAttachMutationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableAttachMutationIdempotency) PrepareCreate(
	ctx context.Context,
	environmentID string,
	request apiTypes.AttachRequest,
) (attachMutationEvidence, error) {
	grants := make([]idempotentintent.Value, len(request.GrantAttachIDs))
	for index, grantID := range request.GrantAttachIDs {
		grants[index] = idempotentintent.String(grantID)
	}
	return service.protect(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost, Route: attachCreationRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Query: idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{
				Name:  "backing_service_id",
				Value: idempotentintent.String(request.BackingServiceID),
			},
			idempotentintent.Field{
				Name: "credential",
				Value: idempotentintent.Object(
					idempotentintent.Field{
						Name:  "attach_id",
						Value: idempotentintent.String(request.Credential.AttachID),
					},
					idempotentintent.Field{
						Name:  "mode",
						Value: idempotentintent.String(string(request.Credential.Mode)),
					},
				),
			},
			idempotentintent.Field{Name: "grant_attach_ids", Value: idempotentintent.List(grants...)},
			idempotentintent.Field{Name: "name", Value: idempotentintent.String(request.Name)},
			idempotentintent.Field{Name: "service_id", Value: idempotentintent.String(request.ServiceID)},
		)),
	})
}

func (service *durableAttachMutationIdempotency) PrepareDetach(
	ctx context.Context,
	environmentID string,
	attachID string,
) (attachMutationEvidence, error) {
	return service.protect(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodDelete, Route: attachDeletionRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: attachID}},
		Query: idempotentintent.Object(), Body: idempotentintent.NoBody(),
	})
}

func (service *durableAttachMutationIdempotency) PrepareRename(
	ctx context.Context,
	environmentID string,
	attachID string,
	request apiTypes.AttachRenameRequest,
) (attachMutationEvidence, error) {
	return service.protect(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost, Route: attachRenameRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: attachID}},
		Query: idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "name", Value: idempotentintent.String(request.Name)},
		)),
	})
}

func (service *durableAttachMutationIdempotency) protect(
	ctx context.Context,
	intent idempotentintent.CanonicalIntentV1,
) (attachMutationEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, intent)
	if err != nil {
		return attachMutationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return attachMutationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return attachMutationEvidence{}, err
	}
	return attachMutationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableAttachMutationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence attachMutationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableAttachMutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence attachMutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableAttachMutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence attachMutationEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func (service *durableAttachMutationIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

type attachMutationService struct {
	repository  attachMutationRepository
	facts       attachMutationFacts
	plans       attachDraftPlanSealer
	runtime     attachRuntimeCapture
	idempotency attachMutationIdempotency
	random      io.Reader
	now         func() time.Time
}

func newAttachMutationService(
	repository attachMutationRepository,
	facts attachMutationFacts,
	plans attachDraftPlanSealer,
	runtime attachRuntimeCapture,
	idempotency attachMutationIdempotency,
) (*attachMutationService, error) {
	if repository == nil || facts == nil || plans == nil || runtime == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutation service is not configured")
	}
	return &attachMutationService{
		repository: repository, facts: facts, plans: plans, runtime: runtime, idempotency: idempotency,
		random: rand.Reader, now: time.Now,
	}, nil
}

func (service *attachMutationService) CreateAttach(
	ctx context.Context,
	request apiTypes.AttachRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach creation context is required")
	}
	normalized, err := normalizeAttachRequest(request)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumAttachMutationTries; attempt++ {
		response, createErr := service.createAttachOnce(ctx, normalized, idempotencyKey)
		if createErr == nil {
			return response, nil
		}
		kind, known := errs.KindOf(createErr)
		retry := known && kind == errs.KindStateConflict
		retry = retry || normalized.Name == "" && known && kind == errs.KindNameConflict
		if !retry || attempt == maximumAttachMutationTries-1 {
			return etcd.IdempotencyResponse{}, createErr
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach creation retry bound was not enforced")
}

func (service *attachMutationService) createAttachOnce(
	ctx context.Context,
	request apiTypes.AttachRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	consumer, err := service.repository.GetService(ctx, request.ServiceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	environmentID := consumer.Record.EnvironmentID
	evidence, err := service.idempotency.PrepareCreate(ctx, environmentID, request)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPost, Route: attachCreationRoute, Key: idempotencyKey,
	}
	if replay, exists, resolveErr := service.idempotency.ResolveExisting(ctx, locator, evidence); resolveErr != nil {
		return etcd.IdempotencyResponse{}, resolveErr
	} else if exists {
		if replay.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Attach creation replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(replay.Response), nil
	}

	scope, currentAttaches, adapter, err := service.resolveAttachScope(
		ctx, consumer, request.BackingServiceID, request.Credential.AttachID, request.GrantAttachIDs,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	runtime, err := service.runtime.CaptureEntryMutationRuntime(ctx, scope.ComposeProjection)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	name := request.Name
	if name == "" {
		existingNames := make(map[string]struct{}, len(currentAttaches))
		for _, current := range currentAttaches {
			existingNames[current.Record.Name] = struct{}{}
		}
		name, err = suggestAttachName(attachNameLabels{
			tenant: scope.Tenant.Record.Slug, project: scope.Project.Record.Slug,
			environment: scope.Environment.Record.Name, service: consumer.Record.Desired.Name,
		}, existingNames)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}

	now := service.now().UTC()
	attachID := ids.New(ids.KindAttach)
	taskID := ids.New(ids.KindTask)
	credentialAttachID := attachID
	grantIDs := attachGrantIDs(scope.Grants)
	ownsCredential := request.Credential.Mode == apiTypes.AttachCredentialNew
	hooked := false
	attachHook := false
	if ownsCredential && adapter.Custom() && scope.BackingService.Record.Desired.Hooks != nil {
		hooks := scope.BackingService.Record.Desired.Hooks
		hooked = hooks.Attach != nil || hooks.Detach != nil
		attachHook = hooks.Attach != nil
	}
	task, artifactID, err := newAttachMutationTask(
		scope.Project.Record, scope.Environment.Record,
		taskID, attachID, environmentID, etcd.TaskAttach,
		scope.ComposeProjection.Record.RenderGeneration,
		attachTaskStepCount(
			adapter, scope.BackingService.Record.Desired.Authentication,
			len(grantIDs), ownsCredential, attachHook,
		),
		idempotencyKey, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	var identity *controllerpkg.AttachPlanIdentity
	var metadata []etcd.AttachFactSetMetadata
	var encryptedFacts *etcd.AttachEncryptedFacts
	var hookInputs *etcd.BackingHookEncryptedInputs
	if ownsCredential {
		identity, metadata, encryptedFacts, hookInputs, err = service.prepareAttachFacts(
			ctx, attachID, task.OperationID, consumer, scope, adapter,
		)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		if hookInputs != nil {
			task, err = etcd.BindBackingHookTaskInputs(task, scope.BackingProject.Record.ID, *hookInputs)
			if err != nil {
				return etcd.IdempotencyResponse{}, err
			}
		}
	} else {
		credentialAttachID = request.Credential.AttachID
		metadata = cloneAttachFactMetadata(scope.CredentialOwner.Record.FactSets)
	}
	if identity != nil {
		defer identity.Clear()
	}
	if encryptedFacts != nil {
		defer clear(encryptedFacts.Ciphertext)
	}
	if hookInputs != nil {
		defer clear(hookInputs.Ciphertext)
	}
	record, err := etcd.NewPendingAttachRecord(
		attachID, environmentID, name, scope.BackingProject.Record.ID, scope.BackingEnvironment.Record.ID,
		scope.BackingService.Record.Desired.ID, scope.BackingService.Record.BackingNetworkID,
		consumer.Record.Desired.ID, credentialAttachID, grantIDs, metadata, taskID, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	record.HookBundle = hooked
	if attachHook {
		task.TimeoutSeconds = max(
			task.TimeoutSeconds,
			int64(scope.BackingService.Record.Desired.Hooks.Attach.TimeoutSeconds)+15,
		)
	}
	renderInput, err := buildAttachTaskRenderInput(scope, runtime, record, currentAttaches, task, artifactID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	prepared, err := service.plans.SealDraft(
		ctx, etcd.Versioned[etcd.AttachRecord]{Record: record}, renderInput, task, identity, encryptedFacts, hookInputs,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task.PlanHash, renderInput.RuntimePreparation = prepared.PlanHash, &prepared
	response, marker, err := newAttachMutationResponse(locator, evidence.durable, task, nil)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	result, createErr := service.repository.CreateAttachWithTaskHookInputs(
		ctx, scope, record, encryptedFacts, hookInputs, renderInput, task, marker,
	)
	return service.resolveMutationResult(ctx, locator, evidence, result, createErr, response)
}

func (service *attachMutationService) DetachAttach(
	ctx context.Context,
	attachID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.detachAttach(ctx, attachID, idempotencyKey, nil)
}

func (service *attachMutationService) DetachAttachWithInitiation(
	ctx context.Context,
	attachID string,
	idempotencyKey string,
	initiation etcd.TaskInitiation,
) (etcd.IdempotencyResponse, error) {
	return service.detachAttach(ctx, attachID, idempotencyKey, &initiation)
}

func (service *attachMutationService) detachAttach(
	ctx context.Context,
	attachID string,
	idempotencyKey string,
	initiation *etcd.TaskInitiation,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach detach context is required")
	}
	if ids.Validate(ids.KindAttach, attachID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Attach id is invalid")
	}
	for attempt := 0; attempt < maximumAttachMutationTries; attempt++ {
		response, err := service.detachAttachOnce(ctx, attachID, idempotencyKey, initiation)
		if err == nil {
			return response, nil
		}
		kind, known := errs.KindOf(err)
		if !known || kind != errs.KindStateConflict || attempt == maximumAttachMutationTries-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach detach retry bound was not enforced")
}

func (service *attachMutationService) detachAttachOnce(
	ctx context.Context,
	attachID string,
	idempotencyKey string,
	initiation *etcd.TaskInitiation,
) (etcd.IdempotencyResponse, error) {
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetAttach, ID: attachID}
	replayLocator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, attachDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		evidence, prepareErr := service.idempotency.PrepareDetach(ctx, replayLocator.ScopeID, attachID)
		if prepareErr != nil {
			return etcd.IdempotencyResponse{}, prepareErr
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, exists, resolveErr := service.idempotency.ResolveExisting(ctx, replayLocator, evidence)
		if resolveErr != nil {
			return etcd.IdempotencyResponse{}, resolveErr
		}
		if !exists || resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach detach replay index is inconsistent")
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	current, err := service.repository.GetAttach(ctx, attachID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.PrepareDetach(ctx, current.Record.EnvironmentID, attachID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: current.Record.EnvironmentID,
		Method: http.MethodDelete, Route: attachDeletionRoute, Key: idempotencyKey,
	}
	if replay, exists, resolveErr := service.idempotency.ResolveExisting(ctx, locator, evidence); resolveErr != nil {
		return etcd.IdempotencyResponse{}, resolveErr
	} else if exists {
		if replay.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach detach replay resolution is invalid")
		}
		return cloneIdempotencyResponse(replay.Response), nil
	}
	consumer, err := service.repository.GetService(ctx, current.Record.ServiceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	credentialOwnerID := ""
	if !current.Record.OwnsCredential() {
		credentialOwnerID = current.Record.CredentialAttachID
	}
	scope, currentAttaches, adapter, err := service.resolveAttachScope(
		ctx, consumer, current.Record.BackingServiceID, credentialOwnerID, current.Record.GrantAttachIDs,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	runtime, err := service.runtime.CaptureEntryMutationRuntime(ctx, scope.ComposeProjection)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	taskID := ids.New(ids.KindTask)
	detaching, err := etcd.BeginAttachDetaching(current.Record, taskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	detachHook := current.Record.HookBundle && scope.BackingService.Record.Desired.Hooks != nil &&
		scope.BackingService.Record.Desired.Hooks.Detach != nil
	task, artifactID, err := newAttachMutationTask(
		scope.Project.Record, scope.Environment.Record,
		taskID, current.Record.ID, current.Record.EnvironmentID, etcd.TaskDetach,
		scope.ComposeProjection.Record.RenderGeneration,
		attachTaskStepCount(
			adapter, scope.BackingService.Record.Desired.Authentication,
			len(current.Record.GrantAttachIDs), current.Record.OwnsCredential(), detachHook,
		),
		idempotencyKey, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if detachHook {
		task.TimeoutSeconds = max(
			task.TimeoutSeconds,
			int64(scope.BackingService.Record.Desired.Hooks.Detach.TimeoutSeconds)+15,
		)
	}
	var hookInputs *etcd.BackingHookEncryptedInputs
	if detachHook {
		hookInputs, err = service.facts.SealBackingHookTaskInputs(
			ctx, task.OperationID, scope.BackingProject.Record.ID, *scope.BackingService.Record.Desired.Hooks,
		)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		if hookInputs != nil {
			defer clear(hookInputs.Ciphertext)
			task, err = etcd.BindBackingHookTaskInputs(task, scope.BackingProject.Record.ID, *hookInputs)
			if err != nil {
				return etcd.IdempotencyResponse{}, err
			}
		}
	}
	if initiation != nil {
		task.Owner = initiation.Owner()
		task.Actor = initiation.Actor()
	}
	renderInput, err := buildAttachTaskRenderInput(scope, runtime, detaching, currentAttaches, task, artifactID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	draft := current
	draft.Record = detaching
	prepared, err := service.plans.SealDraft(ctx, draft, renderInput, task, nil, nil, hookInputs)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task.PlanHash, renderInput.RuntimePreparation = prepared.PlanHash, &prepared
	response, marker, err := newAttachMutationResponse(locator, evidence.durable, task, &target)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	var result etcd.IdempotencyTransactionResult
	var detachErr error
	if initiation == nil {
		result, detachErr = service.repository.BeginAttachDetachWithTaskHookInputs(
			ctx, scope, current, hookInputs, renderInput, task, marker,
		)
	} else {
		result, detachErr = service.repository.BeginAttachDetachWithTaskInitiationHookInputs(
			ctx, scope, current, hookInputs, renderInput, task, marker, *initiation,
		)
	}
	return service.resolveMutationResult(ctx, locator, evidence, result, detachErr, response)
}

func (service *attachMutationService) resolveMutationResult(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence attachMutationEvidence,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	response etcd.IdempotencyResponse,
) (etcd.IdempotencyResponse, error) {
	var resolution idempotentintent.Resolution
	var err error
	if mutationErr != nil {
		if !isUnknownAttachMutationOutcome(mutationErr) {
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == idempotentintent.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	if resolution.Kind != idempotentintent.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach mutation resolution is invalid")
	}
	return cloneIdempotencyResponse(response), nil
}

func (service *attachMutationService) resolveAttachScope(
	ctx context.Context,
	consumer etcd.Versioned[etcd.ServiceRecord],
	backingServiceID string,
	credentialAttachID string,
	grantIDs []string,
) (etcd.AttachCreateScope, []etcd.Versioned[etcd.AttachRecord], adapters.Adapter, error) {
	environment, err := service.repository.GetEnvironment(ctx, consumer.Record.EnvironmentID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	backingService, err := service.repository.GetService(ctx, backingServiceID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	backingEnvironment, err := service.repository.GetEnvironment(ctx, backingService.Record.EnvironmentID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	backingProject, err := service.repository.GetProject(ctx, backingEnvironment.Record.ProjectID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	if environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady ||
		backingEnvironment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady ||
		project.Record.Kind != etcd.ProjectKindTenant || backingProject.Record.Kind != etcd.ProjectKindBacking ||
		consumer.Record.EnvironmentID != environment.Record.ID ||
		consumer.Record.Runtime.RuntimeIntent == core.ServiceRuntimeIntentAbsent ||
		backingService.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach requires ready consumer/backing Environments and a runnable backing Service",
		)
	}
	adapter, registered := adapters.Get(backingService.Record.Desired.Adapter)
	if !registered || adapter.Key() != backingService.Record.Desired.Adapter {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindValidationFailed,
			"Attach backing Service adapter is not registered",
		)
	}
	authentication, authErr := core.ResolveBackingAuthentication(
		adapter.SupportsAuthenticationModes(), backingService.Record.Desired.Authentication,
	)
	if authErr != nil || authentication != backingService.Record.Desired.Authentication {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach backing Service authentication policy is invalid",
		)
	}
	if len(grantIDs) != 0 && !adapter.SupportsGrants() {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindValidationFailed,
			"Attach backing Service adapter does not support grants",
		)
	}
	head, exists, err := service.repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	if !exists {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach requires an applied Environment Blueprint",
		)
	}
	projection, exists, err := service.repository.GetEnvironmentComposeProjectionRevision(
		ctx, environment.Record.ID, head.Record.RevisionID,
	)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	if !exists {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach requires the selected Environment Compose projection",
		)
	}
	if head.Record.EnvironmentID != environment.Record.ID ||
		projection.Record.EnvironmentID != environment.Record.ID ||
		projection.Record.RevisionID != head.Record.RevisionID {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach selected Environment Compose projection is inconsistent",
		)
	}
	grants := make([]etcd.Versioned[etcd.AttachRecord], 0, len(grantIDs))
	for _, grantID := range grantIDs {
		grant, grantErr := service.repository.GetAttach(ctx, grantID)
		if grantErr != nil {
			return etcd.AttachCreateScope{}, nil, nil, grantErr
		}
		grants = append(grants, grant)
	}
	slices.SortFunc(grants, func(left, right etcd.Versioned[etcd.AttachRecord]) int {
		if left.Record.ID < right.Record.ID {
			return -1
		}
		if left.Record.ID > right.Record.ID {
			return 1
		}
		return 0
	})
	var credentialOwner *etcd.Versioned[etcd.AttachRecord]
	if credentialAttachID != "" {
		owner, ownerErr := service.repository.GetAttach(ctx, credentialAttachID)
		if ownerErr != nil {
			return etcd.AttachCreateScope{}, nil, nil, ownerErr
		}
		if owner.Record.Status != core.AttachReady || !owner.Record.OwnsCredential() ||
			owner.Record.EnvironmentID != environment.Record.ID ||
			owner.Record.BackingServiceID != backingService.Record.Desired.ID ||
			owner.Record.BackingNetworkID != backingService.Record.BackingNetworkID {
			return etcd.AttachCreateScope{}, nil, nil, errs.New(
				errs.KindScopeUnauthorized,
				"Existing credential must be a ready direct owner in the same Environment and Backing Service",
			)
		}
		credentialOwner = &owner
	}
	attaches, err := service.listAllAttaches(ctx, environment.Record.ID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	scope := etcd.AttachCreateScope{
		Tenant: tenant, Project: project, Environment: environment,
		DesiredHead: head, ComposeProjection: projection,
		Services:       []etcd.Versioned[etcd.ServiceRecord]{consumer},
		BackingProject: backingProject, BackingEnvironment: backingEnvironment,
		BackingService: backingService, CredentialOwner: credentialOwner, Grants: grants,
	}
	return scope, attaches, adapter, nil
}

func (service *attachMutationService) listAllAttaches(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.AttachRecord], error) {
	var records []etcd.Versioned[etcd.AttachRecord]
	cursor := ""
	revision := int64(0)
	for {
		page, err := service.repository.ListAttaches(ctx, environmentID, etcd.PageRequest{
			Limit: etcd.MaximumPageLimit, Cursor: cursor,
		})
		if err != nil {
			return nil, err
		}
		if page.Revision <= 0 || revision != 0 && page.Revision != revision {
			return nil, errs.New(errs.KindInternal, "Attach topology pages changed revision")
		}
		revision = page.Revision
		records = append(records, page.Items...)
		if page.NextCursor == "" {
			return records, nil
		}
		cursor = page.NextCursor
	}
}

func (service *attachMutationService) prepareAttachFacts(
	ctx context.Context, attachID string, operationID string,
	consumer etcd.Versioned[etcd.ServiceRecord],
	scope etcd.AttachCreateScope,
	adapter adapters.Adapter,
) (*controllerpkg.AttachPlanIdentity, []etcd.AttachFactSetMetadata, *etcd.AttachEncryptedFacts,
	*etcd.BackingHookEncryptedInputs, error,
) {
	if adapter.Custom() {
		hooks := scope.BackingService.Record.Desired.Hooks
		if hooks == nil || hooks.Attach == nil && hooks.Detach == nil {
			return nil, nil, nil, nil, nil
		}
		metadata, encrypted, hookInputs, err := service.facts.SealCustomHookBundle(
			ctx, attachID, scope.BackingProject.Record.ID, operationID, *hooks,
		)
		return nil, metadata, encrypted, hookInputs, err
	}
	authentication := scope.BackingService.Record.Desired.Authentication
	identityName, err := attachProvisionIdentity(attachID, consumer.Record.Desired.Name)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	role := identityName
	var password []byte
	if authentication == core.BackingAuthenticationPassword {
		role = "default"
	}
	if authentication == core.BackingAuthenticationNone {
		role = ""
	} else {
		password, err = generateAttachPassword(service.random)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	defer clear(password)
	own := adapters.Input{
		Authentication: authentication,
		Host:           backingendpoint.New(scope.BackingService.Record.Desired.ID), Port: adapter.Port(),
		Database: identityName, Role: role, Password: password,
	}
	grantFacts := make([]AttachGrantInput, 0, len(scope.Grants))
	planIdentity := &controllerpkg.AttachPlanIdentity{
		Authentication: authentication,
		Database:       identityName, Role: role, Password: append([]byte(nil), password...),
		Grants: make([]controllerpkg.AttachPlanGrantIdentity, 0, len(scope.Grants)),
	}
	failed := true
	defer func() {
		if failed {
			planIdentity.Clear()
		}
	}()
	for _, grant := range scope.Grants {
		database := ""
		if err := service.facts.ResolveReadyDatabase(ctx, grant, func(value string) error {
			database = value
			return nil
		}); err != nil {
			return nil, nil, nil, nil, err
		}
		grantFacts = append(grantFacts, AttachGrantInput{
			AttachID: grant.Record.ID,
			Params: adapters.Input{
				Authentication: authentication,
				Host:           backingendpoint.New(scope.BackingService.Record.Desired.ID), Port: adapter.Port(),
				Database: database, Role: role, Password: password,
			},
		})
		planIdentity.Grants = append(planIdentity.Grants, controllerpkg.AttachPlanGrantIdentity{
			AttachID: grant.Record.ID, Database: database,
		})
	}
	metadata, encrypted, err := service.facts.SealFactSets(ctx, attachID, adapter, own, grantFacts)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	failed = false
	return planIdentity, metadata, encrypted, nil, nil
}

func normalizeAttachRequest(request apiTypes.AttachRequest) (apiTypes.AttachRequest, error) {
	request.GrantAttachIDs = append([]string(nil), request.GrantAttachIDs...)
	if ids.Validate(ids.KindService, request.ServiceID) != nil ||
		ids.Validate(ids.KindService, request.BackingServiceID) != nil {
		return apiTypes.AttachRequest{}, errs.New(
			errs.KindValidationFailed,
			"Attach requires exactly one valid consumer Service and one valid backing Service",
		)
	}
	switch request.Credential.Mode {
	case apiTypes.AttachCredentialNew:
		if request.Credential.AttachID != "" {
			return apiTypes.AttachRequest{}, errs.New(
				errs.KindValidationFailed,
				"New Attach credential cannot reference another Attach",
			)
		}
	case apiTypes.AttachCredentialExisting:
		if ids.Validate(ids.KindAttach, request.Credential.AttachID) != nil {
			return apiTypes.AttachRequest{}, errs.New(
				errs.KindValidationFailed,
				"Existing Attach credential requires a valid owner Attach id",
			)
		}
		if len(request.GrantAttachIDs) != 0 {
			return apiTypes.AttachRequest{}, errs.New(
				errs.KindValidationFailed,
				"Existing Attach credential cannot declare grants",
			)
		}
	default:
		return apiTypes.AttachRequest{}, errs.New(
			errs.KindValidationFailed,
			"Attach credential mode must be new or existing",
		)
	}
	if request.Name != "" {
		if err := etcd.ValidateAttachName(request.Name); err != nil {
			return apiTypes.AttachRequest{}, err
		}
	}
	if len(request.GrantAttachIDs) > etcd.MaximumAttachGrants {
		return apiTypes.AttachRequest{}, errs.Newf(
			errs.KindValidationFailed,
			"Attach may have at most %d grants",
			etcd.MaximumAttachGrants,
		)
	}
	slices.Sort(request.GrantAttachIDs)
	previous := ""
	for _, grantID := range request.GrantAttachIDs {
		if ids.Validate(ids.KindAttach, grantID) != nil || grantID == previous {
			return apiTypes.AttachRequest{}, errs.New(
				errs.KindValidationFailed,
				"Attach grant ids must be valid and unique",
			)
		}
		previous = grantID
	}
	return request, nil
}

func newAttachMutationResponse(
	locator etcd.IdempotencyLocator,
	intent etcd.ProtectedIntentRecord,
	task etcd.TaskRecord,
	replayTarget *etcd.IdempotencyReplayTarget,
) (etcd.IdempotencyResponse, etcd.IdempotencyMarker, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, etcd.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: replayTarget, Intent: intent, Response: response,
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	return response, marker, nil
}

func attachGrantIDs(grants []etcd.Versioned[etcd.AttachRecord]) []string {
	values := make([]string, len(grants))
	for index, grant := range grants {
		values[index] = grant.Record.ID
	}
	return values
}

func cloneAttachFactMetadata(values []etcd.AttachFactSetMetadata) []etcd.AttachFactSetMetadata {
	cloned := make([]etcd.AttachFactSetMetadata, len(values))
	for index, value := range values {
		cloned[index] = etcd.AttachFactSetMetadata{
			GrantAttachID: value.GrantAttachID,
			Facts:         append([]etcd.AttachFactDefinition(nil), value.Facts...),
		}
	}
	return cloned
}

func isUnknownAttachMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

type durableAttachMutationRepository struct {
	hierarchy *etcd.HierarchyRepository
	services  *etcd.ServiceRepository
	attaches  *etcd.AttachRepository
}

func newDurableAttachMutationRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
	attaches *etcd.AttachRepository,
) (*durableAttachMutationRepository, error) {
	if hierarchy == nil || services == nil || attaches == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutation repositories are not configured")
	}
	return &durableAttachMutationRepository{hierarchy: hierarchy, services: services, attaches: attaches}, nil
}

func (repository *durableAttachMutationRepository) GetTenant(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.TenantRecord], error) {
	return repository.hierarchy.GetTenant(ctx, id)
}

func (repository *durableAttachMutationRepository) GetProject(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableAttachMutationRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableAttachMutationRepository) GetEnvironmentBlueprintRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error) {
	return repository.hierarchy.GetEnvironmentBlueprintRevision(ctx, environmentID, revisionID)
}

func (repository *durableAttachMutationRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *durableAttachMutationRepository) GetService(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (repository *durableAttachMutationRepository) GetEnvironmentBlueprintHead(
	ctx context.Context,
	environmentID string,
) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error) {
	return repository.hierarchy.GetEnvironmentBlueprintHead(ctx, environmentID)
}

func (repository *durableAttachMutationRepository) GetEnvironmentComposeProjectionRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
}

func (repository *durableAttachMutationRepository) GetAttach(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.AttachRecord], error) {
	return repository.attaches.GetAttach(ctx, id)
}

func (repository *durableAttachMutationRepository) GetAttachTaskRenderInput(
	ctx context.Context,
	planID string,
) (etcd.Versioned[etcd.AttachTaskRenderInput], error) {
	return repository.attaches.GetAttachTaskRenderInput(ctx, planID)
}

func (repository *durableAttachMutationRepository) ListAttaches(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.AttachRecord], error) {
	return repository.attaches.ListAttaches(ctx, environmentID, request)
}

func (repository *durableAttachMutationRepository) RenameAttachIdempotent(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	current etcd.Versioned[etcd.AttachRecord],
	name string,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.attaches.RenameAttachIdempotent(ctx, environment, project, current, name, marker)
}

func (repository *durableAttachMutationRepository) CreateAttachWithTaskHookInputs(
	ctx context.Context,
	scope etcd.AttachCreateScope,
	record etcd.AttachRecord,
	facts *etcd.AttachEncryptedFacts,
	hookInputs *etcd.BackingHookEncryptedInputs,
	renderInput etcd.AttachTaskRenderInput,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.attaches.CreateAttachWithTaskHookInputs(
		ctx, scope, record, facts, hookInputs, renderInput, task, marker,
	)
}

func (repository *durableAttachMutationRepository) BeginAttachDetachWithTaskHookInputs(
	ctx context.Context,
	scope etcd.AttachCreateScope,
	current etcd.Versioned[etcd.AttachRecord],
	hookInputs *etcd.BackingHookEncryptedInputs,
	renderInput etcd.AttachTaskRenderInput,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.attaches.BeginAttachDetachWithTaskHookInputs(
		ctx, scope, current, hookInputs, renderInput, task, marker,
	)
}

func (repository *durableAttachMutationRepository) BeginAttachDetachWithTaskInitiationHookInputs(
	ctx context.Context,
	scope etcd.AttachCreateScope,
	current etcd.Versioned[etcd.AttachRecord],
	hookInputs *etcd.BackingHookEncryptedInputs,
	renderInput etcd.AttachTaskRenderInput,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
	initiation etcd.TaskInitiation,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.attaches.BeginAttachDetachWithTaskInitiationHookInputs(
		ctx, scope, current, hookInputs, renderInput, task, marker, initiation,
	)
}
