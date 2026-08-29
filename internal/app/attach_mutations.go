package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
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
	CreateAttachWithTask(
		context.Context,
		etcd.AttachCreateScope,
		etcd.AttachRecord,
		*etcd.AttachEncryptedFacts,
		etcd.AttachTaskRenderInput,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	BeginAttachDetachWithTask(
		context.Context,
		etcd.AttachCreateScope,
		etcd.Versioned[etcd.AttachRecord],
		etcd.AttachTaskRenderInput,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	BeginAttachDetachWithTaskInitiation(
		context.Context,
		etcd.AttachCreateScope,
		etcd.Versioned[etcd.AttachRecord],
		etcd.AttachTaskRenderInput,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
		etcd.TaskInitiation,
	) (etcd.IdempotencyTransactionResult, error)
}

type attachMutationFacts interface {
	SealFactSets(
		context.Context,
		string,
		adapters.Adapter,
		adapters.FactParams,
		[]AttachGrantFactParams,
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
	) (string, error)
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
					idempotentintent.Field{Name: "attach_id", Value: idempotentintent.String(request.Credential.AttachID)},
					idempotentintent.Field{Name: "mode", Value: idempotentintent.String(string(request.Credential.Mode))},
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
	idempotency attachMutationIdempotency
	random      io.Reader
	now         func() time.Time
}

func newAttachMutationService(
	repository attachMutationRepository,
	facts attachMutationFacts,
	plans attachDraftPlanSealer,
	idempotency attachMutationIdempotency,
) (*attachMutationService, error) {
	if repository == nil || facts == nil || plans == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutation service is not configured")
	}
	return &attachMutationService{
		repository: repository, facts: facts, plans: plans, idempotency: idempotency,
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
	var identity *controllerpkg.AttachPlanIdentity
	var metadata []etcd.AttachFactSetMetadata
	var encryptedFacts *etcd.AttachEncryptedFacts
	if request.Credential.Mode == apiTypes.AttachCredentialNew {
		identity, metadata, encryptedFacts, err = service.prepareAttachFacts(ctx, attachID, consumer, scope, adapter)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
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
	grantIDs := attachGrantIDs(scope.Grants)
	record, err := etcd.NewPendingAttachRecord(
		attachID, environmentID, name, scope.BackingProject.Record.ID, scope.BackingEnvironment.Record.ID,
		scope.BackingService.Record.Desired.ID, scope.BackingService.Record.BackingNetworkID,
		consumer.Record.Desired.ID, credentialAttachID, grantIDs, metadata, taskID, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task, artifactID, err := newAttachMutationTask(
		scope.Project.Record, scope.Environment.Record,
		taskID, record.ID, record.EnvironmentID, etcd.TaskAttach,
		scope.ComposeProjection.Record.RenderGeneration,
		attachTaskStepCount(adapter, len(grantIDs), record.OwnsCredential()),
		idempotencyKey, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	renderInput, err := buildAttachTaskRenderInput(scope, record, currentAttaches, task, artifactID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task.PlanHash, err = service.plans.SealDraft(
		ctx, etcd.Versioned[etcd.AttachRecord]{Record: record}, renderInput, task, identity,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, marker, err := newAttachMutationResponse(locator, evidence.durable, task, nil)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	result, createErr := service.repository.CreateAttachWithTask(
		ctx, scope, record, encryptedFacts, renderInput, task, marker,
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
	now := service.now().UTC()
	taskID := ids.New(ids.KindTask)
	detaching, err := etcd.BeginAttachDetaching(current.Record, taskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	task, artifactID, err := newAttachMutationTask(
		scope.Project.Record, scope.Environment.Record,
		taskID, current.Record.ID, current.Record.EnvironmentID, etcd.TaskDetach,
		scope.ComposeProjection.Record.RenderGeneration,
		attachTaskStepCount(adapter, len(current.Record.GrantAttachIDs), current.Record.OwnsCredential()),
		idempotencyKey, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if initiation != nil {
		task.Owner = initiation.Owner()
		task.Actor = initiation.Actor()
	}
	renderInput, err := buildAttachTaskRenderInput(scope, detaching, currentAttaches, task, artifactID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	draft := current
	draft.Record = detaching
	task.PlanHash, err = service.plans.SealDraft(ctx, draft, renderInput, task, nil)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	response, marker, err := newAttachMutationResponse(locator, evidence.durable, task, &target)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	var result etcd.IdempotencyTransactionResult
	var detachErr error
	if initiation == nil {
		result, detachErr = service.repository.BeginAttachDetachWithTask(
			ctx, scope, current, renderInput, task, marker,
		)
	} else {
		result, detachErr = service.repository.BeginAttachDetachWithTaskInitiation(
			ctx, scope, current, renderInput, task, marker, *initiation,
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
	revision, exists, err := service.repository.GetEnvironmentBlueprintRevision(
		ctx, environment.Record.ID, head.Record.RevisionID,
	)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	if !exists {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(errs.KindInternal, "Attach Blueprint head is missing")
	}
	projection, exists, err := service.repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil {
		return etcd.AttachCreateScope{}, nil, nil, err
	}
	if !exists || projection.Record.RevisionID != revision.Record.RevisionID {
		return etcd.AttachCreateScope{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach requires the current Environment Compose projection",
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
		BlueprintRevision: revision, ComposeProjection: projection,
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
	ctx context.Context,
	attachID string,
	consumer etcd.Versioned[etcd.ServiceRecord],
	scope etcd.AttachCreateScope,
	adapter adapters.Adapter,
) (*controllerpkg.AttachPlanIdentity, []etcd.AttachFactSetMetadata, *etcd.AttachEncryptedFacts, error) {
	if adapter.Manual() {
		metadata, encrypted, err := service.facts.SealFactSets(
			ctx, attachID, adapter, adapters.FactParams{}, nil,
		)
		return nil, metadata, encrypted, err
	}
	identityName, err := attachProvisionIdentity(attachID, consumer.Record.Desired.Name)
	if err != nil {
		return nil, nil, nil, err
	}
	password, err := generateAttachPassword(service.random)
	if err != nil {
		return nil, nil, nil, err
	}
	defer clear(password)
	own := adapters.FactParams{
		Host: scope.BackingService.Record.Desired.Name, Port: adapter.Port(),
		Database: identityName, Role: identityName, Password: password,
	}
	grantFacts := make([]AttachGrantFactParams, 0, len(scope.Grants))
	planIdentity := &controllerpkg.AttachPlanIdentity{
		Database: identityName, Role: identityName, Password: append([]byte(nil), password...),
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
			return nil, nil, nil, err
		}
		grantFacts = append(grantFacts, AttachGrantFactParams{
			AttachID: grant.Record.ID,
			Params: adapters.FactParams{
				Host: scope.BackingService.Record.Desired.Name, Port: adapter.Port(),
				Database: database, Role: identityName, Password: password,
			},
		})
		planIdentity.Grants = append(planIdentity.Grants, controllerpkg.AttachPlanGrantIdentity{
			AttachID: grant.Record.ID, Database: database,
		})
	}
	metadata, encrypted, err := service.facts.SealFactSets(ctx, attachID, adapter, own, grantFacts)
	if err != nil {
		return nil, nil, nil, err
	}
	failed = false
	return planIdentity, metadata, encrypted, nil
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
			return apiTypes.AttachRequest{}, errs.New(errs.KindValidationFailed, "New Attach credential cannot reference another Attach")
		}
	case apiTypes.AttachCredentialExisting:
		if ids.Validate(ids.KindAttach, request.Credential.AttachID) != nil {
			return apiTypes.AttachRequest{}, errs.New(errs.KindValidationFailed, "Existing Attach credential requires a valid owner Attach id")
		}
		if len(request.GrantAttachIDs) != 0 {
			return apiTypes.AttachRequest{}, errs.New(errs.KindValidationFailed, "Existing Attach credential cannot declare grants")
		}
	default:
		return apiTypes.AttachRequest{}, errs.New(errs.KindValidationFailed, "Attach credential mode must be new or existing")
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

func newAttachMutationTask(
	project etcd.ProjectRecord,
	environment etcd.EnvironmentRecord,
	taskID string,
	attachID string,
	environmentID string,
	taskType etcd.TaskType,
	renderGeneration uint64,
	stepCount int,
	idempotencyKey string,
	createdAt time.Time,
) (etcd.TaskRecord, string, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || ids.Validate(ids.KindAttach, attachID) != nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || renderGeneration == 0 ||
		renderGeneration > math.MaxInt32 || stepCount <= 0 {
		return etcd.TaskRecord{}, "", errs.New(errs.KindValidationFailed, "Attach Task input is invalid")
	}
	owner, err := etcd.EnvironmentTaskOwner(project, environment)
	if err != nil || environment.ID != environmentID {
		return etcd.TaskRecord{}, "", errs.New(errs.KindValidationFailed, "attach task owner is invalid")
	}
	steps := make([]etcd.TaskStepRecord, stepCount)
	for index := range steps {
		steps[index] = etcd.TaskStepRecord{ID: ids.New(ids.KindStep)}
	}
	return etcd.TaskRecord{
		ID: taskID, OperationID: ids.New(ids.KindOperation), IdempotencyKey: idempotencyKey,
		Owner: owner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: int32(renderGeneration), Type: taskType, Target: attachID,
		Params: map[string]string{etcd.TaskMutationEnvironmentParam: environmentID}, Steps: steps,
		TimeoutSeconds: attachMutationTimeout, Status: etcd.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}, ids.New(ids.KindConfig), nil
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

func attachTaskStepCount(adapter adapters.Adapter, grantCount int, ownsCredential bool) int {
	if adapter.Manual() || !ownsCredential {
		return 1
	}
	return grantCount + 2
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

func (repository *durableAttachMutationRepository) GetEnvironmentComposeProjectionRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
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

func (repository *durableAttachMutationRepository) CreateAttachWithTask(
	ctx context.Context,
	scope etcd.AttachCreateScope,
	record etcd.AttachRecord,
	facts *etcd.AttachEncryptedFacts,
	renderInput etcd.AttachTaskRenderInput,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.attaches.CreateAttachWithTask(ctx, scope, record, facts, renderInput, task, marker)
}

func (repository *durableAttachMutationRepository) BeginAttachDetachWithTask(
	ctx context.Context,
	scope etcd.AttachCreateScope,
	current etcd.Versioned[etcd.AttachRecord],
	renderInput etcd.AttachTaskRenderInput,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.attaches.BeginAttachDetachWithTask(ctx, scope, current, renderInput, task, marker)
}

func (repository *durableAttachMutationRepository) BeginAttachDetachWithTaskInitiation(
	ctx context.Context,
	scope etcd.AttachCreateScope,
	current etcd.Versioned[etcd.AttachRecord],
	renderInput etcd.AttachTaskRenderInput,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
	initiation etcd.TaskInitiation,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.attaches.BeginAttachDetachWithTaskInitiation(
		ctx, scope, current, renderInput, task, marker, initiation,
	)
}

type draftAttachPlanSealer struct {
	volumeRoot       string
	repository       attachMutationRepository
	facts            attachMutationFacts
	componentCatalog []controllerpkg.EnvironmentComponentRegistration
}

func newDraftAttachPlanSealer(
	volumeRoot string,
	repository attachMutationRepository,
	facts attachMutationFacts,
	componentCatalog []controllerpkg.EnvironmentComponentRegistration,
) (*draftAttachPlanSealer, error) {
	if repository == nil || facts == nil {
		return nil, errs.New(errs.KindInternal, "Attach draft plan dependencies are required")
	}
	if _, err := controllerpkg.NewTaskPlanResolver(volumeRoot, componentCatalog); err != nil {
		return nil, err
	}
	return &draftAttachPlanSealer{
		volumeRoot: volumeRoot, repository: repository, facts: facts,
		componentCatalog: controllerpkg.CloneEnvironmentComponentCatalog(componentCatalog),
	}, nil
}

func (sealer *draftAttachPlanSealer) SealDraft(
	ctx context.Context,
	current etcd.Versioned[etcd.AttachRecord],
	renderInput etcd.AttachTaskRenderInput,
	task etcd.TaskRecord,
	identity *controllerpkg.AttachPlanIdentity,
) (string, error) {
	state := &draftAttachPlanState{
		repository: sealer.repository, facts: sealer.facts, current: current,
		renderInput: renderInput, identity: identity,
	}
	resolver, err := controllerpkg.NewTaskPlanResolverWithAttachments(
		sealer.volumeRoot, sealer.repository, state, sealer.repository, state,
		sealer.componentCatalog,
	)
	if err != nil {
		return "", err
	}
	plan, err := resolver.ResolveExecutionPlan(ctx, task)
	if err != nil {
		return "", err
	}
	defer clearAttachPlanSecrets(plan)
	return hex.EncodeToString(plan.PlanHash), nil
}

type draftAttachPlanState struct {
	repository  attachMutationRepository
	facts       attachMutationFacts
	current     etcd.Versioned[etcd.AttachRecord]
	renderInput etcd.AttachTaskRenderInput
	identity    *controllerpkg.AttachPlanIdentity
}

func (state *draftAttachPlanState) GetAttach(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.AttachRecord], error) {
	if id == state.current.Record.ID {
		return state.current, nil
	}
	return state.repository.GetAttach(ctx, id)
}

func (state *draftAttachPlanState) GetAttachTaskRenderInput(
	ctx context.Context,
	planID string,
) (etcd.Versioned[etcd.AttachTaskRenderInput], error) {
	if planID == state.renderInput.PlanID {
		return etcd.Versioned[etcd.AttachTaskRenderInput]{Record: state.renderInput}, nil
	}
	return state.repository.GetAttachTaskRenderInput(ctx, planID)
}

func (state *draftAttachPlanState) GetBlueprintAttachTaskIntent(
	context.Context,
	string,
) (etcd.Versioned[etcd.BlueprintAttachTaskIntent], bool, error) {
	return etcd.Versioned[etcd.BlueprintAttachTaskIntent]{}, false, nil
}

func (state *draftAttachPlanState) ResolveTaskIdentity(
	ctx context.Context,
	current etcd.Versioned[etcd.AttachRecord],
	taskID string,
	consume controllerpkg.AttachPlanIdentityConsumer,
) error {
	if current.Record.ID != state.current.Record.ID || state.identity == nil {
		return state.facts.ResolveTaskIdentity(ctx, current, taskID, consume)
	}
	identity := controllerpkg.AttachPlanIdentity{
		Database: state.identity.Database, Role: state.identity.Role,
		Password: append([]byte(nil), state.identity.Password...),
		Grants:   append([]controllerpkg.AttachPlanGrantIdentity(nil), state.identity.Grants...),
	}
	defer identity.Clear()
	return consume(identity)
}

func clearAttachPlanSecrets(plan *agentpb.ExecutionPlan) {
	if plan == nil {
		return
	}
	for _, step := range plan.Steps {
		procedure := step.GetAdapterProcedure()
		if procedure == nil {
			continue
		}
		clear(procedure.Password)
		procedure.Password = nil
	}
}
