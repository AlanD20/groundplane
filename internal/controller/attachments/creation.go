package attachments

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func (service *MutationService) CreateAttach(
	ctx context.Context,
	request apiTypes.AttachRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach creation context is required")
	}
	normalized, err := normalizeAttachRequest(request)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
			return idempotencyrecord.IdempotencyResponse{}, createErr
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach creation retry bound was not enforced")
}

func (service *MutationService) createAttachOnce(
	ctx context.Context,
	request apiTypes.AttachRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	consumer, err := service.repository.GetService(ctx, request.ServiceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	environmentID := consumer.Record.EnvironmentID
	evidence, err := service.idempotency.PrepareCreate(ctx, environmentID, request)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPost, Route: attachCreationRoute, Key: idempotencyKey,
	}
	if replay, exists, resolveErr := service.idempotency.ResolveExisting(ctx, locator, evidence); resolveErr != nil {
		return idempotencyrecord.IdempotencyResponse{}, resolveErr
	} else if exists {
		if replay.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Attach creation replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(replay.Response), nil
	}

	scope, currentAttaches, adapter, err := service.resolveAttachScope(
		ctx, consumer, request.BackingServiceID, request.Credential.AttachID, request.GrantAttachIDs,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	runtime, err := service.runtime.CaptureEntryMutationRuntime(ctx, scope.ComposeProjection)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
			return idempotencyrecord.IdempotencyResponse{}, err
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
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	var identity *taskplanning.AttachPlanIdentity
	var metadata []attachrecord.FactSetMetadata
	var encryptedFacts *attachrecord.EncryptedFacts
	var hookInputs *etcd.BackingHookEncryptedInputs
	if ownsCredential {
		identity, metadata, encryptedFacts, hookInputs, err = service.prepareAttachFacts(
			ctx, attachID, task.OperationID, consumer, scope, adapter,
		)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if hookInputs != nil {
			task, err = etcd.BindBackingHookTaskInputs(task, scope.BackingProject.Record.ID, *hookInputs)
			if err != nil {
				return idempotencyrecord.IdempotencyResponse{}, err
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
	record, err := attachrecord.NewPendingAttachRecord(
		attachID, environmentID, name, scope.BackingProject.Record.ID, scope.BackingEnvironment.Record.ID,
		scope.BackingService.Record.Desired.ID, scope.BackingService.Record.BackingNetworkID,
		consumer.Record.Desired.ID, credentialAttachID, grantIDs, metadata, taskID, now,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	prepared, err := service.plans.SealDraft(
		ctx, etcdstore.Versioned[attachrecord.Record]{Record: record}, renderInput, task, identity, encryptedFacts, hookInputs,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task.PlanHash, renderInput.RuntimePreparation = prepared.PlanHash, &prepared
	response, marker, err := newAttachMutationResponse(locator, evidence.durable, task, nil)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	result, createErr := service.repository.CreateAttachWithTaskHookInputs(
		ctx, scope, record, encryptedFacts, hookInputs, renderInput, task, marker,
	)
	return service.resolveMutationResult(ctx, locator, evidence, result, createErr, response)
}
