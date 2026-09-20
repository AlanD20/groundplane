package attachments

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func (service *MutationService) DetachAttach(
	ctx context.Context,
	attachID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.detachAttach(ctx, attachID, idempotencyKey, nil)
}

func (service *MutationService) DetachAttachWithInitiation(
	ctx context.Context,
	attachID string,
	idempotencyKey string,
	initiation etcd.TaskInitiation,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.detachAttach(ctx, attachID, idempotencyKey, &initiation)
}

func (service *MutationService) detachAttach(
	ctx context.Context,
	attachID string,
	idempotencyKey string,
	initiation *etcd.TaskInitiation,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach detach context is required")
	}
	if ids.Validate(ids.KindAttach, attachID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Attach id is invalid")
	}
	for attempt := 0; attempt < maximumAttachMutationTries; attempt++ {
		response, err := service.detachAttachOnce(ctx, attachID, idempotencyKey, initiation)
		if err == nil {
			return response, nil
		}
		kind, known := errs.KindOf(err)
		if !known || kind != errs.KindStateConflict || attempt == maximumAttachMutationTries-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach detach retry bound was not enforced")
}

func (service *MutationService) detachAttachOnce(
	ctx context.Context,
	attachID string,
	idempotencyKey string,
	initiation *etcd.TaskInitiation,
) (idempotencyrecord.IdempotencyResponse, error) {
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetAttach, ID: attachID}
	replayLocator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, attachDeletionRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		evidence, prepareErr := service.idempotency.PrepareDetach(ctx, replayLocator.ScopeID, attachID)
		if prepareErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, prepareErr
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, exists, resolveErr := service.idempotency.ResolveExisting(ctx, replayLocator, evidence)
		if resolveErr != nil {
			return idempotencyrecord.IdempotencyResponse{}, resolveErr
		}
		if !exists || resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach detach replay index is inconsistent")
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	current, err := service.repository.GetAttach(ctx, attachID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.PrepareDetach(ctx, current.Record.EnvironmentID, attachID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: current.Record.EnvironmentID,
		Method: http.MethodDelete, Route: attachDeletionRoute, Key: idempotencyKey,
	}
	if replay, exists, resolveErr := service.idempotency.ResolveExisting(ctx, locator, evidence); resolveErr != nil {
		return idempotencyrecord.IdempotencyResponse{}, resolveErr
	} else if exists {
		if replay.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach detach replay resolution is invalid")
		}
		return requestidempotency.CloneResponse(replay.Response), nil
	}
	consumer, err := service.repository.GetService(ctx, current.Record.ServiceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	credentialOwnerID := ""
	if !current.Record.OwnsCredential() {
		credentialOwnerID = current.Record.CredentialAttachID
	}
	scope, currentAttaches, adapter, err := service.resolveAttachScope(
		ctx, consumer, current.Record.BackingServiceID, credentialOwnerID, current.Record.GrantAttachIDs,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	runtime, err := service.runtime.CaptureEntryMutationRuntime(ctx, scope.ComposeProjection)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	taskID := ids.New(ids.KindTask)
	detaching, err := attachrecord.BeginAttachDetaching(current.Record, taskID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
		return idempotencyrecord.IdempotencyResponse{}, err
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
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if hookInputs != nil {
			defer clear(hookInputs.Ciphertext)
			task, err = etcd.BindBackingHookTaskInputs(task, scope.BackingProject.Record.ID, *hookInputs)
			if err != nil {
				return idempotencyrecord.IdempotencyResponse{}, err
			}
		}
	}
	if initiation != nil {
		task.Owner = initiation.Owner()
		task.Actor = initiation.Actor()
	}
	renderInput, err := buildAttachTaskRenderInput(scope, runtime, detaching, currentAttaches, task, artifactID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	draft := current
	draft.Record = detaching
	prepared, err := service.plans.SealDraft(ctx, draft, renderInput, task, nil, nil, hookInputs)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	task.PlanHash, renderInput.RuntimePreparation = prepared.PlanHash, &prepared
	response, marker, err := newAttachMutationResponse(locator, evidence.durable, task, &target)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
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
