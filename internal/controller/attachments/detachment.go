package attachments

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
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
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Attach detach retry bound was not enforced",
	)
}

func (service *MutationService) detachAttachOnce(
	ctx context.Context,
	attachID string,
	idempotencyKey string,
	initiation *etcd.TaskInitiation,
) (idempotencyrecord.IdempotencyResponse, error) {
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetAttach,
		ID:   attachID,
	}
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
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Attach detach replay index is inconsistent",
			)
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
	snapshot, err := service.repository.CaptureDetachSnapshot(ctx, current)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer snapshot.Clear()
	placement, err := service.repository.ResolveDetachHost(ctx)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	consumer, err := attachplan.ConsumerTransition(ctx, snapshot, nil, ids.New(ids.KindConfig))
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if consumer != nil {
		defer func() {
			clear(consumer.Baseline.CanonicalYaml)
			clear(consumer.Candidate.CanonicalYaml)
		}()
	}
	now, taskID := service.now().UTC(), ids.New(ids.KindTask)
	workID := ids.DeriveAt(ids.KindWorkItem, now, taskID, "detach")
	input, err := attachremoval.NewDetachInput(ctx, taskID, workID, &snapshot, nil, consumer)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer input.Clear()
	if !input.HasHostEffects() {
		placement.Executor = workexecution.Controller
	}
	reference, err := input.Reference(ctx, ids.DeriveAt(ids.KindPlan, now, workID, "captured-input"))
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	source := input.Snapshot.Source
	timeout := attachexecution.RemovalBudget(0)
	if source.Attach.HookBundle && source.Backing.Desired.Hooks != nil && source.Backing.Desired.Hooks.Detach != nil {
		timeout = attachexecution.RemovalBudget(int64(source.Backing.Desired.Hooks.Detach.TimeoutSeconds))
	}
	actor := workexecution.OperatorActor
	if initiation != nil {
		actor = workexecution.Actor(initiation.Actor())
	}
	task := workexecution.Task{ID: taskID, Actor: actor, Scope: workexecution.Scope{TenantID: source.TenantID,
		ProjectID: source.ProjectID, EnvironmentID: source.Attach.EnvironmentID}, Action: workexecution.ActionDetach,
		TargetID: source.Attach.ID, TargetName: source.Attach.Name,
		Intent:        workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, now, taskID, "request-intent"), SHA256: evidence.durable.CiphertextDigest},
		ExpectedItems: 1, CreatedAt: now, UpdatedAt: now, Deadline: now.Add(timeout)}
	item := workexecution.Item{ID: workID, TaskID: taskID, Operation: workexecution.Detach, TargetID: task.TargetID,
		Placement: placement, Input: &reference, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		Claims: input.Claims(placement.HostID), CreatedAt: now, UpdatedAt: now, Deadline: task.Deadline}
	if err := workexecution.ValidateGraph(task, []workexecution.Item{item}); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	capture, err := attachremoval.NewDetachCapture(task, item, input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer capture.Clear()
	if attachremoval.NeedsAgent(source) && source.Backing.Desired.Adapter == "custom" {
		capture.Hook, err = service.facts.SealBackingHookInputsAtRevision(ctx, capture.OperationID,
			source.Attach.BackingProjectID, *source.Backing.Desired.Hooks, source.SnapshotRevision)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		if capture.Hook != nil {
			capture.Sources = append([]tasksecretpinrecord.Record(nil), capture.Hook.SecretSources...)
		}
	}
	response, marker, err := newAttachMutationResponse(locator, evidence.durable, task.ID, task.CreatedAt, &target)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	result, publicationErr := service.repository.PublishDetachWork(ctx, etcd.DetachWorkPublication{
		Task: task, Item: item, Input: input, Capture: capture, Marker: marker, Initiation: initiation})
	return service.resolveMutationResult(ctx, locator, evidence, result, publicationErr, response)
}
