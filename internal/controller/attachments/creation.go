package attachments

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
	"time"
)

func (service *MutationService) CreateAttach(
	ctx context.Context,
	request apiTypes.AttachRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Attach creation context is required",
		)
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
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Attach creation retry bound was not enforced",
	)
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
	draft, err := service.captureCreateInput(ctx, scope, adapter, attachID, name, taskID, ids.New(ids.KindOperation), now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer blueprintattachinputs.Clear(&draft)
	timeout := attachMutationTimeout
	if draft.Hook.Attach != nil {
		timeout = max(timeout, int64(draft.Hook.Attach.TimeoutSeconds)+15)
	}
	task := workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor,
		Scope:  workexecution.Scope{TenantID: scope.Tenant.Record.ID, ProjectID: scope.Project.Record.ID, EnvironmentID: environmentID},
		Action: workexecution.ActionAttach, TargetID: attachID, TargetName: name,
		Intent:        workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, now, taskID, "request-intent"), SHA256: evidence.durable.CiphertextDigest},
		ExpectedItems: 1, CreatedAt: now, UpdatedAt: now, Deadline: now.Add(time.Duration(timeout) * time.Second)}
	response, marker, err := newAttachMutationResponse(locator, evidence.durable, task.ID, task.CreatedAt, nil)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	result, createErr := service.repository.PublishAttachWork(ctx, etcd.AttachWorkPublication{
		Task: task, WorkID: ids.DeriveAt(ids.KindWorkItem, now, taskID, "attach"), Scope: scope, Generation: draft, Marker: marker})
	return service.resolveMutationResult(ctx, locator, evidence, result, createErr, response)
}
