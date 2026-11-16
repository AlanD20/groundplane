package attachments

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type acceptedAttachDetach struct {
	Request apiTypes.TaskRunAfterRequest      `json:"request"`
	Locator idempotency.IdempotencyLocator    `json:"locator"`
	Intent  idempotency.ProtectedIntentRecord `json:"intent"`
}

func (service *MutationService) DetachAttach(ctx context.Context, attachID string,
	request apiTypes.TaskRunAfterRequest, idempotencyKey string,
) (idempotency.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach detach context is required")
	}
	if ids.Validate(ids.KindAttach, attachID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Attach id is invalid")
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetAttach, ID: attachID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodDelete,
		attachDeletionRoute,
		idempotencyKey,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if indexed {
		evidence, err := service.idempotency.PrepareDetach(ctx, locator.ScopeID, attachID, request)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		defer clear(evidence.durable.Ciphertext)
		resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotency.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Attach detach replay index is inconsistent",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	current, err := service.repository.GetAttach(ctx, attachID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	locator = idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID:   environment.Record.ID,
		Method:    http.MethodDelete,
		Route:     attachDeletionRoute,
		Key:       idempotencyKey,
	}
	evidence, err := service.idempotency.PrepareDetach(ctx, environment.Record.ID, attachID, request)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotency.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Attach detach replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	at := service.now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionDetach,
		TargetID:   attachID,
		TargetName: current.Record.Name,
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		},
		After: append(
			[]string(nil),
			request.After...),
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  at.Add(acceptedaction.QueueTimeout),
	}
	return acceptedaction.Submit(ctx, service.accepted, acceptedaction.Submission{Task: task, Kind: "attach-detach",
		Locator: locator, Intent: evidence.durable, Evidence: evidence.candidate, ReplayTarget: &target},
		acceptedAttachDetach{Request: request, Locator: locator, Intent: evidence.durable})
}
