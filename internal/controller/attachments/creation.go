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

type acceptedAttachCreate struct {
	Request apiTypes.AttachRequest            `json:"request"`
	Locator idempotency.IdempotencyLocator    `json:"locator"`
	Intent  idempotency.ProtectedIntentRecord `json:"intent"`
}

func (service *MutationService) CreateAttach(ctx context.Context, request apiTypes.AttachRequest,
	idempotencyKey string,
) (idempotency.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach creation context is required")
	}
	request, err := normalizeAttachRequest(request)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	consumer, err := service.repository.GetService(ctx, request.ServiceID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, consumer.Record.EnvironmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.PrepareCreate(ctx, environment.Record.ID, request)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID:   environment.Record.ID,
		Method:    http.MethodPost,
		Route:     attachCreationRoute,
		Key:       idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotency.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Attach creation replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	at := service.now().UTC()
	name := request.Name
	if name == "" {
		name = "New connection"
	}
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionAttach,
		TargetID:   ids.New(ids.KindAttach),
		TargetName: name,
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
	return acceptedaction.Submit(ctx, service.accepted, acceptedaction.Submission{Task: task, Kind: "attach-create",
		Locator: locator, Intent: evidence.durable, Evidence: evidence.candidate},
		acceptedAttachCreate{Request: request, Locator: locator, Intent: evidence.durable})
}
