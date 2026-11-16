package scripts

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const scriptRunRoute = "/scripts/{id}/run"

func (service *scriptMutationService) RunScript(ctx context.Context, scriptID string,
	input api.TaskRunAfterRequest, idempotencyKey string,
) (idempotency.IdempotencyResponse, error) {
	if service == nil || service.repository == nil || service.idempotency == nil || service.accepted == nil ||
		ctx == nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Script execution service is not configured",
		)
	}
	if ids.Validate(ids.KindScript, scriptID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Script id is invalid")
	}
	// Only identity and ancestry are needed here. A Run queued after Deploy
	// must not require that Deploy's serving Release before it can be accepted.
	script, err := service.repository.GetScript(ctx, scriptID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, script.Record.EnvironmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	values := make([]requestidempotency.Value, len(input.After))
	for index, id := range input.After {
		values[index] = requestidempotency.String(id)
	}
	intent := scriptMutationIntent{method: http.MethodPost, route: scriptRunRoute,
		environmentID: environment.Record.ID,
		path:          []requestidempotency.PathBinding{{Name: "id", Value: scriptID}},
		body: requestidempotency.Object(
			requestidempotency.Field{Name: "after", Value: requestidempotency.List(values...)},
		)}
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := scriptMutationLocator(intent, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if existing {
		return scriptReplayResponse(resolution)
	}
	at := service.now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Action: workexecution.ActionScript, TargetID: scriptID, TargetName: script.Record.Desired.Slug,
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: environment.Record.ID,
		},
		After: append(
			[]string(nil),
			input.After...), CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
	return acceptedaction.Submit(ctx, service.accepted, acceptedaction.Submission{
		Task: task, Kind: "script-run", Locator: locator, Intent: evidence.durable, Evidence: evidence.candidate}, input)
}
