package attachments

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *MutationService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"attach-create",
			workexecution.ActionAttach,
			ids.KindAttach,
			service.prepareAcceptedAttach,
		),
		acceptedaction.Registration(
			service.accepted,
			"attach-detach",
			workexecution.ActionDetach,
			ids.KindAttach,
			service.prepareAcceptedDetach,
		),
	}
}

func (service *MutationService) prepareAcceptedAttach(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, input acceptedAttachCreate,
) (taskdependencies.Prepared, error) {
	defer clear(input.Intent.Ciphertext)
	task, request := accepted.Task, input.Request
	consumer, err := service.repository.GetService(ctx, request.ServiceID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	scope, currentAttaches, adapter, err := service.resolveAttachScope(ctx, consumer,
		request.BackingServiceID, request.Credential.AttachID, request.GrantAttachIDs)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if task.Scope != (workexecution.Scope{TenantID: scope.Tenant.Record.ID, ProjectID: scope.Project.Record.ID, EnvironmentID: scope.Environment.Record.ID}) {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Attach consumer ownership changed after acceptance",
		)
	}
	name := request.Name
	if name == "" {
		existing := make(map[string]struct{}, len(currentAttaches))
		for _, current := range currentAttaches {
			existing[current.Record.Name] = struct{}{}
		}
		name, err = suggestAttachName(
			attachNameLabels{tenant: scope.Tenant.Record.Slug, project: scope.Project.Record.Slug,
				environment: scope.Environment.Record.Name, service: consumer.Record.Desired.Name},
			existing,
		)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
	}
	draft, err := service.captureCreateInput(ctx, scope, adapter, task.TargetID, name, task.ID,
		ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "attach-operation"), at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer blueprintattachinputs.Clear(&draft)
	timeout := attachMutationTimeout
	if draft.Hook.Attach != nil {
		timeout = max(timeout, int64(draft.Hook.Attach.TimeoutSeconds)+15)
	}
	_, marker, err := newAttachMutationResponse(input.Locator, input.Intent, task.ID, task.CreatedAt, nil)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(marker.Response.Body)
	task.Intent, task.ExpectedItems, task.UpdatedAt = *task.AcceptedIntent, 1, at
	publication, err := service.repository.PrepareAttachWork(ctx, etcd.AttachWorkPublication{
		Task: task, Accepted: accepted, WorkID: ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "attach"),
		Scope: scope, Generation: draft, Marker: marker, At: at,
		Deadline: acceptedaction.ExecutionDeadline(task, at, time.Duration(timeout)*time.Second)})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{Task: publication.Task, Graph: []workexecution.Item{publication.Item},
		Authority: publication.Change, Finalize: publication.Finish}, nil
}
