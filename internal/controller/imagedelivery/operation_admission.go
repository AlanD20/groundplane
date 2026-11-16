package imagedelivery

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
)

type imageRequest struct {
	Requested string `json:"requested"`
	HostID    string `json:"host_id"`
}

func (service *Service) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.submitter,
			"image-fetch",
			workexecution.ActionFetch,
			ids.KindHost,
			service.prepareAcceptedImage,
		),
		acceptedaction.Registration(
			service.submitter,
			"image-remove",
			workexecution.ActionRemove,
			taskdependencies.ImageTarget,
			service.prepareAcceptedImage,
		),
	}
}

func (service *Service) prepareAcceptedImage(ctx context.Context, task workqueue.TaskSnapshot, revision int64,
	at time.Time, request imageRequest,
) (taskdependencies.Prepared, error) {
	host, err := hostbindings.ReadLocalIdentity(ctx, service.store, revision)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if host.Revision == 0 || host.Record != request.HostID {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"The selected image host is no longer available",
		)
	}
	if task.Task.Action == workexecution.ActionFetch && task.Task.TargetID != host.Record ||
		task.Task.Action == workexecution.ActionRemove && task.Task.TargetID != request.Requested {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Image request does not match its accepted host or image",
		)
	}
	input := imageoperations.Input{TaskID: task.Task.ID,
		WorkItemID:  ids.DeriveAt(ids.KindWorkItem, task.Task.CreatedAt, task.Task.ID, "image-operation"),
		PlanID:      ids.DeriveAt(ids.KindPlan, task.Task.CreatedAt, task.Task.ID, "image-capture"),
		OperationID: ids.DeriveAt(ids.KindOperation, task.Task.CreatedAt, task.Task.ID, "image-operation"),
		Placement:   workexecution.Placement{Executor: workexecution.Controller}, CreatedAt: at,
		Action: task.Task.Action}
	if input.Action == workexecution.ActionRemove {
		if err := service.checkRemoval(ctx, request.Requested, task.Task.ID); err != nil {
			return taskdependencies.Prepared{}, err
		}
		input.RemoveID = request.Requested
	} else {
		named, err := imagefetch.ParseRequested(request.Requested)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		if reference.Domain(named) == "localhost:5000" {
			return taskdependencies.Prepared{}, errs.New(errs.KindValidationFailed,
				"Use registry.groundplane.internal:5000 to fetch from GP's private registry; localhost:5000 is the installer's host-local reference")
		}
		plan, err := service.registry.Resolve(ctx, request.Requested)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		if plan.Requested != request.Requested {
			return taskdependencies.Prepared{}, errs.New(errs.KindInternal, "registry resolved another image request")
		}
		input.Fetch = &plan
	}
	captured, item, authority, err := service.operations.PrepareCapturedInput(ctx, input, revision)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	prepared := workexecution.CloneTask(task.Task)
	prepared.Intent, prepared.ExpectedItems, prepared.UpdatedAt = captured.Intent, 1, at
	item.Deadline = acceptedaction.ExecutionDeadline(prepared, at, time.Duration(imagefetch.TimeoutSeconds)*time.Second)
	authority.Conditions = append(
		authority.Conditions,
		keyvalue.Condition{Key: hostbindings.LocalIdentityKey(), ModRevision: host.Revision},
	)
	return taskdependencies.Prepared{Task: prepared, Graph: []workexecution.Item{item}, Authority: authority}, nil
}
