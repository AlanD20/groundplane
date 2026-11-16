package softwarepreparation

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{acceptedaction.Registration(service.submitter, "software-prepare",
		workexecution.ActionPrepare, ids.KindComponent, service.prepareAccepted)}
}

func (service *Service) prepareAccepted(ctx context.Context, accepted workqueue.TaskSnapshot, _ int64,
	at time.Time, request Request,
) (taskdependencies.Prepared, error) {
	resolved, err := service.preparer.Resolve(ctx, preparation.ResolveInput{
		Selection: request.Selection, SourceKind: request.SourceKind, OriginalRef: request.Ref, Platform: request.Platform})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task := workexecution.CloneTask(accepted.Task)
	input := preparation.Input{
		OperationID: ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "software-preparation"),
		Source:      resolved,
	}
	if resolved.SourceKind == preparation.SourceRef && resolved.Selection.IncludesController() {
		catalog, err := postgres16protocol.CompiledManagedRelease()
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		input.ControllerToolCatalog, err = json.Marshal(catalog)
		if err != nil {
			return taskdependencies.Prepared{}, errs.Wrap(errs.KindInternal, err)
		}
		input.ControllerCatalogSHA256 = sha256Value(input.ControllerToolCatalog)
	}
	capture := preparationrecord.Capture{TaskID: task.ID, CreatedAt: at, Input: input,
		WorkItemID: ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "software-preparation"),
		PlanID:     ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "software-preparation")}
	reference, err := capture.Reference()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	record, err := preparationrecord.NewRecord(task.ID, input.OperationID, reference.SHA256, resolved, at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	_, item, err := capture.Work()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = reference, 1, at
	item.Deadline = acceptedaction.ExecutionDeadline(
		task,
		at,
		time.Duration(preparation.TaskTimeoutSeconds)*time.Second,
	)
	authority, err := service.tasks.PrepareSoftwarePreparationAuthority(ctx, capture, record)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, nil
}
