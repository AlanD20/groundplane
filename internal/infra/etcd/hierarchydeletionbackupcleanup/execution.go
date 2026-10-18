package hierarchydeletionbackupcleanup

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Execution binds remote evidence to one started Controller attempt. Forward
// grants additionally require no cancellation and an unexpired deadline.
type Execution struct {
	view        workqueue.ResumeView
	operation   hierarchydeletion.HierarchyDeletionOperation
	action      hierarchydeletion.HierarchyDeletionAction
	inspectOnly bool
}

func NewExecution(view workqueue.ResumeView, operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) (Execution, error) {
	return newExecution(view, operation, action, false)
}

// NewObservation permits accounting for a started attempt after its Controller
// generation was replaced. The resulting authority cannot grant deletion.
func NewObservation(view workqueue.ResumeView, operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) (Execution, error) {
	return newExecution(view, operation, action, true)
}

func newExecution(view workqueue.ResumeView, operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction, inspectOnly bool,
) (Execution, error) {
	item, attempt, task := view.Work.Item.Item, view.Attempt.Attempt, view.Work.Task.Task
	if (!inspectOnly && !view.CurrentExecutor) || !view.ActiveAttempt || view.ExecutorRevision <= 0 ||
		view.Work.Task.Revision <= 0 || view.Work.Item.Revision <= 0 || view.Attempt.Revision <= 0 ||
		task.Action != workexecution.ActionRemove || task.FinishedAt != nil ||
		item.Operation != workexecution.RemovalBackupObject || item.Placement.Executor != workexecution.Controller ||
		(attempt.Direction != workexecution.Forward && attempt.Direction != workexecution.Recovery) || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		workexecution.ValidateDelivery(item, attempt) != nil {
		return Execution{}, errs.New(errs.KindStateConflict, "backup object removal attempt is not active")
	}
	if err := validateActive(operation, action); err != nil {
		return Execution{}, err
	}
	expected, raw, err := hierarchydeletion.ActionWork(task, action, "")
	clear(raw)
	if err != nil || expected.ID != item.ID || expected.TaskID != item.TaskID ||
		expected.Operation != item.Operation || expected.TargetID != item.TargetID ||
		item.Input == nil || expected.Input == nil || *item.Input != *expected.Input {
		return Execution{}, errs.New(errs.KindStateConflict, "backup object removal differs from its captured action")
	}
	return Execution{view: view, operation: operation, action: action, inspectOnly: inspectOnly}, nil
}

func (execution Execution) Deadline() time.Time { return execution.view.Attempt.Attempt.Deadline }

func (execution Execution) IsRecovery() bool {
	return execution.view.Attempt.Attempt.Direction == workexecution.Recovery
}

func (execution Execution) conditions(forward bool) ([]keyvalue.Condition, error) {
	view := execution.view
	if _, err := newExecution(view, execution.operation, execution.action, execution.inspectOnly); err != nil {
		return nil, err
	}
	if forward && (execution.inspectOnly ||
		(!execution.IsRecovery() && (view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked)) ||
		!time.Now().UTC().Before(execution.Deadline())) {
		return nil, errs.New(errs.KindStateConflict, "backup object removal cannot start another remote action")
	}
	conditions, err := operationConditions(execution.operation)
	if err != nil {
		return nil, err
	}
	return append(conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(view.Work.Task.Task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(view.Work.Item.Item.ID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(view.Attempt.Attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(view.Attempt.Attempt.Executor), ModRevision: view.ExecutorRevision},
	), nil
}
