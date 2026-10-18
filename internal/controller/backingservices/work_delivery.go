package backingservices

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backingDelivery struct {
	view   workqueue.ResumeView
	input  backingprovision.HelperInput
	guards []keyvalue.Condition
}

// activeDelivery never chooses latest intent. It authorizes only the current
// saved stage of this attempt and returns comparisons for the final read grant.
func (service *WorkReports) activeDelivery(ctx context.Context, authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity, stageID string, stage backingprovision.Stage,
) (backingDelivery, error) {
	guards, err := authorization.Conditions()
	if err != nil {
		return backingDelivery{}, err
	}
	view, err := service.reportView(ctx, authorization, identity, true)
	if err != nil {
		return backingDelivery{}, err
	}
	attempt, task, item := view.Attempt.Attempt, view.Work.Task.Task, view.Work.Item.Item
	expected, err := backingprovision.StageID(attempt.WorkItemID, stage)
	if err != nil || expected != stageID || attempt.Direction != workexecution.Forward || !view.ActiveAttempt || !view.CurrentExecutor ||
		attempt.FinishedAt != nil || !time.Now().Before(attempt.Deadline) || task.Stop != nil || task.FinishedAt != nil || item.ForwardRevoked {
		return backingDelivery{}, invalidBackingDelivery()
	}
	captured, source, current, err := service.records.ReadAdmission(ctx, view.Work)
	if err != nil {
		return backingDelivery{}, err
	}
	defer clear(captured.Wire)
	if !current {
		return backingDelivery{}, invalidBackingDelivery()
	}
	procedure, err := captured.Procedure()
	if err != nil {
		return backingDelivery{}, err
	}
	progress, err := service.records.LoadProgress(ctx, view)
	if err != nil {
		return backingDelivery{}, err
	}
	if progress == nil || !progress.InFlight || progress.Stage != stage || view.Checkpoint.Record == nil || view.Checkpoint.Record.StageID != stageID {
		return backingDelivery{}, invalidBackingDelivery()
	}
	layout, err := backingprovision.CaptureLayout(attempt, procedure)
	if err != nil {
		return backingDelivery{}, err
	}
	guards = append(guards, source.Conditions...)
	guards = append(guards,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
		keyvalue.Condition{Key: workqueue.HostForwardFenceKey(attempt.Executor.HostID)},
	)
	return backingDelivery{view: view, guards: guards,
		input: backingprovision.HelperInput{Assignment: backingprovision.Assignment{Attempt: attempt, Procedure: procedure}, Layout: layout, Progress: progress}}, nil
}

func (service *WorkReports) grantDelivery(ctx context.Context, selection backingDelivery) error {
	if !time.Now().Before(selection.input.Assignment.Attempt.Deadline) {
		return invalidBackingDelivery()
	}
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: selection.guards})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return invalidBackingDelivery()
	}
	return nil
}

func invalidBackingDelivery() error {
	return errs.New(errs.KindStateConflict, "backing creation no longer owns the requested captured execution stage")
}
