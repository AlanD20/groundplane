package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *InputRepository) LoadAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.BackupWorkAssignment, workqueue.Change, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || !task.PublicationComplete || task.FinishedAt != nil ||
		task.CollectionStartedAt != nil || attempt.Direction != workexecution.Forward ||
		workexecution.ValidateDelivery(item, attempt) != nil {
		return nil, workqueue.Change{}, conflict("Backup no longer owns this host assignment")
	}
	input, inputGuard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	guards, err := repository.operationGuards(ctx, view.Work, input)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	sources, progressGuard, err := repository.LoadProgress(ctx, view.Work, input)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	assignment := &agentpb.BackupWorkAssignment{Attempt: identity, Input: input, Sources: sources}
	owned, _, err := executionplan.OpenBackupWorkAssignment(assignment, attempt.Executor)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	return owned, workqueue.Change{Conditions: append(guards, inputGuard, progressGuard)}, nil
}
