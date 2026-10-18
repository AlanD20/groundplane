package serviceremovalwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LoadRecoveryProgress reads the exact report acknowledged by the queue cursor.
// Consumers must also enforce their phase and executor requirements before
// granting an effect or delivering private configuration bytes.
func (repository *Repository) LoadRecoveryProgress(ctx context.Context, view workqueue.ResumeView,
	delivery *agentpb.ServiceLifecycleWorkInput, capture removalexecution.RecoveryCapture,
) (removalexecution.Progress, keyvalue.Condition, error) {
	attempt := view.Attempt.Attempt
	if view.Work.Item.Item.Operation != workexecution.ServiceRemoval || attempt.Direction != workexecution.Recovery ||
		attempt.Validate() != nil || view.Work.Item.Item.Validate() != nil ||
		attempt.TaskID != view.Work.Task.Task.ID || attempt.WorkItemID != view.Work.Item.Item.ID ||
		view.Work.Item.Item.Input == nil || attempt.Input != *view.Work.Item.Item.Input ||
		attempt.Number > view.Work.Item.Item.AttemptNumber || view.Checkpoint.Record == nil ||
		view.Checkpoint.Revision <= 0 || view.Work.ReadRevision <= 0 {
		return removalexecution.Progress{}, keyvalue.Condition{}, conflict("Service removal recovery has no acknowledged checkpoint")
	}
	key := RecoveryReportKey(attempt.ID, view.Checkpoint.Record.Sequence)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return removalexecution.Progress{}, keyvalue.Condition{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return removalexecution.Progress{}, keyvalue.Condition{}, conflict("Service removal recovery checkpoint report is unavailable")
	}
	progress, err := removalexecution.DecodeProgress(attempt, delivery, capture, read.Values[0].Value)
	if err != nil {
		return removalexecution.Progress{}, keyvalue.Condition{}, err
	}
	checkpoint, err := progress.Checkpoint(attempt, delivery, capture)
	if err != nil || checkpoint != *view.Checkpoint.Record {
		return removalexecution.Progress{}, keyvalue.Condition{}, conflict("Service removal recovery report differs from its acknowledged checkpoint")
	}
	return progress, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
