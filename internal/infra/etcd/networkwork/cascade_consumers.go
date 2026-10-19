package networkwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PublishNextCascadeConsumer claims at most one consumer per publication pass.
// A successful write changes the Task revision; callers then return to the loop
// instead of using their stale snapshot to open the execution barrier.
func (repository *InputRepository) PublishNextCascadeConsumer(ctx context.Context, task workqueue.TaskSnapshot,
	graph []workexecution.Item, authority workqueue.Change, revision int64, at time.Time,
) (bool, error) {
	if task.Task.PublicationComplete || task.Revision <= 0 || revision < task.Revision ||
		len(authority.Conditions) == 0 {
		return false, conflict("Backing Zone publication has no captured graph authority")
	}
	if err := workexecution.ValidateGraph(task.Task, graph); err != nil {
		return false, err
	}
	published := make(map[string]bool)
	after := ""
	for {
		page, err := repository.transactions.ListWork(ctx, task.Task.ID, after, 100, revision)
		if err != nil {
			return false, err
		}
		for _, snapshot := range page.Items {
			published[snapshot.Item.ID] = true
		}
		if !page.More {
			break
		}
		if page.LastID <= after || len(page.Items) == 0 {
			return false, conflict("Backing Zone publication membership did not advance")
		}
		after = page.LastID
	}
	for _, item := range graph {
		if item.Operation != workexecution.Detach || published[item.ID] {
			continue
		}
		inputs, err := attachremoval.NewInputRepository(repository.store)
		if err != nil {
			return false, err
		}
		changed, source, err := inputs.CascadeSourceChange(ctx, workqueue.View{
			Task: task, Item: workqueue.ItemSnapshot{Item: item}, ReadRevision: revision,
		})
		if err != nil {
			return false, err
		}
		if changed {
			guards := append(append([]keyvalue.Condition(nil), authority.Conditions...), source.Conditions...)
			rejection, err := PrepareCascadeRejection(task, CascadeRejection{TaskID: task.Task.ID,
				ZoneID: task.Task.TargetID, WorkItemID: item.ID, Reason: CascadeSourceChanged,
				Outcome: workexecution.Failed, At: at}, guards)
			if err != nil {
				return false, err
			}
			defer keyvalue.ClearMutationValues(rejection.Mutations)
			result, err := repository.transactions.Commit(ctx, rejection)
			keyvalue.ClearValues(result.FailureReads)
			if err != nil {
				return false, err
			}
			if !result.Succeeded {
				return false, conflict("consumer publication changed before its source rejection")
			}
			return true, nil
		}
		change, err := inputs.PrepareCascadeConsumer(ctx, task, item, revision, authority.Conditions, at)
		if err != nil {
			return false, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		result, err := repository.transactions.Commit(ctx, change)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return false, err
		}
		if !result.Succeeded {
			return false, conflict("Backing Zone consumer changed before captured publication")
		}
		return true, nil
	}
	return false, nil
}
