package networkwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareCascadePublication publishes only the Task and captured graph root.
// Work stays behind PublicationComplete until the publisher stages every exact
// item. The caller joins consumer capture authority, the Zone fence and authored
// omission in the same acceptance transaction as this change.
func (repository *InputRepository) PrepareCascadePublication(ctx context.Context, task workexecution.Task,
	graph []workexecution.Item, input *agentpb.NetworkWorkInput, marker idempotency.IdempotencyMarker,
	admission []keyvalue.Condition, desiredRevision string,
) (_ workqueue.Change, resultErr error) {
	if task.Scope.TenantID != "" || len(graph) == 0 || task.ExpectedItems != uint32(len(graph)) {
		return workqueue.Change{}, conflict("Backing Zone publication requires its complete captured work graph")
	}
	if err := workexecution.ValidateGraph(task, graph); err != nil {
		return workqueue.Change{}, err
	}
	bridge := graph[len(graph)-1]
	change, err := workqueue.PrepareTask(task, marker)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(change.Mutations)
		}
	}()
	captured, err := repository.prepareRemovalInput(ctx, task, bridge, input, marker, admission, desiredRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, captured.Conditions...)
	change.Mutations = append(change.Mutations, captured.Mutations...)
	sealed, err := repository.StageCascadeGraph(ctx, task, graph, admission)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, sealed.Conditions...)
	change.Mutations = append(change.Mutations, sealed.Mutations...)
	if len(graph) > 1 {
		progress, err := attachremoval.PrepareCascadeProgress(task, bridge.Placement.HostID, uint32(len(graph)-1))
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, progress.Conditions...)
		change.Mutations = append(change.Mutations, progress.Mutations...)
	}
	return change, nil
}
