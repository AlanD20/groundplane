package networkwork

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareCascadeFinish checks captured graph membership independently of queue
// ordering: the first listed item need not be the bridge. Resource settlement
// already belongs to each typed work owner and is not repeated here.
func (repository *InputRepository) PrepareCascadeFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
) (workqueue.Change, error) {
	task := finished.Task.Task
	if task.Action != workexecution.ActionRemove || task.Scope.TenantID != "" || !task.PublicationComplete ||
		task.ExpectedItems == 0 || len(finished.Items) != int(task.ExpectedItems) {
		return workqueue.Change{}, conflict("Backing Zone completion requires its complete settled graph")
	}
	graph, change, err := repository.LoadCascadeGraph(ctx, finished.Task, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	rejection, evidence, proof, rejected, err := repository.LoadCascadeRejection(
		ctx,
		finished.Task,
		finished.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, proof.Conditions...)
	expected := make(map[string]workexecution.Item, len(graph))
	for _, item := range graph {
		expected[item.ID] = item
	}
	var bridge workqueue.ItemSnapshot
	for _, snapshot := range finished.Items {
		item := snapshot.Item
		captured, exists := expected[item.ID]
		if !exists || snapshot.Revision <= 0 || snapshot.Revision > finished.ReadRevision ||
			item.TaskID != task.ID || item.Operation != captured.Operation || item.TargetID != captured.TargetID ||
			item.Input == nil || captured.Input == nil || *item.Input != *captured.Input || item.Placement != captured.Placement ||
			!slices.Equal(item.Dependencies, captured.Dependencies) || !slices.Equal(item.Claims, captured.Claims) ||
			item.Phase != workexecution.Settled || item.Result == nil || item.ActiveAttemptID != "" ||
			item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
			return workqueue.Change{}, conflict("Backing Zone completion has changed or unresolved work")
		}
		delete(expected, item.ID)
		if rejected && (item.AttemptNumber != 0 || *item.Result != evidence || item.Outcome != rejection.Outcome ||
			item.Cleanup != workexecution.CleanupNone || item.CleanupResult != nil) {
			return workqueue.Change{}, conflict(
				"rejected Backing Zone publication differs from its saved no-execution result",
			)
		}
		if item.Operation == workexecution.Network {
			bridge = snapshot
		}
	}
	if len(expected) != 0 || bridge.Item.ID == "" {
		return workqueue.Change{}, conflict("Backing Zone completion is missing captured work")
	}
	input, conditions, err := repository.Load(
		ctx,
		workqueue.View{Task: finished.Task, Item: bridge, ReadRevision: finished.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if input.NetworkId != task.TargetID || input.EnvironmentId != task.Scope.EnvironmentID {
		return workqueue.Change{}, conflict("Backing Zone bridge result differs from its captured resource")
	}
	change.Conditions = append(change.Conditions, conditions...)
	if rejected {
		// The rejected-publication barrier already settled every consumer and
		// released this exact fence. Completion must not recreate or release it.
		change.Conditions = append(change.Conditions, keyvalue.Condition{
			Key: deletions.TombstoneKey(string(deletions.DeletionTargetZone), task.TargetID),
		})
	}
	// Every item transition updates its Task counters under this revision.
	// One Task CAS protects aggregate membership without a transaction whose
	// comparison count grows with the number of consumer removals.
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: finished.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(bridge.Item.ID), ModRevision: bridge.Revision})
	return change, nil
}
