package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CascadeBridgeAdmission proves every consumer removal settled before the final
// bridge operation. The cursor is updated in the same transaction that removes
// each binding; dependency scheduling alone is not resource-removal evidence.
func CascadeBridgeAdmission(ctx context.Context, store Store, view workqueue.View) ([]keyvalue.Condition, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionRemove || task.Scope.TenantID != "" || task.ExpectedItems <= 1 {
		return nil, nil
	}
	if item.Operation != workexecution.Network || item.TargetID != task.TargetID || item.TaskID != task.ID ||
		len(item.Dependencies) != 1 || item.Input == nil {
		return nil, errs.New(errs.KindStateConflict, "Backing Zone bridge does not identify its final consumer removal")
	}
	repository, err := NewInputRepository(store)
	if err != nil {
		return nil, err
	}
	key := CascadeProgressKey(task.ID)
	fenceKey := deletions.TombstoneKey(string(deletions.DeletionTargetZone), task.TargetID)
	read, err := repository.read(ctx, []string{key, fenceKey}, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return nil, errs.New(errs.KindStateConflict, "Backing Zone removal has lost its progress or deletion fence")
	}
	progress, err := decodeCascadeProgress(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	fence, err := deletions.DecodeDeletionTombstone(read.Values[1].Value)
	if err != nil || fence.TargetKind != deletions.DeletionTargetZone || fence.TargetID != task.TargetID ||
		fence.TaskID != task.ID || fence.Phase != deletions.DeletionPhaseHostEffects ||
		!fence.CreatedAt.Equal(task.CreatedAt) || progress.TaskID != task.ID || progress.ZoneID != task.TargetID ||
		progress.HostID != item.Placement.HostID || progress.Count+1 != task.ExpectedItems ||
		progress.Completed != progress.Count || progress.LastWorkID != item.Dependencies[0] {
		return nil, errs.New(errs.KindStateConflict, "Backing Zone still has unfinished consumer removals")
	}
	previous, err := repository.transactions.ReadWork(ctx, task.ID, progress.LastWorkID, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	last := previous.Item.Item
	if last.Operation != workexecution.Detach || last.Phase != workexecution.Settled ||
		last.Outcome != workexecution.Succeeded ||
		last.ActiveAttemptID != "" ||
		last.Result == nil ||
		last.Input == nil ||
		*last.Input != *progress.LastInput ||
		last.Placement.HostID != progress.HostID ||
		last.Cleanup != workexecution.CleanupNone && last.Cleanup != workexecution.CleanupComplete {
		return nil, errs.New(
			errs.KindStateConflict,
			"Backing Zone final consumer removal has no successful settled result",
		)
	}
	return []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision},
		{Key: fenceKey, ModRevision: read.Values[1].ModRevision},
		{Key: workqueue.ItemKey(last.ID), ModRevision: previous.Item.Revision}}, nil
}
