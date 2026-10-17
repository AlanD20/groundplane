package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type cascadeProgress struct {
	TaskID     string               `json:"task_id"`
	ZoneID     string               `json:"zone_id"`
	HostID     string               `json:"host_id"`
	Count      uint32               `json:"count"`
	Completed  uint32               `json:"completed"`
	LastWorkID string               `json:"last_work_id,omitempty"`
	LastInput  *workexecution.Input `json:"last_input,omitempty"`
}

func CascadeProgressKey(taskID string) string {
	return "/v1/private/backing-zone-removals/" + taskID + "/progress"
}

func decodeCascadeProgress(raw []byte) (cascadeProgress, error) {
	progress, err := recordcodec.Decode[cascadeProgress](raw, "backing-zone-removal-progress")
	if err != nil || ids.Validate(ids.KindTask, progress.TaskID) != nil || ids.Validate(ids.KindNetwork, progress.ZoneID) != nil ||
		ids.Validate(ids.KindHost, progress.HostID) != nil || progress.Count == 0 ||
		uint64(progress.Count) >= uint64(workexecution.MaximumTaskItems) || progress.Completed > progress.Count ||
		progress.Completed == 0 && (progress.LastWorkID != "" || progress.LastInput != nil) ||
		progress.Completed != 0 && (ids.Validate(ids.KindWorkItem, progress.LastWorkID) != nil || progress.LastInput == nil || progress.LastInput.Validate() != nil) {
		return cascadeProgress{}, recordcodec.CorruptRecord()
	}
	return progress, nil
}

// PrepareCascadeProgress joins initial removal publication. It is bounded
// operation authority, not a stream of copied snapshots or diagnostic events.
func PrepareCascadeProgress(task workexecution.Task, hostID string, count uint32) (workqueue.Change, error) {
	if task.Validate() != nil || task.Action != workexecution.ActionRemove || task.Scope.TenantID != "" ||
		task.Scope.ProjectID == "" || task.Scope.EnvironmentID == "" || task.PublicationComplete ||
		task.FinishedAt != nil || task.Stop != nil || task.CollectionStartedAt != nil ||
		count == 0 || uint64(count)+1 != uint64(task.ExpectedItems) || ids.Validate(ids.KindHost, hostID) != nil ||
		ids.Validate(ids.KindNetwork, task.TargetID) != nil {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Backing Zone progress requires its unpublished removal graph")
	}
	raw, err := recordcodec.Encode("backing-zone-removal-progress", cascadeProgress{
		TaskID: task.ID, ZoneID: task.TargetID, HostID: hostID, Count: count})
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: CascadeProgressKey(task.ID)}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: CascadeProgressKey(task.ID), Value: raw}}}, nil
}

func (repository *InputRepository) cascadeStepAdmission(ctx context.Context, view workqueue.View, captured DetachInput) (cascadeProgress, []keyvalue.Condition, error) {
	step, task, item := captured.Cascade, view.Task.Task, view.Item.Item
	if step == nil || step.Validate(captured.Snapshot) != nil || task.Action != workexecution.ActionRemove ||
		task.TargetID != step.ZoneID || task.ExpectedItems != step.Count+1 || task.Scope.TenantID != "" ||
		task.Scope.EnvironmentID != captured.Snapshot.Source.Attach.BackingEnvironmentID ||
		task.Scope.ProjectID != captured.Snapshot.Source.Attach.BackingProjectID || item.Input == nil ||
		item.ID != ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "backing-zone-detach/"+item.TargetID) {
		return cascadeProgress{}, nil, errs.New(errs.KindStateConflict, "Detach does not belong to this frozen Backing Zone removal")
	}
	key := CascadeProgressKey(task.ID)
	fenceKey := deletions.TombstoneKey(string(deletions.DeletionTargetZone), step.ZoneID)
	read, err := repository.read(ctx, []string{key, fenceKey}, view.ReadRevision)
	if err != nil {
		return cascadeProgress{}, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return cascadeProgress{}, nil, errs.New(errs.KindStateConflict, "Backing Zone removal has lost its cursor or deletion fence")
	}
	progress, err := decodeCascadeProgress(read.Values[0].Value)
	if err != nil {
		return cascadeProgress{}, nil, err
	}
	fence, err := deletions.DecodeDeletionTombstone(read.Values[1].Value)
	if err != nil || fence.TargetKind != deletions.DeletionTargetZone || fence.TargetID != step.ZoneID || fence.TaskID != task.ID ||
		fence.Phase != deletions.DeletionPhaseHostEffects || !fence.CreatedAt.Equal(task.CreatedAt) ||
		progress.TaskID != task.ID || progress.ZoneID != step.ZoneID || progress.HostID != item.Placement.HostID ||
		progress.Count != step.Count || progress.Completed != step.Ordinal {
		return cascadeProgress{}, nil, errs.New(errs.KindStateConflict, "Backing Zone removal has not completed this Detach's predecessors")
	}
	guards := []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision},
		{Key: fenceKey, ModRevision: read.Values[1].ModRevision}}
	if step.Ordinal == 0 {
		if len(item.Dependencies) != 0 {
			return cascadeProgress{}, nil, recordcodec.CorruptRecord()
		}
		return progress, guards, nil
	}
	if len(item.Dependencies) != 1 || item.Dependencies[0] != progress.LastWorkID {
		return cascadeProgress{}, nil, recordcodec.CorruptRecord()
	}
	previous, err := repository.transactions.ReadWork(ctx, task.ID, progress.LastWorkID, view.ReadRevision)
	if err != nil {
		return cascadeProgress{}, nil, err
	}
	predecessor := previous.Item.Item
	if predecessor.Operation != workexecution.Detach || predecessor.Phase != workexecution.Settled ||
		predecessor.Outcome != workexecution.Succeeded || predecessor.ActiveAttemptID != "" || predecessor.Result == nil ||
		predecessor.Input == nil || *predecessor.Input != *progress.LastInput || predecessor.Placement.HostID != progress.HostID ||
		predecessor.Cleanup != workexecution.CleanupNone && predecessor.Cleanup != workexecution.CleanupComplete {
		return cascadeProgress{}, nil, errs.New(errs.KindStateConflict, "Backing Zone predecessor has no settled successful Detach result")
	}
	guards = append(guards, keyvalue.Condition{Key: workqueue.ItemKey(predecessor.ID), ModRevision: previous.Item.Revision})
	return progress, guards, nil
}

// prepareCascadeAdvance must join binding removal and native successful work
// settlement. Failure or inspection never advances the cursor, even when the
// interrupted command is proven stopped.
func (repository *InputRepository) prepareCascadeAdvance(ctx context.Context, view workqueue.View, captured DetachInput) (workqueue.Change, error) {
	if captured.Cascade == nil {
		return workqueue.Change{}, nil
	}
	progress, conditions, err := repository.cascadeStepAdmission(ctx, view, captured)
	if err != nil {
		return workqueue.Change{}, err
	}
	progress.Completed++
	progress.LastWorkID = view.Item.Item.ID
	input := *view.Item.Item.Input
	progress.LastInput = &input
	raw, err := recordcodec.Encode("backing-zone-removal-progress", progress)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: conditions,
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: CascadeProgressKey(progress.TaskID), Value: raw}}}, nil
}
