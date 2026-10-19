package attachremoval

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareCascadeConsumer joins one captured consumer, its Secret pins and its
// native queue membership. The Task remains unpublished throughout these bounded
// writes; no consumer command can execute while ownership is partially claimed.
func (repository *InputRepository) PrepareCascadeConsumer(ctx context.Context, task workqueue.TaskSnapshot,
	item workexecution.Item, revision int64, graphGuards []keyvalue.Condition, at time.Time,
) (_ workqueue.Change, resultErr error) {
	if task.Revision <= 0 || revision < task.Revision || task.Task.PublicationComplete || len(graphGuards) == 0 ||
		task.Task.Action != workexecution.ActionRemove || task.Task.Stop != nil || task.Task.FinishedAt != nil ||
		item.Operation != workexecution.Detach || item.TaskID != task.Task.ID || item.Input == nil {
		return workqueue.Change{}, checkpointConflict("Backing Zone consumer requires an open captured publication")
	}
	view := workqueue.View{Task: task, Item: workqueue.ItemSnapshot{Item: item}, ReadRevision: revision}
	capture, err := repository.LoadPreparedCascadeCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer capture.Clear()
	input, source := capture.Input, capture.Input.Snapshot.Source
	if input.Cascade == nil || input.Cascade.ZoneID != task.Task.TargetID ||
		!slices.Equal(item.Claims, input.Claims(item.Placement.HostID)) ||
		input.HasHostEffects() != (item.Placement.Executor == workexecution.Agent) {
		return workqueue.Change{}, checkpointConflict(
			"Backing Zone consumer differs from its captured placement or claims",
		)
	}
	fenceKey := deletions.TombstoneKey(string(deletions.DeletionTargetZone), task.Task.TargetID)
	read, err := repository.read(ctx, []string{fenceKey}, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, checkpointConflict("Backing Zone consumer publication lost its deletion fence")
	}
	fence, err := deletions.DecodeDeletionTombstone(read.Values[0].Value)
	if err != nil || fence.TaskID != task.Task.ID || fence.TargetID != task.Task.TargetID ||
		fence.TargetKind != deletions.DeletionTargetZone || fence.Phase != deletions.DeletionPhaseHostEffects ||
		!fence.CreatedAt.Equal(task.Task.CreatedAt) {
		return workqueue.Change{}, checkpointConflict("Backing Zone consumer publication has another deletion owner")
	}
	detaching, ownership, err := PrepareDetachClaim(ctx, repository.store, source, task.Task.ID, item.Placement.HostID)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() { keyvalue.ClearMutationValues(ownership.Mutations) }()
	exclusion, err := attachments.RequireAttachBackupSourceExclusionAbsent(
		ctx,
		repository.store,
		source.Attach.ID,
		revision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	desired, err := repository.prepareCascadeDesired(ctx, task, item, source.Attach, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() { keyvalue.ClearMutationValues(desired.Mutations) }()
	guards := append([]keyvalue.Condition(nil), graphGuards...)
	guards = append(guards, desired.Conditions...)
	guards = append(guards, capture.Conditions...)
	guards = append(guards, ownership.Conditions...)
	guards = append(guards, input.Snapshot.ConsumerConditions()...)
	guards = append(guards, exclusion,
		keyvalue.Condition{Key: fenceKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: attachments.AttachKey(source.Attach.ID), ModRevision: source.AttachRevision},
		keyvalue.Condition{Key: attachments.AttachFactsKey(source.Attach.ID), ModRevision: source.FactsRevision})
	change, err := repository.PrepareDetachPublicationCapture(ctx, task, item, input, capture.Capture, guards)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(change.Mutations)
		}
	}()
	raw, err := attachments.EncodeAttachRecord(detaching)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, ownership.Mutations...)
	change.Mutations = append(change.Mutations, desired.Mutations...)
	desired.Mutations = nil
	// Transfer ownership of bytes from the separately prepared claim.
	ownership.Mutations = nil
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: attachments.AttachKey(source.Attach.ID), Value: raw})
	queued, err := workqueue.PrepareItems(task, []workexecution.Item{item}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, queued.Conditions...)
	change.Mutations = append(change.Mutations, queued.Mutations...)
	return change, nil
}
