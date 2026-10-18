package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type HierarchyAttachInput struct {
	Input      attachremoval.Input
	Conditions []keyvalue.Condition
}

func (repository *HierarchyDeletionRepository) loadHierarchyAttachSnapshot(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation, action hierarchydeletion.HierarchyDeletionAction, revision int64,
) (HierarchyAttachInput, error) {
	if action.ActionKind != hierarchydeletion.HierarchyDeletionAttachDeprovision || action.AgentProcedure == nil || revision <= 0 {
		return HierarchyAttachInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	key := attachremoval.FrozenKey(operation.Tombstone.OperationID, action.TargetID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return HierarchyAttachInput{}, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].Key != key || read.Values[0].ModRevision <= 0 {
		return HierarchyAttachInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	input, err := attachremoval.Decode(read.Values[0].Value)
	if err != nil {
		return HierarchyAttachInput{}, err
	}
	digest, err := attachremoval.Digest(input)
	if err != nil || digest != action.AgentProcedure.InputDigest ||
		input.SnapshotRevision != operation.Tombstone.SnapshotRevision || input.Attach.ID != action.TargetID ||
		input.AttachRevision != action.TargetRevision ||
		input.HookInputSet != nil || input.HookInputs != nil || input.HookOperationID != "" || !attachremoval.NeedsAgent(input) {
		attachremoval.Clear(&input)
		return HierarchyAttachInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return HierarchyAttachInput{Input: input, Conditions: []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}}, nil
}

// LoadHierarchyAttachInput binds native work to its frozen encrypted source. It
// neither publishes a child Task nor mutates the Attach to fabricate ownership.
func (repository *HierarchyDeletionRepository) LoadHierarchyAttachInput(ctx context.Context, view workqueue.View) (HierarchyAttachInput, error) {
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return HierarchyAttachInput{}, err
	}
	source, err := repository.loadHierarchyAttachSnapshot(ctx, captured.Operation, captured.Action, view.ReadRevision)
	if err != nil {
		return HierarchyAttachInput{}, err
	}
	source.Conditions = append(source.Conditions, captured.Conditions...)
	return source, nil
}

func hierarchyAttachClaims(work workexecution.Item, source attachremoval.Input) workexecution.Item {
	work.Claims = append(work.Claims, workexecution.Claim{ScopeID: source.Attach.BackingServiceID},
		workexecution.Claim{HostID: work.Placement.HostID, ScopeID: source.Attach.BackingServiceID})
	return work
}
