package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type HierarchyEnvironmentInput struct {
	Input      hierarchydeletion.EnvironmentInput
	Conditions []keyvalue.Condition
}

func (repository *HierarchyDeletionRepository) LoadHierarchyEnvironmentInput(
	ctx context.Context,
	view workqueue.View,
) (HierarchyEnvironmentInput, error) {
	captured, err := repository.LoadHierarchyWork(ctx, view)
	if err != nil {
		return HierarchyEnvironmentInput{}, err
	}
	action := captured.Action
	if action.ActionKind != hierarchydeletion.HierarchyDeletionEnvironmentAgentCleanup || action.AgentProcedure == nil {
		return HierarchyEnvironmentInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	key := hierarchydeletion.EnvironmentInputKey(action.ParentOperationID, action.TargetID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return HierarchyEnvironmentInput{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return HierarchyEnvironmentInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	if hierarchydeletion.HierarchyDeletionBytesDigest(read.Values[0].Value) != action.AgentProcedure.InputDigest {
		return HierarchyEnvironmentInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	input, err := hierarchydeletion.DecodeEnvironmentInput(read.Values[0].Value)
	if err != nil {
		return HierarchyEnvironmentInput{}, err
	}
	if input.OperationID != action.ParentOperationID || input.EnvironmentID != action.TargetID ||
		input.EnvironmentRevision != action.TargetRevision ||
		input.SnapshotRevision != captured.Operation.Tombstone.SnapshotRevision {
		clear(input.ComposeArtifact)
		return HierarchyEnvironmentInput{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return HierarchyEnvironmentInput{
		Input:      input,
		Conditions: append(captured.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}),
	}, nil
}
