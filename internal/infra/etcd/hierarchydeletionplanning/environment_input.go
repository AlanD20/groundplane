package hierarchydeletionplanning

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Planner) captureEnvironmentInput(ctx context.Context, operation hierarchydeletion.HierarchyDeletionTombstone, environmentID string, environmentRevision int64, artifact []byte) (string, error) {
	source, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{hierarchy.EnvironmentKey(environmentID)}, Revision: operation.SnapshotRevision})
	if err != nil {
		return "", err
	}
	if source == nil || source.ReadRevision != operation.SnapshotRevision || len(source.Values) != 1 || source.Values[0] == nil || source.Values[0].ModRevision != environmentRevision {
		if source != nil {
			keyvalue.ClearValues(source.Values)
		}
		return "", hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(source.Values)
	environment, err := hierarchy.DecodeEnvironment(source.Values[0].Value)
	if err != nil || environment.ID != environmentID {
		return "", hierarchydeletion.CorruptHierarchyDeletion()
	}
	input := hierarchydeletion.EnvironmentInput{OperationID: operation.OperationID, SnapshotRevision: operation.SnapshotRevision,
		EnvironmentID: environmentID, EnvironmentRevision: environmentRevision, VolumeDirectory: environment.VolumeDir, ComposeArtifact: artifact}
	value, err := hierarchydeletion.EncodeEnvironmentInput(input)
	if err != nil {
		return "", err
	}
	defer clear(value)
	key := hierarchydeletion.EnvironmentInputKey(operation.OperationID, environmentID)
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(string(operation.TargetKind), operation.TargetID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key, tombstoneKey}})
	if err != nil {
		return "", err
	}
	if read == nil || len(read.Values) != 2 || read.Values[1] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return "", hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	digest := hierarchydeletion.HierarchyDeletionBytesDigest(value)
	if read.Values[0] != nil {
		if !bytes.Equal(read.Values[0].Value, value) {
			return "", hierarchydeletion.CorruptHierarchyDeletion()
		}
		return digest, nil
	}
	var current hierarchydeletion.HierarchyDeletionTombstone
	if hierarchydeletion.DecodeHierarchyDeletionRecord(read.Values[1].Value, hierarchydeletion.HierarchyDeletionLargeRecordBytes, &current) != nil ||
		current.OperationID != operation.OperationID || current.SnapshotRevision != operation.SnapshotRevision || current.Phase != hierarchydeletion.HierarchyDeletionPlanning {
		return "", hierarchydeletion.CorruptHierarchyDeletion()
	}
	result, err := repository.store.Transact(ctx, []keyvalue.Condition{{Key: key}, {Key: tombstoneKey, ModRevision: read.Values[1].ModRevision}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}})
	if err != nil {
		return "", err
	}
	keyvalue.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return "", errs.New(errs.KindStateConflict, "removal Environment input changed during publication")
	}
	return digest, nil
}
