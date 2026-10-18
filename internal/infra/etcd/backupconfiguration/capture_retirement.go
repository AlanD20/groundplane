package backupconfiguration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type captureRetirementReader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
}

// PrepareCaptureRetirement removes the snapshot owned exclusively by this Task.
// The caller must join the collecting Task and terminal resource-run guards.
// Native captures never share snapshots or add references after publication.
func PrepareCaptureRetirement(ctx context.Context, reader captureRetirementReader,
	taskID, environmentID, sourceID string, revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if reader == nil || revision <= 0 || ids.Validate(ids.KindTask, taskID) != nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindBackupSource, sourceID) != nil {
		return nil, nil, captureSnapshotConflict()
	}
	keys := []string{BackupConfigSnapshotKey(taskID), BackupConfigSnapshotTaskReferenceKey(taskID, taskID),
		BackupConfigSnapshotReferenceTaskKey(taskID, taskID)}
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, nil, captureSnapshotConflict()
	}
	defer keyvalue.ClearValues(read.Values)
	guards := make([]keyvalue.Condition, len(keys))
	mutations := make([]keyvalue.Mutation, 0, 6)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return nil, nil, captureSnapshotConflict()
		}
		guards[index] = keyvalue.Condition{Key: keys[index], ModRevision: value.ModRevision}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[index]})
	}
	record, err := DecodeBackupConfigSnapshotRecord(read.Values[0].Value)
	if err != nil || record.SnapshotID != taskID || record.EnvironmentID != environmentID || record.SourceID != sourceID ||
		string(read.Values[1].Value) != taskID || string(read.Values[2].Value) != taskID {
		return nil, nil, captureSnapshotConflict()
	}
	for _, prefix := range []string{backupConfigSnapshotTaskReferencePrefix(taskID), backupConfigSnapshotReferenceTaskPrefix(taskID)} {
		page, err := reader.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 2, Revision: revision})
		if err != nil {
			return nil, nil, err
		}
		if page == nil {
			return nil, nil, captureSnapshotConflict()
		}
		valid := page.ReadRevision == revision && !page.More && len(page.Values) == 1 &&
			page.Values[0].Key == prefix+taskID && string(page.Values[0].Value) == taskID
		keyvalue.ClearRangeValues(page.Values)
		if !valid {
			return nil, nil, captureSnapshotConflict()
		}
	}
	for _, prefix := range []string{backupConfigSnapshotEntryPrefix(taskID),
		backupConfigSnapshotDescriptorPrefix + taskID + "/", backupConfigSnapshotValuePrefix + taskID + "/"} {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: prefix, Prefix: true})
	}
	return guards, mutations, nil
}
