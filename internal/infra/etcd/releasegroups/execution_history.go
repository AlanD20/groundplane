package releasegroups

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/storagemaintenance"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

const ExecutionHistoryKind = "release_group_preparation"

// ExecutionHistoryReference pins only the source view needed by unfinished
// member preparation. Captured rollback material has its own immutable lifetime;
// retaining diagnostic history must not retain old MVCC state indefinitely.
func ExecutionHistoryReference(key string, value []byte) (*storagemaintenance.Pin, error) {
	capture, err := recordcodec.Decode[ExecutionCapture](value, "release_group_execution")
	if err != nil {
		return nil, err
	}
	if capture.Validate() != nil || key != ExecutionCaptureKey(capture.Policy.TaskID) {
		return nil, recordcodec.CorruptRecord()
	}
	return &storagemaintenance.Pin{
		Kind:     ExecutionHistoryKind,
		OwnerID:  capture.Policy.TaskID,
		Revision: capture.SourceReadRevision,
	}, nil
}

func (repository *ExecutionRepository) PreparationHistoryHeld(
	ctx context.Context,
	pin *storagemaintenance.Pin,
	revision int64,
) (bool, error) {
	if pin == nil || pin.Kind != ExecutionHistoryKind || pin.Revision <= 0 {
		return false, recordcodec.CorruptRecord()
	}
	task, readRevision, err := repository.queue.ReadTask(ctx, pin.OwnerID, revision)
	if err != nil {
		return false, err
	}
	capture, err := repository.Load(ctx, task, readRevision)
	if err != nil {
		return false, err
	}
	if capture.Record.SourceReadRevision != pin.Revision {
		return false, recordcodec.CorruptRecord()
	}
	if task.Task.FinishedAt != nil || task.Task.CollectionStartedAt != nil {
		return false, nil
	}
	if !task.Task.PublicationComplete {
		return true, nil
	}
	for _, member := range capture.Record.Policy.Members {
		for _, id := range []string{member.ForwardID, member.RestoreID} {
			work, err := repository.queue.ReadWork(ctx, task.Task.ID, id, readRevision)
			if err != nil {
				return false, err
			}
			if work.Item.Item.Input == nil && work.Item.Item.Phase != workexecution.Settled {
				return true, nil
			}
		}
	}
	return false, nil
}
