package backupwork

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// VolumeRecoverySource is one MVCC snapshot used to classify retained files.
// Its conditions must accompany publication of the resulting disposition.
// Reading it alone grants neither replay nor cleanup.
type VolumeRecoverySource struct {
	View       workqueue.ResumeView
	Input      *agentpb.BackupWorkInput
	Sources    []*agentpb.BackupStepResume
	Restore    backupruntime.BackupRestoreRecord
	Revision   int64
	Conditions []keyvalue.Condition
}

func (repository *InputRepository) ReadVolumeRecoverySource(ctx context.Context, queue *workqueue.Repository,
	identity workexecution.Identity, entry *agentpb.BackupRecoveredVolumeRestore,
) (VolumeRecoverySource, error) {
	var zero VolumeRecoverySource
	if queue == nil || identity.Validate() != nil || identity.Executor != workexecution.Agent ||
		executionplan.ValidateBackupStagingInventory(&agentpb.BackupStagingInventory{
			VolumeRestores: []*agentpb.BackupRecoveredVolumeRestore{entry},
		}) != nil {
		return zero, conflict("Volume recovery requires a valid inventory and authenticated host")
	}
	wire := entry.Attempt
	if wire.HostId != identity.HostID || wire.AgentId != identity.AgentID {
		return zero, conflict("Volume recovery inventory belongs to a different host or Agent")
	}
	original := workexecution.Identity{Executor: workexecution.Agent, HostID: wire.HostId,
		AgentID: wire.AgentId, Generation: wire.ExecutorGeneration}
	reported, err := workexecution.DecodeAgentAttempt(wire, original)
	if err != nil {
		return zero, err
	}
	view, err := queue.ReadResume(ctx, reported.TaskID, reported.WorkItemID, reported.ID, original)
	if err != nil {
		return zero, err
	}
	if !workexecution.SameAssignment(view.Attempt.Attempt, reported) || reported.Direction != workexecution.Forward ||
		view.Work.Task.Task.CollectionStartedAt != nil {
		return zero, conflict("Volume recovery has no retained original execution authority")
	}
	input, inputGuard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return zero, err
	}
	if input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE ||
		uint64(entry.SourceOrdinal) >= uint64(len(input.Steps)) {
		return zero, conflict("Volume recovery source is not in the captured Restore")
	}
	step := input.Steps[entry.SourceOrdinal]
	if step.GetRestore().GetVolume() == nil || step.GetRestore().PointId != entry.PointId ||
		step.ExecutionId != entry.RestoreGenerationId || !bytes.Equal(step.StepDigest, entry.AuthoritySha256) {
		return zero, conflict("Volume recovery inventory differs from the captured source")
	}
	sources, progressGuard, err := repository.LoadProgress(ctx, view.Work, input)
	if err != nil {
		return zero, err
	}
	key := backupruntime.BackupRestoreKey(reported.TaskID)
	read, err := repository.readSettlementKeys(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return zero, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return zero, conflict("Volume recovery resource record is missing")
	}
	restore, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil {
		return zero, err
	}
	if restore.TaskID != reported.TaskID || restore.RestoreGenerationID != entry.RestoreGenerationId {
		return zero, conflict("Volume recovery resource belongs to another execution")
	}
	if err := backupruntime.ValidateRestoreWorkInput(restore, input); err != nil {
		return zero, err
	}
	return VolumeRecoverySource{View: view, Input: input, Sources: sources, Restore: restore,
		Revision: read.Values[0].ModRevision,
		Conditions: []keyvalue.Condition{
			inputGuard, progressGuard,
			{Key: key, ModRevision: read.Values[0].ModRevision},
			{Key: workqueue.TaskKey(reported.TaskID), ModRevision: view.Work.Task.Revision},
			{Key: workqueue.ItemKey(reported.WorkItemID), ModRevision: view.Work.Item.Revision},
			{Key: workqueue.AttemptKey(reported.ID), ModRevision: view.Attempt.Revision},
			{Key: workqueue.ExecutorKey(original), ModRevision: view.ExecutorRevision},
		}}, nil
}
