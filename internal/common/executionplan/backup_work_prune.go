package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// AdvanceBackupPrune accepts only absence of the exact captured object. Local
// Recovery Point ownership is released by the Controller in the same commit as
// this cursor and the receipt, never solely on an Agent completion message.
func AdvanceBackupPrune(assignment *agentpb.BackupWorkAssignment,
	request *agentpb.BackupWorkCheckpointRequest, authenticated workexecution.Identity,
) (*agentpb.BackupStepResume, error) {
	owned, _, err := OpenBackupWorkAssignment(assignment, authenticated)
	if err != nil {
		return nil, err
	}
	report, err := ValidateBackupWorkCheckpoint(request, owned.Input, authenticated)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(report.Attempt, owned.Attempt) ||
		owned.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE {
		return nil, invalidBackupCheckpoint("prune report belongs to another operation")
	}
	step := owned.Input.Steps[report.SourceOrdinal]
	current := owned.Sources[report.SourceOrdinal].GetPrune()
	deleted := report.GetPruneObjectDeleted()
	if current == nil || current.CheckpointSequence != 0 || report.CheckpointSequence != 1 ||
		report.PrecedingCheckpoint != nil || deleted == nil || len(step.GetPrune().GetObjects()) != 1 {
		return nil, invalidBackupCheckpoint("prune report does not follow saved progress")
	}
	object := step.GetPrune().Objects[0]
	if deleted.Ordinal != object.Ordinal || deleted.PointId != object.PointId || !proto.Equal(deleted.Object, object.Object) ||
		current.NextObjectOrdinal != object.Ordinal || object.Ordinal == ^uint32(0) {
		return nil, invalidBackupCheckpoint("prune report names a different captured object")
	}
	return &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
		Operation: &agentpb.BackupStepResume_Prune{Prune: &agentpb.BackupPruneResume{
			CheckpointSequence: 1, NextObjectOrdinal: object.Ordinal + 1,
			Checkpoint: &agentpb.BackupPruneResume_ObjectDeleted{ObjectDeleted: proto.CloneOf(deleted)},
		}}}, nil
}
