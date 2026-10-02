package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/agentconfigjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// The completed startup inventory proves physical cleanup. The validated
// native upload receipt supplies the exact evidence, not a new capture.
func (pool *WorkerPool) completeAbsentConfigStage(ctx context.Context, assignment taskassignment.Assignment,
	step *agentpb.BackupStepAuthority, resume *agentpb.BackupCaptureResume, publisher *backupStepCheckpoint,
) error {
	verified := resume.GetUploadVerified()
	if verified == nil {
		return errs.New(errs.KindInternal, "Config cleanup lacks its verified upload")
	}
	binding := backupconfigtransfer.Binding{TaskID: assignment.TaskID, AssignmentID: assignment.AssignmentID,
		StepID: step.StepId, ExecutionID: step.ExecutionId, TransferID: step.ExecutionId,
		Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE}
	if err := publisher.publish(ctx, &agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_SourceCleanupCompleted{
			SourceCleanupCompleted: &agentpb.BackupSourceCleanupCompleted{
				PointId: verified.PointId, Evidence: proto.CloneOf(verified.Evidence),
			},
		},
	}); err != nil {
		return err
	}
	completion := &agentpb.BackupCaptureResume{
		PreparedArtifact:   proto.CloneOf(resume.PreparedArtifact),
		CheckpointSequence: publisher.sequence, PrecedingCheckpoint: proto.CloneOf(publisher.fence),
		Phase:  agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_SERVICE_RECOVERY,
		Cursor: proto.CloneOf(resume.Cursor),
		Checkpoint: &agentpb.BackupCaptureResume_SourceCleanupCompleted{
			SourceCleanupCompleted: proto.CloneOf(publisher.lastRequest.GetSourceCleanupCompleted()),
		},
	}
	return agentconfigjournal.ResumeCleanup(ctx, agentprotocol.StatePath+"/config-transfers/"+binding.TransferID,
		binding, step, &agentpb.BackupStepResume{StepId: step.StepId, ExecutionId: step.ExecutionId,
			Operation: &agentpb.BackupStepResume_Capture{Capture: completion}})
}
