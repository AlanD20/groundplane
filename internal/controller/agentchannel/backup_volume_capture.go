package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (server *Server) receiveBackupVolumeCapture(stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment, assignment *agentpb.TaskAssignment, stepID string,
	exchange *configTransferExchange,
) error {
	if server.checkpoints == nil || exchange == nil || exchange.volumes == nil {
		return errs.New(errs.KindInternal, "Volume manifest receiver is unavailable")
	}
	step := backupVolumeStep(assignment.BackupAuthority, stepID)
	if step == nil || step.GetCapture().GetVolume() == nil {
		return errs.New(errs.KindStateConflict, "Volume capture step is unavailable")
	}
	direction := agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE
	transferID, err := backupvolumetransfer.TransferID(step.ExecutionId, direction)
	if err != nil {
		return err
	}
	agentID, generation := claim.Assignment.Record.AgentID, claim.Assignment.Record.AgentGeneration
	credit, complete, err := server.checkpoints.BeginVolumeManifest(stream.Context(), agentID, generation,
		assignment.TaskId, assignment.AssignmentId, stepID, direction)
	if err != nil {
		return err
	}
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupVolumeManifestAckCredit{
		BackupVolumeManifestAckCredit: credit}}); err != nil {
		return err
	}
	for !complete {
		frame, err := exchange.volumes.readFrame(stream.Context(), transferID)
		if err != nil {
			return err
		}
		credit, err = server.checkpoints.AcceptVolumeManifestFrame(stream.Context(), agentID, generation, frame)
		if err != nil {
			return err
		}
		if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupVolumeManifestAckCredit{
			BackupVolumeManifestAckCredit: credit}}); err != nil {
			return err
		}
		complete = frame.GetEnd() != nil
	}
	return nil
}

func backupVolumeStep(authority *agentpb.BackupTaskAuthority, stepID string) *agentpb.BackupStepAuthority {
	for _, step := range authority.GetSteps() {
		if step.StepId == stepID {
			return step
		}
	}
	return nil
}
