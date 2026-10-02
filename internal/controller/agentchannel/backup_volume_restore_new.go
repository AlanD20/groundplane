package agentchannel

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (server *Server) sendBackupVolumeRestoreNew(stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment, assignment *agentpb.TaskAssignment, stepID string,
	exchange *configTransferExchange,
) error {
	if server.checkpoints == nil || exchange == nil || exchange.volumes == nil {
		return errs.New(errs.KindInternal, "Volume Restore manifest sender is unavailable")
	}
	step := backupVolumeStep(assignment.BackupAuthority, stepID)
	if step == nil || step.GetRestore().GetVolume() == nil {
		return errs.New(errs.KindStateConflict, "Volume Restore step is unavailable")
	}
	direction := agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW
	transferID, err := backupvolumetransfer.TransferID(step.ExecutionId, direction)
	if err != nil {
		return err
	}
	binding := backupvolumetransfer.Binding{TaskID: assignment.TaskId, AssignmentID: assignment.AssignmentId,
		StepID: stepID, TransferID: transferID, Direction: direction}
	copy(binding.AuthorityDigest[:], step.StepDigest)
	complete, err := server.checkpoints.ReadRestoreNewVolumeManifest(stream.Context(),
		claim.Assignment.Record.AgentID, claim.Assignment.Record.AgentGeneration,
		assignment.TaskId, assignment.AssignmentId, stepID)
	if err != nil {
		return err
	}
	if complete.Source == nil || complete.Start.PointId != step.GetRestore().PointId ||
		!bytes.Equal(
			complete.Archive.ContentManifestSHA256[:],
			step.GetRestore().GetVolume().Archive.ContentManifestSha256,
		) ||
		!bytes.Equal(complete.Archive.FullTreeSHA256[:], step.GetRestore().GetVolume().Archive.FullTreeSha256) {
		return errs.New(errs.KindStateConflict, "Volume Restore source manifest differs from sealed Point")
	}
	evidence := backupvolume.ArtifactEvidence{Source: *complete.Source, Archive: complete.Archive}
	frames, err := backupvolumetransfer.BuildFrames(binding, step.GetRestore().PointId,
		step.ExecutionId, agentpb.BackupVolumeManifestRole_BACKUP_VOLUME_MANIFEST_ROLE_RESTORE_NEW,
		complete.Entries, &evidence)
	if err != nil {
		return err
	}
	credit, err := exchange.volumes.readCredit(stream.Context(), transferID)
	if err != nil {
		return err
	}
	index, err := frames.ResumeIndex(binding, credit)
	if err != nil {
		return err
	}
	for ; index < len(frames.Frames); index++ {
		frame := frames.Frames[index]
		if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupVolumeManifestTransfer{
			BackupVolumeManifestTransfer: frame}}); err != nil {
			return err
		}
		credit, err = exchange.volumes.readCredit(stream.Context(), transferID)
		if err != nil {
			return err
		}
		if backupvolumetransfer.ValidateCredit(binding, credit) != nil ||
			credit.CommittedRecordSequence != frame.RecordSequence || credit.NextOrdinal != frames.Next[index] ||
			!bytes.Equal(credit.TransferChainSha256, frames.Chains[index][:]) {
			return errs.New(errs.KindStateConflict, "Volume Restore manifest acknowledgement differs from sent frame")
		}
	}
	return nil
}
