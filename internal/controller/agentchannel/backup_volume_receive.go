package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// receiveBackupVolumeManifest handles capture and stopped-old-tree uploads.
// Both use the same bounded credit protocol, with distinct captured directions.
func (server *Server) receiveBackupVolumeManifest(stream agentpb.AgentChannel_ConnectServer,
	authorization agentregistration.WorkAuthorization, assignment *agentpb.BackupWorkAssignment,
	ordinal uint32, direction agentpb.BackupVolumeManifestDirection, exchange *volumeManifestExchange,
) error {
	if server.backupWorkReports == nil || exchange == nil ||
		(direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE &&
			direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD) {
		return errs.New(errs.KindStateConflict, "Volume manifest receiver is unavailable")
	}
	binding, err := backupvolumetransfer.Bind(assignment, authorization.Identity(), ordinal, direction)
	if err != nil {
		return err
	}
	return server.receiveBoundVolumeManifest(stream, authorization, assignment.Attempt, binding, exchange)
}

func (server *Server) receiveRecoveryVolumeManifest(stream agentpb.AgentChannel_ConnectServer,
	authorization agentregistration.WorkAuthorization, assignment *agentpb.BackupRecoveryAssignment,
	ordinal uint32, exchange *volumeManifestExchange,
) error {
	if server.backupWorkReports == nil || exchange == nil {
		return errs.New(errs.KindStateConflict, "Volume recovery manifest receiver is unavailable")
	}
	binding, err := backupvolumetransfer.BindRecoverySource(assignment, authorization.Identity(), ordinal,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
	if err != nil {
		return err
	}
	return server.receiveBoundVolumeManifest(stream, authorization, assignment.Attempt, binding, exchange)
}

func (server *Server) receiveBoundVolumeManifest(stream agentpb.AgentChannel_ConnectServer,
	authorization agentregistration.WorkAuthorization, attempt *agentpb.WorkAttemptIdentity,
	binding backupvolumetransfer.Binding, exchange *volumeManifestExchange,
) error {
	credit, complete, err := server.backupWorkReports.BeginVolumeManifest(stream.Context(), authorization, attempt, binding.SourceOrdinal, binding.Direction)
	if err != nil {
		return err
	}
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupVolumeManifestAckCredit{BackupVolumeManifestAckCredit: credit}}); err != nil {
		return err
	}
	for !complete {
		frame, err := exchange.readFrame(stream.Context(), binding.TransferID)
		if err != nil {
			return err
		}
		credit, err = server.backupWorkReports.AcceptVolumeManifestFrame(stream.Context(), authorization, attempt, frame)
		if err != nil {
			return err
		}
		if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupVolumeManifestAckCredit{BackupVolumeManifestAckCredit: credit}}); err != nil {
			return err
		}
		complete = frame.GetEnd() != nil
	}
	return nil
}
