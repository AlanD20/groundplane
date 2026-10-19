package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (exchange *nativeExchange) SendVolumeFrame(
	ctx context.Context,
	binding backupvolumetransfer.Binding,
	frame *agentpb.BackupVolumeManifestTransfer,
) error {
	if binding.Direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW {
		return errs.New(errs.KindStateConflict, "Agent cannot upload a Restore source manifest")
	}
	owned, err := backupvolumetransfer.ValidateFrame(binding, frame)
	if err != nil {
		return err
	}
	return exchange.sendVolume(
		ctx,
		binding,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_BackupVolumeManifestTransfer{BackupVolumeManifestTransfer: owned},
		},
	)
}

func (exchange *nativeExchange) SendVolumeCredit(
	ctx context.Context,
	binding backupvolumetransfer.Binding,
	credit *agentpb.BackupVolumeManifestAckCredit,
) error {
	if binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW {
		return errs.New(errs.KindStateConflict, "Agent only acknowledges Restore source manifests")
	}
	if err := backupvolumetransfer.ValidateCredit(binding, credit); err != nil {
		return err
	}
	return exchange.sendVolume(
		ctx,
		binding,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_BackupVolumeManifestAckCredit{
				BackupVolumeManifestAckCredit: proto.CloneOf(credit),
			},
		},
	)
}

func (exchange *nativeExchange) sendVolume(
	ctx context.Context,
	binding backupvolumetransfer.Binding,
	message *agentpb.AgentMessage,
) error {
	slot, err := exchange.volumeSlot(binding)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-slot.done:
		return errs.New(errs.KindStateConflict, "Volume attempt ended")
	case <-exchange.done:
		return errs.New(errs.KindRequestFailed, "Agent stream closed")
	case exchange.outgoing <- message:
		return nil
	}
}
