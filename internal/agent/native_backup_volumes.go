package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type nativeVolumeSlot struct {
	binding backupvolumetransfer.Binding
	frames  chan *agentpb.BackupVolumeManifestTransfer
	credits chan *agentpb.BackupVolumeManifestAckCredit
	done    chan struct{}
}

func (exchange *nativeExchange) RegisterBackupVolumes(assignment *agentpb.BackupWorkAssignment) error {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed {
		return errs.New(errs.KindStateConflict, "Backup Volume session is closed")
	}
	owned, _, err := executionplan.OpenBackupWorkAssignment(assignment, exchange.identity)
	if err != nil {
		return err
	}
	slots := make(map[string]*nativeVolumeSlot)
	for ordinal, step := range owned.Input.Steps {
		var directions []agentpb.BackupVolumeManifestDirection
		if step.GetCapture().GetVolume() != nil {
			directions = append(directions, agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE)
		}
		if step.GetRestore().GetVolume() != nil {
			directions = append(directions, agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW, agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
		}
		for _, direction := range directions {
			binding, err := backupvolumetransfer.Bind(owned, exchange.identity, uint32(ordinal), direction)
			if err != nil {
				return err
			}
			if exchange.backupVolumes[binding.TransferID] != nil || slots[binding.TransferID] != nil {
				return errs.New(errs.KindStateConflict, "Backup Volume transfer is already registered")
			}
			slots[binding.TransferID] = &nativeVolumeSlot{binding: binding, frames: make(chan *agentpb.BackupVolumeManifestTransfer, 1), credits: make(chan *agentpb.BackupVolumeManifestAckCredit, 1), done: make(chan struct{})}
		}
	}
	for id, slot := range slots {
		exchange.backupVolumes[id] = slot
	}
	return nil
}

func (exchange *nativeExchange) ReleaseBackupVolumes(attemptID string) {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	for id, slot := range exchange.backupVolumes {
		if slot.binding.Attempt.ID == attemptID {
			close(slot.done)
			delete(exchange.backupVolumes, id)
		}
	}
}

func (exchange *nativeExchange) acceptBackupVolume(message *agentpb.ControllerMessage) (bool, error) {
	frame, credit := message.GetBackupVolumeManifestTransfer(), message.GetBackupVolumeManifestAckCredit()
	if frame == nil && credit == nil {
		return false, nil
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	id := credit.GetTransferId()
	if frame != nil {
		id = frame.TransferId
	}
	slot := exchange.backupVolumes[id]
	if exchange.closed || slot == nil || executionplan.RejectUnknown(message) != nil {
		return true, errs.New(errs.KindStateConflict, "Volume transfer has no active native attempt")
	}
	if frame != nil {
		if slot.binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW {
			return true, errs.New(errs.KindStateConflict, "Volume frame is not a Restore source")
		}
		owned, err := backupvolumetransfer.ValidateFrame(slot.binding, frame)
		if err != nil {
			return true, err
		}
		select {
		case slot.frames <- owned:
			return true, nil
		default:
		}
	} else {
		if slot.binding.Direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW {
			return true, errs.New(errs.KindStateConflict, "Volume credit is not for an upload")
		}
		if err := backupvolumetransfer.ValidateCredit(slot.binding, credit); err != nil {
			return true, err
		}
		select {
		case slot.credits <- proto.CloneOf(credit):
			return true, nil
		default:
		}
	}
	return true, errs.New(errs.KindStateConflict, "Volume transfer exceeds its bounded inbox")
}

func (exchange *nativeExchange) volumeSlot(binding backupvolumetransfer.Binding) (*nativeVolumeSlot, error) {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	slot := exchange.backupVolumes[binding.TransferID]
	if exchange.closed || slot == nil || slot.binding != binding {
		return nil, errs.New(errs.KindStateConflict, "Volume transfer binding is no longer active")
	}
	return slot, nil
}

func (exchange *nativeExchange) ReceiveVolumeCredit(ctx context.Context, binding backupvolumetransfer.Binding) (*agentpb.BackupVolumeManifestAckCredit, error) {
	slot, err := exchange.volumeSlot(binding)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-slot.done:
		return nil, errs.New(errs.KindStateConflict, "Volume attempt ended")
	case <-exchange.done:
		return nil, errs.New(errs.KindRequestFailed, "Agent stream closed")
	case credit := <-slot.credits:
		return credit, nil
	}
}

func (exchange *nativeExchange) ReceiveVolumeFrame(ctx context.Context, binding backupvolumetransfer.Binding) (*agentpb.BackupVolumeManifestTransfer, error) {
	slot, err := exchange.volumeSlot(binding)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-slot.done:
		return nil, errs.New(errs.KindStateConflict, "Volume attempt ended")
	case <-exchange.done:
		return nil, errs.New(errs.KindRequestFailed, "Agent stream closed")
	case frame := <-slot.frames:
		return frame, nil
	}
}
