package agentchannel

import (
	"context"
	"crypto/sha256"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type volumeManifestSlot struct {
	binding backupvolumetransfer.Binding
	frames  chan *agentpb.BackupVolumeManifestTransfer
	credits chan *agentpb.BackupVolumeManifestAckCredit
}

type volumeManifestExchange struct {
	ctx    context.Context
	mu     sync.Mutex
	slots  map[string]*volumeManifestSlot
	closed bool
}

func newVolumeManifestExchange(ctx context.Context, assignment *agentpb.TaskAssignment) *volumeManifestExchange {
	exchange := &volumeManifestExchange{ctx: ctx, slots: make(map[string]*volumeManifestSlot)}
	for _, step := range assignment.GetBackupAuthority().GetSteps() {
		var directions []agentpb.BackupVolumeManifestDirection
		if step.GetCapture().GetVolume() != nil {
			directions = append(
				directions,
				agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE,
			)
		}
		if step.GetRestore().GetVolume() != nil {
			directions = append(directions,
				agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW,
				agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
		}
		for _, direction := range directions {
			transferID, err := backupvolumetransfer.TransferID(step.ExecutionId, direction)
			if err != nil || len(step.StepDigest) != sha256.Size {
				continue
			}
			binding := backupvolumetransfer.Binding{TaskID: assignment.TaskId, AssignmentID: assignment.AssignmentId,
				StepID: step.StepId, TransferID: transferID, Direction: direction}
			copy(binding.AuthorityDigest[:], step.StepDigest)
			exchange.slots[transferID] = &volumeManifestSlot{binding: binding,
				frames:  make(chan *agentpb.BackupVolumeManifestTransfer, 1),
				credits: make(chan *agentpb.BackupVolumeManifestAckCredit, 1)}
		}
	}
	return exchange
}

func (exchange *volumeManifestExchange) acceptFrame(frame *agentpb.BackupVolumeManifestTransfer) error {
	if frame == nil {
		return errs.New(errs.KindValidationFailed, "Volume manifest frame is missing")
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed || exchange.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Volume manifest exchange is closed")
	}
	slot := exchange.slots[frame.TransferId]
	if slot == nil ||
		slot.binding.Direction == agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW {
		return errs.New(errs.KindStateConflict, "Volume manifest frame has no inbound slot")
	}
	owned, err := backupvolumetransfer.ValidateFrame(slot.binding, frame)
	if err != nil {
		return err
	}
	select {
	case slot.frames <- owned:
		return nil
	default:
		return errs.New(errs.KindStateConflict, "Volume manifest frame exceeds bounded inbox")
	}
}

func (exchange *volumeManifestExchange) acceptCredit(credit *agentpb.BackupVolumeManifestAckCredit) error {
	if credit == nil {
		return errs.New(errs.KindValidationFailed, "Volume manifest credit is missing")
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed || exchange.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Volume manifest exchange is closed")
	}
	slot := exchange.slots[credit.TransferId]
	if slot == nil ||
		slot.binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW ||
		backupvolumetransfer.ValidateCredit(slot.binding, credit) != nil {
		return errs.New(errs.KindStateConflict, "Volume manifest credit differs from sealed Restore")
	}
	select {
	case slot.credits <- credit:
		return nil
	default:
		return errs.New(errs.KindStateConflict, "Volume manifest credit exceeds bounded inbox")
	}
}

func (exchange *volumeManifestExchange) readFrame(
	ctx context.Context,
	transferID string,
) (*agentpb.BackupVolumeManifestTransfer, error) {
	exchange.mu.Lock()
	slot := exchange.slots[transferID]
	exchange.mu.Unlock()
	if slot == nil {
		return nil, errs.New(errs.KindStateConflict, "Volume manifest frame slot is unavailable")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-exchange.ctx.Done():
		return nil, exchange.ctx.Err()
	case frame := <-slot.frames:
		return frame, nil
	}
}

func (exchange *volumeManifestExchange) readCredit(
	ctx context.Context,
	transferID string,
) (*agentpb.BackupVolumeManifestAckCredit, error) {
	exchange.mu.Lock()
	slot := exchange.slots[transferID]
	exchange.mu.Unlock()
	if slot == nil {
		return nil, errs.New(errs.KindStateConflict, "Volume manifest credit slot is unavailable")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-exchange.ctx.Done():
		return nil, exchange.ctx.Err()
	case credit := <-slot.credits:
		return credit, nil
	}
}

func (exchange *volumeManifestExchange) close() {
	exchange.mu.Lock()
	exchange.closed = true
	exchange.mu.Unlock()
}
