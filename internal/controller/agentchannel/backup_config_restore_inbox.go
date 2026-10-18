package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type configRestoreFrameSlot struct {
	binding          backupconfigtransfer.Binding
	frames           chan *agentpb.BackupConfigTransfer
	queuedBytes      uint64
	metadataAccepted bool
	creditIssued     bool
}

func (exchange *configTransferExchange) acceptRestoreFrame(frame *agentpb.BackupConfigTransfer) error {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if err := exchange.ctx.Err(); err != nil {
		return err
	}
	if frame == nil {
		return errs.New(errs.KindValidationFailed, "Config Restore frame is required")
	}
	slot := exchange.restores[frame.SourceOrdinal]
	if exchange.closed || slot == nil || !slot.creditIssued {
		return errs.New(errs.KindStateConflict, "Config restore source has no issued credit")
	}
	owned, err := backupconfigtransfer.ValidateFrame(slot.binding, frame)
	if err != nil {
		return err
	}
	maximumRecords, maximumBytes := backupconfigtransfer.MetadataCreditRecords, backupconfigtransfer.MetadataCreditBytes
	if slot.metadataAccepted {
		maximumRecords, maximumBytes = backupconfigtransfer.ValueCreditRecords, backupconfigtransfer.ValueCreditBytes
	}
	length := uint64(proto.Size(owned))
	// Value credit measures selected bytes; account separately for bounded
	// identity/control envelopes retained alongside those bytes in this inbox.
	maximumBytes += uint64(maximumRecords) * 1024
	if uint32(len(slot.frames)) >= maximumRecords || length > maximumAssignmentPayloadFrameBytes ||
		slot.queuedBytes > maximumBytes || length > maximumBytes-slot.queuedBytes {
		backupconfigtransfer.ClearFrame(owned)
		return errs.New(errs.KindStateConflict, "Config restore exceeds its bounded receive window")
	}
	select {
	case slot.frames <- owned:
		slot.queuedBytes += length
		return nil
	default:
		backupconfigtransfer.ClearFrame(owned)
		return errs.New(errs.KindStateConflict, "Config restore exceeds its bounded inbox")
	}
}

func (exchange *configTransferExchange) prepareRestoreCredit(credit *agentpb.BackupConfigCredit) error {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if err := exchange.ctx.Err(); err != nil {
		return err
	}
	if credit == nil {
		return errs.New(errs.KindValidationFailed, "Config Restore credit is required")
	}
	slot := exchange.restores[credit.SourceOrdinal]
	if exchange.closed || slot == nil {
		return errs.New(errs.KindStateConflict, "Config restore credit has no sealed source")
	}
	binding := slot.binding
	validated, err := backupconfigtransfer.ValidateCredit(binding, credit)
	if err != nil {
		return err
	}
	slot.binding = binding
	slot.creditIssued = true
	slot.metadataAccepted = validated.GetMetadataAccepted() != nil || validated.GetValueCredit() != nil
	return nil
}

func (exchange *configTransferExchange) readRestoreFrame(
	ctx context.Context,
	ordinal uint32,
) (*agentpb.BackupConfigTransfer, error) {
	slot := exchange.restores[ordinal]
	if slot == nil {
		return nil, errs.New(errs.KindInternal, "Config restore source inbox is missing")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-exchange.done:
		return nil, errs.New(errs.KindStateConflict, "Config Restore exchange is closed")
	case <-exchange.ctx.Done():
		return nil, exchange.ctx.Err()
	case frame := <-slot.frames:
		exchange.mu.Lock()
		slot.queuedBytes -= uint64(proto.Size(frame))
		exchange.mu.Unlock()
		return frame, nil
	}
}

func (exchange *configTransferExchange) close() {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed {
		return
	}
	exchange.closed = true
	close(exchange.done)
	for _, slot := range exchange.restores {
		clearRestoreFrameSlot(slot)
	}
}

func clearRestoreFrameSlot(slot *configRestoreFrameSlot) {
	for {
		select {
		case frame := <-slot.frames:
			backupconfigtransfer.ClearFrame(frame)
		default:
			slot.queuedBytes = 0
			return
		}
	}
}
