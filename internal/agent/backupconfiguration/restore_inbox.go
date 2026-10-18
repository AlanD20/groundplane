package backupconfiguration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type restoreSlot struct {
	binding backupconfigtransfer.Binding
	credits chan *agentpb.BackupConfigCredit
	done    chan struct{}
	last    *agentpb.BackupConfigCredit
	claimed bool
}

func (inbox *Inbox) acceptRestoreCredit(credit *agentpb.BackupConfigCredit) error {
	slot := inbox.restores[transferSlotKey{credit.GetAttempt().GetAttemptId(), credit.SourceOrdinal}]
	if slot == nil {
		return invalidRestoreCredit()
	}
	binding := slot.binding
	owned, err := backupconfigtransfer.ValidateCredit(binding, credit)
	if err != nil {
		return err
	}
	if slot.last != nil && (owned.CreditSequence != slot.last.CreditSequence+1 ||
		owned.CommittedRecordSequence <= slot.last.CommittedRecordSequence) {
		return invalidRestoreCredit()
	}
	select {
	case slot.credits <- owned:
		slot.binding = binding
		slot.last = owned
		return nil
	default:
		return invalidRestoreCredit()
	}
}

// RestoreStream claims one reserved sender and waits outside the receive loop
// for the Controller's current native credit. Reconnect may start beyond credit
// one; the local journal, not the inbox, proves the preceding grant history.
func (inbox *Inbox) RestoreStream(ctx context.Context, binding backupconfigtransfer.Binding,
	publish func(context.Context, *agentpb.BackupConfigTransfer) error,
) (*RestoreStream, error) {
	if ctx == nil || publish == nil {
		return nil, invalidRestoreCredit()
	}
	inbox.mu.Lock()
	slot := inbox.restores[transferSlotKey{binding.Attempt.ID, binding.SourceOrdinal}]
	if slot == nil || slot.claimed || slot.binding != binding {
		inbox.mu.Unlock()
		return nil, invalidRestoreCredit()
	}
	slot.claimed = true
	inbox.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-slot.done:
		return nil, invalidRestoreCredit()
	case initial := <-slot.credits:

		return &RestoreStream{slot: slot, binding: binding, initial: initial, publish: publish}, nil
	}
}

type RestoreStream struct {
	slot    *restoreSlot
	binding backupconfigtransfer.Binding
	initial *agentpb.BackupConfigCredit
	publish func(context.Context, *agentpb.BackupConfigTransfer) error
}

func (stream *RestoreStream) Binding() backupconfigtransfer.Binding { return stream.binding }

func (stream *RestoreStream) ReadCredit(ctx context.Context) (*agentpb.BackupConfigCredit, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-stream.slot.done:
		return nil, invalidRestoreCredit()
	case credit := <-stream.slot.credits:
		return credit, nil
	}
}

func invalidRestoreCredit() error {
	return errs.New(errs.KindStateConflict, "Config Restore credit differs from its reserved transfer")
}
