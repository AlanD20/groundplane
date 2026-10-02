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

// registerRestore runs under the Inbox lock. The sealed assignment reserves the
// slot, but only the Controller's first durable grant supplies its transfer ID.
func (inbox *Inbox) registerRestore(authority *agentpb.BackupTaskAuthority, step *agentpb.BackupStepAuthority) error {
	binding := backupconfigtransfer.Binding{TaskID: authority.TaskId, AssignmentID: authority.AssignmentId,
		StepID: step.StepId, ExecutionID: step.ExecutionId, TransferID: step.ExecutionId,
		Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE}
	if err := binding.Validate(); err != nil {
		return err
	}
	if inbox.slots[step.StepId] != nil {
		return invalidRestoreCredit()
	}
	if current := inbox.restores[step.StepId]; current != nil {
		binding.TransferID = current.binding.TransferID
		if current.binding != binding {
			return invalidRestoreCredit()
		}
		return nil
	}
	binding.TransferID = ""
	inbox.restores[step.StepId] = &restoreSlot{binding: binding,
		credits: make(chan *agentpb.BackupConfigCredit, 1), done: make(chan struct{})}
	return nil
}

func (inbox *Inbox) acceptRestoreCredit(credit *agentpb.BackupConfigCredit) error {
	slot := inbox.restores[credit.StepId]
	if slot == nil {
		return invalidRestoreCredit()
	}
	binding := slot.binding
	if binding.TransferID == "" {
		binding.TransferID = credit.TransferId
	}
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
func (inbox *Inbox) RestoreStream(ctx context.Context, taskID, assignmentID, stepID string,
	publish func(context.Context, *agentpb.BackupConfigTransfer) error,
) (*RestoreStream, error) {
	if ctx == nil || publish == nil {
		return nil, invalidRestoreCredit()
	}
	inbox.mu.Lock()
	slot := inbox.restores[stepID]
	if slot == nil || slot.claimed || slot.binding.TaskID != taskID || slot.binding.AssignmentID != assignmentID {
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
		binding := backupconfigtransfer.Binding{TaskID: taskID, AssignmentID: assignmentID, StepID: stepID,
			ExecutionID: initial.ExecutionId, TransferID: initial.TransferId,
			Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE}
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
