package agentchannel

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type configCaptureCreditSlot struct {
	binding backupconfigtransfer.Binding
	credits chan *agentpb.BackupConfigCredit
}

// One bounded slot per sealed Config source. The receive loop only validates
// identity and queues a credit; source replay and native commitment stay with
// its assignment payload producer, never blocking readiness or abort handling.
type configTransferExchange struct {
	ctx      context.Context
	mu       sync.Mutex
	slots    map[string]*configCaptureCreditSlot
	restores map[string]*configRestoreFrameSlot
	volumes  *volumeManifestExchange
	closed   bool
}

func newConfigTransferExchange(ctx context.Context, assignment *agentpb.TaskAssignment) *configTransferExchange {
	exchange := &configTransferExchange{
		ctx:      ctx,
		slots:    make(map[string]*configCaptureCreditSlot),
		restores: make(map[string]*configRestoreFrameSlot),
		volumes:  newVolumeManifestExchange(ctx, assignment),
	}
	for _, step := range assignment.GetBackupAuthority().GetSteps() {
		if step.GetRestore().GetConfig() != nil {
			exchange.restores[step.StepId] = &configRestoreFrameSlot{
				binding: backupconfigtransfer.Binding{TaskID: assignment.TaskId, AssignmentID: assignment.AssignmentId,
					StepID: step.StepId, ExecutionID: step.ExecutionId,
					Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE},
				frames: make(chan *agentpb.BackupConfigTransfer, backupconfigtransfer.MetadataCreditRecords),
			}
			continue
		}
		if step.GetCapture().GetConfig() == nil {
			continue
		}
		exchange.slots[step.StepId] = &configCaptureCreditSlot{
			binding: backupconfigtransfer.Binding{TaskID: assignment.TaskId, AssignmentID: assignment.AssignmentId,
				StepID: step.StepId, ExecutionID: step.ExecutionId,
				Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE},
			credits: make(chan *agentpb.BackupConfigCredit, 1),
		}
	}
	return exchange
}

func (exchange *configTransferExchange) accept(credit *agentpb.BackupConfigCredit) error {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if err := exchange.ctx.Err(); err != nil {
		return err
	}
	if exchange.closed {
		return errs.New(errs.KindStateConflict, "Config assignment exchange is closed")
	}
	slot := exchange.slots[credit.StepId]
	if slot == nil {
		return errs.New(errs.KindStateConflict, "Config credit has no sealed capture source")
	}
	binding := slot.binding
	if binding.TransferID == "" {
		binding.TransferID = credit.TransferId
	}
	owned, err := backupconfigtransfer.ValidateCredit(binding, credit)
	if err != nil {
		return err
	}
	select {
	case slot.credits <- owned:
		slot.binding = binding
		return nil
	default:
		return errs.New(errs.KindStateConflict, "Config capture credit exceeds its bounded inbox")
	}
}

func (exchange *configTransferExchange) read(ctx context.Context, stepID string) (*agentpb.BackupConfigCredit, error) {
	slot := exchange.slots[stepID]
	if slot == nil {
		return nil, errs.New(errs.KindInternal, "Config source credit slot is missing")
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
