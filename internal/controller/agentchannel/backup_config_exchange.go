package agentchannel

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
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
	slots    map[uint32]*configCaptureCreditSlot
	restores map[uint32]*configRestoreFrameSlot
	closed   bool
	done     chan struct{}
}

func newConfigTransferExchange(ctx context.Context, assignment *agentpb.BackupWorkAssignment, identity workexecution.Identity) (*configTransferExchange, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "Config exchange requires its attempt context")
	}
	owned, _, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return nil, err
	}
	exchange := &configTransferExchange{ctx: ctx, slots: make(map[uint32]*configCaptureCreditSlot),
		restores: make(map[uint32]*configRestoreFrameSlot), done: make(chan struct{})}
	for ordinal, step := range owned.Input.Steps {
		direction := agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE
		if step.GetRestore().GetConfig() != nil {
			direction = agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE
		} else if step.GetCapture().GetConfig() == nil {
			continue
		}
		binding, err := backupconfigtransfer.Bind(owned, identity, uint32(ordinal), direction)
		if err != nil {
			return nil, err
		}
		if direction == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
			exchange.restores[uint32(ordinal)] = &configRestoreFrameSlot{binding: binding,
				frames: make(chan *agentpb.BackupConfigTransfer, backupconfigtransfer.MetadataCreditRecords)}
		} else {
			exchange.slots[uint32(ordinal)] = &configCaptureCreditSlot{binding: binding, credits: make(chan *agentpb.BackupConfigCredit, 1)}
		}
	}
	return exchange, nil
}

func (exchange *configTransferExchange) accept(credit *agentpb.BackupConfigCredit) error {
	if credit == nil {
		return errs.New(errs.KindValidationFailed, "Config capture credit is required")
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if err := exchange.ctx.Err(); err != nil {
		return err
	}
	if exchange.closed {
		return errs.New(errs.KindStateConflict, "Config assignment exchange is closed")
	}
	slot := exchange.slots[credit.GetSourceOrdinal()]
	if slot == nil {
		return errs.New(errs.KindStateConflict, "Config credit has no sealed capture source")
	}
	binding := slot.binding
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

func (exchange *configTransferExchange) read(ctx context.Context, ordinal uint32) (*agentpb.BackupConfigCredit, error) {
	slot := exchange.slots[ordinal]
	if slot == nil {
		return nil, errs.New(errs.KindInternal, "Config source credit slot is missing")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-exchange.done:
		return nil, errs.New(errs.KindStateConflict, "Config exchange is closed")
	case <-exchange.ctx.Done():
		return nil, exchange.ctx.Err()
	case credit := <-slot.credits:
		return credit, nil
	}
}
