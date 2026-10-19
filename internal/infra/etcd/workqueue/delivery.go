package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Delivery struct {
	Item    ItemSnapshot
	Attempt AttemptSnapshot
	Slot    SlotSnapshot
}

// Deliveries is rebuilt from durable assignments after each reconnect or lost
// transaction response. It does not create attempts, renew deadlines or infer
// that a worker stopped. Running attempts are included for reconciliation, not
// replay. Older-generation reservations stay occupied but are not delivered to
// the replacement process.
func (repository *Repository) Deliveries(ctx context.Context, identity workexecution.Identity) ([]Delivery, error) {
	return repository.deliveries(ctx, identity, false)
}

// InterruptedDeliveries inventories older-generation reservations for startup
// reconciliation. It never transfers attempt identity or grants execution. The
// caller must establish process termination before invoking effectful cleanup.
// All memberships and records are read at one revision, including old slots
// beyond the replacement executor's reduced capacity.
func (repository *Repository) InterruptedDeliveries(
	ctx context.Context,
	identity workexecution.Identity,
) ([]Delivery, error) {
	return repository.deliveries(ctx, identity, true)
}

func (repository *Repository) deliveries(
	ctx context.Context,
	identity workexecution.Identity,
	interrupted bool,
) ([]Delivery, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	keys := make([]string, 1, maximumExecutorCapacity+1)
	keys[0] = ExecutorKey(identity)
	for index := uint16(0); index < maximumExecutorCapacity; index++ {
		keys = append(keys, SlotKey(identity, index))
	}
	read, err := repository.read(ctx, keys, 0)
	if err != nil {
		return nil, err
	}
	if read.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "executor is not registered")
	}
	executor, err := DecodeExecutor(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if executor.Identity != identity {
		return nil, errs.New(errs.KindStateConflict, "executor registration has been replaced")
	}
	if interrupted && executor.Ready {
		return nil, errs.New(errs.KindStateConflict, "startup reconciliation requires dispatch to be disabled")
	}
	var slots []SlotSnapshot
	var ownedKeys []string
	seen := make(map[string]struct{})
	for index, value := range read.Values[1:] {
		if value == nil {
			continue
		}
		slot, err := recordcodec.Decode[Slot](value.Value, "execution_slot")
		if err != nil || validateOwner(ClaimOwner{TaskID: slot.TaskID, WorkItemID: slot.WorkItemID}) != nil ||
			ids.Validate(ids.KindAttempt, slot.AttemptID) != nil {
			return nil, corrupt()
		}
		if _, exists := seen[slot.WorkItemID]; exists {
			return nil, corrupt()
		}
		seen[slot.WorkItemID] = struct{}{}
		slots = append(slots, SlotSnapshot{Index: uint16(index), Record: slot, Revision: value.ModRevision})
		ownedKeys = append(ownedKeys, ItemKey(slot.WorkItemID), AttemptKey(slot.AttemptID))
	}
	if len(slots) == 0 {
		return nil, nil
	}
	owned, err := repository.read(ctx, ownedKeys, read.ReadRevision)
	if err != nil {
		return nil, err
	}
	result := make([]Delivery, 0, len(slots))
	for index, slot := range slots {
		itemValue, attemptValue := owned.Values[index*2], owned.Values[index*2+1]
		if itemValue == nil || attemptValue == nil {
			return nil, corrupt()
		}
		item, err := DecodeItem(itemValue.Value)
		if err != nil {
			return nil, err
		}
		attempt, err := DecodeAttempt(attemptValue.Value)
		if err != nil {
			return nil, err
		}
		if item.ID != slot.Record.WorkItemID || item.TaskID != slot.Record.TaskID ||
			attempt.ID != slot.Record.AttemptID || workexecution.ValidateDelivery(item, attempt) != nil ||
			attempt.Executor.HostID != identity.HostID || attempt.Executor.Executor != identity.Executor {
			return nil, corrupt()
		}
		if attempt.Executor.Generation > identity.Generation {
			return nil, corrupt()
		}
		if interrupted && attempt.Executor.Generation == identity.Generation ||
			!interrupted && attempt.Executor != identity {
			continue
		}
		result = append(result, Delivery{
			Item:    ItemSnapshot{Item: item, Revision: itemValue.ModRevision},
			Attempt: AttemptSnapshot{Attempt: attempt, Revision: attemptValue.ModRevision},
			Slot:    slot,
		})
	}
	return result, nil
}
