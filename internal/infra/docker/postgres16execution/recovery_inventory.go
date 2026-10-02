package postgres16execution

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// InspectRecoveryInventory opens no database client. Its short administrative
// deadline does not extend any retained execution's original absolute budget.
func (executor *Executor) InspectRecoveryInventory(ctx context.Context, container Container) (
	postgres16protocol.RecoveryInventory, error,
) {
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, _ := bounded.Deadline()
	var nonce postgres16protocol.Nonce
	if _, err := rand.Read(nonce[:]); err != nil || nonce == (postgres16protocol.Nonce{}) {
		return postgres16protocol.RecoveryInventory{}, errs.New(
			errs.KindInternal,
			"PostgreSQL inventory request identity is unavailable",
		)
	}
	result, err := executor.Execute(bounded, container, postgres16protocol.Request{
		Operation: postgres16protocol.OperationRecoveryInventory, Nonce: nonce,
		DeadlineUnixNano: uint64(deadline.UnixNano()),
	}, nil, nil)
	if err != nil {
		return postgres16protocol.RecoveryInventory{}, err
	}
	return postgres16protocol.ParseRecoveryInventory(result.Proof)
}

// RetireInventoriedExecution is administrative cleanup, not another execution
// attempt. The helper rechecks the exact nonce and request before deleting its
// already-settled evidence. An expired client budget does not prevent cleanup.
func (executor *Executor) RetireInventoriedExecution(ctx context.Context, container Container,
	record postgres16protocol.RecoveryRecord,
) error {
	if !record.Retirable() {
		return errs.New(errs.KindStateConflict, "PostgreSQL retained execution is not safe to retire")
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	deadline, _ := bounded.Deadline()
	_, err := executor.Execute(bounded, container, postgres16protocol.Request{
		Operation: postgres16protocol.OperationRetire, Nonce: record.Nonce,
		DeadlineUnixNano: uint64(deadline.UnixNano()), RequestSHA256: record.RequestSHA256,
	}, nil, nil)
	return err
}
