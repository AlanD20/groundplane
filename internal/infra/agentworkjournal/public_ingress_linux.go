package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type standaloneRecord struct {
	Receipt *workexecution.Attempt `json:"receipt,omitempty"`
}

// BeginPublicIngress durably marks the local effect boundary. A repeated Start
// can only inspect. Any exact outbound report already saved by nativeExchange
// is returned for replay without another host call.
func (journal *Journal) BeginPublicIngress(
	ctx context.Context,
	attempt workexecution.Attempt,
) (bool, *ingressexecution.Report, error) {
	if attempt.Validate() != nil || attempt.StartedAt == nil || attempt.FinishedAt != nil {
		return false, nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return false, nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return false, nil, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return false, nil, err
		}
		if len(attempts) >= MaximumAttempts {
			return false, nil, conflict()
		}
		err = journal.writeRecord(ctx, attemptRecord{
			Attempt: attempt, UpdatedAt: *attempt.StartedAt, PublicIngress: &standaloneRecord{},
		})
		return true, nil, err
	}
	if state.PublicIngress == nil || !workexecution.SameAssignment(state.Attempt, attempt) ||
		state.Attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*attempt.StartedAt) {
		return false, nil, conflict()
	}
	if state.TerminalDelivery == nil {
		return false, nil, nil
	}
	message, err := state.TerminalDelivery.Message()
	if err != nil {
		return false, nil, err
	}
	_, report, err := ingressexecution.DecodeReport(message.GetPublicIngressResultRequest(), attempt.Executor)
	return false, &report, err
}

func (record attemptRecord) validateStandalone(receipt *workexecution.Attempt) error {
	if record.Sequence != 0 || record.Attempt.Validate() != nil || record.Attempt.StartedAt == nil ||
		record.Attempt.FinishedAt != nil || !record.UpdatedAt.Equal(*record.Attempt.StartedAt) ||
		record.Resolver != nil || record.BackingRecovery != nil || record.Backing != nil ||
		record.EnvironmentProvision != nil || record.Backup != nil || record.RemovalRecovery != nil ||
		record.LifecycleRecovery != nil || record.Lifecycle != nil || record.AttachProvisionRecovery != nil ||
		record.AttachProvisionFailure != nil || record.AttachProvision != nil || record.EntryRecovery != nil ||
		record.EntryConfiguration != nil || record.ComponentRecovery != nil || record.Component != nil ||
		record.NetworkRecovery != nil || record.Network != nil || record.VolumeRecovery != nil ||
		record.Volume != nil || record.AttachRecovery != nil || record.AttachFailure != nil ||
		record.AttachRemoval != nil || record.EnvironmentFailure != nil || record.EnvironmentRemoval != nil ||
		record.ScriptFailure != nil || record.ScriptRecovery != nil || len(record.Script) != 0 ||
		record.Release != nil || record.Effect != nil || record.Completion != nil || record.Failure != nil ||
		record.Recovery != nil || record.ScriptCompletion != nil {
		return invalid()
	}
	if receipt != nil && (receipt.Validate() != nil || !workexecution.SameAssignment(*receipt, record.Attempt) ||
		receipt.FinishedAt == nil || receipt.Result == nil || receipt.StartedAt == nil ||
		!receipt.StartedAt.Equal(*record.Attempt.StartedAt)) {
		return invalid()
	}
	return nil
}
