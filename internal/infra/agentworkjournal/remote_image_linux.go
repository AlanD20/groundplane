package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// BeginRemoteImage durably marks the local effect boundary. A repeated Start
// can only inspect. Any exact outbound report already saved by nativeExchange
// is returned for replay without another host call.
func (journal *Journal) BeginRemoteImage(
	ctx context.Context,
	attempt workexecution.Attempt,
) (bool, *string, error) {
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
			Attempt: attempt, UpdatedAt: *attempt.StartedAt, RemoteImage: &standaloneRecord{},
		})
		return true, nil, err
	}
	if state.RemoteImage == nil || !workexecution.SameAssignment(state.Attempt, attempt) ||
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
	_, report, err := remotefetch.DecodeReport(message.GetRemoteImageResultRequest(), attempt.Executor)
	return false, &report, err
}
