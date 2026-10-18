package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) CompleteAttachProvision(ctx context.Context, input attachexecution.ProvisionInput,
	ack *agentpb.AttachProvisionCompletionAck,
) (workexecution.Attempt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if !matchesAttachProvisionState(state, input) || !state.AttachProvision.Acknowledged || state.AttachProvisionFailure != nil {
		return workexecution.Attempt{}, conflict()
	}
	terminal, err := attachexecution.DecodeProvisionCompletion(input, state.AttachProvision.Report.Progress, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if previous := state.AttachProvision.Completion; previous != nil {
		if previous.Outcome != terminal.Outcome || !previous.FinishedAt.Equal(*terminal.FinishedAt) || *previous.Result != *terminal.Result {
			return workexecution.Attempt{}, conflict()
		}
		return *previous, nil
	}
	state.AttachProvision.Completion = &terminal
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
