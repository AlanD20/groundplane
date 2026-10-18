package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (workers *executionWorkers) executionContext(attempt workexecution.Attempt) context.Context {
	workers.mu.Lock()
	defer workers.mu.Unlock()
	active, exists := workers.active[attempt.WorkItemID]
	if !exists || !workexecution.SameAssignment(active.attempt, attempt) || workers.closed {
		cancelled, cancel := context.WithCancel(workers.ctx)
		cancel()
		return cancelled
	}
	if active.ctx == nil {
		active.ctx, active.cancel = context.WithCancel(workers.ctx)
		if active.cancelRequested {
			active.cancel()
		}
		workers.active[attempt.WorkItemID] = active
	}
	return active.ctx
}

func (workers *executionWorkers) cancelAttempt(message *agentpb.WorkCancel) error {
	if message == nil {
		return errs.New(errs.KindValidationFailed, "work cancellation is missing")
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, workers.identity)
	if err != nil {
		return err
	}
	if attempt.Direction != workexecution.Forward {
		return errs.New(errs.KindValidationFailed, "forward cancellation cannot stop recovery")
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	active, exists := workers.active[attempt.WorkItemID]
	if !exists {
		return nil
	} // Start still rechecks durable Stop if delivery follows this hint.
	if !workexecution.SameAssignment(active.attempt, attempt) {
		// An old hint can arrive after a new attempt was accepted. It must
		// neither cancel that attempt nor disconnect unrelated executions.
		return nil
	}
	active.cancelRequested = true
	if active.cancel != nil {
		active.cancel()
	}
	workers.active[attempt.WorkItemID] = active
	return nil
}

// Caller holds workers.mu and has already consumed the operation's terminal
// acknowledgement or joined its failed local execution. Cancellation is not
// authority to delete this slot or any runtime/recovery journal.
func (workers *executionWorkers) retireLocalExecution(workID string) {
	if active, exists := workers.active[workID]; exists && active.cancel != nil {
		active.cancel()
	}
	delete(workers.active, workID)
}
