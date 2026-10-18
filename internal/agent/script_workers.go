package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Scripts share the same slots and process ownership as Release work. The
// assignment's plaintext is never retained in the active-worker index.
func (workers *executionWorkers) submitScript(delivery *agentpb.ScriptWorkAssignment, available int) error {
	invocation, err := scriptruntime.DecodeAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	transferred := false
	defer func() {
		if !transferred {
			invocation.Clear()
		}
	}()
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Script worker session is closed")
	}
	attempt := invocation.Attempt
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if !active.script || !workexecution.SameAssignment(active.attempt, attempt) {
			return errs.New(errs.KindStateConflict, "Script work already has a different active assignment")
		}
		return nil
	}
	if available <= 0 || len(workers.active) >= workers.limit {
		return errs.New(errs.KindStateConflict, "Agent execution capacity is exhausted")
	}
	execution, err := workers.ownership.Begin(attempt)
	if err != nil {
		return err
	}
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, script: true}
	workers.wait.Add(1)
	transferred = true
	go func() {
		defer workers.wait.Done()
		result, err := workers.scripts.Run(workers.executionContext(attempt), invocation, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, script: result.Completion, scriptFailure: result.Failure, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
