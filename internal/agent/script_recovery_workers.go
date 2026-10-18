package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitScriptRecovery(delivery *agentpb.ScriptRecoveryAssignment, available int) error {
	input, err := scriptexecution.DecodeRecoveryAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	attempt := input.Attempt
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Script recovery worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.scriptRecovery == nil || !proto.Equal(active.scriptRecovery, delivery) {
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
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, scriptRecovery: proto.CloneOf(delivery)}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		result, err := workers.scripts.RunRecovery(workers.ctx, input, workers.exchange, workers.ownership)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, scriptRecovery: result.Recovery, scriptFailure: result.Failure, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
