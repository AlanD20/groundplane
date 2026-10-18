package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/agent/releaseruntime"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitRecovery(delivery *agentpb.ReleaseRecoveryAssignment, available int) error {
	input, err := releaseexecution.DecodeRecoveryAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	attempt := input.Input.Attempt
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Release worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.recovery == nil || !proto.Equal(active.recovery, delivery) {
			return errs.New(errs.KindStateConflict, "Release work already has a different active delivery")
		}
		return nil
	}
	if available <= 0 || len(workers.active) >= workers.limit {
		return errs.New(errs.KindStateConflict, "Release execution capacity is exhausted")
	}
	execution, err := workers.ownership.Begin(attempt)
	if err != nil {
		return err
	}
	owned := proto.CloneOf(delivery)
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, recovery: owned}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		result, err := workers.recovery.Execute(workers.executionContext(attempt), owned, workers.identity, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, recovery: result.Report, result: releaseruntime.ExecutionResult{Failure: result.Failure}, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
