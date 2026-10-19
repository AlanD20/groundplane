package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitBacking(delivery *agentpb.BackingProvisionAssignment, available int) error {
	input, err := backingprovision.DecodeAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	attempt := input.Attempt
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "backing creation worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.backing == nil || !proto.Equal(active.backing, delivery) {
			return errs.New(errs.KindStateConflict, "backing creation already has a different active assignment")
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
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, backing: proto.CloneOf(delivery)}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		terminal, err := workers.backings.Run(workers.executionContext(attempt), input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, backing: &terminal, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}

func (workers *executionWorkers) finishBacking(output executionOutput, active activeExecution) (bool, error) {
	if active.backing == nil {
		if output.backing != nil {
			return true, errs.New(errs.KindStateConflict, "backing creation result belongs to another active operation")
		}
		return false, nil
	}
	if output.err == nil {
		if output.backing == nil || output.backing.Validate() != nil || output.backing.FinishedAt == nil ||
			output.backing.Result == nil || !workexecution.SameAssignment(*output.backing, output.attempt) {
			return true, errs.New(
				errs.KindStateConflict,
				"backing creation requires its saved terminal acknowledgement",
			)
		}
		if err := workers.backings.Retire(workers.ctx, *output.backing); err != nil {
			return true, err
		}
	}
	workers.retireLocalExecution(output.attempt.WorkItemID)
	return true, nil
}
