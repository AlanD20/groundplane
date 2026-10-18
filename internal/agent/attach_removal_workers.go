package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitAttachRemoval(delivery *agentpb.AttachRemovalAssignment, available int) error {
	input, err := attachexecution.DecodeRemovalAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	transferred := false
	defer func() {
		if !transferred {
			attachexecution.ClearRemoval(input.Removal)
		}
	}()
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Attach removal worker session is closed")
	}
	if active, exists := workers.active[input.Attempt.WorkItemID]; exists {
		if active.attach == nil || !proto.Equal(active.attach, delivery) {
			return errs.New(errs.KindStateConflict, "Attach removal already has a different active assignment")
		}
		return nil
	}
	if available <= 0 || len(workers.active) >= workers.limit {
		return errs.New(errs.KindStateConflict, "Agent execution capacity is exhausted")
	}
	execution, err := workers.ownership.Begin(input.Attempt)
	if err != nil {
		return err
	}
	workers.active[input.Attempt.WorkItemID] = activeExecution{attempt: input.Attempt, attach: proto.CloneOf(delivery)}
	workers.wait.Add(1)
	transferred = true
	go func() {
		defer workers.wait.Done()
		defer attachexecution.ClearRemoval(input.Removal)
		result, err := workers.attachRemoval.Run(workers.executionContext(input.Attempt), input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: input.Attempt, attach: result.Progress, attachFailure: result.Failure, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
