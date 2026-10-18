package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitAttachRecovery(delivery *agentpb.AttachRecoveryAssignment, available int) error {
	input, err := attachexecution.DecodeRecoveryAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Attach recovery worker session is closed")
	}
	if active, exists := workers.active[input.Attempt.WorkItemID]; exists {
		if active.attachRecovery == nil || !proto.Equal(active.attachRecovery, delivery) {
			return errs.New(errs.KindStateConflict, "Attach recovery already has another active assignment")
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
	workers.active[input.Attempt.WorkItemID] = activeExecution{
		attempt:        input.Attempt,
		attachRecovery: proto.CloneOf(delivery),
	}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		defer attachexecution.ClearRecoveryInput(&input)
		result, err := workers.attachRemoval.Recover(workers.executionContext(input.Attempt), input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: input.Attempt, attachRecovery: result, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
