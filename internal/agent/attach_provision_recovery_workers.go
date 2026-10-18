package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitProvisionRecovery(
	delivery *agentpb.AttachProvisionRecoveryAssignment,
	available int,
) error {
	input, err := attachexecution.DecodeProvisionRecoveryAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Attach inspection worker session is closed")
	}
	if active, exists := workers.active[input.Attempt.WorkItemID]; exists {
		if active.provisionRecovery == nil || !proto.Equal(active.provisionRecovery, delivery) {
			return errs.New(errs.KindStateConflict, "Attach inspection already has another active assignment")
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
		attempt:           input.Attempt,
		provisionRecovery: proto.CloneOf(delivery),
	}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		result, err := workers.attachProvision.Recover(workers.executionContext(input.Attempt), input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: input.Attempt, provisionRecovery: result, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
