package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/agent/componentruntime"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitComponent(delivery *agentpb.ComponentWorkAssignment, available int) error {
	input, err := componentruntime.DecodeAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	attempt, err := workexecution.DecodeAgentAttempt(delivery.Attempt, workers.identity)
	if err != nil {
		return err
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Component worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.component == nil || !proto.Equal(active.component, delivery) {
			return errs.New(errs.KindStateConflict, "Component already has a different active assignment")
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
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, component: proto.CloneOf(delivery)}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		terminal, err := workers.components.Run(workers.executionContext(attempt), input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, component: &terminal, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
