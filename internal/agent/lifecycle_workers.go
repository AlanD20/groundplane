package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/agent/lifecycleruntime"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitLifecycle(delivery *agentpb.ServiceLifecycleAssignment, available int) error {
	input, err := lifecycleruntime.DecodeAssignment(delivery, workers.identity)
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
		return errs.New(errs.KindStateConflict, "Service lifecycle worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.lifecycle == nil || !proto.Equal(active.lifecycle, delivery) {
			return errs.New(errs.KindStateConflict, "Service lifecycle already has a different active assignment")
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
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, lifecycle: proto.CloneOf(delivery)}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		terminal, err := workers.lifecycles.Run(workers.executionContext(attempt), input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, lifecycle: &terminal, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
